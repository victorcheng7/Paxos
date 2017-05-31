#!/usr/bin/env python
from termios import tcflush, TCIFLUSH
from miscFuncs import getSplit, getFileLen, validFile, checkIsReducedFile, printDot
import socket
import time
import threading
import sys
import re
import subprocess

def main():
	if(len(sys.argv) != 3):
		print("USAGE: python [cli_id] [setup_file]")
		exit(1)
	cli_id = int(sys.argv[1])
	cli = Cli(cli_id)
	setup_file = sys.argv[2]
	setup(cli, setup_file)

	cThread = threading.Thread(target = commThread, args=(cli,))
	cThread.daemon = True
	cThread.start()

	# make prm connect to all other prms to confirm initialization
	cli.prm[1].send("confirmInit")

	print "I am CLI {0}".format(cli_id)
	time.sleep(1.5)
	while True:

		if cli.prmReplicating:
			sys.stdout.write("PRM in the middle of replicating")
			while cli.prmReplicating:
				printDot()
			tcflush(sys.stdin, TCIFLUSH)
		
		command = None
		# make sure command not empty
		while not command:
			command = raw_input("[CLI]$ ")
			splitCommand = command.split()


		# map
		if splitCommand[0] == "map" and len(splitCommand) == 2:
			try:
				filename = splitCommand[1]
				validFile(filename)
				offset = getSplit(filename)
				size = getFileLen(filename)

				cli.mapper1[1].send("map {0} {1} {2}".format(filename, 0, offset - 1))
				cli.mapper2[1].send("map {0} {1} {2}".format(filename, offset, size - offset))
			except:
				print "USAGE: map [filename]. File must exist in folder"
				continue
			
		# reduce
		elif splitCommand[0] == "reduce" and len(splitCommand) > 2:
			try:
				message = "reduce"
				for file in splitCommand[1:]:
					validFile(file)
					message += " " + file

				cli.reducer[1].send(message)

			except:
				print "USAGE: reduce [filename1] [filename2]... Files must exist in folder"
				continue

		# replicate
		elif splitCommand[0] == "replicate" and len(splitCommand) == 2:
			try:
				validFile(splitCommand[1])
				validContents = checkIsReducedFile(command.split()[1])
				if not validContents:
					continue
				cli.prm[1].send("replicate " + command.split()[1]) 
				cli.prmReplicating = True
				cli.toReplicate = command.split()[1]
			except:
				print "USAGE: replicate [filename]. File must exist in folder"
				continue

		elif splitCommand[0] == "stop" and len(splitCommand) == 1:
			cli.prm[1].send("stop")

		elif splitCommand[0] == "resume" and len(splitCommand) == 1:
			cli.prm[1].send("resume")	

		elif splitCommand[0] == "print" and len(splitCommand) == 1:
			cli.prm[1].send("print")

		elif splitCommand[0] == "merge" and len(splitCommand) == 3:
			try:
				pos1 = abs(int(splitCommand[1]))
				pos2 = abs(int(splitCommand[2]))
				cli.prm[1].send("{0} {1} {2}".format(splitCommand[0], pos1, pos2))
			
			except:
				print "Error: arguments must be valid integers"
				continue

		elif splitCommand[0] == "total" and len(splitCommand) > 1:

			try:
				message = "total"
				for num in splitCommand[1:]:
					pos = abs(int(num))
					message += " {0}".format(pos)

				cli.prm[1].send(message)

			except:
				print "Error: arguments must be valid integers"
				continue

		else:
			print ""
			print "Valid cli commands:"
			print "--------------------------------"
			print "map [filename]"
			print "reduce [filename1] [filename2] ..."
			print "replicate [filename]"
			print "stop"
			print "resume"
			print "print"
			print "total [pos1] [pos2] ..."
			print "merge [pos1] [pos2]"
			print "--------------------------------"
			print ""

def commThread(cli):

	while True:
		try:
			data = cli.prm[0].recv(1024)
			splitData = data.split(" ")

			if data == "stopped":
				cli.prmReplicating = False
				time.sleep(1.5)
				print "\nError: Prm is stopped."

			if data == "finishedSetup":
				print "setup finished"

			if splitData[0] == "finishReplicating":
				time.sleep(1.5)
				if cli.prmReplicating:
					print ""
					if splitData[2] != cli.toReplicate:
						print "Unsucessful replication.",
					else:
						print "Sucessful replication.",
				cli.prmReplicating = False
				print "The file \"{0}\" was decided for index {1}".format(splitData[2], splitData[1])
				cli.toReplicate = ""

		except socket.error, e:
			pass


		# read from all other incoming connections
		for con in cli.connections[1:]:

			try:
				data = con[0].recv(1024)
				
				splitData = data.split(" ")

				if splitData[0] == "taskFinished":
					time.sleep(1)
					print ""
					print data[len("taskFinished"):]
			
			except socket.error, e:
				continue



def setup(cli, setup_file):
	
	# start mappers and reducer
	subprocess.Popen(["python", "mapper.py", "0", str(cli.id), "setup2.txt"])
	subprocess.Popen(["python", "mapper.py", "1", str(cli.id), "setup2.txt"])
	subprocess.Popen(["python", "reducer.py", str(cli.id), "setup2.txt"])

	#Read setup file. ex - setup.txt
	with open(setup_file, 'r') as f:
		N = int(f.readline().strip())
		process_id = 0
		for line in f.readlines():
			process_id += 1
			if process_id == cli.id :
				IP1, port1, IP2, port2, map1IP, map1Port, map2IP, map2Port, reducerIP, reducerPort = line.strip().split()
				port1 = int(port1)
				port2 = int(port2)
				map1Port = int(map1Port)
				map2Port = int(map2Port)
				reducerPort = int(reducerPort)

				cli.openListeningSocket(IP1, port1)
				cli.prm = cli.establishConnection((IP2, port2))
				cli.mapper1 = cli.establishConnection((map1IP, map1Port))
				cli.mapper2 = cli.establishConnection((map2IP, map2Port))
				cli.reducer = cli.establishConnection((reducerIP, reducerPort))

				cli.connections = [cli.prm, cli.mapper1, cli.mapper2, cli.reducer]
				
class Cli(object):
	def __init__(self, cli_id):
		self.id = cli_id
		self.listeningSocket = None

		self.prm = [None]*2 #format: [incoming,outgoing]
		self.mapper1 = [None]*2 
		self.mapper2 = [None]*2 
		self.reducer = [None]*2

		self.connections = []

		self.prmReplicating = False
		self.toReplicate = ""


	def establishConnection(self, addr):
		# establish incoming and outgoing connection

		outgoingSocket = None
		incomingStream = None

		# outgoing
		while True:
			try: 
				sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
				sock.connect(addr)
				outgoingSocket = sock
				break
			except Exception:
				continue

		# incoming
		while True:
			try:
				con, addr = self.listeningSocket.accept()
				con.setblocking(0)
				incomingStream = con
				break
			except socket.error:
				continue

		return [incomingStream, outgoingSocket]

	def openListeningSocket(self, IP, port):
		self.listeningSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.listeningSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.listeningSocket.bind( (IP, port) )
		self.listeningSocket.setblocking(0) 
		self.listeningSocket.listen(10)

main()

