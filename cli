#!/usr/bin/env python
from termios import tcflush, TCIFLUSH
import socket
import time
import threading
import sys
import re


cli = None

def main():
	global cli
	if(len(sys.argv) != 3):
		print("USAGE: python [cli_id] [setup_file]")
		exit(1)
	cli_id = int(sys.argv[1])
	cli = Cli(cli_id)
	setup_file = sys.argv[2]
	setup(cli, setup_file)

	cThread = threading.Thread(target = commThread)
	cThread.daemon = True
	cThread.start()

	# make prm connect to all other prms to confirm initialization
	cli.outgoingSocket.send("confirmInit")

	print "I am CLI {0}".format(cli_id)
	time.sleep(1.5)
	while True:

		if cli.prmReplicating:
			sys.stdout.write("PRM in the middle of replicating")
			while cli.prmReplicating:
				sys.stdout.write(".")
				sys.stdout.flush()
				time.sleep(1)
				
			tcflush(sys.stdin, TCIFLUSH)
		
		command = None
		# make sure command not empty
		while not command:
			command = raw_input("[CLI]$ ")
			splitCommand = command.split()

		if splitCommand[0] == "map" and len(splitCommand) == 2:
			try:
				validFile(splitCommand[1])
			except:
				print "USAGE: map [filename]. File must exist in folder"
				continue
			print "map"

		elif splitCommand[0] == "reduce" and len(splitCommand) == 3:
			try:
				validFile(splitCommand[1])
				validFile(splitCommand[2])
			except:
				print "USAGE: reduce [filename1] [filename2]. Files must exist in folder"
				continue
			print "reduce"

		elif splitCommand[0] == "replicate" and len(splitCommand) == 2:
			try:
				validFile(splitCommand[1])
				validContents = checkIsReducedFile(command.split()[1])
				if not validContents:
					continue
				cli.outgoingSocket.send("replicate " + command.split()[1]) 
				cli.prmReplicating = True
				cli.toReplicate = command.split()[1]
			except:
				print "USAGE: replicate [filename]. File must exist in folder"
				continue

		elif splitCommand[0] == "stop" and len(splitCommand) == 1:
			cli.outgoingSocket.send("stop")

		elif splitCommand[0] == "resume" and len(splitCommand) == 1:
			cli.outgoingSocket.send("resume")	

		elif splitCommand[0] == "print" and len(splitCommand) == 1:
			cli.outgoingSocket.send("print")

		elif splitCommand[0] == "merge" and len(splitCommand) == 3:
			try:
				pos1 = abs(int(splitCommand[1]))
				pos2 = abs(int(splitCommand[2]))
				cli.outgoingSocket.send("{0} {1} {2}".format(splitCommand[0], pos1, pos2))
			
			except:
				print "Error: arguments must be valid integers"
				continue

		elif splitCommand[0] == "total" and len(splitCommand) > 1:

			try:
				message = "total"
				for num in splitCommand[1:]:
					pos = abs(int(num))
					message += " {0}".format(pos)

				cli.outgoingSocket.send(message)

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
def commThread():

	while True:
		try:
			data = cli.incomingStream.recv(1024)
			splitData = data.split(" ")

			#print data
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
			continue

def validFile(filename):
	file = open(filename, "r")
	#TODO check to see if file is reduced file
	file.close()

def checkIsReducedFile(filename):

	entry_list = []
	word_list = []
	reduce_obj = open(filename, "r")

	for line in reduce_obj:
	    entry_list.append(line.strip('\n\r'))

	if len(entry_list) == 0:
		print "Error: Empty file"
		return False

	matcher = re.compile(r'[a-zA-z][a-z]*')
	#check if all lines valid
	for entry in entry_list:
		
		#check only two words in line
		if len(entry.split()) == 2:
			
			# check 1st is valid word 
			x = matcher.findall(entry.split()[0])
			if len(x) == 1 :
				if len(x[0]) == len(entry.split()[0]):
					
					# check 2nd is valid number
					try:
						num = int(entry.split()[1])
						if num > 0:
							word_list.append(entry.split()[0])
							continue
					except:
						pass

		# error if this is reached
		print "Error: invalid line \"{0}\"".format(entry)
		return False

	if len(word_list) != len(set(word_list)):
		print "Error: duplicate words found in file"
		return False

	return True

def setup(cli, setup_file):
	#Read setup file. ex - setup.txt     
	with open(setup_file, 'r') as f:
		N = int(f.readline().strip())
		cli.num_proc = N
		process_id = 0
		for line in f.readlines():
			process_id += 1
			if process_id == cli.id :
				IP1, port1, IP2, port2 = line.strip().split()
				port1 = int(port1)
				port2 = int(port2)

				cli.openListeningSocket(IP1, port1)

				# connect to prm
				while True:
					try: 
						sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
						sock.connect((IP2, port2))
						cli.outgoingSocket = sock
						break
					except Exception:
						continue

				# accept incoming prm connection
				while True:
					try:
						con, _ = cli.listeningSocket.accept()
						con.setblocking(0)
						cli.incomingStream = con
						break
					except socket.error:
						continue

class Cli(object):
	def __init__(self, cli_id):
		self.id = cli_id
		self.num_proc = 0

		self.outgoingSocket = None
		self.incomingStream = None
		self.listeningSocket = None

		self.prmReplicating = False
		self.toReplicate = ""

	def openListeningSocket(self, IP, port):
		self.listeningSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.listeningSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.listeningSocket.bind( (IP, port) )
		self.listeningSocket.setblocking(0) 
		self.listeningSocket.listen(10)


main()

