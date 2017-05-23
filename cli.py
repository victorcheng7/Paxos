#!/usr/bin/env python

import socket
import time
import threading
import sys


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

	print "I am cli {0}".format(cli_id)
	while True:
		

		# if prmReplicating:
		# 	print("PRM in the middle of replicating")
		# 	# while prmReplicating:


		command = raw_input()
		# make sure command not empty
		while not command:
			command = raw_input()

		if command.split()[0] == "map":
			try:
				validFile(command.split()[1])
			except:
				print "USAGE: map [filename]. File must exist in folder"
				continue
			print "map"

		elif command.split()[0] == "reduce":
			try:
				validFile(command.split()[1])
				validFile(command.split()[2])
			except:
				print "USAGE: reduce [filename1] [filename2]. Files must exist in folder"
				continue
			print "reduce"

		elif command.split()[0] == "replicate":
			try:
				validFile(command.split()[1])
			except:
				print "USAGE: replicate [filename]. File must exist in folder"
				continue
			cli.outgoingSocket.send("replicate!")
			cli.prmReplicating = True

		elif command.split()[0] == "stop":
			cli.outgoingSocket.send("stop")

		elif command.split()[0] == "resume":
			cli.outgoingSocket.send("resume")	

		else:
			print ""
			print "Valid cli commands:"
			print "--------------------------------"
			print "map [filename]"
			print "reduce [filename1] [filename2]"
			print "replicate [filename]"
			print "stop"
			print "resume"
			print "print"
			print "total [pos1] [pos2]"
			print "merge [pos1] [pos2]"
			print "--------------------------------"
			print ""

def commThread():

	while True:
		try:
			data = cli.incomingStream.recv(1024)
			print data
		except socket.error, e:
			continue

def validFile(filename):
	file = open(filename, "r")
	file.close()

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

	def openListeningSocket(self, IP, port):
		self.listeningSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.listeningSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.listeningSocket.bind( (IP, port) )
		self.listeningSocket.setblocking(0) 
		self.listeningSocket.listen(10)


main()

