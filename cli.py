#!/usr/bin/env python

import socket
import time
import threading
import sys

def main():
	if(len(sys.argv) != 3):
		print("USAGE: python [machine_id] [setup_file]")
		exit(1)
	machine_id = int(sys.argv[1])
	machine = Machine(machine_id)
	setup_file = sys.argv[2]
	# setup(machine, setup_file)

	while True:
		command = raw_input()

		if command == "replicate":
			print command

		elif command == "map":
			print "map"

		elif command == "stop":
			print "stop"

		elif command == "resume":
			print "resume"	
	# execute_commands(site, command_file)

def setup(machine, setup_file):
	#Read setup file. ex - setup.txt     
	with open(setup_file, 'r') as f:
		N = int(f.readline().strip())
		machine.num_proc = N
		process_id = 0
		for line in f.readlines():
			process_id += 1
			if process_id <= N:
				# 1 -> cli 2-> prm
				IP1, port1, IP2, port2 = line.strip().split()
				port1 = int(port1)
				port2 = int(port2)
				print "{0} {1}".format(port1, port2)

			 	machine.addr_book_cli.append( (IP1, port1) )
			 	machine.addr_book_prm.append( (IP2, port2) )
			 	machine.addOutgoingConn()

			 	if process_id != machine_id:
			 		machine.addOutgoingConn(process_id)
			 		# machine.addIncomingConn(process_id)

				else:
					machine.openListeningSocket( IP1, port1 ) #open for traffic
			# else:
			# 	source_id, dest_id = line.strip().split()
			# 	source_id = int(source_id)
			# 	dest_id = int(dest_id)
			# 	if source_id == site.id: #I am the sender
			# 		site.addOutgoingChannel(dest_id)
			# 	if dest_id == site.id: #I am the receive
			# 		site.addIncomingChannel(source_id)
	machine.openOutgoingConn()
	# machine.openIncomingConn()


class Machine(object):
	def __init__(self, machine_id):
		self.id = machine_id
		self.num_proc = 0
		self.addr_book_cli = []
		self.addr_book_prm = []
		self.outgoing_conn = {}
		self.listeningSocket = None

	def openListeningSocket(self, IP, port):
		self.listeningSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.listeningSocket.bind( (IP, port) )
		self.listeningSocket.setblocking(0) 
		self.listeningSocket.listen(10)

	def addOutgoingConn(self, dest_id):
		cli = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		prm = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.outgoing_conn[dest_id] = [cli, prm]


	def openOutgoingConn(self):

		for dest_id, sock in self.outgoing_conn.iteritems():
			if dest_id != self.id:
				# connect to cli
				while True:
					try: 
						sock[0].connect(self.addr_book_cli[dest_id - 1])
						break
					except Exception:
						continue
				#connect to prm
				while True:
					try: 
						sock[1].connect(self.addr_book_prm[dest_id - 1])
						break
					except Exception:
						continue

	def openIncomingConn(self):
		print " "
		# while len(self.incoming_channels) != len(self.SnapIDTableLastEntryTemplate):
		# 	try:
		# 		con, _ = self.listeningSocket.accept()
		# 		con.setblocking(0)
		# 		self.incoming_channels.append(con)
		# 	except socket.error:
		# 		continue






main()

