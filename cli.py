#!/usr/bin/env python

import socket
import time
import threading
import sys


cli = None


def main():
	global cli
	if(len(sys.argv) != 3):
		print("USAGE: python [machine_id] [setup_file]")
		exit(1)
	machine_id = int(sys.argv[1])
	cli = Machine(machine_id)
	setup_file = sys.argv[2]
	setup(cli, setup_file)

	cThread = threading.Thread(target = commThread)
	cThread.daemon = True
	cThread.start()

	while True:
		command = raw_input()

		if command == "replicate":
			cli.outgoingSocket.send("replicate!")


		elif command == "map":
			print "map"

		elif command == "stop":
			print "stop"

		elif command == "resume":
			print "resume"	

		elif command == "exit":
			cli.outgoingSocket.send("exit")
			break
	# execute_commands(site, command_file)

def commThread():

	while True:
		try:
			con, _ = cli.listeningSocket.accept()
			con.setblocking(0)
			cli.incomingStream = con
			break
		except socket.error:
			continue

	while True:
		try:
			data = cli.incomingStream.recv(1024)
			print data		
		except socket.error, e:
			continue

def setup(machine, setup_file):
	#Read setup file. ex - setup.txt     
	with open(setup_file, 'r') as f:
		N = int(f.readline().strip())
		machine.num_proc = N
		process_id = 0
		for line in f.readlines():
			process_id += 1
			if process_id == machine.id :
				IP1, port1, IP2, port2 = line.strip().split()
				port1 = int(port1)
				port2 = int(port2)
				print "{0} {1}".format(port1, port2)
				machine.cli = (IP1, port1)
				machine.prm = (IP2, port2)


				machine.openListeningSocket(IP1, port1)
				print "got here!"
				# connect to prm
				while True:
					try: 
						sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
						sock.connect(machine.prm)
						machine.outgoingSocket = sock
						break
					except Exception:
						continue
				print "got here!"

				




			# if process_id <= N:
			# 	# 1 -> cli 2-> prm
			# 	IP1, port1, IP2, port2 = line.strip().split()
			# 	port1 = int(port1)
			# 	port2 = int(port2)
			# 	print "{0} {1}".format(port1, port2)

			#  	machine.addr_book_cli.append( (IP1, port1) )
			#  	machine.addr_book_prm.append( (IP2, port2) )
			#  	machine.addOutgoingConn()

			#  	if process_id != machine_id:
			#  		machine.addOutgoingConn(process_id)
			#  		# machine.addIncomingConn(process_id)

			# 	else:
			# 		machine.openListeningSocket( IP1, port1 ) #open for traffic
			# else:
			# 	source_id, dest_id = line.strip().split()
			# 	source_id = int(source_id)
			# 	dest_id = int(dest_id)
			# 	if source_id == site.id: #I am the sender
			# 		site.addOutgoingChannel(dest_id)
			# 	if dest_id == site.id: #I am the receive
			# 		site.addIncomingChannel(source_id)
	# machine.openOutgoingConn()
	# machine.openIncomingConn()


class Machine(object):
	def __init__(self, machine_id):
		self.id = machine_id
		self.num_proc = 0
		self.cli = ()
		self.prm = ()

		self.outgoingSocket = None
		self.incomingStream = None
		self.outgoing_conn = {}
		self.listeningSocket = None

	def openListeningSocket(self, IP, port):
		self.listeningSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.listeningSocket.bind( (IP, port) )
		self.listeningSocket.setblocking(0) 
		self.listeningSocket.listen(10)


	def acceptPRM(self):
		while True:
			try:
				con, _ = self.listeningSocket.accept()
				con.setblocking(0)
				self.incomingStream = con
				break
			except socket.error:
				continue





main()

