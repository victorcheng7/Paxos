#!/usr/bin/env python
# from termios import tcflush, TCIFLUSH
import socket
import time
import threading
import sys
import re

def main():
	if(len(sys.argv) != 3):
		print("USAGE: python reducer.py [cli_id] [setup_file]")
		exit(1)
	my_id = int(sys.argv[1])
	reducer = Reducer(my_id)
	setup_file = sys.argv[2]
	setup(reducer, setup_file)

	cThread = threading.Thread(target = commThread, args=(reducer,))
	cThread.start()

def commThread(reducer):

	while True:
		try:
			data = reducer.incomingStream.recv(1024)
			splitData = data.split(" ")

			#print data
			print "Reducer received " + data
		except socket.error, e:
			continue


def setup(reducer, setup_file):
	#Read setup file. ex - setup.txt      
	with open(setup_file, 'r') as f:
		N = int(f.readline().strip())
		lineNum = 0
		for line in f.readlines():
			lineNum += 1
			if lineNum == reducer.cli_id:
				IP1, port1, _, _, _, _, _, _, reducerIP, reducerPort = line.strip().split()

				reducer.openListeningSocket((reducerIP, int(reducerPort)))

				# connect with cli
				while True:
					try: 
						sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
						sock.connect((IP1, int(port1)))
						reducer.outgoingSocket = sock
						break
					except Exception:
						continue

				# accept from cli
				while True:
					try:
						con, _ = reducer.listeningSocket.accept()
						con.setblocking(0)
						reducer.incomingStream = con
						break
					except socket.error:
						continue

class Reducer(object):
	def __init__(self, my_id):
		self.cli_id = my_id
		self.outgoingSocket = None
		self.incomingStream = None
		self.listeningSocket = None


	def openListeningSocket(self, addr):
		self.listeningSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.listeningSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.listeningSocket.bind( addr )
		self.listeningSocket.setblocking(0) 
		self.listeningSocket.listen(10)

main()

