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
		for con in reducer.cons:
			incoming = con[0]
			try:

				data = incoming.recv(1024)
				splitData = data.split(" ")
				print data

				if splitData[0] == "reduce":
					print "got reduce"
					orig_file = splitData[1][:splitData[1].find("_I_")]
					word_dict = {}
					for file in splitData[1:]:
						reducer.addWords(file, word_dict)

					reducer.writetoFile(orig_file + "_reduced", word_dict)
					print reducer.cons
					for con in reducer.cons:
						outgoing = con[1]
						print "sending file over"
						outgoing.sendall("receive {0}_reduced ".format(orig_file))

						with open(orig_file + "_reduced",'rb') as f:
							outgoing.sendall(f.read())

					reducer.outgoingSocket.send("Finished. Reduced file is " + orig_file + "_reduced")

				if splitData[0] == "receive":
					print "got receive"

					contents = []
					contents.append(" ".join(splitData[2:]))
					fname = splitData[1]
					while True:

						try:
							data = incoming.recv(1024)

							if not data:
								print "print stopped receiving"
								break

							contents.append(data)
						except socket.error, e:
							break

					with open(fname , 'wb') as f:
						f.write(b''.join(contents))

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

				cli_con = reducer.establishConnection((IP1, int(port1)))
				reducer.incomingStream = cli_con[0]
				reducer.outgoingSocket = cli_con[1]

				reducer.cons.append(cli_con)

	with open(setup_file, 'r') as ff:
		N = int(ff.readline().strip())
		lineNum = 0
		for line in ff.readlines():
			lineNum += 1
			if lineNum != reducer.cli_id:
				_, _, _, _, _, _, _, _, reducerIP, reducerPort = line.strip().split()

				reducer.cons.append(reducer.establishConnection((reducerIP, int(reducerPort))))
	print "reducer established connections!"
class Reducer(object):
	def __init__(self, my_id):
		self.cli_id = my_id
		self.outgoingSocket = None
		self.incomingStream = None
		self.listeningSocket = None
		self.cons = []

	def openListeningSocket(self, addr):
		self.listeningSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.listeningSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.listeningSocket.bind( addr )
		self.listeningSocket.setblocking(0) 
		self.listeningSocket.listen(10)

	def addWords(self, filename, word_dict):
		file = open(filename, "r")

		for line in file:
			lineSplit = line.strip('\n\r').split()

			word = lineSplit[0]
			count = int(lineSplit[1])

			if word in word_dict:
				word_dict[word] += count
			elif word != "":
				word_dict[word] = count
		
		file.close()

	def writetoFile(self, filename, word_dict):
		file = open(filename, "w+")
		for word, count in word_dict.iteritems():
			file.write(word + " {0}\n".format(count))

		# delete whitespace at end
		file.seek(-1,2)
		while file.read(1).isspace():
			file.truncate(file.tell()-1)
			file.seek(-1,2)

		file.close()

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



main()

