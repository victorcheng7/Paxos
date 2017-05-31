#!/usr/bin/env python
# from termios import tcflush, TCIFLUSH
import socket
import time
import threading
import sys
import re

def main():
	if(len(sys.argv) != 4):
		print("USAGE: python mapper.py [mapper_id] [cli_id] [setup_file]")
		exit(1)
	mapper_id = int(sys.argv[1])
	cli_id = int(sys.argv[2])
	mapper = Mapper(mapper_id, cli_id)
	setup_file = sys.argv[3]
	setup(mapper, setup_file)

	cThread = threading.Thread(target = commThread, args=(mapper,))
	cThread.start()

def commThread(mapper):

	while True:

		try:
			data = mapper.incomingStream.recv(1024)
			splitData = data.split()

			if splitData[0] == "map":
				filename = splitData[1]
				offset = splitData[2]
				size = splitData[3]
				mapper.mapFile(filename, int(offset), int(size))
				mapper.outgoingSocket.send("taskFinished Finished. Mapped file is {0}_I_{1}".format(filename, mapper.mapper_id))

		except socket.error, e:
			continue


def setup(mapper, setup_file):
	#Read setup file. ex - setup.txt      
	with open(setup_file, 'r') as f:
		N = int(f.readline().strip())
		lineNum = 0
		for line in f.readlines():
			lineNum += 1
			if lineNum == mapper.cli_id :
				IP1, port1, _, _, map1IP, map1Port, map2IP, map2Port, _, _ = line.strip().split()
				mapInfo = [(map1IP, int(map1Port)), (map2IP, int(map2Port))]


				mapper.openListeningSocket(mapInfo[mapper.mapper_id])

				# connect with cli
				while True:
					try: 
						sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
						sock.connect((IP1, int(port1)))
						mapper.outgoingSocket = sock
						break
					except Exception:
						continue

				# accept from cli
				while True:
					try:
						con, _ = mapper.listeningSocket.accept()
						con.setblocking(0)
						mapper.incomingStream = con
						break
					except socket.error:
						continue

class Mapper(object):
	def __init__(self, id1, id2):
		self.mapper_id = id1
		self.cli_id = id2
		self.outgoingSocket = None
		self.incomingStream = None
		self.listeningSocket = None


	def openListeningSocket(self, addr):
		self.listeningSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.listeningSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.listeningSocket.bind( addr )
		self.listeningSocket.setblocking(0) 
		self.listeningSocket.listen(10)

	def mapFile(self, filename, offset, size):
		i_file = open("{0}_I_{1}".format(filename, self.mapper_id), "w+")
		orig_file = open(filename, "r")

		orig_file.seek(offset)
		curOffset = orig_file.tell() - offset

		words = []

		while curOffset < size:
			line = orig_file.readline()

			lineSplit = line.split()
			if curOffset + len(line) > size:
				lineSplit = line[:size-curOffset].split()


			for word in lineSplit:
				words.append(word)

			curOffset = orig_file.tell() - offset

		for word in words[:-1]:
			i_file.write(word + " 1\n")

		i_file.write(words[-1] + " 1")

		i_file.close()
		orig_file.close()



main()

