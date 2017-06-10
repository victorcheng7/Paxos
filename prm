#!/usr/bin/env python
import socket
import time
import threading
import sys
import Queue
import copy
from random import randint

def main():
	if(len(sys.argv) != 3):
		print("USAGE: python [prm_id] [setup_file]")
		exit(1)
	prm_id = int(sys.argv[1])
	prm = Prm(prm_id)
	print "I am PRM {0}".format(prm_id)
	setup_file = sys.argv[2]
	setup(prm, setup_file)

	while True:
		pass

def commThread(prm):
	while (len(prm.incoming_channels) < (prm.num_nodes-1)) or prm.cli[0] == None:
		for con in prm.incoming_channels_unordered:
			try:
				data = con.recv(1024)
				if data == "confirmInit":
					prm.cli[0] = con
					for dest_id, sock in prm.outgoing_channels.iteritems():
						sock.send("sendID from {0}".format(prm.id))

				# add to ordered incoming channels 
				elif data.split()[0] == "sendID":
					src = int(data.split()[2])
					prm.incoming_channels[src] = con
			except socket.error, e:
				continue

	
	# separate cli from prm connections
	prm.cli[1] = prm.outgoing_channels[prm.id]
	prm.outgoing_channels.pop(prm.id, None)
	prm.incoming_channels.pop(prm.cli[0], None)
	prm.cli[1].send("finishedSetup")


	while True:
		for source_id, con in prm.incoming_channels.iteritems():  
			try:
				data = con.recv(1024)
				for msg in Message.split(data):
					try:
						msg = Message.reconstructFromString(msg.strip())
						if msg.msgType == Message.ISDECIDEDFALSE:
							prm.isDecided = False 
					except Exception:
						continue
					
				if prm.listening and not prm.isDecided:
					#incoming messages from other PRMS
					for msg in Message.split(data):
						msg = Message.reconstructFromString(msg.strip())

						if msg.msgType == Message.PREPARE:
							print ("Received Prepare Message from ", msg.source_id, msg.ballot, msg.index, msg.originalPRM, msg.msgType)

							heartBeatMsg = Message(prm.id, prm.ballot, None, None, prm.index, None, None, Message.HEARTBEAT)
							prm.outgoing_channels[msg.source_id].send(str(heartBeatMsg))
							
							if msg.index < prm.index: 
								print "The index you proposed has already been set in the log"
							else:
								msgBallotTuple = msg.ballot.split(",") # ex "1,0"
								prmBallotTuple = prm.ballot.split(",") # ex. "1,0"
								if ((msgBallotTuple[0] > prmBallotTuple[0]) or ((msgBallotTuple[0] == prmBallotTuple[0]) and (msgBallotTuple[1] > prmBallotTuple[1]))): #is ballot > local ballot
							   		prm.ballot = msg.ballot
									ackMsg = Message(prm.id, prm.ballot, prm.acceptTuple, prm.acceptVal, msg.index, msg.originalPRM, None, Message.ACK)

									prm.outgoing_channels[msg.source_id].send(str(ackMsg)) #send prepare message to original proposer
									print ("Sent Ack message to node ", dest_id)


						if msg.msgType == Message.ACK:
							print ("Received ACK Message from ", msg.source_id, msg.ballot, prm.acceptTuple, prm.acceptVal, msg.index, msg.originalPRM, msg.log, msg.msgType)
							if not prm.checkingMajorityAcks: 
								prm.checkingMajorityAcks = True
								prm.ackarray.append(msg)
								majorityAckThread = threading.Thread(target=prm.checkMajorityAcks)
								majorityAckThread.daemon = True
								majorityAckThread.start()
							else:
								prm.ackarray.append(msg)


						if msg.msgType == Message.ACCEPT:
							print "inside of Message.ACCEPT receive"
							if prm.id == msg.originalPRM: #Am I the original proposer?, check for majority Accepts
								if not prm.checkingMajorityAccepts:
									prm.checkingMajorityAccepts = True
									majorityAcceptThread = threading.Thread(target=prm.checkMajorityAccepts)
									majorityAcceptThread.daemon = True
									majorityAcceptThread.start()
								prm.acceptarray.append(msg)
							else: #you're not the original Proposer
								if msg.index == prm.index: 
									if (msg.ballot >= prm.ballot) and (msg.ballot not in prm.seenballotarray) and (not prm.isDecided): #first time, relay message
										prm.acceptTuple = msg.ballot 
										prm.acceptVal = msg.acceptVal
										prm.seenballotarray.append(msg.ballot)
										print ("Relay Accept message to all other nodes")
										for dest_id, sock in prm.outgoing_channels.iteritems():#Send all prms a prepare message
											msg = Message(prm.id, prm.ballot, None, prm.acceptVal, msg.index, msg.originalPRM, None, Message.ACCEPT)
											sock.send(str(msg))	
											print ("Sent Accept message to node ", dest_id)
									
							
						if msg.msgType == Message.DECIDE:
							print "INSIDE OF Message.DECIDE receive from ", msg.source_id
							prm.isDecided = True
							if msg.index > prm.index:
								print "The index you proposed has already been set in the log"
								#send(prm.id, prm.index, "UPDATE") to msg.originalPRM
							elif msg.index == prm.index: 
								prm.addToLog(msg.log, msg.index) #filename into log
								prm.newRoundCleanUp()
								#send the cli that you've finished replicating and what the log entry will be
								prm.cli[1].send("finishReplicating " + str(msg.index) + " " + prm.log[msg.index])


						if msg.msgType == Message.UPDATE:
							#print "Updated message received"
							prm.addToLog(msg.log, msg.index)
							#once you updated your log, send back the update source_id that you've completed and stop sending once everyone is updated, prm.updatedarray keep track of state on how many are updated
						

						if msg.msgType == Message.HEARTBEAT:
							prm.heartBeat[msg.source_id-1] = True

			except socket.error, e:
				continue

		  # listen to cli
		try:
			data = prm.cli[0].recv(1024)
			splitData = data.split(" ")

			if prm.listening:
				print "received {0} from cli".format(data)
				if splitData[0] == "replicate": #ex. replicate words.txt
					prm.isReplicating = True
					time.sleep(1)
					prm.isDecided = False
					for dest_id, sock in prm.outgoing_channels.iteritems():
						msg = Message(prm.id, prm.ballot, None, None, prm.index, None, None, Message.ISDECIDEDFALSE)
						sock.send(str(msg))
					prm.proposedFile = splitData[1]
					prm.incrementBallot()
					for dest_id, sock in prm.outgoing_channels.iteritems():#Send all prms a prepare message
						print ("Sent Prepare message to node ", dest_id)
						msg = Message(prm.id, prm.ballot, None, prm.proposedFile, prm.index, prm.id, None, Message.PREPARE)
						sock.send(str(msg)) 
					checkHeartBeat = threading.Thread(target = prm.checkHeartBeat)
					checkHeartBeat.daemon = True
					checkHeartBeat.start()

				elif data == "stop":
					print "Stopping PRM"
					prm.listening = False

				elif data == "print":
					print "Replicated Log"
					print "---------------------"
					for idx, val in enumerate(prm.log):
						if val != None:
							print "Index {0} : {1}".format(idx, val)
							
				elif splitData[0] == "total":
					try:
						count = 0
						for num in splitData[1:]:
							pos = int(num)
							file = open(prm.log[pos], "r")
							for line in file:
								count += int(line.strip('\n\r').split()[1])
							file.close()

						print "Total word count is {0}".format(count)

					except:
						print "Error: Invalid indices, log has only {0} entries.".format(len(prm.log))

				elif splitData[0] == "merge":
					word_dict = {}

					try:
						word_dict = {}
						for num in splitData[1:]:
							pos = int(num)
							file = open(prm.log[pos], "r")

							for line in file:
								lineSplit = line.strip('\n\r').split()

								word = lineSplit[0]
								count = int(lineSplit[1])

								if word in word_dict:
									word_dict[word] += count
								else:
									word_dict[word] = count
							file.close()



						print "Word List"
						print "---------------------"

						for key, val in word_dict.iteritems():
							print "{0} {1}".format(key, val)

					except:
						print "Error: Invalid indices, log has only {0} entries.".format(len(prm.log))
			elif data == "resume":
				print "Resuming PRM"
				prm.listening = True
				#for dest_id, sock in prm.outgoing_channels.iteritems():
					#sendUpdate(everything in message)
				#send a message to all PRMs asking for updates
			
			else:
				prm.cli[1].send("stopped")
		except socket.error, e:
			pass

def setup(prm, setup_file):
	#Read setup file. ex - setup.txt  
	with open(setup_file, 'r') as f:
	  N = int(f.readline().strip())
	  prm.num_nodes = N
	  process_id = 0
	  for line in f.readlines():
		  process_id += 1
		  if process_id <= N:
			  IP1, port1, IP2, port2, _, _, _, _, _, _ = line.strip().split()
			  port1 = int(port1)
			  port2 = int(port2)

			  if process_id == prm.id:
				  prm.addr_book.append( (IP1, port1) )
				  prm.openListeningSocket( IP2, port2) #open for traffic

			  else:
				  prm.addr_book.append( (IP2, port2) )

			  prm.addOutgoingChannel(process_id)

	prm.openOutgoingChannels()
	# start commThread
	cThread = threading.Thread(target = commThread, args=(prm,))
	cThread.daemon = True
	cThread.start()

	prm.openIncomingChannels()
	print "setup finished"

class Message(object):
	PREPARE = 0
	ACK = 1
	ACCEPT = 2
	DECIDE = 3
	UPDATE = 4
	ISDECIDEDFALSE = 5
	HEARTBEAT = 6
	#NEEDUPDATE = 5
	def __init__(self, source_id, ballot, acceptTuple, acceptVal, index, originalPRM, log, msgType):
		self.source_id = source_id
		self.ballot = ballot
		self.acceptTuple = acceptTuple
		self.acceptVal = acceptVal
		self.index = index
		self.originalPRM = originalPRM
		self.log = log
		self.msgType = msgType

	def __str__(self):
		res = str(self.source_id) + "&" + str(self.ballot) + "&" + str(self.index) + "&" + str(self.msgType) 
		if self.acceptTuple != None:
			res += "&" + str(self.acceptTuple)
		if self.acceptVal != None: 
			res += "&" + str(self.acceptVal)
		if self.originalPRM != None:
			res += "&" + str(self.originalPRM)
		if self.log != None:
			res += "&" + str(self.log)
		res += "||"
		return res


	def __repr__(self):
		return self.__str__()

	@staticmethod
	def reconstructFromString(str):
		keyWords = str.strip().split("&")
		source_id = int(keyWords[0])
		ballot = keyWords[1]
		index = int(keyWords[2])
		msgType = int(keyWords[3])
		acceptTuple = None
		acceptVal = None
		originalPRM = None
		log = None
		if msgType == Message.PREPARE: 
			acceptVal = keyWords[4]
			originalPRM = int(keyWords[5])
		if msgType == Message.ACK:
			acceptTuple = keyWords[4]
			acceptVal = keyWords[5]
			originalPRM = int(keyWords[6])
		if msgType == Message.ACCEPT:
			acceptVal = keyWords[4]
			originalPRM = int(keyWords[5])
		if msgType == Message.DECIDE:
			originalPRM = int(keyWords[4])
			log = keyWords[5]
		if msgType == Message.UPDATE:
			log = keyWords[4]		
		return Message(source_id, ballot, acceptTuple, acceptVal, index, originalPRM, log, msgType)

	@staticmethod
	def split(str):
		res = []
		for msg in str.strip().split("||"):
			res.append(msg)
		del res[-1]
		return res

class Prm(object):
	def __init__(self, site_id):
		self.id = site_id
		self.addr_book = []
		self.incoming_channels_unordered = []
		self.incoming_channels = {}
		self.outgoing_channels = {}
		self.cli = [None]*2 #format: [incoming,outgoing]
		self.listeningSocket = None
		self.listening = True
		self.done_processes = set()

		self.isDecided = False
		self.proposedFile = None
		self.num_nodes = 0
		self.heartBeat = [False]*15
		self.heartBeat[self.id-1] = True
		self.ballot = "0," + str(site_id) 
		self.acceptTuple =  "0,0" #Default acceptTuple if None
		self.acceptVal = "-1" #Default acceptVal if None
		self.index = 0
		self.ackarray = []
		self.acceptarray = []
		self.seenballotarray = [] #have you seen this ballot in an accept message before?
		self.checkingMajorityAcks = False
		self.checkingMajorityAccepts = False
		self.log = []

	def newRoundCleanUp(self):
		self.proposedFile = None
		self.ballot = "0," + str(self.id) 
		#self.ballot = [0, site_id]
		self.acceptTuple = "0,0"
		self.acceptVal = "-1"
		self.ackarray = []
		self.acceptarray = []
		self.seenballotarray = [] #have you seen this ballot in an accept message before?
		self.checkingMajorityAcks = False
		self.checkingMajorityAccepts = False
		self.heartBeat = [False]*15
		self.heartBeat[self.id-1] = True


	def stop():
		self.listening = False
	def resume():
		self.listening = True


	def checkMajorityAcks(self):
		if not self.isDecided:
			print "Checking to see if I have Majority Acks..."
			time.sleep(0.5) 
			counter = 0
			for msg in self.ackarray: 
				if msg.originalPRM == self.id:
					counter += 1
			if (counter + 1) >= 0.5*(self.num_nodes+1):
				print "I have majority Acks!"
				highestAcceptNum = "0,0" #message with the highest acceptTuple
				highestAcceptVal = "-1" #acceptVal with the highest acceptTuple
				checkAcceptNum = [0,0]

				for msg in self.ackarray: #set highestAcceptNum = highest AcceptNum out of all the messages
					tempAcceptNum = msg.acceptTuple.split(",")
					if ((tempAcceptNum[0] > checkAcceptNum[0]) or ((tempAcceptNum[0] == checkAcceptNum[0]) and (tempAcceptNum[1] > checkAcceptNum[1]))) and (msg.originalPRM == self.id):
						highestAcceptNum = msg.acceptTuple
						highestAcceptVal = msg.acceptVal

				if not self.isDecided:
					if highestAcceptNum == "0,0": #None of the msgs have acceptNum set already
						self.acceptTuple = self.ackarray[0].ballot
						self.acceptVal = self.ackarray[0].acceptVal
					else: 
						self.acceptTuple = highestAcceptNum
						self.acceptVal = highestAcceptVal
					print "Setting acceptNum to ", self.acceptTuple, self.acceptVal
					for dest_id, sock in self.outgoing_channels.iteritems():#Send all prms an accept message
							print ("Sent Accept message to node ", dest_id)
							msg = Message(self.id, self.ballot, None, self.acceptVal, self.index, self.id, None, Message.ACCEPT) #MAY CAUSE ERROR CAUSE SELF.INDEX IS NOT ACCURATE
							sock.send(str(msg))	
			
			else: 
				time.sleep(randint(10,30)/10.0)
				self.incrementBallot()
				if not self.isDecided:
					for dest_id, sock in self.outgoing_channels.iteritems():#Send all prms a prepare message
						print ("Sending Repropose to node ", dest_id)
						msg = Message(self.id, self.ballot, None, self.proposedFile, self.index, self.id, None, Message.PREPARE)
						sock.send(str(msg))		
			
			self.checkingMajorityAcks = False
			self.ackarray = []

	def checkMajorityAccepts(self):
		if not self.isDecided:
			time.sleep(0.5)
			counter = 0
			for msg in self.acceptarray: 
				if msg.originalPRM == self.id:
					counter += 1
			print "Checking to see if I have Majority Accepts..."
			if (counter + 1) >= 0.5*(self.num_nodes+1):
				print "I have majority Accepts!"
				#self.log.append(self.proposedFile)
				#self.log[self.acceptarray[0].index] = self.proposedFile
				if not self.isDecided:
					for dest_id, sock in self.outgoing_channels.iteritems():#Send all prms a decide message
						print ("Sent Decide message to node ", dest_id)
						#self.proposedFile is None because it got reset already
						msg = Message(self.id, self.ballot, None, None, self.index, self.id, self.proposedFile, Message.DECIDE)
						sock.send(str(msg))	
					print "ADDING TO LOG", self.proposedFile, self.index
					self.addToLog(self.proposedFile, self.index)
					#periodically send updates to all other nodes
					sendUpdatesThread = threading.Thread(target = self.sendUpdates, args=(self.index-1,))
					sendUpdatesThread.daemon = True
					sendUpdatesThread.start()
					

				self.isDecided = True
				self.checkingMajorityAccepts = False		
				self.acceptarray = []

				#Tell the CLI you finished replicating
				self.cli[1].send("finishReplicating " + str(self.index-1) + " " + self.log[self.index-1])
				time.sleep(0.5)
				self.newRoundCleanUp()
			
			else: 
				time.sleep(randint(10,30)/10.0)
				self.incrementBallot()
				print "Reproposing my ballot, no majority accept :("
				if not self.isDecided:
					for dest_id, sock in self.outgoing_channels.iteritems():#Send all prms a prepare message
						print ("Sending Prepare to node ", dest_id)
						msg = Message(self.id, self.ballot, None, self.proposedFile, self.index, self.id, None, Message.PREPARE)
						sock.send(str(msg))	
			
			self.checkingMajorityAccepts = False
			self.acceptarray = []
	
	def sendUpdates(self, index):
		print "Inside of sendUpdates"
		while True:
			time.sleep(1)
			for dest_id, sock in self.outgoing_channels.iteritems():#Send all prms an update message
				#print ("Sending Update Message to node ", dest_id)
				msg = Message(self.id, self.ballot, None, None, index, None, self.log[index], Message.UPDATE)
				sock.send(str(msg))	    	
	
	def checkHeartBeat(self):
		time.sleep(0.4)
		counter = 0
		if not self.isDecided:
			while (counter) < 0.5*(self.num_nodes+1):
				self.heartBeat = [False]*15
				counter = 0
				for alive in self.heartBeat:
					if alive:
						counter += 1
				time.sleep(0.2)
				if not self.isDecided:
					for dest_id, sock in self.outgoing_channels.iteritems():#Send all prms a prepare message
						msg = Message(self.id, self.ballot, None, self.proposedFile, self.index, self.id, None, Message.PREPARE)
						sock.send(str(msg))	  


	def addToLog(self, value, index): #if get an update with higher index than your log, resize
		try: 
			if self.log[index] == None:
				self.log[index] = value
				self.index += 1
		except Exception: 
			temp = [None]*(index+1)
			counter = 0
			for logValue in self.log:
				temp[counter] = logValue
				counter += 1
			temp[index] = value
			self.log = temp[:]
			self.index += 1



	def incrementBallot(self): 
		tempBallot = self.ballot.split(",")
		ballotFirstIndex = int(tempBallot[0])
		ballotFirstIndex += 1
		self.ballot = str(ballotFirstIndex) + "," + str(self.id)
	def changeAcceptTuple(self, first, second): #passed in as int
		self.acceptTuple = str(first) + "," + str(second)



	def openListeningSocket(self, IP, port):
		self.listeningSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.listeningSocket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.listeningSocket.bind( (IP, port) )
		self.listeningSocket.setblocking(0) 
		self.listeningSocket.listen(1)
		self.listening = True

	def addOutgoingChannel(self, dest_id):
		self.outgoing_channels[dest_id] = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

	def openOutgoingChannels(self):
		#time.sleep(5) #To make sure all other processes are up
		for dest_id, sock in self.outgoing_channels.iteritems():
			while True:
				try: 
					sock.connect(self.addr_book[dest_id - 1])
					break
				except Exception:
					continue

	def openIncomingChannels(self):
		while len(self.incoming_channels_unordered) != self.num_nodes:
			try:
				con, addr = self.listeningSocket.accept()
				con.setblocking(0)
				self.incoming_channels_unordered.append(con)
			except socket.error:
				continue           

main()