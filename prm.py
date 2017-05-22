import socket
import time
import threading
import sys
import Queue
import copy
 
def main():
	if(len(sys.argv) != 3):
		print("USAGE: python [prm_id] [setup_file]")
		exit(1)
	prm_id = int(sys.argv[1])
	prm = Prm(prm_id)
	setup_file = sys.argv[2]
	setup(prm, setup_file)
	print "Setup success"

	cThread = threading.Thread(target = commThread, args=(site,))
	cThread.daemon = True
	cThread.start()

	while True:
		pass
	#site.checkIncomingChannels

	# Do all the paxos protocol related stuff

	#commThread(site)

	#assume that all connections are there prm and cli connections


def commThread(site):

	finish = 0
	while finish == 0:
		incomingChannelIndex = 0
		for con in site.incoming_channels:	
			try:
				data = con.recv(1024)
				print data	
				if incomingChannelIndex+1 == site.id:
					if data == "replicate!":
						for dest_id, sock in site.outgoing_channels.iteritems():
							#sock.send("replicate from node {0}".format(dest_id))
							site.sendProposal(dest_id, site.id)
					elif data == "exit"	:
						for dest_id, sock in site.outgoing_channels.iteritems():
							sock.send("exit")
						finish = 1

				else:
					for msg in Message.split(data):
						msg = Message.reconstructFromString(msg.strip())
						if msg.type == Message.PREPARE:
							print msg
			except socket.error, e:
				continue
			incomingChannelIndex += 1

def setup(site, setup_file):
	#Read setup file. ex - setup.txt  
	print "gothere"   
	with open(setup_file, 'r') as f:
		N = int(f.readline().strip())
		site.num_nodes = N
		process_id = 0
		for line in f.readlines():
			process_id += 1
			if process_id <= N+2:
				IP1, port1, IP2, port2 = line.strip().split()
				port1 = int(port1)
				port2 = int(port2)

				if process_id == site.id:
					site.addr_book.append( (IP1, port1) )
					site.openListeningSocket( IP2, port2) #open for traffic

				else:
					site.addr_book.append( (IP2, port2) )

				site.addOutgoingChannel(process_id)
				site.addIncomingChannel(process_id)

	print site.outgoing_channels
	print site.incoming_channels
	site.openOutgoingChannels()
	print "am"
	site.openIncomingChannels()

class Ballot(object):
	def __init__(self, ballot1, ballot2):
		self.ballot1 = ballot1
		self.ballot2 = ballot2


class Message(object):
	PREPARE = 0
	ACK = 1
	ACCEPT = 2
	DECIDE = 3
	def __init__(self, source_id, ballot, acceptTuple, acceptVal, index, type):
		self.source_id = source_id
		self.ballot = ballot
		self.acceptTuple = acceptTuple
		self.acceptVal = acceptVal
		self.index = index
		self.type = type

	def __str__(self):
		res = str(self.source_id) + " " + str(self.ballot) + " " + str(self.index) + " " + str(self.type) 
		if self.acceptTuple != None:
			res += " " + str(self.acceptTuple)
		if self.acceptVal != None: 
			res += " " + str(self.acceptVal)
		res += "||"
		return res

	def __repr__(self):
		return self.__str__()

	@staticmethod
	def reconstructFromString(str):
		keyWords = str.strip().split()
		source_id = int(keyWords[0])
		ballotTuple = keyWords[1]
		index = int(keyWords[2])
		msg_type = int(keyWords[3])
		acceptTuple = None
		acceptVal = None
		if msg_type == Message.ACK:
			acceptTuple = int(keyWords[4])
			acceptVal = int(keyWords[5])
		if msg_type == Message.ACCEPT:
			acceptTuple = int(keyWords[4]) #this is Done Process ID
			acceptVal = int(keyWords[5])
		return Message(source_id, ballotTuple, acceptTuple, acceptVal, index, msg_type)

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
		self.snap_count = 0
		self.snap_count = 0
		self.balance = 10
		self.incoming_channels = []
		self.SnapIDTableLastEntryTemplate = {}
		self.addr_book = []
		self.outgoing_channels = {}
		self.listeningSocket = None
		self.snapID_table = {}
		self.done_processes = set()

		self.num_nodes = 0
		self.numAccepts = 0
		self.numAcks = 0
		self.ballot = Ballot(0, site_id)
		#self.ballotTuple = [0, site_id]
		self.acceptTuple = [0, site_id]
		self.acceptVal = ""
		self.logs = []

	def openListeningSocket(self, IP, port):
		self.listeningSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		self.listeningSocket.bind( (IP, port) )
		self.listeningSocket.setblocking(0) 
		self.listeningSocket.listen(1)

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

	def addIncomingChannel(self, source_id):
		self.SnapIDTableLastEntryTemplate[source_id] = [False, 0]

	def openIncomingChannels(self):
		while len(self.incoming_channels) != len(self.SnapIDTableLastEntryTemplate):
			try:
				con, _ = self.listeningSocket.accept()
				con.setblocking(0)
				self.incoming_channels.append(con)
			except socket.error:
				continue

	def execute(self, command):
		self.checkIncomingMsgs()
		keyWords = command.split()
		if "send" == keyWords[0]:
			dest_id = int(keyWords[1])
			amount = int(keyWords[2])
			self.sendMoney(dest_id, amount)
		elif "snapshot" == keyWords[0]:
			self.startSnapshot()
		elif "sleep" == keyWords[0]:
			time = float(keyWords[1])
			self.sleep(time)
		else:
			print "CANNOT RECOGNIZE THE COMMAND: " + command
			exit(1)
		self.checkIncomingMsgs()


	def sendProposal(self, dest_id, site_id):
			#def __init__(self, source_id, ballotTuple, acceptTuple, acceptVal, index, type):
		self.ballot[0] += 1
		msg = Message(self.id, self.ballot, None, None, len(self.logs), Message.PREPARE)
		self.outgoing_channels[dest_id].send(str(msg))
		'''
			def __init__(self, source_id, ballot_num, accept_num, accept_val, index, type):
			self.num_nodes = 0
				self.numAccepts = 0
				self.numAcks = 0
				self.ballotTuple = (0, 0)
				self.acceptTuple = (0, 0)
				self.acceptVal = 0
				self.logs = []

			PREPARE = 0
			ACK = 1
			ACCEPT = 2
			DECIDE = 3
			def __init__(self, source_id, ballot_num, accept_num, accept_val, index, type):
				self.source_id = source_id
				self.ballot_num = ballot_num
				self.accept_num = accept_num
				self.accept_val = accept_val
				self.index = index
				self.type = type
						if msg.type === Message.PREPARE:
		'''

	def sendMarkers(self, snap_id):
		msg = Message(self.id, snap_id, None, Message.MARKER_TYPE)
		for dest_id, sock in self.outgoing_channels.iteritems():
			sock.send(str(msg))

	def sendDone(self, done_process_id):
		#Piggy Back amount with done_process_id
		msg = Message(self.id, None, done_process_id, Message.DONE_TYPE)
		for dest_id, sock in self.outgoing_channels.iteritems():
			sock.send(str(msg))

	def startSnapshot(self):
		self.snap_count += 1
		counter = 0
		snap_id = str(self.id) + "." + str(self.snap_count)
		site_state = self.balance #Take Local State Snapshot
		incoming_channels_states = copy.deepcopy(self.SnapIDTableLastEntryTemplate)
		self.snapID_table[snap_id] = [counter, site_state, incoming_channels_states]
		self.sendMarkers(snap_id)


	def checkIncomingMsgs(self):
		BUF_SIZE = 1024
		for con in self.incoming_channels:
			try:
				msgs = con.recv(BUF_SIZE)
				for msg in Message.split(msgs):
					msg = Message.reconstructFromString(msg.strip())
					if msg.type == Message.MARKER_TYPE:
						if msg.snap_id not in self.snapID_table: #First Marker -> input into snapID_table
							counter = 1
							site_state = self.balance #Take Local State Snapshot
							incoming_channels_states = copy.deepcopy(self.SnapIDTableLastEntryTemplate)
							self.snapID_table[msg.snap_id] = [counter, site_state, incoming_channels_states]
							self.snapID_table[msg.snap_id][2][msg.source_id][0] = True #Shut down the incoming channel of 1st Marker
							self.sendMarkers(msg.snap_id)
							if self.snapID_table[msg.snap_id][0] == len(self.incoming_channels): #Received all markers
								self.outputLocalSnapshotAt(msg.snap_id)
						else: #Not the first marker
							self.snapID_table[msg.snap_id][0] += 1 #increase counter by 1
							self.snapID_table[msg.snap_id][2][msg.source_id][0] = True #Take no more Snapshot on the channel
							if self.snapID_table[msg.snap_id][0] == len(self.incoming_channels): #Received all markers
								self.outputLocalSnapshotAt(msg.snap_id)
					elif msg.type == Message.MONEY_TRANSFER_TYPE: #received a msg
						self.balance += msg.amount #Fix the real-time balance
						for _, v in self.snapID_table.iteritems():
							if v[0] == len(self.incoming_channels): #Finished Snapshot
								continue
							if v[2][msg.source_id][0] == False: #The current money message is before Marker
								v[2][msg.source_id][1] += msg.amount #Record the increase in amount
					elif msg.type == Message.DONE_TYPE:
						#Piggy back ammount as done_process_id
						done_process_id = msg.amount
						if done_process_id not in self.done_processes:
							self.done_processes.add(done_process_id)
							self.sendDone(done_process_id)
					else:
						print "ERROR: DO NOT UNDERSTAND MESSAGE TYPE"
						print msg
						exit(1)
			except socket.error, e:
				continue

main()
