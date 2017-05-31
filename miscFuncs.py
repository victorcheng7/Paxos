#!/usr/bin/env python

def getSplit(filename):
	fileLen = getFileLen(filename)
	file = open(filename, "r")

	split = int(fileLen/2)
	file.seek(split)
	curChar = file.read(1)

	while not curChar.isspace():
		curChar = file.read(1)
		split += 1

	return split

def getFileLen(filename):
	file = open(filename, "r")
	lenFile = 0
	for line in file:
		lenFile += len(line)
	file.close()
	return lenFile

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