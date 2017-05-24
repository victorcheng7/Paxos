#!/usr/bin/env python
import sys
import re

# order:
'''
1. check file empty
2. check all have two
3. check 


'''
def main():

	reduce_file = sys.argv[1]
	entry_list = []
	word_list = []
	reduce_obj = open(reduce_file, "r")

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

	return True


main()