#!/usr/bin/env python

def total():

	file = open("m.txt_reduced", "r")

	count = 0
	for line in file:
		lineSplit = line.strip('\n\r').split()

		count += int(lineSplit[1])

	
	file.close()

	print count

total()