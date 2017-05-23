import random

def outputReduceFile():
	file = open("reducedFile.txt", "w")
	words = open("words.txt", "r")
	rng = random.SystemRandom()

	for line in words.readlines():
		file.write(line.split()[0] + " ")
		file.write("{0}\n".format(rng.randint(1,10)))

	file.close()


outputReduceFile()

