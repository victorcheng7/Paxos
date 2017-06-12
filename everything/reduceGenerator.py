import random

def outputReduceFile(num):


	file = open("reducedFile{0}.txt".format(num), "w")
	words = open("words.txt", "r")
	rng = random.SystemRandom()

	for line in words.readlines():
		file.write(line.split()[0] + " ")
		file.write("{0}\n".format(rng.randint(1,10)))

	file.close()

for x in range(0,3):
	outputReduceFile(x)

