fname = input("Enter file name: ")
if len(fname) < 1 : fname = "mbox-short.txt"

fh = open(fname)
count = 0
dictionary=dict()
for line in fh:
	if line.startswith("From") and len(line.split())>2:
		words=line.split()
		time=words[5].split(":")
		hr=time[0]
		dictionary[hr]= dictionary.get(hr,0)+1


for key in sorted(dictionary):
	print(key,dictionary[key])



