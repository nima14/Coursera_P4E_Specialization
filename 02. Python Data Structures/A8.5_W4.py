fname = input("Enter file name: ")
if len(fname) < 1 : fname = "mbox-short.txt"

fh = open(fname)
count = 0
lst=list()
for line in fh:
	if line.startswith("From") and len(line.split())>2:
		words=line.split()
		lst.append(words[1])
		count=count+1
for i in lst:
	print(i)
	#print("\n")
print("There were", count, "lines in the file with From as the first word")
