fname = input("Enter file name: ")
if len(fname) < 1 : fname = "mbox-short.txt"

fh = open(fname)
count = 0
dictionary=dict()
for line in fh:
	if line.startswith("From") and len(line.split())>2:
		words=line.split()
		dictionary[words[1]]= dictionary.get(words[1],0)+1


max_v=None
max_k=None

for k,v in dictionary.items():
	if max_v is None or max_v<v:
		max_v=v
		max_k=k
print(max_k,max_v)



