# Use the file name mbox-short.txt as the file name
fname = input("Enter file name: ")
fh = open(fname)
compute=0
count=0
for line in fh:
    if not line.startswith("X-DSPAM-Confidence:") : continue
    idx= line.find(':')
    res= line[idx+1:]
    res =float(res)
    compute=compute+res
    count=count+1
print("Average spam confidence:",compute/count )
