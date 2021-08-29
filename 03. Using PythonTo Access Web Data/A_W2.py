import re
fh=open("regex_real.txt")

all_num= list()
for line in fh:
	
		lst=re.findall('[0-9]+',line)
		if not lst==[]:
			for num in lst:
				num=int(num)
				all_num.append(num)
			
print(sum(all_num))
