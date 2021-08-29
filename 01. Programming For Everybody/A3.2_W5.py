score = input("Enter Score: ")

try:
  score=float(score)
except:
  print("Something went wrong")
  quit()

if score>1 or score<0:
	print("Not in range 0 to 1")
	quit()

elif score>=0.9:
	res="A"
elif 0.8<=score<0.9:
	res="B"
elif 0.7<=score<0.8:
	res="C"
elif 0.6<=score<0.7:
	res="D"
else:
	res="F"
print(res)



