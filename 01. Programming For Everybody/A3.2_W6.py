score = input("Enter Score: ")

try:
    score=float(score)
except:
    print("Please enter number")
    quit()

if score<0 or score>1:
    print("Score should be between 0 & 1")
    quit()

#elif 0.9<=score<=1:
 #   res="A"
elif 0.8<=score<0.9:
    res="B"
elif 0.7<=score<0.8:
    res="C"
elif 0.6<=score<0.7:
    res="D"
else:
    res="F"
print(res)



