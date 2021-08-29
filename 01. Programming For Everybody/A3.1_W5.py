hrs = input("Enter Hours:")
hrs = float(hrs)
rate=input("Enter Rate:")
rate=float(rate)

if hrs<=40:
	pay=hrs*rate
else :
	pay=(40*rate)+(hrs-40)*rate*1.5
print(pay)