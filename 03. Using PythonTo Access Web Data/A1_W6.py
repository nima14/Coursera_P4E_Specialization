import urllib.request , urllib.parse , urllib.error
import ssl
import json

ctx= ssl.create_default_context()
ctx.check_hostname=False
ctx.verify_mode=ssl.CERT_NONE

url= input('Enter URL:')

print("Retrieving",url)

uh= urllib.request.urlopen(url,context=ctx)

data=uh.read()

info=json.loads(data)

sum=0
len_char=0
count=0

for item in info["comments"]:
    sum=sum+(item["count"])
    len_char=len_char+len(item["name"])
    count=count+1 



print("Retrieved",len_char, "characters")
print("Count:",count)
print("Sum:",sum)    

