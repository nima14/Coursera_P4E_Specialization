import urllib.request, urllib.parse , urllib.error
import json
import ssl

ctx= ssl.create_default_context()
ctx.check_hostname=False
ctx.verify_mode=ssl.CERT_NONE

serviceurl="http://py4e-data.dr-chuck.net/json?"

address=input("Enetr Loaction:")

parms= dict()

parms['address']=address
parms["key"]="42"

url=serviceurl+urllib.parse.urlencode(parms)

print("Retieving",url)

uh=urllib.request.urlopen(url)
data=uh.read().decode()

try:
    js=json.loads(data)
except:
    js=None

if not js or "status" not in js or js["status"] != "OK":
    print('==== Failure To Retrieve ====')
    print(data)


print(json.dumps(js,indent=4))

print(js["results"][0]["place_id"])