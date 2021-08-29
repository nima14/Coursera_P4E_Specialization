#This url does not work!
import urllib.request,urllib.parse,urllib.error
import json

serviceurl='http://maps.googleapis.com/maps/api/geocode/json?'

while True:
    address=input('Enter Location:')
    if len(address)<1: break

    url=serviceurl+ urllib.parse.urlencode({"address":address})
    print(url)
    uh=urllib.request.urlopen(url)
    data=uh.read().decode()
    print(data)

    js=json.loads(data)