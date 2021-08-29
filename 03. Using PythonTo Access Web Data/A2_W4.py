import urllib.request, urllib.parse, urllib.error
from bs4 import BeautifulSoup
import ssl

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

url = input('Enter - ')
count=input('Enter Count:')
Position=input('Enter Position:')
count=int(count)
Position=int(Position)
print('Retrieving: ',url) 
i=1
while i<=count:
    html = urllib.request.urlopen(url, context=ctx).read()
    soup = BeautifulSoup(html, 'html.parser')

    tags = soup('a')
    lst=list()
    for tag in tags:
        lst.append(tag.get('href', None))
   
    url= lst[Position-1]
    print('Retrieving: ',url) 
    i= i+1


