import urllib.request, urllib.parse, urllib.error
import xml.etree.ElementTree as ET
import ssl

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

url=input('Enter address:')
uh=urllib.request.urlopen(url,context=ctx)
data=uh.read()
tree=ET.fromstring(data)

sum=0
count=0
char=0
for comments in tree.findall('comments'):
    for comment in comments.findall('comment'):
        sum=sum+int(comment.find('count').text)
        char=char+len(comment.find('name').text)
        count=count+1

print('Retrieved',char,'characters')
print('Count:',count)
print('Sum:',sum)

