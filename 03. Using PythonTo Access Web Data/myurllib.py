import urllib.request, urllib.parse, urllib.error

url = 'http://data.pr4e.org/romeo.txt'

fhand=urllib.request.urlopen(url)


print(urllib.request.urlopen(url).read())
for line in fhand:
    print(line.decode().strip())