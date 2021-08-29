import urllib.request, urllib.parse, urllib.error
import json
import ssl
import sqlite3

api_key = False
# If you have a Google Places API key, enter it here
# api_key = 'AIzaSy___IDByT70'
# https://developers.google.com/maps/documentation/geocoding/intro

if api_key is False:
    api_key = 42
    serviceurl = 'http://py4e-data.dr-chuck.net/json?'
else:
    serviceurl = 'https://maps.googleapis.com/maps/api/geocode/json?'



conn=sqlite3.connect('geodata.sqlite')
cur=conn.cursor()

cur.execute('''CREATE TABLE IF NOT EXISTS locations(address TEXT, geodata TEXT)''')

# # Ignore SSL certificate errors

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE


fh=open('where.data')

for line in fh:
    address=line.strip()
    cur.execute('''Select geodata From Locations Where address=?''',(memoryview(address.encode()),))

    try:
        data=cur.fetchone()[0]
        print('Found in Database',address)
        continue
    except:
        print('Nothing Found', address)
        pass



    parms = dict()
    parms['query'] = address
    if api_key is not False: parms['key'] = api_key
    url = serviceurl + urllib.parse.urlencode(parms)

    print('Retrieving', url)
    uh = urllib.request.urlopen(url, context=ctx)
    data = uh.read().decode()
    print('Retrieved', len(data), 'characters')

    try:
        js = json.loads(data)
    except:
        continue

    if 'status' not in js or (js['status'] != 'OK' ) or js['status'] == 'ZERO_RESULTS':
        print('==== Failure To Retrieve ====')
        
        continue

    else:
        print('Record will be inserted to the database')

        cur.execute('''Insert into Locations(Address,geodata) Values(?,?)''',(memoryview(address.encode()),memoryview(data.encode())))
        conn.commit()

sqlstr='Select count(*) from Locations'

for row in cur.execute(sqlstr):
    print(row[0])