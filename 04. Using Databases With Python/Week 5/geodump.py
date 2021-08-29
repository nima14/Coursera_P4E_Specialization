import sqlite3
import json
import codecs

conn=sqlite3.connect('geodata.sqlite')
cur=conn.cursor()

cur.execute('Select * From Locations')

fhand=codecs.open('where.js','w','utf-8')
fhand.write('myData= [\n')

count=0

for row in cur:
    data=str(row[1].decode())

    try: js=json.loads(data)
    except: continue

    if 'status' not in js or (js['status'] != 'OK') or js['status'] == 'ZERO_RESULTS': continue

    lat=js['results'][0]['geometry']['location']['lat']
    lng = js['results'][0]['geometry']['location']['lng']

    if lat==0 or lng==0: continue

    where=js['results'][0]['formatted_address']
    where = where.replace("'", "")

    count=count+1
    if count>1:
        fhand.write(',\n')
    output='['+str(lat)+','+str(lng)+", '"+where+"']"
    fhand.write(output)

fhand.write("\n];\n")
fhand.close()
print (count, "records written to where.js")
print ("Open where.html to view the data in a browser")

