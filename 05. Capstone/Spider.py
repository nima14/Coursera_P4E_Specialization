import sqlite3
import urllib.request
import ssl
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from urllib.parse import urlparse
from urllib.request import urlopen
#Ignore SSL Certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

#Connect to database
conn=sqlite3.connect('spider.sqlite')
cur=conn.cursor()


#
# cur.execute('''Drop TABLE IF  EXISTS Pages''')
# cur.execute('''Drop TABLE IF  EXISTS Links''')
# cur.execute('''Drop TABLE IF  EXISTS Webs''')

#Create tables in database
cur.execute('''CREATE TABLE IF NOT EXISTS Pages
    (id INTEGER PRIMARY KEY, url TEXT UNIQUE, html TEXT,
     error INTEGER, old_rank REAL, new_rank REAL)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Links
    (from_id INTEGER, to_id INTEGER)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Webs (url TEXT UNIQUE)''')


cur.execute('''Select id,url from Pages Where html is null And error is null Order By random() limit 1''')

row=cur.fetchone()

if row is not None:
    print("Restarting existing crawl.  Remove spider.sqlite to start a fresh crawl.")

#Insert url into Webs
else:
    starturl=input('Enter web url')
    if(len(starturl) < 1): starturl = 'http://www.dr-chuck.com/'
    if starturl.endswith('/'): starturl = starturl[:-1]

    position=starturl.find('www')
    init_position=position

    if position==-1:
        init_position= starturl.find('//')
        position=init_position+2

    if init_position==-1:
        position= 0

    starturl=starturl[position:]


    web_position=starturl.rfind('/')
    web=starturl[:web_position]
    if web_position == -1: web=starturl

    cur.execute('''INSERT OR IGNORE INTO Webs(url) Values(?)''', (web,))
    cur.execute('''INSERT OR IGNORE INTO Pages(url,new_rank) Values(?,1)''', (starturl, ))
    conn.commit()

many=input('How many urls do you want to retrieve?')
many=int(many)

#Get the From_id
i=1
while(i<=many):

    try:
        cur.execute('''Select id,url from Pages Where html is null And error is null Order By random() limit 1''')
        row=cur.fetchone()
        from_id=row[0]
        from_url=row[1]
        from_url='https://'+from_url

        print('Getting information from ',from_url, '(id=',from_id,')' )
    except:

        print('No unretrieved HTML pages found')
        many = 0
        break



    if (from_url.endswith('.png') or from_url.endswith('.jpg') or from_url.endswith('.gif')):
        cur.execute('''Update Pages Set error=20 Where id=?''', (from_id,))
        conn.commit()
        i = i + 1
        continue

    try:
        uh=urllib.request.urlopen(from_url,context=ctx)
        html=uh.read()

    except: continue

    cur.execute('''Update Pages Set html=? Where id=?''', (html,from_id))
    conn.commit()


    #Get the anchor tags

    soup = BeautifulSoup(html,'html.parser')
    tags=soup('a')
    href_list=list()

    for tag in tags:
        href=tag.get('href')
        if href is None: continue


        # Resolve relative references like href="/contact"
        up = urlparse(href)

        if (len(up.scheme) < 1):
            href = urljoin(from_url, href)
        ipos = href.find('#')
        if (ipos > 1): href = href[:ipos]
        if (href.endswith('.png') or href.endswith('.jpg') or href.endswith('.gif')): continue
        if (href.endswith('/')): href = href[:-1]

        if (len(href) < 1): continue
        href_list.append(href)

    href_list=set(href_list)


    webs_list=list()
    cur.execute('''Select url from Webs''')
    rows=cur.fetchall()[0]

    for row in rows:

        webs_list.append(row)

    if href_list is None: continue
    for link in href_list:


        try:
            position = link.find('www')
            init_position= position
            if position == -1:
                init_position = link.find('//')
                position=init_position+2

            if init_position == -1:
                position = 0
            link = link[position:]
        except: continue


        webs_tuple=tuple(webs_list)
        if  link.startswith(webs_tuple)==False: continue
        cur.execute('''INSERT OR IGNORE INTO Pages(url,new_rank) Values(?,1)''', (link,))

        cur.execute('''Select id from Pages Where url=?''',(link,))
        to_id=cur.fetchone()[0]

        cur.execute('''INSERT INTO Links(from_id,to_id) Values(?,?)''', (from_id, to_id))
        conn.commit()
    i=i+1


