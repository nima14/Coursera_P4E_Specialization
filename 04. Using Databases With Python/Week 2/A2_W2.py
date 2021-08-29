import sqlite3

conn=sqlite3.connect('org.sqlite')
cur=conn.cursor()

cur.execute('DROP TABLE IF EXISTS Counts')

cur.execute('CREATE TABLE Counts (org TEXT, count INTEGER)')


fh= open("mbox.txt", "r")

i=0
for line in fh:
    i=i+1
    if not (line.startswith('From: ')): continue
    pieces=line.split()
    email=pieces[1]
    org = pieces[1].split('@')[1]
    cur.execute('SELECT count(*) FROM Counts WHERE org = ? ',(org,))
    pre_count= cur.fetchone()

    if pre_count==(0,):
        cur.execute('Insert into Counts (org, count)  Values (?,1)',(org,))

    else :
        cur.execute('Update Counts Set count=count+1 where org=?',(org,))


conn.commit()
sqlstr='Select * from counts Order By count Desc Limit 10'
for row in cur.execute(sqlstr):
    print (str(row[0]), row[1])
cur.close()

