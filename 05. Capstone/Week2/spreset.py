import sqlite3
conn= sqlite3.connect('spider_google.sqlite')
cur=conn.cursor()

cur.execute('''Update Pages Set old_rank=0 , new_rank=1''')
conn.commit()

conn.close()