import json
import sqlite3

conn=sqlite3.connect('roster.sqlite')
cur=conn.cursor()

# Do some setup
cur.executescript('''
DROP TABLE IF EXISTS User;
DROP TABLE IF EXISTS Member;
DROP TABLE IF EXISTS Course;

CREATE TABLE User (
    id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name   TEXT UNIQUE
);

CREATE TABLE Course (
    id     INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    title  TEXT UNIQUE
);

CREATE TABLE Member (
    user_id     INTEGER,
    course_id   INTEGER,
    role        INTEGER,
    PRIMARY KEY (user_id, course_id)
)
''')


fname='roster_data.json'

str_data=open(fname).read()
json_data=json.loads(str_data)

for entry in json_data:
    name=entry[0]
    course=entry[1]
    role=entry[2]

    cur.execute('''INSERT OR IGNORE INTO User (name)
         VALUES ( ? )''', (name,))
    cur.execute('SELECT id FROM User WHERE name = ? ', (name,))
    user_id = cur.fetchone()[0]

    cur.execute('''INSERT OR IGNORE INTO Course (title)
         VALUES ( ? )''', (course,))
    cur.execute('SELECT id FROM Course WHERE title = ? ', (course,))
    course_id = cur.fetchone()[0]

    cur.execute('''INSERT OR REPLACE INTO Member
         (user_id, course_id,role) VALUES ( ?, ?, ? )''',
                (user_id, course_id,role))


conn.commit()


sqlstr='''SELECT 'XYZZY' || hex(User.name || Course.title || Member.role ) AS X FROM 
    User JOIN Member JOIN Course 
    ON User.id = Member.user_id AND Member.course_id = Course.id
    ORDER BY X LIMIT 1;'''

for row in cur.execute(sqlstr):
    print(row)






