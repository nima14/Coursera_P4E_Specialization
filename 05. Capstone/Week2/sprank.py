import sqlite3
conn= sqlite3.connect('spider.sqlite')
cur=conn.cursor()


#Fill in from_ids , to_ids & links
cur.execute('''SELECT DISTINCT from_id FROM Links''')
from_ids = list()
for row in cur:
    from_ids.append(row[0])

to_ids=list()
links=list()

cur.execute('''Select DISTINCT from_id,to_id From Links''')

for row in cur:
    from_id = row[0]
    to_id = row[1]
    if from_id == to_id: continue
    if from_id not in from_ids: continue
    if to_id not in from_ids: continue
    links.append(row)
    if to_id not in to_ids: to_ids.append(to_id)




#Fill in prev_ranks
prev_ranks=dict()

for node in from_ids:
    cur.execute('''Select new_rank From Pages where id=?''',(node,))
    row=cur.fetchone()
    prev_ranks[node]=row[0]


#Set new page ranks

many = input('How many iterations:')
many=int(many)
for i in range(many):

    next_ranks=dict()
    total=0
    for node,prev_rank in prev_ranks.items():
        total=total+prev_rank
        next_ranks[node]=0

        from_nodes=[link[0] for link in links if link[1]==node]

        for from_node in from_nodes:
            prev_rank_from_node=prev_ranks[from_node]


            count_from_node=0
            for link in links:
                if link[0]==from_node: count_from_node=count_from_node+1

            amount= prev_rank_from_node/count_from_node
            next_ranks[node]=next_ranks[node]+amount


    newtotal = 0
    for (node, next_rank) in list(next_ranks.items()):
        newtotal = newtotal + next_rank
        evap = (total - newtotal) / len(next_ranks)


    for node in next_ranks:
        next_ranks[node] = next_ranks[node] + evap

    newtotal = 0
    for (node, next_rank) in list(next_ranks.items()):
        newtotal = newtotal + next_rank


    totaldiff=0

    for node,prev_rank in prev_ranks.items():
        next_rank=next_ranks[node]
        diff=abs(next_rank-prev_rank)
        totaldiff=diff+totaldiff



    avg_diff=totaldiff/len(prev_ranks)

    print('Average Difference:',avg_diff)
    prev_ranks=next_ranks
    i=i+1

cur.execute('''UPDATE Pages SET old_rank=new_rank''')

for (id, new_rank) in list(next_ranks.items()) :
    cur.execute('''UPDATE Pages SET new_rank=? WHERE id=?''', (new_rank, id))

conn.commit()
conn.close()