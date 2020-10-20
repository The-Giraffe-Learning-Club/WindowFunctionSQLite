'''  @author Keshav Thakur
 @email k.e.s.h.a.v@hotmail.com
 @create date 2020-10-20 14:31:34
 @modify date 2020-10-20 14:31:34
 @desc Pandas && SQLite Based Windowing Functions '''
 

import sqlite3
import pandas as pd

""" Create connect with SQLite3 DB, 
It create connection with the Name you specify to the DB
 """
 
conn = sqlite3.connect('mydb.db')
prd = pd.read_csv('C:\\dev\\com\\johnson\\app\\blogsite\\blogs\\sampleData.csv')
prd.to_sql('prd_data', conn, if_exists='append', index = [0], chunksize = 10000)

cur = conn.cursor()
cur.execute("select * from prd_data where product_codes = '966119' or product_description like '%demarcate%';")
ttt = cur.fetchall()
print(ttt)

req=['demarcate','966119']
#print(req)

for j in req:
    print(j)
    if j.isalpha():
        print('in IFFF condition')
        fa_list = [i[2] for i in ttt if (i[4] == 288 and j in i[3])]
        print(fa_list)
    else:
        fa_list = [i[1] for i in ttt ]
        print('in else condition')
        print(fa_list)




# faa_list = " , ".join([str(i) for i in set(fa_list)])
# print('faa_list '+faa_list)
# pia_list = [i['record_id'] for i in ttt if i['project_id']==342]
# print('pia_list '+pia_list)






