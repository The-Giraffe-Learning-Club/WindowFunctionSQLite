'''  @author Keshav Thakur
 @email k.e.s.h.a.v@hotmail.com
 @create date 2020-10-20 14:31:34
 @modify date 2020-10-20 14:31:34
 @desc Pandas && SQLite Based Windowing Functions '''

import sqlite3
import pandas as pd

conn = sqlite3.connect('my_data.db')
# LeadData = pd.read_csv('LagCSV.csv')

# LeadData.to_sql('LeadData_SQL', conn, if_exists='append', index = [0], chunksize = 10000)

cur = conn.cursor()
cur.execute("select AppName,Time,Bytes,Bytes-a.Diff_Bytes as diff from (SELECT AppName,Time,Bytes, LAG(Bytes,1) OVER (PARTITION BY AppName ORDER BY Time) Diff_Bytes FROM LeadData_SQL) a;")

results = cur.fetchall()
print(results)