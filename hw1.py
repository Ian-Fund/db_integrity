
import psycopg2
# table name, list of PKs, list of FKs
table_info = ['T1', ['k11','a'], []]

#team7

conn = psycopg2.connect(
    database="team7",
    user="team7",
    host="/tmp/",
    password="team7"
)


cur = conn.cursor()

#cur.execute("create table pk_table (" table_info[1][0]" varchar(50)



#cur.execute("INSERT INTO qm (TableName) VALUES (T1)")

# Testing something
cur.execute("insert into qm_table  (tablename,entityerrpt,referentialerrpt, ok)values ('D2', (select count  (distinct k11)/ count (k11)*1.0 from T1), 5.7,'O')")

conn.commit()


sql_command = ""
cur.execute("create table pk_counts ( total decimal(10,1), dist decimal(10,1), name varchar(50))")
conn.commit()

# insert into pk_counts select count (name), count (distinct name)  , 'T1' from pk_tester; I don't think this one matters anymore

#loops once for each PK. Puts total and distinct in columns. The min of distinct and max of total are used to find error
for pk in table_info[1]:
    sql_command = "insert into pk_counts select count(" +pk +"), count (distinct "+pk+"),'"+table_info[0]+"' from "+table_info[0]
    print("SQL Command is : ", sql_command)
    cur.execute(sql_command)
    conn.commit()
# insert into pk_counts select count (k11), count (distinct k11)  , 'T1' from T1;


# We'll add more to this line later. This is the answer table.
sql_command =  "insert into qm_table (entityerrpt ) values (1.0 - (select min (dist)/ max(total) from pk_counts  where name = '"+table_info[0]+"'))"
print("SQL Command is: ",sql_command)
cur.execute(sql_command)
conn.commit()
# insert into qm_table (entityerrpt ) values (1.0 - (select min (dist)/ max(total) from pk_counts  where name = 'T1'));

#  (1.0 - (select min (dist)/ max(total) from pk_counts )

cur.execute("drop table pk_counts")
conn.commit()



cur.close()
conn.close()


