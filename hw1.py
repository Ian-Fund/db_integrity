import psycopg2

#team7

conn = psycopg2.connect(
    database="team7",
    user="team7",
    host="/tmp/",
    password="team7"
)
# table name, list of PKs, list of FKs
f = open("dbxyz_ta.txt", "r")

if f.mode == 'r':
    contents = f.readlines()

    for i in contents:
        table_info = []
        numPK = 0
        numFK = 0
        PKlist = []
        FKlist = []
        RTlist = []
        ogTable = False
        x = i.split(",")
        for z in x:
            for a in range(len(z)):
                if z[a] == "T" and ogTable == False:
                    table = z[a]+z[a+1]
                    table_info.append(table)
                    ogTable = True
                    #print("Table = ", table)
                elif z[a] == "p" and numPK == 0:
                    while a >= 0:
                        if z[a] == "K":
                            pk = z[a]+z[a+1]+z[a+2]
                            PKlist.append(pk)
                            numPK = 1
                            #print("PK = ", pk)
                            a = -1
                        else:
                            a = a - 1
                elif z[a] == "p" and numPK == 1:
                    while a >= 0:
                        if z[a] == "K":
                            pk2 = z[a] + z[a + 1] + z[a + 2]
                            PKlist.append(pk2)
                            numPK = 2
                            #print("PK2 = ", pk2)
                            a = -1
                        else:
                            a = a - 1
                elif z[a] == "p" and numPK == 2:
                    while a >= 0:
                        if z[a] == "K":
                            pk3 = z[a] + z[a + 1] + z[a + 2]
                            PKlist.append(pk3)
                            numPK = 3
                            #print("PK3 = ", pk3)
                            a = -1
                        else:
                            a = a - 1
                elif z[a] == 'f' and numFK == 0:
                    refTable = z[a+3] + z[a+4]
                    RTlist.append(refTable)
                    while a >= 0:
                        if z[a] == "K":
                            fk = z[a] + z[a + 1] + z[a + 2]
                            numFK = 1
                            FKlist.append(fk)
                            #print("FK = ", fk)
                            #print("Reference table = ", refTable)
                            a = -1
                        else:
                            a = a - 1
                elif z[a] == 'f' and numFK == 1:
                    refTable2 = z[a + 3] + z[a + 4]
                    RTlist.append(refTable2)
                    while a >= 0:
                        if z[a] == "K":
                            fk2 = z[a] + z[a + 1] + z[a + 2]
                            numFK = 2
                            FKlist.append(fk2)

                            # print("FK = ", fk)
                            # print("Reference table = ", refTable)
                            a = -1
                        else:
                            a = a - 1


        table_info.append(PKlist)
        table_info.append(FKlist)
        table_info.append(RTlist)
        for i in table_info:
            print(i)
        cur = conn.cursor()

        #cur.execute("create table pk_table ("          table_info[1][0]" varchar(50)



        #cur.execute("INSERT INTO qm (TableName) VALUES (T1)")

        # Testing something
        #cur.execute("insert into qm_table  (tablename,entityerrpt,referentialerrpt, ok) values ('D2', (select count  (distinct k11)/ count (k11)*1.0 from T1), 5.7,'O')")
        #conn.commit()


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

        
else:
    print("File failed to open!")

conn.close()
