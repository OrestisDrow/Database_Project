import pymysql
from random import randint
import datetime
import time


h = 'localhost'
while True:
    try:
        #dbname = input("Βάση δεδομένων: ")
        dbname = "storagedb"
        con = pymysql.connect(host = h, \
             user='root', passwd=None, db=dbname,charset='utf8')
    except pymysql.Error as e:
        print ('Σφάλμα', e)
    else:
        con.isolation_level = None
        cur = con.cursor()
        cur.execute('select version()')
        print("connected to:" +dbname+ " database")
        break
    
while True:

    choice = input('Choose action \n 1:Insert SQL commands \n 2:Check Tables \n 3:Transaction Validation Mode \n 4:Check for store item shortages \n Press anything else to exit \n >>>')

    #----------------Exit case----------------
    if choice not in "1234" or choice =="":
        print('Bye')
        break
    
    #----------------SQL case----------------
    elif choice == "1":
        buffer = ""
        print ("ΒΔ:", dbname, " Give SQL commands: ")
        while True:
            line = input('>>>')
            if line == "":
                print ("Bye")
                break
            buffer += line
            print (buffer)
            if True: 
                try:
                    buffer = buffer.strip()
                    if buffer.lstrip().upper().startswith("SELECT"):
                        count=0
                        cur.execute(buffer)
                        desc = [x[0] for x in cur.description]
                        print(*desc, sep='\t\t\t')
                        for row in cur.fetchall():
                            for i in row :
                                print (i, end = '\t\t\t')
                            print()
                            count +=1
                            if count%30==0:
                                reply = input ( "....more ? (y/n)")
                                if reply != "y" :
                                    break
                                else:
                                    print ()
                    else:
                        cur.execute(buffer)
                        cur.execute('commit')
                    print ("σύνολο :", cur.rowcount )
                except pymysql.Error as e:
                    print ("An error occurred:", e)
                buffer = ""
        cur.execute('commit')
        continue

    #----------------Check Tables case----------------
    elif choice == "2":
        
        table_names=('costumer','item','store','transaction','warehouse','wholesale_supplier','wh_s_supply','ws_wh_supply')
        #Check Tables
        print('  Available table names are:')
        for i in range(len(table_names)):
            print("  ",i,table_names[i])
        choice2_1=input("   Select which of the above tables you'd like to inspect\n   >>>")
        choice2_1=int(choice2_1)
        #SELECT
        data=table_names[choice2_1]
        buffer = "SELECT * FROM "
        buffer += data
        cur.execute(buffer)
        desc = [x[0] for x in cur.description]
        count=0
        print(*desc, sep='\t\t')
        for row in cur.fetchall():
            for i in row :
                print (i, end='\t\t')
            print()
            count +=1
            if count%30==0:
                reply = input ( "....more ? (y/n)")
                if reply != "y" :
                    break
                else:
                    print()
    #----------------Transaction Validation Case----------------
    elif choice == "3":
        #Validating all transactions(ws_wh_supply and wh_s_supply) by comparing their completion date to today
        #I supplose the transaction table is validating all their tuples by default because the transaction happens automatically

        #ws_wh_supply validation
            #Checking all transactions and their keys=(ws_id,wh_id,i_id,qty)
        select_stmt = ("SELECT ws_id,wh_id,i_id,qty,date_completed,validated FROM ws_wh_supply" )
        dates=[]
        keys=[]
        validated=[]
        cur.execute(select_stmt)
        desc = [x[0] for x in cur.description]
        for row in cur.fetchall():
            keylog=[]
            for i in row :
                if isinstance(i,int):
                    keylog.append(i)
                elif i==b'1' or i==b'0':
                    validated.append(i)
                else:
                    dates.append(i)
            keys.append(keylog)
            #Checking which transactions have date_completed != NULL AND < today
            # AND are not validated
        flag=0;
        for i in range(len(validated)):
            if validated[i]==b'0' and dates[i] != None: 
                flag=1;
                print("--Pending validation on wholesale-warehouse transaction:\n(ws_id,wh_id,i_id,date_completed):",keys[i][0],keys[i][1],keys[i][2],dates[i])
                ans= input("You want to validate the above transaction? y/n:")  
                if ans=="y":
                    qty=str(keys[i][3])
                    ws_id=str(keys[i][0])
                    wh_id=str(keys[i][1])
                    i_id=str(keys[i][2])
                    try:
                        cur.execute ("""UPDATE warehouse SET qty = qty + %s WHERE wh_id=%s AND i_id=%s""",(qty, wh_id, i_id))
                        cur.execute('commit')
                        #Putting the validation bit on the transaction
                        cur.execute ("""UPDATE ws_wh_supply SET validated=1 WHERE ws_id=%s AND wh_id=%s AND i_id=%s""",(ws_id, wh_id, i_id))
                        cur.execute('commit')
                    except pymysql.Error as e:
                        print('HERE')
                        print ("An error occurred:", e)
        #----------------wh_s_supply validation + failsafe for warehouse qty shortages----------------
        #Checking all transactions and their keys=(ws_id,wh_id,i_id,qty)
        select_stmt = ("SELECT wh_id,s_id,i_id,qty,date_completed,validated FROM wh_s_supply" )
        dates=[]
        keys=[]
        validated=[]
        cur.execute(select_stmt)
        desc = [x[0] for x in cur.description]
        for row in cur.fetchall():
            keylog=[]
            for i in row :
                if isinstance(i,int):
                    keylog.append(i)
                elif i==b'1' or i==b'0':
                    validated.append(i)
                else:
                    dates.append(i)
            keys.append(keylog)
            #Checking which transactions have date_completed != NULL AND < today
            # AND are not validated
        for i in range(len(validated)):
            if validated[i]==b'0' and dates[i] != None: 
                flag=1;
                print("--Pending validation on warehouse-store transaction:\n(wh_id,s_id,i_id,date_completed):",keys[i][0],keys[i][1],keys[i][2],dates[i])
                ans= input("You want to validate the above transaction? y/n:")  
                if ans=="y":
                    #Making the transaction
                    #Checking if the transaction can be made
                    qty=str(keys[i][3])
                    wh_id=str(keys[i][0])
                    s_id=str(keys[i][1])
                    i_id=str(keys[i][2])
                    select_stmt = "SELECT qty FROM warehouse WHERE wh_id=%s AND i_id = %s"
                    cur.execute(select_stmt,(wh_id,i_id))
                    for row in cur.fetchall():
                        for j in row:
                            if isinstance(j,int):
                                wh_qty=j
                    if int(qty)<wh_qty:
                        try:
                            #Updating the warehouse
                            cur.execute ("""UPDATE warehouse SET qty = qty - %s WHERE wh_id=%s AND i_id=%s""",(qty, wh_id, i_id))
                            cur.execute('commit')
                            #Updating the storage
                            cur.execute ("""UPDATE store SET qty = qty + %s WHERE s_id=%s AND i_id=%s""",(qty, wh_id, i_id))
                            cur.execute('commit')
                            #Putting the validation bit on the transaction
                            cur.execute ("""UPDATE wh_s_supply SET validated=1 WHERE wh_id=%s AND s_id=%s AND i_id=%s""",(wh_id, s_id, i_id))
                            cur.execute('commit')
                        except pymysql.Error as e:
                            print ("An error occurred:", e)
                    else:
                        print ("--This transaction cant be done because warehouse %s has %d of item %s and the transaction requires %s" %(wh_id,wh_qty,i_id,qty))
    
        if flag==0:
            print("There is no transactions to validate")
#INSERT INTO ws_wh_supply(ws_id,wh_id,i_id,qty,date_scheduled,date_completed,ws_price) VALUES(1,1,3,10,'2011-01-01','2011-02-03',10)
#INSERT INTO wh_s_supply(wh_id,s_id,i_id,qty,date_scheduled,date_completed) VALUES(1,1,2,10,'2011-01-01','2011-02-03')

    #----------------Store Shortage Check Case----------------
    elif choice == "4":
        select_stmt = "SELECT s_id,i_id,qty,s_name FROM store WHERE qty<20"
        keys=[]
        names=[]
        cur.execute(select_stmt)
        desc = [x[0] for x in cur.description]
        for row in cur.fetchall():
            keylog=[]
            for i in row :
                if isinstance(i,int):
                    keylog.append(i)
                else:
                    names.append(i)
            keys.append(keylog)

        if keys != []:
            for i in range(len(keys)):
                print("store:%d has shortage of item:%d(%d left)" %(keys[i][0],keys[i][1],keys[i][2]))

con.close()
    #----------------INSERT IN NEW TRANSACTION----------------
'''
    elif choice =="5":
        inpt = input("1->Store-Costumer Transaction\n2->Warehouse-Store Transaction\n3->Wholesale-Warehouse Transaction\n>>>")
        if inpt not in "123":
            continue
        elif inpt=="1":
            insert_stmt = (
              "INSERT INTO transaction (c_id, s_id, i_id, qty, date_) "
              "VALUES (%s, %s, %s, %s, %s)"
                )
            #default date = today
            data = [int(input("c_id:")),int(input("s_id:")),int(input("i_id:")),int(input("qty:")),time.strftime("%d-%m-%Y")]

            cur.execute(insert_stmt,data)
            cur.execute('commit')
        elif inpt=="2":
            break
        elif inpt=="3":
            break
'''




