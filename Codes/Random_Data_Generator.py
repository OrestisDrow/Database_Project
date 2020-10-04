import pymysql
from random import randint
import datetime
import sys

h = 'localhost'
while True:
    try:
        dbname = 'storagedb'
        con = pymysql.connect(host = h, \
             user='root', passwd=None, db=dbname,charset='utf8')
    except pymysql.Error as e:
        print ('Σφάλμα', e)
    else:
        con.isolation_level = None
        cur = con.cursor()
        cur.execute('select version()')
        break

table_names=('costumer','item','store','transaction','warehouse','wholesale_supplier','wh_s_supply','ws_wh_supply')


ans=input("Are you sure you want to generate random data in storagedb database?y/n")
if ans!="y":
    sys.exit()
    

#_______________Wholesale Supplier Initialisation_______________

insert_stmt = ("INSERT INTO wholesale_supplier (ws_name, ws_location, ws_addr) \
VALUES (%s, %s, %s)")
               
for i in range(10):
    data = ('ws_name'+str(i+1), 'ws_location'+str(i+1), 'ws_addr'+str(i+1))
    cur.execute(insert_stmt, data)
    cur.execute('commit')
    

#_______________Costumer Initialisation_______________

insert_stmt = ("INSERT INTO costumer (c_name, c_surename) \
VALUES (%s, %s)")
               
for i in range(1000):
    data = ('c_name'+str(i+1), 'c_surename'+str(i+1))
    cur.execute(insert_stmt, data)
    cur.execute('commit')
    

#Item Initialise



types = ('Laptop','Desktop','Mobile')
insert_stmt = ( "INSERT INTO item (manufacturer, model, type, price) \
VALUES (%s, %s, %s, %s)")

    #Samsung Items    
for i in range(10):
    data = ('SAMSUNG', 'Smodel'+ str(i+1), types[randint(0,2)], randint(0,500))
    cur.execute(insert_stmt, data)
    cur.execute('commit')
        
    #Dell Items
for i in range(10):
    data = ('DELL', 'Dmodel'+ str(i+1), types[randint(0,2)], randint(0,500))
    cur.execute(insert_stmt, data)
    cur.execute('commit')
        
    #Apple Items
for i in range(10):
    data = ('APPLE', 'Amodel'+ str(i+1), types[randint(0,2)], randint(0,500))
    cur.execute(insert_stmt, data)
    cur.execute('commit')



#_______________Warehouse Initialisation_______________
shelf = []
for i in range(30):
    print(i)
    shelf.append('Shelf'+str(i+1))
    
insert_stmt = ( "INSERT INTO warehouse (wh_id, i_id, wh_name, wh_location, wh_addr, qty, shelf) \
VALUES (%s, %s, %s, %s, %s, %s, %s)")


for i in range(5):
    
    list1=[]
    for j in range(40):
        r = randint(1,30)
        
        if r not in list1:
            list1.append(r)
            data = (i+1, r, 'wh_name'+str(i+1), 'wh_location'+str(i+1), 'wh_addr'+str(i+1),randint(0,200),shelf[randint(0,29)])
            cur.execute(insert_stmt, data)
            cur.execute('commit')
        else:
            continue


#_______________Storage Initialisation_______________
shelf = []
for i in range(30):
    shelf.append('Shelf'+str(i+1))
    
insert_stmt = ( "INSERT INTO store (s_id, i_id, s_name, s_location, s_addr, qty, shelf) \
VALUES (%s, %s, %s, %s, %s, %s, %s)")


for i in range(10):
    
    list1=[]
    for j in range(50):
        r = randint(1,30)
        
        if r not in list1:
            list1.append(r)
            data = (i+1, r, 's_name'+str(i+1), 's_location'+str(i+1), 's_addr'+str(i+1),randint(10,90),shelf[randint(0,29)])
            cur.execute(insert_stmt, data)
            cur.execute('commit')
        else:
            continue


    

#_______________Transaction Initialisation_______________

    #Checking which stores have what items

select_stmt = ("SELECT i_id FROM store WHERE s_id=%s")
s_items=[]
for storenum in range(11):
    items=[]
    count=0
    cur.execute(select_stmt,(str(storenum)))
    desc = [x[0] for x in cur.description]
    for row in cur.fetchall():
        for i in row :
            items.append(i)
        count +=1
    s_items.append(items) #s_items[1] = [1,2,3,5,6,...29,30]


    #Inserting random edible transactions
insert_stmt = (
  "INSERT INTO transaction (c_id, s_id, i_id, qty, date_) "
  "VALUES (%s, %s, %s, %s, %s)"
)
for i in range(400):
    s_id = randint(1,10)
    i_id = randint(1,30)
        #making sure that the store indeed has the randomised item
    while i_id not in s_items[s_id]:
        i_id = randint(1,30)
        
    data = (randint(1,999), s_id, i_id, randint(1,5), datetime.date(randint(2000,2017),randint(1,12),randint(1,25)))
    cur.execute(insert_stmt,data)
    cur.execute('commit')


#_______________ws_wh_supply Initialisation_______________

 #Checking which warehouses have what items

select_stmt = ("SELECT i_id FROM warehouse WHERE wh_id=%s")
wh_items=[]
for whnum in range(6):
    items=[]
    count=0
    cur.execute(select_stmt,(str(whnum)))
    desc = [x[0] for x in cur.description]
    #print(desc,"\t")
    for row in cur.fetchall():
        for i in row :
            items.append(i)
        count +=1
    wh_items.append(items) #wh_items[1] = [1,2,3,5,6,...29,30]


    #Inserting random edible transactions

insert_stmt = (
  "INSERT INTO ws_wh_supply (ws_id, wh_id, i_id, qty, date_scheduled, date_completed, ws_price) "
  "VALUES (%s, %s, %s, %s, %s, %s, %s)"
)
for i in range(100):
        
        #making sure that the warehouse indeed has the randomised item
    wh_id = randint(1,5)
    i_id = randint(1,30)
    while i_id not in wh_items[wh_id]:
        i_id = randint(1,30)
        
        #making sure the ws_wh exchange prices are better than normal item prices
    select_stmt =("SELECT price FROM item WHERE i_id=%s")
    cur.execute(select_stmt,i_id)
    for row in cur.fetchall():
        for price in row:
            print(i_id)
            print(price)
            print("\t")
    
        #making sure that date_completed>=date_scheduled
    date_scheduled = datetime.date(randint(2000,2017),randint(1,12),randint(1,25))
    date_completed = datetime.date(randint(2000,2017),randint(1,12),randint(1,25))
    while date_completed<date_scheduled:
        date_completed = datetime.date(randint(2000,2017),randint(1,12),randint(1,25))
    
    qty = randint(100,200)
    better_price = price*qty *80/100
    
    data = (randint(1,5), wh_id, i_id, qty, date_scheduled, date_completed, better_price)
    cur.execute(insert_stmt,data)
    cur.execute('commit')



#_______________wh_s_supply_initialisation_______________

    #Checking which stores have what items

select_stmt = ("SELECT i_id FROM store WHERE s_id=%s")
s_items=[]
for storenum in range(11):
    items=[]
    count=0
    cur.execute(select_stmt,(str(storenum)))
    desc = [x[0] for x in cur.description]
    for row in cur.fetchall():
        for i in row :
            items.append(i)
        count +=1
    s_items.append(items) #s_items[1] = [1,2,3,5,6,...29,30]
    #Checking which warehouses have what items

select_stmt = ("SELECT i_id FROM warehouse WHERE wh_id=%s")
wh_items=[]
for whnum in range(6):
    items=[]
    count=0
    cur.execute(select_stmt,(str(whnum)))
    desc = [x[0] for x in cur.description]
    #print(desc,"\t")
    for row in cur.fetchall():
        for i in row :
            items.append(i)
        count +=1
    wh_items.append(items) #wh_items[1] = [1,2,3,5,6,...29,30]
    
    #Inserting random edible transactions

insert_stmt = (
  "INSERT INTO wh_s_supply (wh_id, s_id, i_id, qty, date_scheduled, date_completed) "
  "VALUES (%s, %s, %s, %s, %s, %s)"
)

for i in range(90):
        
        #making sure that the warehouse AND the store indeed has the randomised item
    wh_id = randint(1,5)
    s_id = randint (1,10)
    i_id = randint(1,30)
    while (i_id not in wh_items[wh_id]) and (i_id not in s_items[s_id]):
        i_id = randint(1,30)
        

        #making sure that date_completed>=date_scheduled
    date_scheduled = datetime.date(randint(2000,2017),randint(1,12),randint(1,25))
    date_completed = datetime.date(randint(2000,2017),randint(1,12),randint(1,25))
    while date_completed<date_scheduled:
        date_completed = datetime.date(randint(2000,2017),randint(1,12),randint(1,25))
    
    qty = randint(10,100)
    
    data = (wh_id, s_id, i_id, qty, date_scheduled, date_completed)
    cur.execute(insert_stmt,data)
    cur.execute('commit')

con.close()
