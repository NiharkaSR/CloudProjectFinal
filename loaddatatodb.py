
"""
CREATE TABLE households (PRODUCT_NUM int,DEPARTMENT varchar(255),COMMODITY varchar(255),BRAND_TY varchar(255),
NATURAL_ORGANIC_FLAG varchar(255),PRIMARY KEY (PRODUCT_NUM));
"""
import mysql.connector
import pandas as pd
from mysql.connector import errorcode

config = {
    'host': 'cloudserveruc.mysql.database.azure.com',
    'user': 'niharika',
    'password': 'Root@1234',
    'database': 'clouddb'
}
try:
    conn = mysql.connector.connect(**config)
    print("Connection established")
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with the user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)

cursor = conn.cursor()

df = pd.read_csv("/Users/kavyagurram/Documents/Fall Semester/Cloud computing/ProjectData/400_transactions.csv")
df.columns = df.columns.str.replace(' ', '')
df=df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
dftotuple=list(df.to_records(index=False))
loopctrl=1;
progressbar=0;
for eachtuple in dftotuple:
    try:
        cursor.execute(
        '''INSERT INTO transactions (BASKET_NUM,HSHD_NUM,PURCHASE_,PRODUCT_NUM,SPEND,UNITS,STORE_R,WEEK_NUM,YEAR_NUM) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
            (int(eachtuple.BASKET_NUM),int(eachtuple.HSHD_NUM),str(eachtuple.PURCHASE_),int(eachtuple.PRODUCT_NUM),int(eachtuple.SPEND),int(eachtuple.UNITS),str(eachtuple.STORE_R),int(eachtuple.WEEK_NUM),int(eachtuple.YEAR)));
        loopctrl=loopctrl+1;
        if(loopctrl>20000):
            break;
        progressbar=progressbar+(loopctrl/20000);
        print(progressbar);
    except Exception as e:  # work on python 3.x
            print('Failed to upload to ftp: ' + str(e))

conn.commit()
cursor.close()
conn.close()