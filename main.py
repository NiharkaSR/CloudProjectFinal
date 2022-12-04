import sqlite3
import mysql.connector
import pandas as pd
import shutil
import json
import os
from mysql.connector import errorcode
from decimal import *
from werkzeug.utils import secure_filename
from flask import Flask, request, g, render_template, send_file


DATABASE="/Users/kavyagurram/Documents/Fall Semester/Cloud computing/Kavya/initial.db"
app = Flask(__name__)
app.config.from_object(__name__)
app.config['HouseHoldsData']='static/UploadFiles/HouseholdData'
app.config['TransactionsData']='static/UploadFiles/TransactionData'
app.config['ProductsData']='static/UploadFiles/ProductsData'


if not os.path.exists(app.config['HouseHoldsData']):
   os.mkdir(app.config['HouseHoldsData'])
else:
   shutil.rmtree(app.config['HouseHoldsData'])
   os.mkdir(app.config['HouseHoldsData'])

if not os.path.exists(app.config['ProductsData']):
   os.mkdir(app.config['ProductsData'])
else:
    shutil.rmtree(app.config['ProductsData'])
    os.mkdir(app.config['ProductsData'])

if not os.path.exists(app.config['TransactionsData']):
   os.mkdir(app.config['TransactionsData'])
else:
    shutil.rmtree(app.config['TransactionsData'])
    os.mkdir(app.config['TransactionsData'])

extensions_expected = ['csv']

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)

def checkFileType(filename):
    return filename.split('.')[-1] in extensions_expected

def startDatabase():
    return sqlite3.connect(app.config['DATABASE'])

def connect_db():
    db = getattr(g, 'db', None)
    if db is None:
        db = g.db = startDatabase()
    return db

@app.teardown_appcontext
def close_db(exception):
    db = getattr(g, 'db', None)
    if db is not None:
        db.close()

def execute_query(query, args=()):
    res = connect_db().execute(query, args)
    data = res.fetchall()
    res.close()
    return data

def commit():
    connect_db().commit()

@app.route("/")
def hello():
    execute_query("DROP TABLE IF EXISTS users")
    execute_query("CREATE TABLE users (Username text,Password text,firstname text, lastname text, email text, count integer)")
    return render_template('loginPage.html')

@app.route('/loginPage', methods =['POST', 'GET'])
def loginPage():
    message = ''
    if request.method == 'POST' and str(request.form['username']) !="" and str(request.form['password']) != "":
        username = str(request.form['username'])
        password = str(request.form['password'])
        res = execute_query("""SELECT firstname,lastname,email,count  FROM users WHERE Username  = (?) AND Password = (?)""", (username, password ))
        if res:
            print("You are now logged in Successfully")
            return render_template('mainPage.html', message=message)
        else:
            message = 'Invalid Credentials!'
    elif request.method == 'POST':
        message = 'Please enter your Credentials'
    return render_template('loginPage.html', message=message)

@app.route('/home', methods =['GET','POST'])
def home():
    return render_template('mainPage.html')

@app.route('/uploaddatasets', methods =['GET','POST'])
def uploaddatasets():
    return render_template('dataUpload.html')

@app.route('/registrationPage', methods =['GET','POST'])
def registrationPage():
    message = ''
    if request.method == 'POST' and str(request.form['username']) !="" and str(request.form['password']) !="" and str(request.form['firstname']) !="" and str(request.form['lastname']) !="" and str(request.form['email']) !="":
        username = str(request.form['username'])
        password = str(request.form['password'])
        firstname = str(request.form['firstname'])
        lastname = str(request.form['lastname'])
        email = str(request.form['email'])
        res = execute_query("""SELECT * FROM users WHERE Username  = (?)""", (username, ))
        if res:
            message = 'User is registered already!'
        else:
            res1 = execute_query("""INSERT INTO users (username, password, firstname, lastname, email) values (?, ?, ?, ?, ?)""", (username, password, firstname, lastname, email))
            commit()
            res2 = execute_query( """SELECT firstname,lastname,email,count  FROM users WHERE Username  = (?) AND Password = (?)""",(username, password))
            if res2:
                message = 'user registration completed successfully! click Login.'
                return render_template('registrationComplete.html', message=message)
    elif request.method == 'POST':
        message = 'Some fields are missing!!'
    return render_template('registrationPage.html', message=message)

@app.route('/loadhhmtable', methods =['GET', 'POST'])
def loadhhmtable():
    return loadhhmtable("10");

@app.route('/queryhhm', methods =['GET','POST'])
def queryhhm():
    if request.method == 'POST' and str(request.form['hshd_num']) != "":
        try:
            response=loadhhmtable(str(request.form['hshd_num']));
        except:
            message = "Please enter Valid HSHD_NUM!"
            return render_template('queryHHM.html', message=message)
    else:
        message = "Please enter Valid HSHD_NUM!"
        return render_template('queryHHM.html',message=message)
    return response;

@app.route('/storenewhouseholddata', methods =['GET','POST'])
def storenewhouseholddata():
    message = 'Please upload file again!!'
    if request.method == 'POST':
        f = request.files['file']
        if checkFileType(f.filename):
            f.save(os.path.join(app.config['HouseHoldsData'],secure_filename(f.filename)))
            readloaddata(os.path.join(app.config['HouseHoldsData'], secure_filename(f.filename)),"households");
            message='file uploaded successfully'
        else:
            message='This extension is not allowed'

    return render_template('dataUpload.html',message=message)

@app.route('/storenewproductdata', methods =['GET','POST'])
def storenewproductdata():
    message = 'Please upload file again!!'
    if request.method == 'POST':
        f = request.files['file']
        if checkFileType(f.filename):
            f.save(os.path.join(app.config['ProductsData'],secure_filename(f.filename)))
            readloaddata(os.path.join(app.config['ProductsData'], secure_filename(f.filename)),"products");
            message='file uploaded successfully'
        else:
            message='The extension is not allowed'

    return render_template('dataUpload.html',messageProducts=message)

@app.route('/storenewtransactiondata', methods =['GET','POST'])
def storenewtransactiondata():
    message = 'Please upload file again!!'
    if request.method == 'POST':
        f = request.files['file']
        if checkFileType(f.filename):
            f.save(os.path.join(app.config['TransactionsData'], secure_filename(f.filename)))  # this will secure the file
            readloaddata(os.path.join(app.config['TransactionsData'], secure_filename(f.filename)),"transactions");
            message='file uploaded successfully'
        else:
            message='The file extension is not allowed'

    return render_template('dataUpload.html',messageTransactions=message)


def exeselect(query):
    db = connectDB()
    return pd.read_sql(query,db)

def loadhhmtable(hshd_num):

    db = connectDB()
    connect = db.cursor()
    connect.execute("""Select a.HSHD_NUM,b.BASKET_NUM,b.PURCHASE_,b.PRODUCT_NUM,c.DEPARTMENT,c.COMMODITY,b.SPEND,b.UNITS,b.STORE_R,b.WEEK_NUM,b.YEAR_NUM,a.L,
    a.AGE_RANGE,a.MARITAL,a.INCOME_RANGE,a.HOMEOWNER,a.HSHD_COMPOSITION,a.HH_SIZE,a.CHILDREN from households as a inner join transactions as b inner join 
    products as c on a.HSHD_NUM=b.HSHD_NUM and b.PRODUCT_NUM=c.PRODUCT_NUM where a.HSHD_NUM="""+hshd_num+" order by a.HSHD_NUM,b.BASKET_NUM,b.PURCHASE_,b.PRODUCT_NUM,c.DEPARTMENT,c.COMMODITY;");
    out = connect.fetchall()
    db.commit()
    connect.close()
    db.close()
    return render_template('displayHHMTable.html',data=out)

def connectDB():
    config={
        'host': 'cloudserveruc.mysql.database.azure.com',
        'user': 'niharika',
        'password': 'Root@1234',
        'database': 'clouddb'
        }
    try:
        db = mysql.connector.connect(**config)
        print("Connection is Successful")
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Invalid user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("DB doesn't exist")
        else:
            print(err)
    return db;

def readloaddata(csvfilepath,dataFrom):
    db = connectDB()
    connect = db.cursor()
    input = pd.read_csv(csvfilepath)
    input.columns = input.columns.str.replace(' ', '')
    input = input.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    dftotuple = list(input.to_records(index=False))
    if(dataFrom=='households'):
        for eachtuple in dftotuple:
            try:
                connect.execute(
                    """INSERT INTO households (HSHD_NUM,L,AGE_RANGE,MARITAL,INCOME_RANGE,HOMEOWNER,HSHD_COMPOSITION,HH_SIZE,CHILDREN) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (int(eachtuple.HSHD_NUM), str(eachtuple.L), str(eachtuple.AGE_RANGE), str(eachtuple.MARITAL),
                     str(eachtuple.INCOME_RANGE), str(eachtuple.HOMEOWNER), str(eachtuple.HSHD_COMPOSITION),
                     str(eachtuple.HH_SIZE), str(eachtuple.CHILDREN)));
            except Exception as e:
                print('Failed to upload to ftp: ' + str(e))
    if (dataFrom == 'transactions'):
        for eachtuple in dftotuple:
            try:
                connect.execute(
                    '''INSERT INTO transactions (BASKET_NUM,HSHD_NUM,PURCHASE_,PRODUCT_NUM,SPEND,UNITS,STORE_R,WEEK_NUM,YEAR_NUM) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)''',
                    (int(eachtuple.BASKET_NUM), int(eachtuple.HSHD_NUM), str(eachtuple.PURCHASE_),
                     int(eachtuple.PRODUCT_NUM), int(eachtuple.SPEND), int(eachtuple.UNITS), str(eachtuple.STORE_R),
                     int(eachtuple.WEEK_NUM), int(eachtuple.YEAR)));
            except Exception as e:
                print('Failed to upload to ftp: ' + str(e))
    if (dataFrom == 'products'):
        for eachtuple in dftotuple:
            try:
                connect.execute("""INSERT INTO products (PRODUCT_NUM,DEPARTMENT,COMMODITY,BRAND_TY,NATURAL_ORGANIC_FLAG) VALUES (%s,%s,%s,%s,%s)""",
                    (int(eachtuple.PRODUCT_NUM), str(eachtuple.DEPARTMENT), str(eachtuple.COMMODITY),str(eachtuple.BRAND_TY),
                     str(eachtuple.NATURAL_ORGANIC_FLAG)));
            except Exception as e:
                print('Failed to upload to ftp: ' + str(e))
    db.commit()
    connect.close()
    db.close()

@app.route('/viewDashboard', methods =['GET','POST'])
def viewDashboard():
    #PieGraph

    data=exeselect("Select AVG(q.SPEND) as Expense,p.MARITAL as marital_Status from households as p inner join transactions as q on p.HSHD_NUM=q.HSHD_NUM where upper(p.MARITAL) in ('MARRIED','SINGLE') group by p.MARITAL;");
    data['Expense'] = data['Expense'].astype(str);
    data['marital_Status'] = data['marital_Status'].astype(str);

    #BarGraph
    Seconddata = exeselect(
        "Select AVG(b.SPEND) as Expense,a.AGE_RANGE as Age_Group from households as a inner join transactions as b inner join products as c  on b.PRODUCT_NUM=c.PRODUCT_NUM and a.HSHD_NUM=b.HSHD_NUM and a.AGE_RANGE is not null and c.NATURAL_ORGANIC_FLAG='Y'  group by c.NATURAL_ORGANIC_FLAG,a.AGE_RANGE;");
    Seconddata['Expense'] = Seconddata['Expense'].astype(str);
    Seconddata['Age_Group'] = Seconddata['Age_Group'].astype(str);

    SeconddataTwo = exeselect(
        "Select AVG(b.SPEND) as Expense,a.AGE_RANGE as Age_Group from households as a inner join transactions as b inner join products as c  on b.PRODUCT_NUM=c.PRODUCT_NUM and a.HSHD_NUM=b.HSHD_NUM and a.AGE_RANGE is not null and c.NATURAL_ORGANIC_FLAG='N'  group by c.NATURAL_ORGANIC_FLAG,a.AGE_RANGE;");
    SeconddataTwo['Expense'] = SeconddataTwo['Expense'].astype(str);

    #LineGraph
    Threedata = exeselect(
        "Select AVG(b.SPEND) as Expense,a.INCOME_RANGE as Income  from households as a inner join transactions as b   on a.HSHD_NUM=b.HSHD_NUM  group by a.INCOME_RANGE;");
    Threedata['Expense'] = Threedata['Expense'].astype(str);
    Threedata['Income'] = Threedata['Income'].astype(str);

    #Fourthdata = exeselect(
     #   "Select sum(SPEND) as spend,YEAR_NUM as year from transactions as a group by a.YEAR_NUM;");
    #Fourthdata['spend'] = Fourthdata['spend'].astype(str);
    #Fourthdata['year'] = Fourthdata['year'].astype(str);

    Fourthdata = exeselect(
            "Select AVG(b.SPEND) as Expense,a.MARITAL as marital_Status, c.COMMODITY as Commodity from households as a inner join transactions as b inner join products as c  on b.PRODUCT_NUM=c.PRODUCT_NUM and a.HSHD_NUM=b.HSHD_NUM  and upper(a.MARITAL)='MARRIED'  group by a.MARITAL,c.COMMODITY;");
    Fourthdata['Expense'] = Fourthdata['Expense'].astype(str);
    Fourthdata['Commodity'] = Fourthdata['Commodity'].astype(str);

    FourthdataTwo = exeselect(
            "Select AVG(b.SPEND) as Expense,a.MARITAL as marital_Status, c.COMMODITY as Commodity  from households as a inner join transactions as b inner join products as c  on b.PRODUCT_NUM=c.PRODUCT_NUM and a.HSHD_NUM=b.HSHD_NUM and upper(a.MARITAL)='SINGLE'  group by a.MARITAL,c.COMMODITY;");
    FourthdataTwo['Expense'] = FourthdataTwo['Expense'].astype(str);

    print("values-->",data['Expense'].values.tolist())
    return render_template("viewDashboard.html",title='Expense(Avg) vs Marital Status ', max=17000,titletwo="Expense(Avg) vs Age_Group vs Organic Products",
                           labels=data['marital_Status'].values.tolist(), values=data['Expense'].values.tolist(),
                           labelstwo=Seconddata['Age_Group'].values.tolist(),
                           valuestwo=Seconddata['Expense'].values.tolist(),
                           valuestwotwo=SeconddataTwo['Expense'].values.tolist(),
                           titlethree="Expense(Avg) vs Income",
                           labelsthree=Threedata['Income'].values.tolist(),
                           valuesthree=Threedata['Expense'].values.tolist(),
                           titlefour="Expense(Avg) vs Commodity vs Marital Status",
                           labelsfour=Fourthdata['Commodity'].values.tolist(),
                           valuesfour=Fourthdata['Expense'].values.tolist(),
                           valuestwofour=FourthdataTwo['Expense'].values.tolist());

if __name__ == '__main__':
  app.run();