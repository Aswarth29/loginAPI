
from asyncio.log import logger
from cgitb import text
import email
from multiprocessing import connection
from os import abort,path
import re
from traceback import TracebackException
from venv import create
from flask import Flask, jsonify, request,abort
from requests import Response
from sqlalchemy  import create_engine, outparam
from pytest import Calcmain
import pandas as pd
from logfile import log_insert
from hdbcli import dbapi
import json
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import logging
import sys
from flask import Flask,render_template,request,redirect
from flask_login import login_required, current_user, login_user, logout_user, user_logged_in
from models import UserModel,db,login

#from usertable import *
    

app = Flask(__name__)
app.secret_key = 'Aswartha'

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///users.db'
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db.init_app(app)
login.init_app(app)
login.login_view = 'login'
 
@app.before_first_request
def create_table():
    db.create_all()
     
@app.route('/blog')
@login_required
def blog():
    return render_template('blog.html')

@app.route('/login', methods = ['POST', 'GET'])
def login():
    if current_user.is_authenticated:
        return redirect('/blog')
     
    if request.method == 'POST':
        
        username = request.form['username']
        user_role = request.form['user_role']
        email = request.form['email']
        user = UserModel.query.filter_by(email = email).first()
        user1= UserModel.query.filter_by(username = username).first()
        user2= UserModel.query.filter_by(user_role = user_role).first()
        if user is not None and user.check_password(request.form['password']):
            login_user(user)
            output = [{"email": email,"user": username, "role": user_role}]
            return jsonify(output)
        elif user1 is not None and user1.check_password(request.form['password']):
            login_user(user1)
            output = [{"email": email,"user": id, "role": user_role}]
            return jsonify(output)
        elif user2 is not None and user2.check_password(request.form['password']):
            login_user(user2)
            output = [{"email": email,"user": id, "role": user_role}]
            return jsonify(output)
        else:
            return "Incorrect Username"
               
    return render_template('login.html')
 
@app.route('/register', methods=['POST', 'GET'])
def register():
    if current_user.is_authenticated:
        return redirect('/blog')
     
    if request.method == 'POST':
        email = request.form['email']
        username = request.form['username']
        password = request.form['password']
        user_role = request.form['user_role']
 
        if UserModel.query.filter_by(email=email).first():
            return ('User already Present')
             
        user = UserModel(email=email, username=username, user_role=user_role)
        user.set_password(password)
        db.session.add(user)
        db.session.commit()
        return "User added successful"
    return render_template('register.html')
 

Data = pd.read_csv("https://configurationfilestorage.blob.core.windows.net/credentialscontainer/credentials.csv")
Sap_data = Data.loc[Data['Database'] == 'SAP HANA']
Snowflake_Data = Data.loc[Data['Database'] == 'Snowflake']
Sap_Data = Sap_data['Response'].to_list()
Sap_field = Sap_data['Category'].to_list()
Snowflake_data = Snowflake_Data['Response'].to_list()
Snowflake_field = Snowflake_Data['Category'].to_list()
res_sap = {}
for key in Sap_field:
    for value in Sap_Data:
        res_sap[key] = value
        Sap_Data.remove(value)
        break
res_snow = {}
for Skey in Snowflake_field:
    for Svalue in Snowflake_data:
        res_snow[Skey] = Svalue
        Snowflake_data.remove(Svalue)
        break
#Sap credentials
Address = res_sap['address']
Port = res_sap['port']
SAP_User = res_sap['user']
SAP_Password = res_sap['password']
CurrentSchema = res_sap['currentSchema']
#snowflake credentials
warehouse = res_snow['warehouse']
Account = res_snow['account']
SNOW_User = res_snow['user']
SNOW_Password = res_snow['password']
Database = res_snow['database']
Schema = res_snow['schema']
#sap Hana connection
# conn = dbapi.connect(address=Address, port=Port, user=SAP_User,
#                     password=SAP_Password, currentSchema=CurrentSchema)
# cursor = conn.cursor()
# #snowflake connnection
# cnn = snowflake.connector.connect(
#     user=SNOW_User,
#     password=SNOW_Password,
#     account=Account,
#     warehouse=warehouse,
#     database=Database,
#     schema=Schema)
# cur = cnn.cursor()

@app.route('/Table', methods=['GET'])
def Table():
    tablenamequery = f"Select TABLE_NAME from TABLES WHERE SCHEMA_NAME='{CurrentSchema}';"
    try:
        cursor.execute(tablenamequery)
        dbs = pd.DataFrame(cursor.fetchall())
        column_headers = [i[0] for i in cursor.description]
        dbs.columns = column_headers 
        Tableop =dbs['TABLE_NAME'].tolist()
        output_val = jsonify(Tableop)
    except Exception as e:
        exception_type, exception_object, exception_traceback = sys.exc_info()
        filename = exception_traceback.tb_frame.f_code.co_filename
        line_number = exception_traceback.tb_lineno, exception_object
        log_message = path.basename(filename) + str(line_number) 
        formatLOG = logging.Formatter( 
            '%(asctime)s, %(levelname)s , %(message)s', "%m/%d/%Y %H:%M:%S" )
        log_insert(logging.ERROR,formatLOG,log_message)
        output_val = ("Error: We coun't retrieve any data please check the SAP HANA connection") 
    return output_val
    # jsonify(Tableop)  
    
@app.route('/view', methods=['GET'])
def view():
    query = f"select * from sys.views where schema_name = '{CurrentSchema}' and view_type = 'ROW';"
    try:
        cursor.execute(query)
        dftviewdata = pd.DataFrame(cursor.fetchall())
        column_headers = [i[0] for i in cursor.description]
        dftviewdata.columns = column_headers   
        Namedata = (dftviewdata['VIEW_NAME'])
        View_Name = Namedata.tolist()
        View_select_query = (dftviewdata[['VIEW_NAME','DEFINITION']])
        op = dftviewdata['VIEW_NAME'].tolist()
        output_val = jsonify(op)
    except Exception as e:
        exception_type, exception_object, exception_traceback = sys.exc_info()
        filename = exception_traceback.tb_frame.f_code.co_filename
        line_number = exception_traceback.tb_lineno, exception_object
        log_message = path.basename(filename) + str(line_number) 
        formatLOG = logging.Formatter( 
            '%(asctime)s, %(levelname)s , %(message)s', "%m/%d/%Y %H:%M:%S" )
        log_insert(logging.ERROR,formatLOG,log_message)
        output_val = ("Error: We coun't retrieve any data please check the SAP HANA connection") 
    return output_val 

    
@app.route('/TableSubmit', methods=['POST'])
def Data_Processing():
   
    tn = json.loads(request.data)

    temp = (len(tn))
    z = 0
    Table_Name = []
    Status = []
    Count =[]
    while z < temp:
        # SAP Query ---------------------------
        Queryinfo = f"SELECT COLUMN_NAME,DATA_TYPE_NAME,IS_NULLABLE,GENERATION_TYPE FROM SYS.TABLE_COLUMNS WHERE SCHEMA_NAME= '{CurrentSchema}' AND TABLE_NAME= '{tn[z]}' ORDER BY POSITION;"
        QueryPrimary = f"SELECT COLUMN_NAME,IS_PRIMARY_KEY,IS_UNIQUE_KEY FROM SYS.CONSTRAINTS WHERE SCHEMA_NAME = '{CurrentSchema}' and TABLE_NAME= '{tn[z]}';"
        export_data = f'SELECT * FROM "{CurrentSchema}"."{tn[z]}";'

        try:
            
            cursor.execute(Queryinfo)
            dfInfo = pd.DataFrame(cursor.fetchall(),
                                columns=["COLUMN_NAME", "DATA_TYPE_NAME", "IS_NULLABLE", "GENERATION_TYPE"])
            cursor.execute(QueryPrimary)
            dfPrimary = pd.DataFrame(cursor.fetchall(), columns=["COLUMN_NAME", "IS_PRIMARY_KEY", "IS_UNIQUE_KEY"])
            cursor.execute(export_data)
            df_exported_data = pd.DataFrame(cursor.fetchall())
            column_headers = [i[0] for i in cursor.description]  # gets the column name.
            df_exported_data.columns = column_headers  # adds header to the data frame with the data
            Column_Name = list(dfInfo.iloc[0:, 0])
            Column_Data_Type = list(dfInfo.iloc[0:, 1])
            for k in range(len(Column_Data_Type)):
                Column_Data_Type[k] = Column_Data_Type[k].upper()
            try:
                Primary_Key = list(dfPrimary.iloc[0, :])
            except:
                Primary_Key = ''
            for i in range(len(Column_Data_Type)):
                if Column_Data_Type[i] == 'NVARCHAR':
                    Column_Data_Type[i] = 'VARCHAR'
                if Column_Data_Type[i] == 'INTEGER':
                    Column_Data_Type[i] = 'NUMERIC'
                if Column_Data_Type[i] == 'BLOB':
                    Column_Data_Type[i] = 'BINARY'
                if Column_Data_Type[i] == 'CLOB':
                    Column_Data_Type[i] = 'VARCHAR'
                if Column_Data_Type[i] == 'ST_GEOMETRY':
                    Column_Data_Type[i] = 'VARCHAR'
                if Column_Data_Type[i] == 'ST_POINT':
                    Column_Data_Type = 'VARCHAR'
            length = len(Column_Name)
            # print(Column_Name)
            data = ""
            # print(length)
            for j in range(length):
                if Column_Name[j] in Primary_Key and j < length - 1:
                    data += f'{Column_Name[j]} {Column_Data_Type[j]}  Primary Key,'
                elif Column_Name[j] in Primary_Key and j == length:
                    data += f'{Column_Name[j]} {Column_Data_Type[j]}  Primary Key'
                elif Column_Name[j] not in Primary_Key and j < length - 1:
                    data += f'{Column_Name[j]} {Column_Data_Type[j]},'
                else:
                    data += f'{Column_Name[j]} {Column_Data_Type[j]} '
        except Exception as e:
            abort(500,e)
        query = f'CREATE TABLE IF NOT EXISTS {tn[z]} ({data});'
        try:
            cnn.execute_string(query)
            success, nchunks, nrows, _ = write_pandas(cnn, df_exported_data, tn[z], quote_identifiers=False)
            Table_Name.append(tn[z])
            if success == True:
                Status.append('True')
            else:
                Status.append('False')
            Count.append(str(nrows))
            z = z + 1
        except Exception as e:
            abort(500,e)
    df = pd.DataFrame(list(zip(Table_Name,Count,Status)),columns=['TableName','Count','Status'])
    df = df.to_json(orient='records')
    return df



@app.route('/ViewSubmit', methods=['POST'])
def View_Processing():
    
    VN = json.loads(request.data)
    query = f"select VIEW_NAME,DEFINITION from sys.views where schema_name = '{CurrentSchema}' and view_type = 'ROW';"
    try:
        cursor.execute(query)
        dftviewdata = pd.DataFrame(cursor.fetchall())
        column_headers = [i[0] for i in cursor.description]
        dftviewdata.columns = column_headers
        voutput = []

        for i in range(len(VN)):
            data = dftviewdata[dftviewdata['VIEW_NAME'] == VN[i]]  # Gets the view name and the definition by comparing whether the user inputed viewname is there in the dataframe
            VIEW_NAME = (data['VIEW_NAME'].tolist())
            definition = (data['DEFINITION'].tolist())
            VIEW_NAME = ''.join(VIEW_NAME)
            definition = ''.join(definition)
            View_query = f'CREATE OR REPLACE VIEW {VIEW_NAME} as {definition};'
            View_query = View_query.upper()

            cnn.execute_string(View_query)
            # View_Name += ''.join(VIEW_NAME)
            voutput.append(VIEW_NAME)
    except Exception as e:
        abort(500,e)
    return jsonify(voutput)


@app.route('/SnowflakeTable', methods=['GET'])
def SnowflakeTable():
    SFTquery = f"SELECT TABLE_NAME FROM {Database}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'PUBLIC' AND TABLE_TYPE ='BASE TABLE';"
    #print(connection)
    #print(SFTquery)
    try:
        cur.execute(SFTquery)
        df_snow_table = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])

        df = df_snow_table['TABLE_NAME'].tolist()
    except Exception as e:
        abort(500,e)
    return jsonify(df)



@app.route('/SnowflakeView', methods=['GET'])
def SnowflakeView():
    SFVquery = f"SELECT TABLE_NAME FROM {Database}.INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = 'PUBLIC';"
    #print(SFVquery)
    try:
        cur.execute(SFVquery)
        
        df_snow_view = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
        df_view = df_snow_view['TABLE_NAME'].tolist()
    except Exception as e:
        abort(500,e)
    return jsonify(df_view)
@app.route('/SnowflakeTableData', methods=['POST'])
def Snowdata():
    Table_data = json.loads(request.data)
    Table_Name = ''.join(Table_data)
    query = f"SELECT * FROM {Database}.{Schema}.{Table_Name}"
    try:
        cur.execute(query)
        output = []
        df_snowtable_data = pd.DataFrame.from_records(iter(cur), columns=[x[0] for x in cur.description])
        column_headers = [i[0] for i in cur.description]
        df_snowtable_data.columns = column_headers 
        df = df_snowtable_data.to_json(orient='records')
        OP = json.dumps(json.loads(df))
    except Exception as e:
        abort(500,e)
    #output = df.tolist()
    return OP
@app.route('/CalcView',methods = ['GET','POST'])
def CalcView():
    if request.method == 'GET':
        query = f"SELECT VIEW_NAME FROM SYS.VIEWS WHERE VIEW_TYPE = 'CALC' AND SCHEMA_NAME = '{CurrentSchema}'"
        try:
            cursor.execute(query)
            dftviewdata = pd.DataFrame(cursor.fetchall())
            column_headers = [i[0] for i in cursor.description]
            dftviewdata.columns = column_headers   
            df = dftviewdata.to_json(orient='records')
            output_val = jsonify(df)
        except Exception as e:
            exception_type, exception_object, exception_traceback = sys.exc_info()
        filename = exception_traceback.tb_frame.f_code.co_filename
        line_number = exception_traceback.tb_lineno, exception_object
        log_message = path.basename(filename) + str(line_number) 
        formatLOG = logging.Formatter( 
            '%(asctime)s, %(levelname)s , %(message)s', "%m/%d/%Y %H:%M:%S" )
        log_insert(logging.ERROR,formatLOG,log_message)
        output_val = ("Error: We coun't retrieve any data please check the SAP HANA connection") 
        return output_val
    
    if request.method == 'POST':
        CalcView = json.loads(request.data)
        CalcName = ''.join(CalcView)
        if CalcName == 'PERFORMANCE_SALARIES':
            Xml_file = 'calcview_ps.xml'
            Result = Calcmain(Xml_file)
        elif CalcName == 'SALARIES_ANONYMIZED':
            Xml_file = 'calcview_sa.xml'
            Result = Calcmain(Xml_file)
    try:
        cnn.execute_string(Result)
        Final_OP = 'Success'
    except:
        Final_OP = 'Error View Not created Please Contact Support'
    return jsonify(Final_OP)


@app.route('/logout')
def logout():
    logout_user()
    return redirect('/blogs')


if __name__ == '__main__':
    db.init_app(app)
app.run(debug=True,port=5000)
