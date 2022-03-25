import pandas as pd
import csv
import os
import json
import string
import mysql.connector
from mysql.connector import Error

#choose a transformation mode for def jason2csv
def modeselect():
    print('+ mode selection + \nwith input int you can choose transformmode: \n1 for single file transformation\n2 for transforming all .json file into .csv file\n3 for insert data from .json to database')
    i = input('please input 1 or 2 or 3')
    return int(i)

#dive into .json file to return information in recurssive way
def parsingGenerator(data, pre=[]):
    if isinstance(data, dict):#if it is json object enclosed by {}
        for k, v in data.items():#k for keys, v for values
            if isinstance(v, (dict,list)):# start recurssive procedure
                for subData in parsingGenerator(v, pre+[k]):
                    yield subData
            else:
                yield (pre+[k], v)
    elif isinstance(data, list):#if it is a list enclosed by []
        for item in data:
            if isinstance(item, (dict,list)):
                for subData in parsingGenerator(item, pre):
                    yield subData
    else:
      yield (pre, item)

#function to check if keywords included
def is_in(full_str, sub_str):
    return full_str.count(sub_str) > 0

#function to check if the .csv file with the same name already
#exists in the same folder where .json file is in
def check_csv(filepath,filenames):
    newpath = filepath.replace(".json", ".csv")
    check = 0;
    for i in filenames:
        if i in newpath:
            print(newpath,' exist')
            check = 1;
    return check == 0        

#transform .json file to .csv file with the same name
#and store it to the same folder
def jsontransform(filepath):
    with open(filepath) as js_file:
        js=json.load(js_file)
                #print(js)
    df = []
    for i in parsingGenerator(js):
     df.append(i)            
    final_df = pd.DataFrame(df)
    final_df.index += 1
    k = 0
#change the format from "[v1,v2,v3]" to ^rec_v1_v2_v3^
#for importing to database(for ex "column terminated by ','" command in sql)
    for i in final_df.iloc[:, 0]:
      st = 'rec'
      for j in i:
        st = st+ '_' + j
      final_df.iloc[k, 0] = '^'+st+'^'  
      k = k+1
#set Nonevalue to NULL        
    k = 0
    for i in final_df.iloc[:, 1]:
      if type(i) != str:       
       final_df.iloc[k, 1] = "NULL"
      k = k+1
    newpath = filepath.replace(".json", ".csv")
    final_df.to_csv(newpath)
    print(newpath)

#extract data .json file to Dtaframe 
#and insert it to the table in Mysql database "interview" 
# with the same table name as .json file
def json2sql(filepath):
    
    with open(filepath) as js_file:
        js=json.load(js_file)
                #print(js)
    df = []
#delete the path and keep only the file name
    filename = os.path.basename(filepath)
    tab_name = filename.replace('.json','')
#delete "-" in filename for creating the table with this name
    tab_name = tab_name.replace('-','')
    print(tab_name)

    for i in parsingGenerator(js):     
     df.append(i)            
    final_df = pd.DataFrame(df)
    final_df.index += 1
    k = 0
    for i in final_df.iloc[:, 0]:
      st = 'rec'
      for j in i:
        st = st+ '_' + j
      final_df.iloc[k, 0] = st  
      
      k = k+1
        
    k = 0
    for i in final_df.iloc[:, 1]:
      if type(i) != str:       
       final_df.iloc[k, 1] = "NULL"
      k = k+1
#connect the Mysql database with information below        
    try:
      connection = mysql.connector.connect(host='localhost',
                                         user='root',
                                         password='ba',
                                         database = 'interview')
      if connection.is_connected():
        db_Info = connection.get_server_info()
        print("Connected to MySQL Server version ", db_Info)
        cursor = connection.cursor()
        cursor.execute("select database();")
        record = cursor.fetchone()
        print("You're connected to database: ", record)  
        print(tab_name)
#create table
        cursor.execute("CREATE TABLE IF NOT EXISTS %s ( ID int NOT null PRIMARY KEY AUTO_INCREMENT,rec_names varchar(256),  rec_values  varchar(256) )"%(tab_name))
        print("create table successfully:", tab_name) 
#insert to the table        
        for i in range(0,len(final_df)):    
         cursor.execute("insert into %s (ID, rec_names, rec_values) values('%s','%s','%s')"%(tab_name,i+1,final_df.iloc[i, 0],final_df.iloc[i, 1]))
        print("insert successfully")
       
        connection.commit()
        print("save changes")

#disconnect the database
    except Error as e:
     print("Error while connecting to MySQL", e)
    finally:
     if connection.is_connected():
        cursor.close()
        connection.close()
        print("MySQL connection is closed")   
        
#function with 3 modes
def json2csv(case):
   num = 0
   if case == 1:#select the file path need to be transformed
      print('Please input the path of a .json file')
      filepath =  input('please input file path：')
      if  os.path.isfile(filepath) & is_in(filepath,".json"): 
          num = num+ 1 
          jsontransform(filepath)
          print('The number of JSON files have been transformed:',num)
      else:
          print('not a JSON file')
        
   elif case == 2:#transform all the newly uploaded .json files in 
#the current folder to .csv file and save them to the same folder
       #get path of current file
      filedir = os.getcwd()
    #get list of files
      filenames=os.listdir(filedir)
      for file in filenames:
        filepath=filedir+'/'+file
        if  os.path.isfile(filepath):
          if is_in(filepath,".json"):
            if check_csv(filepath,filenames):
             num = num+ 1
             print(filepath)  
             jsontransform(filepath)
      print('The number of JSON files have been transformed:',num)
    
   elif case == 3:#transform all the newly uploaded .json files in 
#the current folder to mysql database into the tables with the same names
       #get path of current file
      filedir = os.getcwd()
    #get list of files
      filenames=os.listdir(filedir)
      for file in filenames:
        filepath=filedir+'/'+file
        if  os.path.isfile(filepath):
          if is_in(filepath,".json"):
            if check_csv(filepath,filenames):
             num = num+ 1
             print(filepath)  
             json2sql(filepath)
      print('The number of JSON files have been import to database:',num)


json2csv(2)
#json2csv(case = modeselect())

