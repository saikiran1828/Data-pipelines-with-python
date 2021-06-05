# -*- coding: utf-8 -*-
"""
Created on Thu Mar 11 13:29:25 2021

@author: sai kiran Reddy
"""
import sys
import os
import mysql.connector
from mysql.connector import Error
import psycopg2
import pandas as pd
import gc
import MySQLdb
from sqlalchemy import create_engine
db = MySQLdb.connect(host='localhost',database= 'idiot',user='root', password='1234')
print("mysql coneccted")
encoding = 'latin1'
dbx = db.cursor()
dbname_trgt = 'idiot'
host_trgt = 'localhost'
port_trgt = '5432'
user_trgt = 'postgres'
pwd_trgt = 'saikiran'
DB = psycopg2.connect(dbname=dbname_trgt, host=host_trgt, port=port_trgt,user=user_trgt, password=pwd_trgt)
print('postgresql conn')
Dc = DB.cursor()
Dc.execute("set client_encoding = "+encoding)
mysql='''show tables from idiot'''
dbx.execute(mysql); ts = dbx.fetchall(); tables=[]
print(ts)
for table in ts: tables.append(table[0])
print(tables)
for table in tables:
        print(table)
        mysql='''describe idiot.%s'''%(table)
        print(mysql)
        dbx.execute(mysql); rows=dbx.fetchall()
        print(rows)
        #psql='drop table %s'%(table)
        #Dc.execute(psql); DB.commit()
        
        psql='create table  %s ('%(table)
        for row in rows:
            name=row[0]; type=row[1]
            if  'int'    in type : type = 'int8'
            if  'blob'   in type : type = 'byte'
            if 'datetime'in type : type = 'timestamptz'
            psql+='%s %s,'%(name,type)
        psql=psql.strip(',')+')'
        print(psql)
        print("%s table is created"%(table))
        try:Dc.execute(psql);DB.commit()
        except: pass 
