import os
import mysql.connector
from mysql.connector import Error
import psycopg2
import pandas as pd
import gc
# from config import config
from sqlalchemy import create_engine
# db_post='public'
outpath='/home/gowrish/Desktop/sample_out.csv'
table_list=[
# 'fare_analysis.simi_data',
# 'fare_analysis.landing_page',
# 'fare_analysis.landing_page_check',
# 'fare_analysis.landing_page_check_dna',
# 'fare_analysis.landing_page_check_dna1',
# 'fare_analysis.landing_page_check_dna2',
# 'fare_analysis.taluka',
# 'fare_analysis.plant',
# 'fare_analysis.plantlatlong',
# 'fare_analysis.plant_latlong',
# 'fare_analysis.truck',
#  'fare_analysis.truck_type',
# 'fare_analysis.zonewise',
# 'fare_analysis.destination_latlong',
# 'fare_analysis.global_ptpk',
# 'fare_analysis.gps_aggry'
    
# 'fare_analysis.base_freightdata',
# 'fare_analysis.basefreightdata_newdestination',
# 'fare_analysis.diesel_data',
# 'fare_analysis.dispatchdata_newdestination',
# 'fare_analysis.entitlement_data',
# 'fare_analysis.hierarchydata',
# 'fare_analysis.input_for_similarity',
# 'fare_analysis.input_for_taluka_and_outlier',
# 'fare_analysis.landing_page_check_withref',
# 'fare_analysis.latlong',
    
    
# 'fare_analysis.maps_info_all'
    
    
# 'fare_analysis.northzone_tableau',
# 'fare_analysis.northzone_tableau1',
# 'fare_analysis.output_outlieranalysis',
# 'fare_analysis.output_talukaanalysis',
# 'fare_analysis.previous_base_freight',
# 'fare_analysis.previous_dispatchdata',
# 'fare_analysis.simiinput_newdestination'
]


# table_list=['gowrish.age40',]

dbname_trgt = ''
host_trgt = ''
port_trgt = '5432'
user_trgt = ''
pwd_trgt = ''





def connect_mysql():
    try:
        conn_src = mysql.connector.connect(host='',
                                         database='',
                                         user='',
                                         password='')

        print("You're connected to database")
        return conn_src
    
    except Error as e:
        print("Error while connecting to MySQL", e)

    

def extract_from_source(extr_qury,tab_name):
    print("read data into pandas dataframe")
    conn_src=connect_mysql()
    df=pd.read_sql_query(extr_qury,conn_src)
    df.to_csv(outpath,index=False)
#     df=pd.read_csv(outpath,index_col=None)
#     rec_cunt=df.count()
#     print("extracted "+rec_cunt+" from "+tab_name)
    return outpath,df

def pg_load_table(outpath, tab_name, dbname_trgt, host_trgt, port_trgt, user_trgt, pwd_trgt):
    '''
    This function upload csv to a target table
    '''
    try:
        conn = psycopg2.connect(dbname=dbname_trgt, host=host_trgt, port=port_trgt,\
        user=user_trgt, password=pwd_trgt)
        print("Connecting to Database")
        cur = conn.cursor()
        f = open(outpath, "r")
        # Truncate the table first
        cur.execute("Truncate {} Cascade;".format(tab_name))
        print("Truncated {}".format(tab_name))
        # Load table from the file with header
        cur.copy_expert("copy {} from STDIN CSV HEADER QUOTE '\"'".format(tab_name), f)
        cur.execute("commit;")
        print("Loaded data into {}".format(tab_name))
        conn.close()
        print("DB connection closed.")

    except Exception as e:
        print("Error: {}".format(str(e)))
        sys.exit(1)
    

if __name__ == '__main__':
    for tab_name in table_list:
        print("generating extract query for "+tab_name+" table")
        extr_qury= "select * from "+tab_name+" ;"
        print(extr_qury)
        df=extract_from_source(extr_qury,tab_name)
        print("loading data into target table")
        tab_name="dna_ultratech_fare_dev."+tab_name[14:]+"_raw"
        pg_load_table(outpath, tab_name, dbname_trgt, host_trgt, port_trgt, user_trgt, pwd_trgt)
        del df
        gc.collect()
        os.remove(outpath)

