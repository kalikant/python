import numpy as np
import pandas as pd
import os
import ConfigParser

config = ConfigParser.RawConfigParser()
config.read('Documents\\automation\\config.ini')

#print config.get('Path', 'output.file.path')

df_config = pd.read_excel(config.get('Path', 'input.file.path')+'table_details.xlsx')

file_list = [config.get('Path', 'output.file.path')+"src_ddl.sql",
config.get('Path', 'output.file.path')+"ddl.sql",
config.get('Path', 'output.file.path')+"src_vs_join_query.sql",
config.get('Path', 'output.file.path')+"vs_src_join_query.sql",
config.get('Path', 'output.file.path')+"src_load_script.sql",
config.get('Path', 'output.file.path')+"load_script.sql",
config.get('Path', 'output.file.path')+"src_union_query.sql"]



for file_name in file_list:
    try:
        os.remove(file_name)
    except OSError:
        pass

database=config.get('Hive', 'database')
hdfs_base_path=config.get('Hive', 'hdfs.base.path')
print('start running')

def generateSRCDDL(data):
    src_ddl=open(config.get('Path', 'output.file.path')+"src_ddl.sql","a+")
    dot_index=data[0].find('.')
    table_name=data[0]
    table_name=table_name[0:dot_index]
    table_prefix='SRC_'
    table_name =table_prefix.lower()+table_name.lower()
    col_name=data[2].replace(',',' string, ')
    
    drop_query='DROP TABLE '+database+ '.' +table_name+';\n'
    create_query='CREATE EXTERNAL TABLE '+ database + '.' + table_name+' ( \n'+col_name+' string \n) '
    tbl_properties='''ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION \n''' +'\''+hdfs_base_path+table_name+'\''+ '\n TBLPROPERTIES (\'skip.header.line.count\'=\'1\');\n\n\n'
    final_query=create_query+tbl_properties
    src_ddl.write(drop_query)
    src_ddl.write(final_query)
    src_ddl.close()

def generateDDL(data):
    ddl=open(config.get('Path', 'output.file.path')+"ddl.sql","a+")
    
    dot_index=data[4].find('.')
    table_name=data[4]
    table_name=table_name[0:dot_index]
    table_prefix='data_'
    table_name =table_prefix.lower()+table_name.lower()
    col_name=data[5].replace(',',' string, ')
    
    drop_query='DROP TABLE '+database+ '.' +table_name+';\n'
    create_query='CREATE EXTERNAL TABLE '+ database + '.' + table_name+' ( \n'+col_name+' string \n) '
    tbl_properties='''ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '|'
    STORED AS INPUTFORMAT
    'org.apache.hadoop.mapred.TextInputFormat'
    OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    LOCATION \n''' +'\''+hdfs_base_path+table_name+'\''+ '\n TBLPROPERTIES (\'skip.header.line.count\'=\'1\');\n\n\n'
    final_query=create_query+tbl_properties
    ddl.write(drop_query)
    ddl.write(final_query)
    ddl.close()
    

def generateInnerJoinQuerySRCvTGT(data):
    src_vs_join_query_file=open(config.get('Path', 'output.file.path')+"src_vs_join_query.sql","a+")
    
    src_dot_index=data[0].find('.')
    src_table_name=data[0]
    src_table_name=src_table_name[0:src_dot_index]
    src_table_prefix='SRC_'
    src_table_name =src_table_prefix.lower()+src_table_name.lower()
        
    dot_index=data[4].find('.')
    table_name=data[4]
    table_name=table_name[0:dot_index]
    table_prefix='TGT'
    table_name =table_prefix.lower()+table_name.lower()
    
    prm_key=data[1].split(',')
    prm_key_val=''
    for key in prm_key:
        prm_key_val+='b.'+key+' IS NULL and '
    
    prm_key_val=prm_key_val[:len(prm_key_val)-5]
    #print prm_key_val
    col_name=data[2].split(',')
    ignore_col=str(data[3])
    ignore_col=ignore_col.split(',')
    col_list=''
    for col in col_name:
        for cl in ignore_col:
            if(cl.strip().lower() == col.strip().lower() ):
                col_list+= '-- NVL(TRIM(LOWER(a.' +col+ ')),\'\') = NVL(TRIM(LOWER(b.' +col+ ')),\'\') AND \n'
            else:
                col_list+= 'NVL(TRIM(LOWER(a.' +col+ ')),\'\') = NVL(TRIM(LOWER(b.' +col+ ')),\'\') AND \n' 
    col_list=col_list[:len(col_list)-5]
        
    src_vs_join_qyery='''SELECT \'<src_table_name>\', * FROM <database>.<src_table_name> a 
    LEFT OUTER JOIN <database>.<table_name> b ON 
    ( <col_list> ) 
    WHERE <prim_key_list> limit 5;\n\n\n'''
    
    src_vs_join_query=src_vs_join_qyery.replace('<src_table_name>',src_table_name).replace('<table_name>',table_name).replace('<database>',database).replace('<col_list>',col_list).replace('<prim_key_list>',prm_key_val)
    src_vs_join_query_file.write(src_vs_join_query)
    #print src_vs_join_qyery
    src_vs_join_query_file.close()
    
def generateInnerJoinQueryTGTvsSRC(data):
    vs_src_join_query_file=open(config.get('Path', 'output.file.path')+"vs_src_join_query.sql","a+")
    
    src_dot_index=data[0].find('.')
    src_table_name=data[0]
    src_table_name=src_table_name[0:src_dot_index]
    src_table_prefix='SRC_'
    src_table_name =src_table_prefix.lower()+src_table_name.lower()
    
    dot_index=data[4].find('.')
    table_name=data[4]
    table_name=table_name[0:dot_index]
    table_prefix='TGT_'
    table_name =table_prefix.lower()+table_name.lower()
    
    prm_key=data[1].split(',')
    prm_key_val=''
    for key in prm_key:
        prm_key_val+='b.'+key+' IS NULL and '
    
    prm_key_val=prm_key_val[:len(prm_key_val)-5]
    
    col_name=data[2].split(',')
    col_list=''
    for col in col_name:
        col_list+= 'NVL(TRIM(LOWER(a.' +col+ ')),\'\') = NVL(TRIM(LOWER(b.' +col+ ')),\'\') AND \n' 
    
    col_list=col_list[:len(col_list)-5]
    #print col_list
    
    vs_src_join_qyery='''SELECT \'<table_name>\', * FROM <database>.<table_name> a 
    LEFT OUTER JOIN <database>.<src_table_name> b ON 
    ( <col_list> ) 
    WHERE <prim_key_list> limit 5;\n\n\n'''
    
    vs_src_join_query=vs_src_join_qyery.replace('<src_table_name>',src_table_name).replace('<table_name>',table_name).replace('<database>',database).replace('<col_list>',col_list).replace('<prim_key_list>',prm_key_val)
    vs_src_join_query_file.write(vs_src_join_query)
    vs_src_join_query_file.close()
    #print vs_src_join_qyery

def generateUNIONQuery(data):
    src_union_query_file=open(config.get('Path', 'output.file.path')+"src_union_query.sql","a+")
    
    src_dot_index=data[0].find('.')
    src_table_name=data[0]
    src_table_name=src_table_name[0:src_dot_index]
    src_table_prefix='SRC_'
    src_table_name =src_table_prefix.lower()+src_table_name.lower()
    
    dot_index=data[4].find('.')
    table_name=data[4]
    table_name=table_name[0:dot_index]
    table_prefix='TGT_'
    table_name =table_prefix.lower()+table_name.lower()
    
    prm_key=data[1].split(',')
    prm_key_val=''
    for key in prm_key:
        prm_key_val+='TRIM('+key+') IN() and '
    
    prm_key_val=prm_key_val[:len(prm_key_val)-5]
   
    col_name=data[2].split(',')
    col_list=''
    for col in col_name:
        col_list+= 'NVL(TRIM(LOWER(' +col+ ')),\'\') as ' +col +',\n' 
    col_list=col_list[:len(col_list)-2]
    
    union_query='''SELECT \'SRC\', <col_list> 
    FROM <database>.<src_table_name> 
    WHERE <prm_key_val>
    UNION ALL
SELECT \'DATA\', <col_list> 
    FROM <database>.<table_name>
    WHERE <prm_key_val> ;\n\n\n'''
     
    union_query=union_query.replace('<col_list>',col_list).replace('<database>',database).replace('<src_table_name>',src_table_name).replace('<table_name>',table_name).replace('<prm_key_val>',prm_key_val)
    
    src_union_query_file.write(union_query)
    src_union_query_file.close()
    
def generateHiveLoadScript(data):
    src_unix_base_path=config.get('Path', 'src.unix.base.path')
    unix_base_path=config.get('Path', 'data.unix.base.path')
    
    src_dot_index=data[0].find('.')
    src_file_name=data[0]
    src_table_name=data[0]
    src_table_name=src_table_name[0:src_dot_index]
    src_table_prefix='SRC_'
    src_table_name =src_table_prefix.lower()+src_table_name.lower()
        
    dot_index=data[4].find('.')
    file_name=data[4]
    table_name=data[4]
    table_name=table_name[0:dot_index]
    table_prefix='TGT_'
    table_name =table_prefix.lower()+table_name.lower()
    
       
    src_load_script='LOAD DATA LOCAL INPATH \'<src_unix_base_path>/<src_file_name>\' OVERWRITE INTO TABLE <database>.<src_table_name> ;\n'
    load_script='LOAD DATA LOCAL INPATH \'<unix_base_path>/<file_name>\' OVERWRITE INTO TABLE <database>.<table_name> ;\n'
    
    src_load_script_file=open(config.get('Path', 'output.file.path')+"src_load_script.sql","a+")
    load_script_file=open(config.get('Path', 'output.file.path')+"load_script.sql","a+")

    src_load_script=src_load_script.replace('<src_unix_base_path>',src_unix_base_path).replace('<src_file_name>',src_file_name).replace('<database>',database).replace('<src_table_name>',src_table_name)
    load_script=load_script.replace('<unix_base_path>',unix_base_path).replace('<file_name>',file_name).replace('<database>',database).replace('<table_name>',table_name)
    
    src_load_script_file.write(src_load_script)
    load_script_file.write(load_script)
    src_load_script_file.close()
    load_script_file.close()
    
    
    
    
    
for row in df_config.iterrows():
    eachRow=row[1].tolist()
    print ("completed")
    generateSRCDDL(eachRow)
    print ("completed")
    generateTGTDDL(eachRow)
    print ("completed")
    generateInnerJoinQuerySRCvsTGT(eachRow)
    print ("completed")
    generateInnerJoinQueryTGTvsSRC(eachRow)
    generateHiveLoadScript(eachRow)
    generateUNIONQuery(eachRow)
print ("completed")
