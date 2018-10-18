import os
import ConfigParser

config = ConfigParser.RawConfigParser()
config.read('/document/config.ini')

# print config.get('Path', 'output.file.path')

df_config = pd.read_excel(config.get('Path', 'input.file.path') + 'table_details.xlsx')

file_list = [config.get('Path', 'output.file.path') + "src_ddl.sql",
             config.get('Path', 'output.file.path') + "tgt_ddl.sql",
             config.get('Path', 'output.file.path') + "src_vs_tgt_join_query.sql",
             config.get('Path', 'output.file.path') + "tgt_vs_src_join_query.sql",
             config.get('Path', 'output.file.path') + "src_load_script.sql",
             config.get('Path', 'output.file.path') + "tgt_load_script.sql",
             config.get('Path', 'output.file.path') + "src_edm_union_query.sql"]
             
# config.ini file format as below             
[DateFormat]
src.dt.frmt='%d-%b-%Y'
tgt.dt.frmt='%Y-%m-%d'
std.dt.frmt='%Y-%m-%d'
[RegexPattern]
numeric.pattern='((!?:[a-zA-Z]).)*$ | [-+]? (?: (?: \d* \. \d+ ) | (?: \d+ \.? ) )(?: [Ee] [+-]? \d+ ) ?'
date.pattern='^[0-9]{4}-(((0[13578]|(10|12))-(0[1-9]|[1-2][0-9]|3[0-1]))|(02-(0[1-9]|[1-2][0-9]))|((0[469]|11)-(0[1-9]|[1-2][0-9]|30))) | ^((31(?! (FEB|APR|JUN|SEP|NOV)))|((30|29)(?! FEB))|(29(?= FEB (((1[6-9]|[2-9]\d)(0[48]|[2468][048]|[13579][26])|((16|[2468][048]|[3579][26])00)))))|(0?[1-9])|1\d|2[0-8])-(JAN|FEB|MAR|MAY|APR|JUL|JUN|AUG|OCT|SEP|NOV|DEC)-((1[6-9]|[2-9]\d)\d{2})'
[Path]
tgt.unix.base.path=XXXX/source_data/
[Hive]
database=XXXXX
hdfs.base.path=hdfs://XXXXXX/apps/hive/warehouse/XXXXXX.db/
