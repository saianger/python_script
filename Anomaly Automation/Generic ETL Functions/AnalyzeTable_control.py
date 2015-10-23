__author__ = 'leo.li'
import psycopg2;
import AnomSys;

# This script accept a json config file, analyze the tables based on the table_list element
# version 1.1, created on 23/06/2015

# create an AnomSys instance
anom = AnomSys.AnomSys();

# variable initialization
dbname = anom.json_obj['db_config']['dbname']
host = anom.json_obj['db_config']['host']
port = anom.json_obj['db_config']['port']
user = anom.json_obj['db_config']['user']
password = anom.json_obj['db_config']['pwd']
schema = anom.json_obj['db_config']['schema']

# connection definition
def create_conn():
    try:
        conn=psycopg2.connect(dbname=dbname, host=host, port=port, user=user, password=password)
    except Exception as e:
        sys.exit('%s: Cannot connect to database' % str(e));
    return conn

# function to prepare sql list for execution
def create_sql_list():
    sql_list = []
    for f in anom.json_obj["table_list"]:
        sql_str = ""
        if anom.json_obj["table_list"][f]:
            column_list = [x for x in anom.json_obj["table_list"][f]]
            sql_str = "analyze %s.%s" % (schema,f) + '(' + ','.join(column_list) + ')'
        else:
            sql_str = "analyze %s.%s" % (schema,f)
        sql_list.append(sql_str)
    return sql_list


with create_conn() as conn:
    with conn.cursor() as cursor:
        for q in create_sql_list():
            try:
                cursor.execute(q)
                print "analyze successful: %s" % q
            except psycopg2.Error as e:
                anom.wrt_log("analyze failed on statement: %s" % q + e.pgerror)
                exit(1)
