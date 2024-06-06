import sqlparse
import re


def format_sql_query(sql_query):
    
    sql_query = sql_query.replace('@prod.kccd.edu', '')
    
    sql_query = sql_query.replace('&gt;', '>').replace('&lt;', '<')
    
    sql_query = sql_query.replace('NVL', 'COALESCE')
    
    schemas = ['SATURN', 'saturn', 'ODSMGR', 'ODSMGR_CUST']
    for schema in schemas:
        sql_query = re.sub(r'\b' + schema + r'\b', 'bankreportschema', sql_query)
    
    formatted_query = sqlparse.format(sql_query, reindent=True, keyword_case='upper')
    return formatted_query



    