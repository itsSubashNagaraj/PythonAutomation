import config_handler
import extraction
import tranformation

sql_query = """ SELECT DISTINCT NVL(SIRNIST_COLL_CODE,CollegeCode) college,
                    ID, pap.PERSON_UID,
                    pap.LAST_NAME ,
                    pap.FIRST_NAME,
                    pap.MIDDLE_NAME,
                    pap.FULL_NAME_LFMI,
--                NVL(SIRNIST_TERM_CODE ,SIRASGN_TERM_CODE) TERM,
--                SIRNIST_PIDM INST_PIDM, 
--                SIRDPCL_COLL_CODE,
--              SUBSTR(F_GET_DESC_FNC('STVCOLL',SIRDPCL_COLL_CODE,30),1,23),
                SUM(NVL(banked,0) + NVL(LOAD_BANKED ,0)) "BANKED_LOAD"  , SUM(NVL( used,0)) "USED_LOAD" , 
                SUM(NVL(banked,0) + NVL(LOAD_BANKED ,0)) - SUM(NVL( used,0)) "Balance"   --,
                HOME_IND, PERC, SIRDPCL_ROWID
      FROM 
         (select distinct pd.ID, pd.PERSON_UID, Pd.LAST_NAME, pd.FIRST_NAME, pd.MIDDLE_NAME, pd.FULL_NAME_LFMI from ODSMGR.PERSON_DETAIL pd JOIN ODSMGR_CUST.ACTIVE_EMPLOYEE_view ea
            on pd.person_uid = ea.PERSON_UID)
           pap 
        LEFT JOIN 
         (SELECT  SIRNIST_PIDM ,SIRNIST_COLL_CODE,SIRNIST_TERM_CODE,
                   sum( case when SIRNIST_NIST_CODE = 'BKLD' then 
                        SIRNIST_NIST_WORKLOAD end) banked,
                   sum( case when SIRNIST_NIST_CODE = 'LBLV' then 
                        SIRNIST_NIST_WORKLOAD end) USED ,
                   sum( case when SIRNIST_NIST_CODE = 'C+UP' then 
                        SIRNIST_NIST_WORKLOAD end) Cleanup1,
                   sum( case when SIRNIST_NIST_CODE = 'C-UP' then 
                        SIRNIST_NIST_WORKLOAD end) Cleanup2
              FROM
                   saturn.SIRNIST@prod.kccd.edu  
            WHERE SIRNIST_TERM_CODE  BETWEEN '200330' AND '999999'
              GROUP BY SIRNIST_PIDM, SIRNIST_COLL_CODE,SIRNIST_TERM_CODE
              having sum( case when SIRNIST_NIST_CODE in ( 'BKLD','LBLV', 'C+UP','C-UP') then SIRNIST_NIST_WORKLOAD end ) &gt; 0  )  NIST
          ON PAP.person_uid = NIST.SIRNIST_PIDM 
         JOIN
         (SELECT SIRDPCL_PIDM, SIRDPCL_COLL_CODE, NVL(SIRDPCL_HOME_IND,'N') HOME_IND,
                NVL(SIRDPCL_PERCENTAGE,0) perc,
                ROWID  SIRDPCL_ROWID
           FROM SIRDPCL a
          WHERE  SIRDPCL_TERM_CODE_EFF =
                  (SELECT MAX(SIRDPCL_TERM_CODE_EFF)
                     FROM SIRDPCL
                    WHERE SIRDPCL_PIDM = a.SIRDPCL_PIDM
                      AND SIRDPCL_TERM_CODE_EFF &lt;= '999999' )
          ORDER BY NVL(SIRDPCL_HOME_IND,'N') DESC,
                   NVL(SIRDPCL_PERCENTAGE,0) DESC) 
          ON  SIRDPCL_PIDM = SIRNIST_PIDM
             and SIRDPCL_COLL_CODE =  SIRNIST_COLL_CODE
             and pap.PERSON_UID = SIRNIST_PIDM 
        LEFT JOIN (  SELECT ai.SIRASGN_FCNT_CODE,
                    ai.SIRASGN_PIDM,
                    substr(ss.SSBSECT_CAMP_CODE,1,1) || 'C' CollegeCode,
                    ai.SIRASGN_TERM_CODE,
                     MAX(ss.SSBSECT_CRSE_TITLE),
                    SUM(ai.SIRASGN_WORKLOAD_ADJUST / ai.SIRASGN_PERCENT_RESPONSE   ) * 100 as "LOAD_BANKED"
              FROM SATURN.SIRASGN  ai
                   JOIN SATURN.SSBSECT ss ON ai.SIRASGN_CRN = ss.SSBSECT_CRN AND ai.SIRASGN_TERM_CODE = ss.SSBSECT_TERM_CODE
             WHERE ai.SIRASGN_FCNT_CODE = 'B'
             group by ai.SIRASGN_FCNT_CODE,
                   ai.SIRASGN_PIDM,
                   ai.SIRASGN_TERM_CODE,
                   ss.SSBSECT_CAMP_CODE  ) ASGN
        ON pap.PERSON_UID  = ASGN.SIRASGN_PIDM  AND SIRNIST_TERM_CODE =SIRASGN_TERM_CODE
   GROUP BY NVL(SIRNIST_COLL_CODE,CollegeCode) ,
                    ID, pap.PERSON_UID,
                    pap.LAST_NAME ,
                    pap.FIRST_NAME,
                    pap.MIDDLE_NAME,
                    pap.FULL_NAME_LFMI
     ORDER BY 
          pap.LAST_NAME ,
                    pap.FIRST_NAME,
                    pap.MIDDLE_NAME
"""


sql_parser =tranformation.format_sql_query(sql_query)
print(sql_parser)


run_analysis = config_handler.read_config()


if run_analysis.lower() == "yes":
    tables, columns, table_aliases, column_aliases = extraction.parse_sql_query(sql_parser)
    
    print("Tables:")
    for table in tables:
        print(f"- {table}")
    print()

    print("Columns:")
    for column in columns:
        print(f"- {column}")
    print()

    print("Table Aliases:")
    for alias, table in table_aliases.items():
        print(f"- {alias}: {table}")
    print()

    print("Column Aliases:")
    for alias, column in column_aliases.items():
        print(f"- {alias}: {column}")
    print()

    print("**********TABLE WITH ASSOCIATED COLUMN************\n")
    table_columns = extraction.map_columns_to_tables(columns, tables)

    for table, cols in table_columns.items():
        print(f"Table: {table}")
        for col in cols:
            print(f" - {col}")
        print()
    print()

    print("*************Script Query******************\n")
    create_table_scripts = extraction.create_table_scripts(tables)
    for script in create_table_scripts:
        print(script)
        print()
else:
    print("Analysis was not run.")
