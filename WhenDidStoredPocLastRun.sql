SELECT object_name(m.object_id), MAX(qs.last_execution_time)
   FROM   sys.sql_modules m
   LEFT   JOIN (sys.dm_exec_query_stats qs
                CROSS APPLY sys.dm_exec_sql_text (qs.sql_handle) st)
          ON m.object_id = st.objectid
         AND st.dbid = db_id()
      where (qs.last_execution_time)  is not null
   GROUP  BY object_name(m.object_id)
   ORDER BY object_name(m.object_id)