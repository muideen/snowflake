/*This is the initial script to set up the Snowflake environment.
This script creates the appropriate Custom roles for Data Loading as well as objects including Stages, File formats, Streams, Tasks, and Stored Procedures
*/
SET (Database, Raw_Schema, ODS_Schema, File_Format_Name, Stage_Name, File_Path)=('TRAINING_DB', 'RAW', 'ODS', 'my_parquet_format','PARQUET_ONLY_STAGE','file:///Users/deen/Documents/Work/Utilities/Parquet_Files/2023-05-13_10-38/');
SET(Raw_Schema, ODS_Schema) = ($Database || '.' || $Raw_Schema, $Database || '.' || $ODS_Schema);
SET(File_Format_Name, Stage_Name) = ($Raw_Schema || '.' || $File_Format_Name, $Raw_Schema || '.' || $Stage_Name);

USE ROLE sysadmin;

CREATE DATABASE IF NOT EXISTS identifier($Database);

CREATE SCHEMA IF NOT EXISTS identifier($ODS_Schema);

CREATE SCHEMA IF NOT EXISTS identifier($Raw_Schema);
USE SCHEMA identifier($Raw_Schema);
USE ROLE SECURITYADMIN;

CREATE ROLE IF NOT EXISTS DATA_LOADER;


CREATE ROLE IF NOT EXISTS DATA_ARCHITECT;

GRANT ROLE DATA_ARCHITECT TO ROLE SYSADMIN;

GRANT USAGE ON DATABASE identifier($Database) TO ROLE DATA_LOADER;
GRANT USAGE ON DATABASE identifier($Database) TO ROLE DATA_ARCHITECT;
GRANT USAGE, CREATE PROCEDURE ON SCHEMA identifier($ODS_Schema) TO ROLE DATA_ARCHITECT;
GRANT USAGE, CREATE PROCEDURE, create TABLE ON SCHEMA identifier($Raw_Schema) TO ROLE DATA_ARCHITECT;
GRANT USAGE, CREATE FILE FORMAT, CREATE STAGE, CREATE PIPE, CREATE TABLE, CREATE VIEW, CREATE STREAM, CREATE TASK ON SCHEMA identifier($Raw_Schema) TO ROLE DATA_LOADER;

GRANT SELECT ON FUTURE TABLES IN SCHEMA identifier($Raw_Schema)TO ROLE DATA_LOADER;

GRANT USAGE, CREATE TABLE ON SCHEMA identifier($ODS_Schema) TO ROLE DATA_LOADER;

GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE DATA_LOADER;

GRANT USAGE ON WAREHOUSE COMPUTE_WH TO ROLE DATA_ARCHITECT;

GRANT ROLE DATA_LOADER TO ROLE SYSADMIN;
USE ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE DATA_LOADER;

GRANT EXECUTE MANAGED TASK ON ACCOUNT TO ROLE DATA_LOADER;
use role DATA_LOADER;

CREATE OR REPLACE FILE FORMAT identifier($File_Format_Name)
  TYPE = PARQUET;

CREATE STAGE IF NOT EXISTS identifier($Stage_Name)
  FILE_FORMAT = $File_Format_Name;

/*
CREATE STAGE IF NOT EXISTS my_int_stage
  FILE_FORMAT = my_csv_format;
  */
  
PUT 'file:///Users/deen/Documents/Work/Utilities/Parquet_Files/2023-05-13_10-38/categories.parquet' '@TRAINING_DB.RAW.PARQUET_ONLY_STAGE/' AUTO_COMPRESS=false OVERWRITE=false;

PUT 'file:///Users/deen/Documents/Work/Utilities/Parquet_Files/2023-05-13_10-38/order_items.parquet' '@TRAINING_DB.RAW.PARQUET_ONLY_STAGE/' AUTO_COMPRESS=false OVERWRITE=false;

PUT 'file:///Users/deen/Documents/Work/Utilities/Parquet_Files/2023-05-13_10-38/orders.parquet' '@TRAINING_DB.RAW.PARQUET_ONLY_STAGE/' AUTO_COMPRESS=false OVERWRITE=false;

PUT 'file:///Users/deen/Documents/Work/Utilities/Parquet_Files/2023-05-13_10-38/products.parquet' '@TRAINING_DB.RAW.PARQUET_ONLY_STAGE/' AUTO_COMPRESS=false OVERWRITE=false;
USE ROLE 
data_Architect;


CREATE PROCEDURE IF NOT EXISTS TRAINING_DB.RAW.CREATE_LOAD_TASK(LOAD_TASK varchar,VARIANT_TABLE varchar, FILE_FORMAT_NAME varchar, STAGE_PATTERN varchar)
returns varchar
language javascript
EXECUTE AS CALLER
as
$$
  /*Create the landing table if it doesn't exist. the landing tables are single variant column tables where staged files are loaded into.
  They will typically have a -src suffice in their names
  */
   var create_src_table_ddl = "CREATE TABLE IF NOT EXISTS " + VARIANT_TABLE + "(SRC VARIANT)\n CHANGE_TRACKING = TRUE;";
   var create_src_table = snowflake.createStatement({sqlText:create_src_table_ddl});
   create_src_table.execute();  
     
var create_task_stmt = "CREATE TASK IF NOT EXISTS " + LOAD_TASK + "\n" + 
           "warehouse = 'compute_wh' \n" +
           "SCHEDULE = 'USING CRON 0     5     *     *     * UTC' --Runs at 5AM everyday\n" +
           "COMMENT = 'This task loads all staged data into the " +  VARIANT_TABLE + " landing table' \n" +
           "AS \n" +
           "COPY INTO " + VARIANT_TABLE + " \n" +
           "FROM @TRAINING_DB.RAW.parquet_only_stage \n" +
           "FILE_FORMAT = (FORMAT_NAME = " + FILE_FORMAT_NAME + " ) \n" +
           "PATTERN = '" + STAGE_PATTERN + "'; ";

var task_stmt = snowflake.createStatement({sqlText:create_task_stmt});
task_stmt.execute();

return "task "+ LOAD_TASK + " successfully created.";
$$;

GRANT USAGE ON PROCEDURE TRAINING_DB.RAW.CREATE_LOAD_TASK(varchar, varchar, varchar, varchar) TO ROLE data_loader;


create or replace procedure TRAINING_DB.RAW.create_view_over_json (TABLE_NAME varchar, COL_NAME varchar, VIEW_NAME varchar)
returns varchar
language javascript
EXECUTE AS CALLER
as
$$
// CREATE_VIEW_OVER_JSON - Craig Warman, Snowflake Computing, DEC 2019
//
// This stored procedure creates a view on a table that contains JSON data in a column.
// of type VARIANT.  It can be used for easily generating views that enable access to 
// this data for BI tools without the need for manual view creation based on the underlying 
// JSON document structure.  
//
// Parameters:
// TABLE_NAME    - Name of table that contains the semi-structured data.
// COL_NAME      - Name of VARIANT column in the aforementioned table.
// VIEW_NAME     - Name of view to be created by this stored procedure.
//
// Usage Example:
// call create_view_over_json('db.schema.semistruct_data', 'variant_col', 'db.schema.semistruct_data_vw');
//
// Important notes:
//   - This is the "basic" version of a more sophisticated procedure. Its primary purpose
//     is to illustrate the view generation concept.
//   - This version of the procedure does not support:
//         - Column case preservation (all view column names will be case-insensitive).
//         - JSON document attributes that are SQL reserved words (like TYPE or NUMBER).
//         - "Exploding" arrays into separate view columns - instead, arrays are simply
//           materialized as view columns of type ARRAY.
//   - Execution of this procedure may take an extended period of time for very 
//     large datasets, or for datasets with a wide variety of document attributes
//     (since the view will have a large number of columns).
//
// Attribution:
// I leveraged code developed by Alan Eldridge as the basis for this stored procedure.

var path_name = "regexp_replace(regexp_replace(f.path,'\\\\[(.+)\\\\]'),'(\\\\w+)','\"\\\\1\"')"                           // This generates paths with levels enclosed by double quotes (ex: "path"."to"."element").  It also strips any bracket-enclosed array element references (like "[0]")
var attribute_type = "DECODE (substr(typeof(f.value),1,1),'A','ARRAY','B','BOOLEAN','I','FLOAT','D','FLOAT','STRING')";    // This generates column datatypes of ARRAY, BOOLEAN, FLOAT, and STRING only
var alias_name = "REGEXP_REPLACE(REGEXP_REPLACE(f.path, '\\\\[(.+)\\\\]'),'[^a-zA-Z0-9]','_')" ;                           // This generates column aliases based on the path
var col_list = "";

// Build a query that returns a list of elements which will be used to build the column list for the CREATE VIEW statement
var element_query = "SELECT DISTINCT \n" +
                    path_name + " AS path_name, \n" +
                    attribute_type + " AS attribute_type, \n" +
                    alias_name + " AS alias_name \n" +
                    "FROM \n" + 
                    TABLE_NAME + ", \n" +
                    "LATERAL FLATTEN(" + COL_NAME + ", RECURSIVE=>true) f \n" +
                    "WHERE TYPEOF(f.value) != 'OBJECT' \n" +
                    "AND NOT contains(f.path,'[') ";      // This prevents traversal down into arrays;

// Run the query...
var element_stmt = snowflake.createStatement({sqlText:element_query});
var element_res = element_stmt.execute();

// ...And loop through the list that was returned
while (element_res.next()) {

// Add elements and datatypes to the column list
// They will look something like this when added: 
//    col_name:"name"."first"::STRING as name_first, 
//    col_name:"name"."last"::STRING as name_last   

   if (col_list != "") {
      col_list += ", \n";}
   col_list += COL_NAME + ":" + element_res.getColumnValue(1);   // Start with the element path name
   col_list += "::" + element_res.getColumnValue(2);             // Add the datatype
   col_list += " as " + element_res.getColumnValue(3);           // And finally the element alias 
}

// Now build the CREATE VIEW statement and enable change tracking
var view_ddl = "CREATE OR REPLACE VIEW " + VIEW_NAME + 
               "\nCHANGE_TRACKING = TRUE\n AS \n" +
               "SELECT \n" + col_list + "\n" +
               "FROM " + TABLE_NAME;

// Now run the CREATE VIEW statement
var view_stmt = snowflake.createStatement({sqlText:view_ddl});
var view_res = view_stmt.execute();
return view_res.next();
$$;

--CREATE TABLE IF NOT EXISTS TRAINING_DB.ODS.order_items AS SELECT * FROM TRAINING_DB.RAW.orders_view LIMIT 0;
--CREATE TABLE IF NOT EXISTS TRAINING_DB.ODS.orders AS SELECT * FROM TRAINING_DB.RAW.order_items_view LIMIT 0;

GRANT USAGE ON PROCEDURE TRAINING_DB.RAW.create_view_over_json (varchar, varchar, varchar) TO ROLE data_loader;

CREATE OR REPLACE TABLE TRAINING_DB.RAW.TASKS_METADATA (RAWSCHEMA varchar,LOAD_TASK varchar,VARIANT_TABLE varchar, FILE_FORMAT_NAME varchar, STAGE_PATTERN varchar, VIEW_NAME varchar, STREAM_NAME varchar, CDCTASK varchar, ODS_SCHEMA varchar, ODS_TABLE varchar, FILE_NAME varchar);
insert into TRAINING_DB.RAW.TASKS_METADATA values
('TRAINING_DB.RAW','categories_load','categories_src','my_parquet_format','.*categories\.parquet$', 'categories_view', 'categories_stream', 'categories_cdc', 'TRAINING_DB.ODS', 'Categories', 'categories.parquet')
,('TRAINING_DB.RAW','order_items_load','order_items_src','my_parquet_format','.*order_items\.parquet$', 'order_items_view', 'order_items_stream', 'order_items_cdc', 'TRAINING_DB.ODS', 'order_items', 'order_items.parquet')
,('TRAINING_DB.RAW','orders_load','orders_src','my_parquet_format','.*orders\.parquet$', 'orders_view', 'order_stream', 'order_cdc', 'TRAINING_DB.ODS', 'orders', 'orders.parquet')
,('TRAINING_DB.RAW','Product_load','Product_src','my_parquet_format','.*products\.parquet$', 'Product_view', 'Product_stream', 'Product_cdc', 'TRAINING_DB.ODS', 'Product', 'product.parquet')
;

CREATE OR REPLACE PROCEDURE TRAINING_DB.RAW.CREATE_CDC_TASK(CDC_TASK varchar, VARIANT_TABLE varchar, VIEW_NAME varchar, ODS_TABLE varchar, STREAM_NAME varchar)
returns varchar
language javascript
EXECUTE AS CALLER
as
$$
  /*
  Create Stream from the View
  */
   var create_stream_ddl = "CREATE OR REPLACE STREAM " + STREAM_NAME +
                           "  ON VIEW " + VIEW_NAME +
                           "  SHOW_INITIAL_ROWS=TRUE;";
   var create_stream = snowflake.createStatement({sqlText:create_stream_ddl});
   create_stream.execute();  


   var create_ods_table_ddl = "CREATE TABLE IF NOT EXISTS " + ODS_TABLE + " AS SELECT * FROM " + VIEW_NAME + " LIMIT 0;";
   var create_ods_table = snowflake.createStatement({sqlText:create_ods_table_ddl});
   create_ods_table.execute();  

   var create_task_stmt = "CREATE TASK IF NOT EXISTS " + CDC_TASK + "\n" + 
                       "  warehouse = 'compute_wh' \n" +
                       "  SUSPEND_TASK_AFTER_NUM_FAILURES = 3 \n" +
                       "  SCHEDULE = 'USING CRON 0     5     *     *     * UTC' --Runs at 5AM everyday\n" +
                       "  COMMENT = 'This task loads new recordsfrom source " + VARIANT_TABLE + " table into the " +  ODS_TABLE + " ODS table' \n" +
                       "  WHEN\n" +
                       "  SYSTEM$STREAM_HAS_DATA('" + STREAM_NAME + "')\n" +
                       "  AS \n" +
                       "  BEGIN \n" +
                       "    INSERT INTO " + ODS_TABLE + " \n" +
                       "    (\n" +
                       "      SELECT *  EXCLUDE (METADATA$ROW_ID, METADATA$ACTION, METADATA$ISUPDATE) \n" +
                       "        FROM " + STREAM_NAME +
                       "    ); \n" +
                       "  END;";


var task_stmt = snowflake.createStatement({sqlText:create_task_stmt});
task_stmt.execute();

return "task "+ CDC_TASK + " successfully created.";
$$;

GRANT USAGE ON PROCEDURE TRAINING_DB.RAW.CREATE_CDC_TASK(varchar, varchar, varchar, varchar, varchar) TO ROLE data_loader;
CREATE or replace PROCEDURE TRAINING_DB.RAW.CREATE_ALL_TASKS(FILE_PATH varchar, STAGE_NAME varchar)
returns varchar
language javascript
EXECUTE AS CALLER
as
$$ 

//initialize your variables, a counter for the number of tables/tasks created
//var Tasks_List = "";
var Tasks_List = [];
var get_tasks_metata_sql = "SELECT \n" +
                            "  CONCAT(RAWSCHEMA, '.', LOAD_TASK) AS LOAD_TASK,\n" +
                            "  CONCAT(RAWSCHEMA, '.', CDCTASK) AS CDCTASK,\n" +
                            "  CONCAT(RAWSCHEMA, '.', VARIANT_TABLE) AS VARIANT_TABLE,\n" +
                            "  CONCAT(RAWSCHEMA, '.', FILE_FORMAT_NAME) AS FILE_FORMAT_NAME,\n" +
                            "  CONCAT(RAWSCHEMA, '.', VIEW_NAME) AS VIEW_NAME,\n" +
                            "  CONCAT(RAWSCHEMA, '.', STREAM_NAME) AS STREAM_NAME,\n" +
                            "  CONCAT(ODS_SCHEMA, '.', ODS_TABLE) AS ODS_TABLE,\n" +
                            /*"  CONCAT('PUT \''," + FILE_PATH + "FILE_NAME, '\' \'@'," + STAGE_NAME + ",'/\' AUTO_COMPRESS=false OVERWRITE=false') AS PUT_STMT,\n" +*/
                            "  STAGE_PATTERN  \n" +
                            "FROM\n" +
                            "  TASKS_METADATA;";

// Run the above query to get the metadata of the resources to create on Snowflake
var get_tasks_metata = snowflake.createStatement({sqlText:get_tasks_metata_sql});
var tasks_metata = get_tasks_metata.execute();
var tasks_count = get_tasks_metata.getRowCount();
// Iterate through every record
while (tasks_metata.next()) {

   //Tasks_List = Tasks_List + tasks_metata.getColumnValue(1) + "\n";
   Tasks_List.push(tasks_metata.getColumnValue('LOAD_TASK'));

   //Create the Load task by calling the CREATE_LOAD_TASK stoerd procedure
   var call_create_task_sql = "CALL CREATE_LOAD_TASK('" + tasks_metata.LOAD_TASK + "', '" + tasks_metata.VARIANT_TABLE + "', '" + tasks_metata.FILE_FORMAT_NAME + "', '" + tasks_metata.STAGE_PATTERN + "' );";
   var call_create_task = snowflake.createStatement({sqlText:call_create_task_sql});
   call_create_task.execute();
/* 
   var Putfile_stmt = tasks_metata.getColumnValue('PUT_STMT');
   var stagefile_cmd = snowflake.createStatement({sqlText:Putfile_stmt});
   stagefile_cmd.execute();*/
   //We can now load the initial set of data into the source landing table by executing the newly created procedure
   //var execute_task_stmt = "EXECUTE TASK " + tasks_metata.LOAD_TASK + ";";
    var execute_task_stmt = "COPY INTO " + tasks_metata.VARIANT_TABLE + " \n" +
                                  "FROM @" + STAGE_NAME + " \n" +
                                  "FILE_FORMAT = (FORMAT_NAME = " + tasks_metata.FILE_FORMAT_NAME + " ) \n" +
                                  "PATTERN = '" + tasks_metata.STAGE_PATTERN + "'; ";
   var execute_task = snowflake.createStatement({sqlText:execute_task_stmt});
   execute_task.execute();

   //Now that we have created the resources to load the staged files into the landing table, Next step is to create a view on top of the table. We call the procedure to do this
   //TRAINING_DB.RAW.create_view_over_json (TABLE_NAME varchar, COL_NAME varchar, VIEW_NAME varchar)
   var create_view_proc_ddl = "CALL TRAINING_DB.RAW.create_view_over_json('" + tasks_metata.VARIANT_TABLE + "', 'SRC', '" + tasks_metata.VIEW_NAME + "' );";
   var create_view_proc = snowflake.createStatement({sqlText:create_view_proc_ddl});
   create_view_proc.execute();    

   //Now that we have created the resources to load the staged files into the landing table, Next step is to create a view on top of the table. We call the procedure to do this
   //TRAINING_DB.RAW.CREATE_CDC_TASK(CDC_TASK varchar, VARIANT_TABLE varchar, VIEW_NAME varchar, ODS_TABLE varchar, STREAM_NAME varchar)
   var create_proc_ddl = "CALL TRAINING_DB.RAW.CREATE_CDC_TASK('" + tasks_metata.CDCTASK + "', '" + tasks_metata.VARIANT_TABLE + "', '" + tasks_metata.VIEW_NAME + "', '" + tasks_metata.ODS_TABLE + "', '" + tasks_metata.STREAM_NAME + "'  );";
   var create_proc = snowflake.createStatement({sqlText:create_proc_ddl});
   create_proc.execute();    
}

return tasks_count + " task(s) were created successfuly. The tasks created are \n" + Tasks_List;
$$
;
grant usage on procedure TRAINING_DB.RAW.CREATE_ALL_TASKS(varchar, varchar) to role data_loader;
use role data_loader;

--SELECT * FROM TRAINING_DB.RAW.TASKS_METADATA;

CALL TRAINING_DB.RAW.CREATE_ALL_TASKS($File_Path, $Stage_Name);

select GET_DDL('PROCEDURE', 'TRAINING_DB.RAW.CREATE_ALL_TASKS(varchar, varchar)');




/*Clean up. below scripts cleans up all resources created; */
USE ROLE SYSADMIN;
DROP DATABASE IF EXISTS TRAINING_DB;

USE ROLE SECURITYADMIN;

DROP ROLE IF EXISTS DATA_LOADER;

DROP ROLE IF EXISTS DATA_ARCHITECT;

select * EXCLUDE (METADATA$ROW_ID, METADATA$ACTION, METADATA$ISUPDATE) from TRAINING_DB.RAW.CATEGORIES_STREAM;
set th = 'file:///Users/deen/Documents/Work/Utilities/Parquet_Files/2023-05-13_10-38/';

show variables;
PUT 'file:///Users/deen/Documents/Work/Utilities/Parquet_Files/2023-05-13_10-38/products.parquet' '@TRAINING_DB.RAW.PARQUET_ONLY_STAGE/' AUTO_COMPRESS=false OVERWRITE=false;
create or replace temp table puttable as select 'products.parquet' as col1;
--select '\'file:///Users/deen/Documents/Work/Utilities/Parquet_Files/2023-05-13_10-38/products.parquet\' \'@TRAINING_DB.RAW.PARQUET_ONLY_STAGE/\' AUTO_COMPRESS=false OVERWRITE=false' as col1;

select concat('PUT \'file:///',$th, col1, '\' \' @',$Stage_Name,'/\' AUTO_COMPRESS=false OVERWRITE=false') put_stmt from puttable;
select * from TRAINING_DB.RAW.ORDER_ITEMS_STREAM;

desc stream TRAINING_DB.RAW.ORDER_ITEMS_STREAM;
select * from TRAINING_DB.RAW.CATEGORIES_VIEW;
