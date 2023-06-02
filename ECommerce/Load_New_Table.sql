

USE ROLE data_loader;

SET (Raw_Schema, ODS_Schema, Table_Name, stage_pattern)=('TRAINING_DB.RAW', 'TRAINING_DB.ODS', 'Products','.*products\.parquet$');
SET (File_Format_Name) = ('TRAINING_DB.RAW.my_parquet_format');
SET (Table_Full_Name, ODS_Table) = ($Raw_Schema || '.' || $Table_Name, $ODS_Schema || '.' || $Table_Name);
SET (Variant_Table, Pipe_Name, View_Name,Stream_Name, Task_Name, Load_Task) = ($Table_Full_Name || '_var', $Table_Full_Name || '_pipe', $Table_Full_Name || '_view', $Table_Full_Name || '_stream',$Table_Full_Name || '_task',$Table_Full_Name || '_loadtask') ;


CREATE OR REPLACE TABLE identifier($Variant_Table)(SRC VARIANT);

use role data_architect;

use role data_loader;
call CREATE_TASK($Load_Task, $Variant_Table, $File_Format_Name, $stage_pattern);

execute task TRAINING_DB.RAW.Products_loadtask;

CREATE OR REPLACE PIPE identifier($Pipe_Name)
--AUTO_INGEST = TRUE
AS
 COPY INTO identifier($Variant_Table)
FROM @TRAINING_DB.RAW.parquet_only_stage
FILE_FORMAT = (FORMAT_NAME = $File_Format_Name)
PATTERN = $stage_pattern;

CALL TRAINING_DB.RAW.create_view_over_json($Variant_Table,'src',$View_Name);


/*This section implements change tracking with the creation of streams on both the variant table and its associated view */

ALTER TABLE identifier($Variant_Table) SET CHANGE_TRACKING = true;
ALTER VIEW identifier($View_Name) SET CHANGE_TRACKING = true;

create or replace stream identifier($Stream_Name) on view identifier($View_Name) SHOW_INITIAL_ROWS=TRUE;
