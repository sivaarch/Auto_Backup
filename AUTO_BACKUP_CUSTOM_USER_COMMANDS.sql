-------------------------------------------------------------------------------------------------
Custom Role Name:CUSTOM_ROLE_FOR_BACKUP
Warehouse Name: COE_DW_S
Source Database Name,Backup to be taken: BACKUP_DEMO_DB
Stored Procedure Database: COE_OPERATIONS
Stored Procedure Schema: BACKUP_FRAMEWORK
-------------------------------------------------------------------------------------------------
USE ROLE CUSTOM_ROLE_FOR_BACKUP;
USE DATABASE COE_OPERATIONS;
USE SCHEMA BACKUP_FRAMEWORK;

/* Create auto backup stored procedure */

CREATE OR REPLACE PROCEDURE AUTO_BACKUP_PROC(DB_NAME VARCHAR,SCHEMA_NAME VARCHAR)
/****************************************************************************************\
DESC: backup database (and all schemas) 
      or a specific schema 
      or tables within DB & Schema 
      to a specific database ( if required) via CLONE
Version History:
15/09/2023 - Initial version
21/09/2023 - Higher level entities like database/schema's will be created automatically.
             This is to avoid manual creation of target database/schema where backup need to
             be created.
11/10/2023 - Added new column exclusion_tables which is useful to add tables which need to 
             be excluded from backup process. This is applicable for schema/database level backups
             and while taking PHYSICAL backup, But not CLONE

             Also added another column target_backup_type which is defaulted to TRANSIENT. Now user 
             can choose to create PERMANENT tables as well.

             SP is limited to TABLE_TYPEs BASE TABLE. Apart from Standard base tables rest of the objects
             are not in scope
Please read user manual carefully before configuring backups and scheduling
\****************************************************************************************/
  RETURNS STRING
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
AS $$
  var sqlCmd = "";
  var sqlStmt = "";
  var rs = "";
  var result = "";
  var giventargetDBNm = "";
  var backupType = "";
  var backupMode = "";
  var backupSrcNm = "";
  var backupTgtNm = "";
  var backupDelNm = "";
  var backupDelNm_1= "";
  var backupDelNm_2= "";
  var backupDelNm_3= "";
  var emailAccounts = "";
  var emailBody = "";
  var currentTime = "";
  var backupID = "";
  var formattedTableName = "";
  var formattedTableNameRes = "";
  var sourceTableFormatted = "";
  var finalResult = "";  
  var targetBackupType = "";  
  var sourceTableNames = "";  
  var exclusionTableNames = "";  
  var tablesQuery = "";  
  try {
  
     //  Return date in YYYYMMDD_HHMMSS
     let current_datetime = new Date();
     let formatted_date = current_datetime.getFullYear() 
            + ("0" + (current_datetime.getMonth() + 1)).slice(-2) 
            + ("0" + current_datetime.getDate()).slice(-2) + "_" 
            + ("0" + current_datetime.getHours()).slice(-2) 
            + ("0" + current_datetime.getMinutes()).slice(-2) 
            + ("0" + current_datetime.getSeconds()).slice(-2);  
   
     // build cursor to loop through db schemas tables that are enabled
 
        sqlCmd = `SELECT
        TARGET_DB_NM,
        DECODE(source_schema_nm, '', 'DATABASE', 
              (decode(source_table_nm,'','SCHEMA','TABLE'))) AS backup_type,
        SOURCE_DB_NM || DECODE(source_schema_nm, '', '', '.' || source_schema_nm)
              || DECODE(source_table_nm, '', '', '.' || source_table_nm) AS backup_source_nm, 
        decode(target_db_nm,'',source_db_nm,target_db_nm) || DECODE(source_schema_nm, '', '', '.' || source_schema_nm) 
              || DECODE(source_table_nm, '', '', '.' || source_table_nm) AS prep_backup_tgt_nm,    
        case when day(current_date()) = monthly_day_of_month then 'M' 
           when dayofweekiso(current_date()) = weekly_day_of_week then 'W'
           else 'D'
        end AS todays_backup,
        case when todays_backup = 'D' then 
                prep_backup_tgt_nm || '_' || TO_CHAR(CURRENT_DATE(),'YYYYMMDD') 
            when todays_backup = 'W' then      
                prep_backup_tgt_nm || '_' || TO_CHAR(CURRENT_DATE(),'YYYYMMDD') || '_W' 
            else
                prep_backup_tgt_nm || '_' || TO_CHAR(CURRENT_DATE(),'YYYYMMDD') || '_M' 
           end AS backup_target_nm,
        case when todays_backup = 'M' and retain_monthly_backups > 0 then
              prep_backup_tgt_nm || '_' || TO_CHAR(DATEADD('MONTH', -retain_monthly_backups, CURRENT_DATE()),'YYYYMMDD') || '_M'
           when todays_backup = 'W' and retain_weekly_backups > 0 then
             prep_backup_tgt_nm || '_' || TO_CHAR(DATEADD('WEEK', -retain_weekly_backups, CURRENT_DATE()),'YYYYMMDD') || '_W'
           when todays_backup = 'D' and retain_daily_backups > 0 then
              prep_backup_tgt_nm || '_' || TO_CHAR(DATEADD('DAY', -retain_daily_backups, CURRENT_DATE()),'YYYYMMDD')
           else
              prep_backup_tgt_nm || '_' || TO_CHAR(DATEADD('DAY', -1, CURRENT_DATE()),'YYYYMMDD')
        end AS backup_DelNm,
        split_part(backup_DelNm,'.',1) as del_part1,
        split_part(backup_DelNm,'.',2) as del_part2,
        split_part(backup_DelNm,'.',3) as del_part3,
        email_accounts as email_accounts,
        backup_mode as backup_mode,
        split_part(backup_source_nm,'.',1) as src_part1,
        split_part(backup_source_nm,'.',2) as src_part2,
        split_part(backup_source_nm,'.',3) as src_part3,
        split_part(backup_target_nm,'.',1) as tgt_part1,
        split_part(backup_target_nm,'.',2) as tgt_part2,
        split_part(backup_target_nm,'.',3) as tgt_part3,
        backup_id as backupID,
        exclusion_tables,
        target_backup_type
        FROM `+DB_NAME+`.`+SCHEMA_NAME+`.BACKUP_TBL
        WHERE enabled_ind = 1;
        `;    
    rs = snowflake.execute( {sqlText: sqlCmd } );

    while (rs.next()) {
      giventargetDBNm = rs.getColumnValue(1);
      backupType = rs.getColumnValue(2);
      backupSrcNm = rs.getColumnValue(3);
      backupTgtNm = rs.getColumnValue(6);
      backupDelNm = rs.getColumnValue(7);
      backupDelNm_1 = rs.getColumnValue(8);
      backupDelNm_2 = rs.getColumnValue(9);
      backupDelNm_3 = rs.getColumnValue(10);
      emailAccounts = rs.getColumnValue(11);
      backupMode = rs.getColumnValue(12);
      sourceNm_1 = rs.getColumnValue(13);
      sourceNm_2 = rs.getColumnValue(14);
      sourceNm_3 = rs.getColumnValue(15);
      targetNm_1 = rs.getColumnValue(16);
      targetNm_2 = rs.getColumnValue(17);
      targetNm_3 = rs.getColumnValue(18);
      backupID = rs.getColumnValue(19);      
      exclusionTableNames = rs.getColumnValue(20);
      targetBackupType = rs.getColumnValue(21);

      if (exclusionTableNames != "") {
          exclusionTableNames = exclusionTableNames.replace(/,/g, '\',\'');
          tablesQuery += ` AND TABLE_NAME NOT IN ('`+ exclusionTableNames +`')`;
      }
      if (targetBackupType != "TRANSIENT")
          targetBackupType = "";
      
      if (giventargetDBNm == sourceNm_1)
      {
          finalResult = "Source Database and Target Database cannot be same"; 
          result = "Failed";
      }
      else if (backupMode == "CLONE") {
           if (backupType == "TABLE"){              
              
              sqlCmd = `CREATE ` + targetBackupType + ` DATABASE IF NOT EXISTS "` + giventargetDBNm + `" COMMENT =  "`+ giventargetDBNm +` database created by stored procedure on `+ current_datetime + `";`;
              result += sqlCmd + "\n";
              snowflake.execute( {sqlText: sqlCmd} );

              sqlCmd = `USE "` + giventargetDBNm + `";`;// Switching context to target before we create a schema
              result += sqlCmd + "\n";
              snowflake.execute( {sqlText: sqlCmd} );
                
              sqlCmd = `CREATE ` + targetBackupType + ` SCHEMA IF NOT EXISTS "` + sourceNm_2 + `" COMMENT =  "`+ sourceNm_2 +` schema created by stored procedure on `+ current_datetime + `";`;
              result += sqlCmd + "\n";
              snowflake.execute( {sqlText: sqlCmd} );
              
              //Create a new backup of the source
              sqlCmd = `CREATE OR REPLACE ` + targetBackupType + ` `+ backupType +` "`+ targetNm_1 
                + `"."`+ targetNm_2 + `"."`+ targetNm_3 +`" CLONE "` + sourceNm_1 
                + `"."`+ sourceNm_2 + `"."`+ sourceNm_3 +`" 
                 COMMENT =  '`+ backupType +` backup created by stored procedure on `+ current_datetime + `';`;    
              result += sqlCmd + "\n";
              snowflake.execute( {sqlText: sqlCmd} );              
            }
            else if (backupType == "SCHEMA"){
              sqlCmd = `CREATE ` + targetBackupType + ` DATABASE IF NOT EXISTS "` + giventargetDBNm + `" COMMENT =  '`+ giventargetDBNm +` database created by stored procedure on `+ current_datetime + `';`;
              result += sqlCmd + "\n";
              snowflake.execute( {sqlText: sqlCmd} );
              
              sqlCmd = `USE "` + giventargetDBNm + `";`// Switching context to target before we create a schema
              result += sqlCmd + "\n";
              snowflake.execute( {sqlText: sqlCmd} );
                
              //Create a new backup of the source
              sqlCmd = `CREATE OR REPLACE ` + targetBackupType + ` ` + backupType + ` "`+ targetNm_1 
                + `"."`+targetNm_2+`" CLONE "` + sourceNm_1 
                + `"."`+ sourceNm_2 + `" COMMENT =  '`+ backupType +` backup created by stored procedure on `+ current_datetime + `';`;    
              result += sqlCmd + "\n";
              snowflake.execute( {sqlText: sqlCmd} );
            }
            else if (backupType == "DATABASE"){
              //Create a new backup of the source
              sqlCmd = `CREATE OR REPLACE ` + targetBackupType + ` ` + backupType + ` "` + backupTgtNm 
                + `" CLONE "` + backupSrcNm 
                + `" COMMENT =  '`+ backupType +` backup created by stored procedure on `+ current_datetime + `';`;
    
              result += sqlCmd + "\n";
              snowflake.execute( {sqlText: sqlCmd} );
           }          
        }
        else if (backupMode == "PHYSICAL")
        {
            if (backupType == "TABLE"){
                sqlCmd = `CREATE ` + targetBackupType + ` DATABASE IF NOT EXISTS "` + giventargetDBNm + `" COMMENT = '`+ giventargetDBNm +` database created by stored procedure on `+ current_datetime + `';`;
                result += sqlCmd + "\n";
                snowflake.execute( {sqlText: sqlCmd} );
                                
                sqlCmd = `USE "` + giventargetDBNm + `";`// Switching context to target before we create a schema
                result += sqlCmd + "\n";
                snowflake.execute( {sqlText: sqlCmd} );
                
                sqlCmd = `CREATE ` + targetBackupType + ` SCHEMA IF NOT EXISTS "` + sourceNm_2 + `" COMMENT =  '`+ sourceNm_2 +` schema created by stored procedure on `+ current_datetime + `';`;
                result += sqlCmd + "\n";
                snowflake.execute( {sqlText: sqlCmd} );

                sqlCmd = `USE SCHEMA "` + sourceNm_2 + `";`// Switching context to target before we create a schema
                result += sqlCmd + "\n";
                snowflake.execute( {sqlText: sqlCmd} );
                
                sqlCmd = `CREATE ` + targetBackupType + ` TABLE IF NOT EXISTS "` + giventargetDBNm + `"."` + sourceNm_2 + `"."`+ sourceNm_3 +`" AS SELECT * FROM "` + sourceNm_1 
                + `"."`+ sourceNm_2 + `"."`+ sourceNm_3 +`";`;
                result += sqlCmd + "\n";
                snowflake.execute( {sqlText: sqlCmd} );
            }
            else if (backupType == "SCHEMA"){                
                sqlCmd = `CREATE ` + targetBackupType + ` DATABASE IF NOT EXISTS "` + giventargetDBNm + `" COMMENT =  '`+ giventargetDBNm +` database created by stored procedure on `+ current_datetime + `';`;
                result += sqlCmd + "\n";
                snowflake.execute( {sqlText: sqlCmd} );
                
                sqlCmd = `USE "` + giventargetDBNm + `";`// Switching context to target before we create a schema
                result += sqlCmd + "\n";
                snowflake.execute( {sqlText: sqlCmd} );
                
                sqlCmd = `CREATE ` + targetBackupType + ` SCHEMA IF NOT EXISTS "` + targetNm_2 + `" COMMENT =  '`+ targetNm_2 +` schema created by stored procedure on `+ current_datetime + `';`;
                result += sqlCmd + "\n";
                snowflake.execute( {sqlText: sqlCmd} );
                
                sqlCmd = `USE "` + sourceNm_1 + `";`;// Switching context to source to skim through list of tables
                result += sqlCmd + "\n";
                snowflake.execute( {sqlText: sqlCmd} );
                
                sqlCmd = `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA='` + sourceNm_2 + `' AND TABLE_CATALOG='` + sourceNm_1 + `'`+ tablesQuery +`;`;
                result += sqlCmd + "\n";
                table_rs = snowflake.execute( {sqlText: sqlCmd } );                
              
                sqlCmd = `USE "` + targetNm_1 + `";`// Switching context to target before we start creating backups
                result += sqlCmd + "\n";
                snowflake.execute( {sqlText: sqlCmd} );

                sqlCmd = `USE SCHEMA "` + targetNm_2+ `";`// Switching context to target before we start creating backups
                result += sqlCmd + "\n";
                snowflake.execute( {sqlText: sqlCmd} );
                
                while (table_rs.next()) { 
                    tableNm = table_rs.getColumnValue(1);
                    //fullTableNm = backupTgtNm+"."+tableNm;
                    sqlCmd = `CREATE ` + targetBackupType + ` TABLE "` + tableNm + `" IF NOT EXISTS AS SELECT * FROM "`+ sourceNm_1 +`"."`+ sourceNm_2 +`"."`+ tableNm +`";`;
                    result += sqlCmd + "\n";
                    snowflake.execute( {sqlText: sqlCmd} );                    
                }     
            }
            else if (backupType == "DATABASE"){
                sqlCmd = `CREATE ` + targetBackupType + ` DATABASE IF NOT EXISTS "` + targetNm_1 + `" COMMENT =  '`+ targetNm_1 +` database created by stored procedure on `+ current_datetime + `';`;
                result += sqlCmd + "\n";
                snowflake.execute( {sqlText: sqlCmd} );
                
                sqlCmd = `USE "` + sourceNm_1 + `";`;// Switching context to source to skim through list of schemas
                result += sqlCmd + "\n";
                snowflake.execute( {sqlText: sqlCmd} );
                
                sqlCmd = `SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME!='INFORMATION_SCHEMA' AND CATALOG_NAME='` + sourceNm_1 + `';`;
                result += sqlCmd + "\n";
                schema_rs = snowflake.execute( {sqlText: sqlCmd } );
    
                sqlCmd = `USE "` + targetNm_1 + `";` // Switching context to target before we start creating backups
                result += sqlCmd + "\n";
                snowflake.execute( {sqlText: sqlCmd} );                
                
                while (schema_rs.next()) {
                    sourceSchemaNm = schema_rs.getColumnValue(1);
                    targetSchemaNm = targetNm_1+"."+sourceSchemaNm;
                    
                    sqlCmd = `CREATE ` + targetBackupType + ` SCHEMA IF NOT EXISTS "` + sourceSchemaNm + `" COMMENT =  '`+ sourceSchemaNm +` schema created by stored procedure on `+ current_datetime + `';`;
                    result += sqlCmd + "\n";
                    snowflake.execute( {sqlText: sqlCmd} );
    
                    sqlCmd = `USE "` + sourceNm_1 + `";`;// Switching context to source to skim through list of tables
                    result += sqlCmd + "\n";
                    snowflake.execute( {sqlText: sqlCmd} );
                
                    sqlCmd = `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA='` + sourceSchemaNm + `' AND TABLE_CATALOG='` + sourceNm_1 + `'`+ tablesQuery +`;`;                    
                    result += sqlCmd + "\n";
                    table_rs = snowflake.execute( {sqlText: sqlCmd } );
                    
                    sqlCmd = `USE "` + targetNm_1 + `";`;// Switching context to target before we start creating backups
                    result += sqlCmd + "\n";
                    snowflake.execute( {sqlText: sqlCmd} );

                    sqlCmd = `USE SCHEMA "` + sourceSchemaNm + `";`;// Switching context to target before we start creating backups
                    result += sqlCmd + "\n";
                    snowflake.execute( {sqlText: sqlCmd} );
                
                    while (table_rs.next()) {
                        tableNm = table_rs.getColumnValue(1);
                        completeTargetTableName = targetNm_1+"."+sourceSchemaNm+"."+tableNm;                        
                        sqlCmd = `CREATE ` + targetBackupType + ` TABLE "` + tableNm + `" IF NOT EXISTS AS SELECT * FROM "`+ sourceNm_1 +`"."`+ sourceSchemaNm +`"."`+ tableNm +`";`;
                        result += sqlCmd + "\n";
                        snowflake.execute( {sqlText: sqlCmd} );
                    }          
                  }     
              }   // if physical backup and for database         
          }// if physical or clone condition
          
          //Now drop the oldest backup based on type (daily, weekly or monthly)
          
          if (backupType == "TABLE") {
              sqlCmd = `SHOW ` + backupType + `S LIKE '` + backupDelNm_3 + `' IN SCHEMA "` + backupDelNm_1
                        + `"."` + backupDelNm_2 + `";`;
          } else
          if (backupType == "SCHEMA") {
              sqlCmd = `SHOW ` + backupType + `S LIKE '` + backupDelNm_2 + `' IN DATABASE "` + backupDelNm_1 + `";`;
          } else {
              sqlCmd = `SHOW ` + backupType + `S LIKE '` + backupDelNm + `';`;
          }
    
          sqlStmt = snowflake.createStatement( {sqlText: sqlCmd} );
          result += sqlCmd + "\n";
          sqlStmt.execute();            
    
          //backupType - older backups that should be deleted
          if (sqlStmt.getRowCount() !== 0) {
              if (backupType !== "DATABASE") {
                  sqlCmd = `DROP ` + backupType + ` IF EXISTS "` + backupDelNm + `";`;
              } else {
                  sqlCmd = `DROP ` + backupType + ` IF EXISTS "` + backupDelNm + `";`;
              }
              result += sqlCmd + "\n";
              snowflake.execute( {sqlText: sqlCmd} );    
          }
      } // while loop for backup config table 
    } // try block
   catch (err) {
    if (err.code === undefined) {
      result = err.message
    } else {      
      result +=  "Failed: Code: " + err.code + " | State: " + err.state;
      result += "\n  Message: " + err.message;
      result += "\nStack Trace:\n" + err.stackTraceTxt;
      result += "\nsqlCmd: " + sqlCmd;
    }
  }	  
  currentTime = new Date();
  if (result.indexOf("Failed") == -1) 
  {
      finalResult += "SUCCESSFUL Execution\n"
      emailBody = "Backup for source "+ backupSrcNm + ",of type "+ backupType +" successfully completed at "+currentTime+"\n Target:"+backupTgtNm
  }
  else
  {
      finalResult += "FAILED Execution\n"
      emailBody = "Backup for source "+ backupSrcNm + ",of type "+ backupType +" failed at "+currentTime
  }      
  //SYSTEM$SEND_EMAIL sends an email to email accounts mentioned in backup conig table
  
  var proc = `CALL SYSTEM$SEND_EMAIL('BACKUP_NOTIFICATIONS','`+ emailAccounts +`', 'Backup Process Notification','`+ emailBody +`');`;      
  snowflake.execute( {sqlText: proc} );
  
  sqlCmd = `USE `+DB_NAME+`.`+SCHEMA_NAME+`;`; // Switching context to original database where backup config and log tables resides
  result += sqlCmd + "\n";
  sqlStmt = snowflake.createStatement( {sqlText: sqlCmd} );  
  sqlStmt.execute();   
 
  result = result.replace(/'/g, '"'); // Replace single quotes with double as we need to stored entire list of commands in log table
  sqlCmd = "INSERT INTO BACKUP_PROCESS_LOG (backup_id,completed_at,log_message) VALUES (" + backupID + ",'"+ currentTime +"','"+ result +"');";
  result += sqlCmd + "\n";
  sqlStmt = snowflake.createStatement( {sqlText: sqlCmd} );  
  sqlStmt.execute(); 
  finalResult += result;
  return finalResult;  
$$;

/* Insert a row into metdata table, This is for demonistration, Please use your own values according to user manual */
INSERT INTO BACKUP_TBL (
SOURCE_DB_NM,SOURCE_SCHEMA_NM,SOURCE_TABLE_NM,TARGET_DB_NM,backup_mode,ENABLED_IND,EMAIL_ACCOUNTS)
VALUES
('BACKUP_DEMO_DB','DEMO','BACKUP_PROCESS_LOG','BACKUP_TARGET_DB','PHYSICAL',1,'sivam@archinsurance.com');

/* Before executing SP, create target database and schema. Please refer user manual */

CREATE DATABASE IF NOT EXISTS BACKUP_TARGET_DB;
CREATE SCHEMA IF NOT EXISTS DEMO;

/* Manual execution of SP */

USER DATABASE COE_OPERATIONS;
USE SCHEMA BACKUP_FRAMEWORK;

CALL COE_OPERATIONS.BACKUP_FRAMEWORK.AUTO_BACKUP_PROC('COE_OPERATIONS','BACKUP_FRAMEWORK');

/* Create task to automate/schedule stored procedure run  */

CREATE OR REPLACE TASK TASK_AUTO_BACKUP
WAREHOUSE = COE_DW_S
SCHEDULE = 'USING CRON 30 15 * * * America/New_York' -- Schedule daily at 3:30 PM America/New_York
AS
CALL COE_OPERATIONS.BACKUP_FRAMEWORK.AUTO_BACKUP_PROC('COE_OPERATIONS','BACKUP_FRAMEWORK');

/* Task need to be resume to run according to schedule  */

ALTER TASK COE_OPERATIONS.BACKUP_FRAMEWORK.TASK_AUTO_BACKUP RESUME;
