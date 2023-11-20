---- Request for Production database backup in Snow flake dated 11162023
---- Follow the Steps to configure database backup
---- Login as ACCOUNT ADMIN 
---- Use Production ID SVC.PRODREPLICATE ID
---- WAREHOUSE ARCHRE_USER_WH 
---- DATABASE  ARCHDM and DW_ARCHRE_PRD 
---- Create database ARCHRE_BKP_OPERS to capture backup framework and manage tasks

USE ROLE ACCOUNTADMIN;

CREATE DATABASE IF NOT EXISTS ARCHRE_BKP_OPERS;

CREATE SCHEMA IF NOT EXISTS BACKUP_FRAMEWORK;

CREATE OR REPLACE ROLE CUSTOM_ROLE_FOR_BACKUP;

---- Assign Custom role to a user who is supposed to execute backup stored procedure
---- Replace sguntuka with SVC.PRODREPLICATE ID
GRANT ROLE CUSTOM_ROLE_FOR_BACKUP TO USER "SGUNTUKA@CORP.ARCHCAPSERVICES.COM";

---- Grant USAGE on Warehouse/Database/Schema 
GRANT USAGE ON WAREHOUSE ARCHRE_USER_WH TO ROLE CUSTOM_ROLE_FOR_BACKUP;

---- Role need to create target database where backup need to be stored 
GRANT CREATE DATABASE ON ACCOUNT TO ROLE CUSTOM_ROLE_FOR_BACKUP;

---- Validate Email SGUNTUKA@ARCHRE.COM for notifications
---- Create NOTIFICATION INTEGRATION  to enable email notification

CREATE OR REPLACE NOTIFICATION INTEGRATION 
BACKUP_NOTIFICATIONS
TYPE=EMAIL
ENABLED=TRUE
ALLOWED_RECIPIENTS=('SGUNTUKA@ARCHRE.COM')
COMMENT = 'Created for Backup notifications';

---- Assign USAGE on Integration to Custom role 
GRANT USAGE ON INTEGRATION BACKUP_NOTIFICATIONS TO ROLE CUSTOM_ROLE_FOR_BACKUP;

GRANT ALL ON DATABASE ARCHRE_BKP_OPERS TO ROLE CUSTOM_ROLE_FOR_BACKUP;

GRANT ALL ON SCHEMA BACKUP_FRAMEWORK TO ROLE CUSTOM_ROLE_FOR_BACKUP;

USE DATABASE ARCHRE_BKP_OPERS;
USE SCHEMA BACKUP_FRAMEWORK;

----  Metadata table to store backup configuration details 
CREATE OR REPLACE TABLE BACKUP_TBL
(
    backup_id INT AUTOINCREMENT,
    source_db_nm VARCHAR NOT NULL,
    source_schema_nm TEXT DEFAULT '',
    source_table_nm TEXT DEFAULT '',
    target_db_nm VARCHAR NOT NULL,
    target_backup_type VARCHAR NOT NULL DEFAULT 'TRANSIENT',
    backup_mode VARCHAR NOT NULL DEFAULT 'CLONE',    
    enabled_ind INT NOT NULL DEFAULT 1,
    exclusion_tables TEXT DEFAULT '',
    weekly_day_of_week INT,
    monthly_day_of_month INT,
    retain_daily_backups INT,
    retain_weekly_backups INT,
    retain_monthly_backups INT,
    email_accounts VARCHAR NOT NULL
);

---- Process log table to store backup configuration details 

CREATE OR REPLACE TABLE BACKUP_PROCESS_LOG
(
    backup_log_id INT AUTOINCREMENT,
    backup_id INT NOT NULL,
    completed_at VARCHAR,
    log_message TEXT
);

---- Grant all permissions to custom role on metadata tables 
GRANT ALL ON TABLE BACKUP_TBL TO ROLE CUSTOM_ROLE_FOR_BACKUP;

GRANT ALL ON TABLE BACKUP_PROCESS_LOG TO ROLE CUSTOM_ROLE_FOR_BACKUP;

---- Create stored procedure which grants required privileges on source database 

CREATE OR REPLACE PROCEDURE GRANTS_ON_BACKUP_DATABASE (ROLE_NAME VARCHAR)
/****************************************************************************************\
DESC: 
ROLE_NAME - Custom Role Name
DATABASE_NAME - source database which used for backup
\****************************************************************************************/
  RETURNS STRING
  LANGUAGE JAVASCRIPT
  EXECUTE AS CALLER
AS $$
var sqlCmd = "";
var result = "";
var sqlStmt = "";
var databaseName = "";
var rs = "";
try {
    sqlCmd = "SELECT DISTINCT source_db_nm FROM BACKUP_TBL WHERE enabled_ind=1";
    rs = snowflake.execute( {sqlText: sqlCmd } );
    while (rs.next()) {
        databaseName = rs.getColumnValue(1);       
        
        sqlCmd = "GRANT USAGE ON DATABASE " + databaseName + " TO ROLE " + ROLE_NAME + ";";
        result += sqlCmd + "\n";
        snowflake.execute( {sqlText: sqlCmd} );
    
        sqlCmd = "GRANT USAGE ON ALL SCHEMAS IN DATABASE " + databaseName + " TO ROLE " + ROLE_NAME + ";";
        result += sqlCmd + "\n";
        snowflake.execute( {sqlText: sqlCmd} );

	sqlCmd = "GRANT USAGE ON FUTURE SCHEMAS IN DATABASE " + databaseName + " TO ROLE " + ROLE_NAME + ";";
        result += sqlCmd + "\n";
        snowflake.execute( {sqlText: sqlCmd} );
    
        sqlCmd = "GRANT SELECT ON ALL TABLES IN DATABASE " + databaseName + " TO ROLE " + ROLE_NAME + ";";
        result += sqlCmd + "\n";
        snowflake.execute( {sqlText: sqlCmd} );
    
        sqlCmd = "GRANT SELECT ON FUTURE TABLES IN DATABASE " + databaseName + " TO ROLE " + ROLE_NAME + ";";
        result += sqlCmd + "\n";
        snowflake.execute( {sqlText: sqlCmd} );
    
        sqlCmd = "GRANT SELECT ON ALL VIEWS IN DATABASE " + databaseName + " TO ROLE " + ROLE_NAME + ";";
        result += sqlCmd + "\n";
        snowflake.execute( {sqlText: sqlCmd} );
    
        sqlCmd = "GRANT SELECT ON FUTURE VIEWS IN DATABASE " + databaseName + " TO ROLE " + ROLE_NAME + ";";
        result += sqlCmd + "\n";
        snowflake.execute( {sqlText: sqlCmd} );
    }
}
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
  return result;  
$$;

----- Custom User Commands

USE ROLE CUSTOM_ROLE_FOR_BACKUP;
USE DATABASE ARCHRE_BKP_OPERS;
USE SCHEMA BACKUP_FRAMEWORK;

/* Create auto backup stored procedure */

CREATE OR REPLACE PROCEDURE AUTO_BACKUP_PROC(DB_NAME VARCHAR,SCHEMA_NAME VARCHAR)
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

--- Insert a row into metdata table 

INSERT INTO BACKUP_TBL (
SOURCE_DB_NM,TARGET_DB_NM,backup_mode,ENABLED_IND,EMAIL_ACCOUNTS,retain_daily_backups)
VALUES
('ARCHDM','ZZ_ARCHDM_BKP','PHYSICAL',1,'SGUNTUKA@ARCHRE.COM',7);

INSERT INTO BACKUP_TBL (
SOURCE_DB_NM,TARGET_DB_NM,backup_mode,ENABLED_IND,EMAIL_ACCOUNTS,retain_daily_backups)
VALUES
('DW_ARCHRE_PRD','ZZ_DW_ARCHRE_PRD_BKP','PHYSICAL',1,'SGUNTUKA@ARCHRE.COM',7);

COMMIT;

---- Create task to automate/schedule stored procedure run

CREATE OR REPLACE TASK TASK_AUTO_BACKUP
WAREHOUSE = ARCHRE_USER_WH
SCHEDULE = 'USING CRON 30 22 * * * America/New_York' -- Schedule daily at 10:30 PM America/New_York
AS
CALL ARCHRE_BKP_OPERS.BACKUP_FRAMEWORK.AUTO_BACKUP_PROC('ARCHRE_BKP_OPERS','BACKUP_FRAMEWORK');

---- Grant task execute to custom role

GRANT EXECUTE TASK ON ACCOUNT TO ROLE CUSTOM_ROLE_FOR_BACKUP;

---- Task need to be resume to run according to schedule 

ALTER TASK ARCHRE_BKP_OPERS.BACKUP_FRAMEWORK.TASK_AUTO_BACKUP RESUME;

---- Every time when new row added to table BACKUP_TBL , below SP need to be executed as it grants select permissions to custom role on data objects

USE ROLE ACCOUNTADMIN;
USE ARCHRE_BKP_OPERS;
USE BACKUP_FRAMEWORK;

CALL GRANTS_ON_BACKUP_DATABASE ('CUSTOM_ROLE_FOR_BACKUP');

---- End of Configuration and backup setup