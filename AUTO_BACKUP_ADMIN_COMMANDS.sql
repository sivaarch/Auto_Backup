-------------------------------------------------------------------------------------------------
Custom Role Name:CUSTOM_ROLE_FOR_BACKUP
WH: COE_DW_S
Source Database Name,Backup to be taken: BACKUP_DEMO_DB
Stored Procedure Database: COE_OPERATIONS
Stored Procedure Schema: BACKUP_FRAMEWORK
-------------------------------------------------------------------------------------------------
CREATE DATABASE IF NOT EXISTS COE_OPERATIONS;

CREATE SCHEMA IF NOT EXISTS BACKUP_FRAMEWORK;

CREATE OR REPLACE ROLE CUSTOM_ROLE_FOR_BACKUP;

/* Assign Custom role to a user who executed backup stored procedure */
GRANT ROLE CUSTOM_ROLE_FOR_BACKUP TO USER "SMADDA@ARCHINSURANCE.COM";

/* Grant USAGE on Warehouse/Database/Schema */
GRANT USAGE ON WAREHOUSE COE_DW_S TO ROLE CUSTOM_ROLE_FOR_BACKUP;

/* Role need to create target database where backup need to be stored */
GRANT CREATE DATABASE ON ACCOUNT TO ROLE CUSTOM_ROLE_FOR_BACKUP;

/* Create NOTIFICATION INTEGRATION  to enable email notification */

CREATE OR REPLACE NOTIFICATION INTEGRATION 
BACKUP_NOTIFICATIONS
TYPE=EMAIL
ENABLED=TRUE
ALLOWED_RECIPIENTS=('sivam@archinsurance.com','csutherland@archgroup.com')
COMMENT = 'Created for Backup notifications';

/* Assign USAGE on Integration to Custom role */
GRANT USAGE ON INTEGRATION BACKUP_NOTIFICATIONS TO ROLE CUSTOM_ROLE_FOR_BACKUP;

GRANT ALL ON DATABASE COE_OPERATIONS TO ROLE CUSTOM_ROLE_FOR_BACKUP;

GRANT ALL ON SCHEMA BACKUP_FRAMEWORK TO ROLE CUSTOM_ROLE_FOR_BACKUP;

USE DATABASE COE_OPERATIONS;
USE SCHEMA BACKUP_FRAMEWORK;

/* Metadata table to store backup configuration details */
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

/* Process log table to store backup configuration details */

CREATE OR REPLACE TABLE BACKUP_PROCESS_LOG
(
    backup_log_id INT AUTOINCREMENT,
    backup_id INT NOT NULL,
    completed_at VARCHAR,
    log_message TEXT
);

/* Grant all permissions to custom role on metadata tables */
GRANT ALL ON TABLE BACKUP_TBL TO ROLE CUSTOM_ROLE_FOR_BACKUP;

GRANT ALL ON TABLE BACKUP_PROCESS_LOG TO ROLE CUSTOM_ROLE_FOR_BACKUP;

/* Create stored procedure which grants required privileges on source database  */

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

USE COE_OPERATIONS
USE BACKUP_FRAMEWORK;

CALL GRANTS_ON_BACKUP_DATABASE ('CUSTOM_ROLE_FOR_BACKUP');
