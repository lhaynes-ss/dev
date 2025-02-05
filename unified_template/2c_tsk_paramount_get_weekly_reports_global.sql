-- connection settings
USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD_GLOBAL;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_GLOBAL_LARGE;
USE DATABASE UDW_PROD_GLOBAL;
USE SCHEMA UDW_CLIENTSOLUTIONS_CS;


-- get report
-- Every Monday at 9 AM UTC (4 AM EST)
CREATE OR REPLACE TASK udw_clientsolutions_cs.tsk_paramount_get_weekly_reports_global
    WAREHOUSE = 'UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_GLOBAL_LARGE'
    SCHEDULE = 'USING CRON  0 9 * * 1 UTC'
AS
    EXECUTE IMMEDIATE
    $$
    BEGIN

        -- get report
        CALL udw_clientsolutions_cs.sp_paramount_get_weekly_reports_global(
            save_receipt_YorN => 'Y'
            ,start_date => ''
            ,end_date => ''
        );

    END;
    $$

/**

-- increase default time limit to 8 hours
ALTER TASK udw_clientsolutions_cs.tsk_paramount_get_weekly_reports_global SET USER_TASK_TIMEOUT_MS = 28800000;

-- start or restart task
ALTER TASK udw_clientsolutions_cs.tsk_paramount_get_weekly_reports_global RESUME;

-- stop task
ALTER TASK udw_clientsolutions_cs.tsk_paramount_get_weekly_reports_global SUSPEND;

-- check time limit
SHOW PARAMETERS LIKE '%USER_TASK_TIMEOUT_MS%' IN TASK udw_clientsolutions_cs.tsk_paramount_get_weekly_reports_global;

-- show tasks
SHOW TASKS;

-- manually execute task
EXECUTE TASK udw_clientsolutions_cs.tsk_paramount_get_weekly_reports_global;

-- check status
SELECT *
  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
  ORDER BY SCHEDULED_TIME;
**/