-- connection settings
USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD_GLOBAL;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_GLOBAL_LARGE;
USE DATABASE UDW_PROD_GLOBAL;
USE SCHEMA UDW_CLIENTSOLUTIONS_CS;


-- get map
-- Everyday at 7:05 AM UTC (2:05 AM EST)
CREATE OR REPLACE TASK udw_clientsolutions_cs.tsk_update_paramount_creative_mapping_global
    WAREHOUSE = 'UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_GLOBAL_LARGE'
    SCHEDULE = 'USING CRON  5 7 * * * UTC'
AS 
    EXECUTE IMMEDIATE
    $$
    BEGIN

        -- paramount map
        CALL udw_clientsolutions_cs.sp_update_custom_creative_mapping_global(
            advertiser_list => '13186, 13187'
            ,destination_table => 'udw_clientsolutions_cs.paramount_custom_creative_mapping'
        );

    END;
    $$

/**

-- increase default time limit to 8 hours
ALTER TASK udw_clientsolutions_cs.tsk_update_paramount_creative_mapping_global SET USER_TASK_TIMEOUT_MS = 28800000;

-- start or restart task
ALTER TASK udw_clientsolutions_cs.tsk_update_paramount_creative_mapping_global RESUME;

-- stop task
ALTER TASK udw_clientsolutions_cs.tsk_update_paramount_creative_mapping_global SUSPEND;

-- check time limit
SHOW PARAMETERS LIKE '%USER_TASK_TIMEOUT_MS%' IN TASK udw_clientsolutions_cs.tsk_update_paramount_creative_mapping_global;

-- show tasks
SHOW TASKS;

-- manually execute task
EXECUTE TASK udw_clientsolutions_cs.tsk_update_paramount_creative_mapping_global;

-- check status
SELECT *
  FROM TABLE(INFORMATION_SCHEMA.TASK_HISTORY())
  ORDER BY SCHEDULED_TIME;
**/