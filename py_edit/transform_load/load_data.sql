
-- STEP 1. LOAD DATA FROM s3
-- empty table and load new data
TRUNCATE TABLE IF EXISTS udw_prod.udw_clientsolutions_cs.org_time_tracking_raw;

COPY INTO udw_prod.udw_clientsolutions_cs.org_time_tracking_raw
  FROM @adbiz_data.samsung_ads_data_share/analytics/custom/vaughn/test/foo/
  FILE_FORMAT = (FORMAT_NAME = adbiz_data.mycsvformat3)
  PATTERN='.*.csv';


SELECT * FROM udw_prod.udw_clientsolutions_cs.org_time_tracking_raw LIMIT 1000;



-- STEP 2. TRANSFORM DATA
-- transform data and load into final table
DROP TABLE IF EXISTS org_time_tracking_temp;
CREATE TEMP TABLE org_time_tracking_temp AS (
    SELECT 
        name
        ,role
        ,region
        ,vertical
        ,team
        ,sales_group
        ,category
        ,task
        ,date_15_apr
        ,date_16_apr
        ,date_17_apr
        ,date_18_apr
        ,date_19_apr
        ,date_22_apr
        ,date_23_apr
        ,date_24_apr
        ,date_25_apr
        ,date_26_apr
        ,date_29_apr
        ,date_30_apr
        ,date_1_may
        ,date_2_may
        ,date_3_may
        ,date_6_may
        ,date_7_may
        ,date_8_may
        ,date_9_may
        ,date_10_may
        ,date_13_may
        ,date_14_may
        ,date_15_may
        ,date_16_may
        ,date_17_may
    FROM udw_prod.udw_clientsolutions_cs.org_time_tracking_raw
);


-- STEP 3. LOAD DATA INTO FINAL TABLE
CREATE OR REPLACE TABLE udw_prod.udw_clientsolutions_cs.org_time_tracking AS 
SELECT * FROM org_time_tracking_temp;


-- preview data
SELECT * FROM udw_prod.udw_clientsolutions_cs.org_time_tracking LIMIT 1000;


