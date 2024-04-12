-- https://adgear.atlassian.net/browse/SAI-6350
-- navigate to my project: cd Desktop/py_edit/
-- aws --profile nyc s3 cp output/ s3://samsung.ads.data.share/analytics/custom/vaughn/test/foo/output/ --recursive 


SET year = (SELECT LEFT(CURRENT_DATE(), 4));


-- STEP 1. LOAD DATA FROM s3
-- empty table and load new data
TRUNCATE TABLE IF EXISTS udw_prod.udw_clientsolutions_cs.org_time_tracking_raw;

COPY INTO udw_prod.udw_clientsolutions_cs.org_time_tracking_raw
    FROM @adbiz_data.samsung_ads_data_share/analytics/custom/vaughn/test/foo/output/
    FILE_FORMAT = (FORMAT_NAME = adbiz_data.mycsvformat3)
    PATTERN='.*.csv';


SELECT * FROM udw_prod.udw_clientsolutions_cs.org_time_tracking_raw LIMIT 1000;



-- STEP 2. TRANSFORM DATA
-- transform data and load into final table
DROP TABLE IF EXISTS org_time_tracking_temp;
CREATE TEMP TABLE org_time_tracking_temp AS (

    -- transform data
    -- only include rows (tasks) where time was logged
    -- convert time to minutes
    WITH cte AS (
        SELECT 
            TRIM(name) AS name
            ,TRIM(role) AS role
            ,TRIM(region) AS region
            ,TRIM(vertical) AS vertical
            ,TRIM(team) AS team
            ,TRIM(sales_group) AS sales_group
            ,TRIM(select_department) AS department
            ,TRIM(category) AS category
            ,TRIM(task) AS task
            ,CAST(date_15_apr AS FLOAT) * 60 AS minutes_15_apr
            ,CAST(date_16_apr AS FLOAT) * 60 AS minutes_16_apr
            ,CAST(date_17_apr AS FLOAT) * 60 AS minutes_17_apr
            ,CAST(date_18_apr AS FLOAT) * 60 AS minutes_18_apr
            ,CAST(date_19_apr AS FLOAT) * 60 AS minutes_19_apr
            ,CAST(date_22_apr AS FLOAT) * 60 AS minutes_22_apr
            ,CAST(date_23_apr AS FLOAT) * 60 AS minutes_23_apr
            ,CAST(date_24_apr AS FLOAT) * 60 AS minutes_24_apr
            ,CAST(date_25_apr AS FLOAT) * 60 AS minutes_25_apr
            ,CAST(date_26_apr AS FLOAT) * 60 AS minutes_26_apr
            ,CAST(date_29_apr AS FLOAT) * 60 AS minutes_29_apr
            ,CAST(date_30_apr AS FLOAT) * 60 AS minutes_30_apr
            ,CAST(date_1_may AS FLOAT) * 60 AS minutes_1_may
            ,CAST(date_2_may AS FLOAT) * 60 AS minutes_2_may
            ,CAST(date_3_may AS FLOAT) * 60 AS minutes_3_may
            ,CAST(date_6_may AS FLOAT) * 60 AS minutes_6_may
            ,CAST(date_7_may AS FLOAT) * 60 AS minutes_7_may
            ,CAST(date_8_may AS FLOAT) * 60 AS minutes_8_may
            ,CAST(date_9_may AS FLOAT) * 60 AS minutes_9_may
            ,CAST(date_10_may AS FLOAT) * 60 AS minutes_10_may
            ,CAST(date_13_may AS FLOAT) * 60 AS minutes_13_may
            ,CAST(date_14_may AS FLOAT) * 60 AS minutes_14_may
            ,CAST(date_15_may AS FLOAT) * 60 AS minutes_15_may
            ,CAST(date_16_may AS FLOAT) * 60 AS minutes_16_may
            ,CAST(date_17_may AS FLOAT) * 60 AS minutes_17_may
        FROM udw_prod.udw_clientsolutions_cs.org_time_tracking_raw
        WHERE 
            CAST(total AS FLOAT) > 0
    )

    -- convert date label columns to date values
    ,pivot_cte AS (
        SELECT 
            *
        FROM cte
            UNPIVOT(
                minutes_active FOR log_date IN (minutes_15_apr, minutes_16_apr, minutes_17_apr, minutes_18_apr, minutes_19_apr, minutes_22_apr, minutes_23_apr, minutes_24_apr, minutes_25_apr, minutes_26_apr, minutes_29_apr, minutes_30_apr, minutes_1_may, minutes_2_may, minutes_3_may, minutes_6_may, minutes_7_may, minutes_8_may, minutes_9_may, minutes_10_may, minutes_13_may, minutes_14_may, minutes_15_may, minutes_16_may, minutes_17_may)
            )
    )

    -- get distinct date label values
    ,dates_cte AS (
        SELECT DISTINCT log_date 
        FROM pivot_cte
    )

    -- create map from date label to date
    ,date_map_cte AS (
        SELECT 
            log_date
            ,TO_DATE(REPLACE(log_date, 'MINUTES', $year), 'YYYY_DD_MON') AS log_date_to_date
        FROM dates_cte
    )

    -- create final selection
    -- only include dates where time was logged
    SELECT 
        p.name
        ,p.role
        ,p.region
        ,CASE WHEN p.vertical = '0' THEN '' ELSE p.vertical END AS vertical
        ,CASE WHEN p.team = '0' THEN '' ELSE p.team END AS team
        ,CASE WHEN p.sales_group = '0' THEN '' ELSE p.sales_group END AS sales_group
        ,p.department
        ,p.category
        ,p.task
        ,d.log_date_to_date AS log_date
        ,p.minutes_active
    FROM pivot_cte p 
        JOIN date_map_cte d USING(log_date)
    WHERE 
        p.minutes_active > 0
      
);


-- STEP 3. LOAD DATA INTO FINAL TABLE
CREATE OR REPLACE TABLE udw_prod.udw_clientsolutions_cs.org_time_tracking AS 
SELECT * FROM org_time_tracking_temp;


-- preview data
SELECT * FROM udw_prod.udw_clientsolutions_cs.org_time_tracking LIMIT 1000;


