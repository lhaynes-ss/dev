/**
    =================
    Pluto US and Canada Weekly
    =================
    Dates: prev week.  (M - Sun)
    DB: UDW_PROD
    Instructions: https://adgear.atlassian.net/wiki/spaces/~631683943/pages/19879789397/P+and+Pluto+Coverage+Instructions

    =================
    FIND AND REPLACE
    =================
    Previous Monday: '2024-02-26' -- 'YYYY-MM-DD'
    Previous Sunday: '2024-03-03' -- 'YYYY-MM-DD'
    Country: 'CA' -- US | CA
    mapping file: @adbiz_data.samsung_ads_data_share/analytics/custom/vaughn/pluto/20240304_pluto_ca.csv
**/


-- connection settings
USE ROLE udw_marketing_analytics_default_consumer_role_prod;
USE WAREHOUSE udw_marketing_analytics_default_wh_prod;
USE DATABASE udw_prod;
USE SCHEMA public;


-- set variables
SET start_dt = '2024-02-26'; -- 'YYYY-MM-DD'
SET end_dt = '2024-03-03'; -- 'YYYY-MM-DD'
SET country = 'CA'; -- 'US' | 'CA'


-- auto-set variables
SET (
    report_start_date
    ,report_end_date
    ,lookback
) = (
    (TO_CHAR(CAST($start_dt AS DATE), 'yyyymmdd'))                              -- YYYYMMDD - report_start_date
    ,(TO_CHAR(CAST($end_dt AS DATE), 'yyyymmdd'))                               -- YYYYMMDD - report_end_date 
    ,(TO_CHAR(DATEADD('MONTH', -18, CAST($end_dt AS DATE)), 'yyyymmdd'))        -- YYYYMMDD - lookback (18 months before end date) 
);


-- import raw mapping file
DROP TABLE IF EXISTS raw_place_mapping;
CREATE TEMP TABLE raw_place_mapping (
    delete_insertion_id INT, 
    line_item_name VARCHAR(556), 
    camp_start VARCHAR(556), 
    camp_end VARCHAR(556), 
    campaign_name VARCHAR(556), 
    campaign_id INT,
    flight_name VARCHAR(556), 
    flight_id INT, 
    creative_name VARCHAR(556), 
    creative_id INT,
    delete_country VARCHAR(556),
    line_item_id INT,
    delete_impressions INT
);
COPY INTO raw_place_mapping
FROM @adbiz_data.samsung_ads_data_share/analytics/custom/vaughn/pluto/20240304_pluto_ca.csv
file_format = (format_name = adbiz_data.mycsvformat3);

SELECT * FROM raw_place_mapping;


-- update file
DROP TABLE IF EXISTS place_mapping;
CREATE TEMP TABLE place_mapping AS (
    SELECT
        line_item_name
        ,CAST(camp_start AS TIMESTAMP) AS camp_start
        ,CAST(camp_end AS TIMESTAMP) AS camp_end
        ,campaign_name 
        ,campaign_id 
        ,flight_name 
        ,flight_id 
        ,creative_name 
        ,creative_id 
        ,line_item_id
    FROM raw_place_mapping
);

SELECT * FROM place_mapping;




DROP TABLE IF EXISTS exposure_log;
CREATE TEMP TABLE exposure_log AS (
    SELECT
        samsung_tvid_pii_virtual_id AS vtifa,
        LEFT(fact.partition_datehour, 8) AS expose_date,
        udw_partition_datetime,
        creative_id,
        campaign_id
    FROM data_ad_xdevice.fact_delivery_event_without_pii AS fact
    WHERE 
        LEFT(fact.partition_datehour, 8) BETWEEN $report_start_date AND $report_end_date -- (SELECT CAST(REPLACE(MIN(whole_cmpgn_start),'-','') AS INT) FROM campaign_meta) AND (SELECT CAST(REPLACE(MAX(whole_cmpgn_end),'-','') AS INT) FROM campaign_meta)
        AND fact.campaign_id IN (SELECT DISTINCT campaign_id FROM place_mapping) -- (SELECT DISTINCT campaign_id FROM campaign_meta)
        AND type= 1
        AND (dropped != TRUE OR dropped IS NULL)
        AND device_country = $country
);




DROP TABLE IF EXISTS exposure_stats;
CREATE TEMP TABLE exposure_stats AS (
    SELECT  
        a.creative_id, 
        a.campaign_id, 
        COUNT(a.vtifa) AS impression
    FROM exposure_log a 
    GROUP BY 1, 2
);



DROP TABLE IF EXISTS click_log;
CREATE TEMP TABLE click_log AS (
SELECT
    samsung_tvid_pii_virtual_id AS vtifa,
    LEFT(fact.partition_datehour, 8) AS expose_date,
    udw_partition_datetime,
    creative_id,
    campaign_id
FROM data_ad_xdevice.fact_delivery_event_without_pii AS fact
WHERE 
    LEFT(fact.partition_datehour, 8) BETWEEN $report_start_date AND $report_end_date -- (SELECT CAST(REPLACE(MIN(whole_cmpgn_start),'-','') AS INT) FROM campaign_meta) AND (SELECT CAST(REPLACE(MAX(whole_cmpgn_end),'-','') AS INT) FROM campaign_meta)
    AND fact.campaign_id IN (SELECT DISTINCT campaign_id FROM place_mapping) -- (SELECT DISTINCT campaign_id FROM campaign_meta)
    AND type = 2
    AND (dropped != TRUE OR dropped IS NULL)
    AND device_country = $country
);

--select * from click_log limit 10;



DROP TABLE IF EXISTS click_stats;
CREATE TEMP TABLE click_stats AS (
    SELECT 
        creative_id, 
        campaign_id, 
        zeroifnull(COUNT(vtifa)) AS click
    FROM click_log
    GROUP BY 1, 2
);

--select * from click_stats limit 10;



DROP TABLE IF EXISTS creative_map;
CREATE TEMP TABLE creative_map AS (
    SELECT DISTINCT
        campaign_id,
        campaign_name,
        creative_id,
        creative_name,  --creative_nm AS creative_name,
        line_item_id AS placement_id,
        line_item_name AS placement_name,
        camp_start AS campaign_start_date,
        camp_end AS campaign_end_date
    FROM place_mapping
);



DROP TABLE IF EXISTS app_usage;
CREATE TEMP TABLE app_usage AS (
    SELECT
        vtifa,
        vpsid,
        LEFT(partition_datehour,8) AS partition_date,
        udw_partition_datetime,
        SUM(DATEDIFF('minutes',start_timestamp,end_timestamp)) AS time_spent_min,
        ROUND(time_spent_min/60,2) AS time_spent_hour
    FROM data_tv_acr.fact_app_usage_session_without_pii AS fact
        LEFT JOIN udw_prod.udw_lib.virtual_psid_tifa_mapping_v AS m ON m.vpsid = fact.psid_pii_virtual_id
    WHERE 
        app_id IN (SELECT app_id FROM adbiz_data.LUP_APP_CAT_GENRE_2023 WHERE prod_nm = 'Pluto TV')  -- Change Channel name
        AND fact.country = $country
        AND DATEDIFF('second', start_timestamp, end_timestamp) > 60
        and LEFT(fact.partition_datehour, 8) BETWEEN $lookback AND $report_end_date
    GROUP BY 1, 2, 3, 4
);

--select * from app_usage limit 10;




DROP TABLE IF EXISTS first_app_open;
CREATE TEMP TABLE first_app_open AS (
    SELECT 
        vtifa, 
        vpsid, 
        MIN(udw_partition_datetime) AS date_first_open
    FROM app_usage
    GROUP BY 1, 2
);

--select * from first_app_open limit 10;



DROP TABLE IF EXISTS daily_downloads_table;
CREATE TEMP TABLE daily_downloads_table AS (
    SELECT 
        COUNT(DISTINCT vtifa) AS daily_downloads
    FROM app_usage
    JOIN first_app_open USING(vtifa)
    WHERE 
        TO_DATE(partition_date, 'YYYYMMDD') = date_first_open
        AND partition_date BETWEEN $report_start_date AND $report_end_date
);

--select * from daily_downloads_table limit 10;




DROP TABLE IF EXISTS exposed_app_open_time;
CREATE TEMP TABLE exposed_app_open_time AS (
    SELECT DISTINCT 
        partition_date,  
        creative_id, 
        campaign_id, 
        a.vtifa, 
        time_spent_min
    FROM (SELECT * FROM exposure_log UNION SELECT * FROM click_log) a
        JOIN (
            SELECT * 
            FROM app_usage 
            WHERE udw_partition_datetime >= (SELECT MIN(campaign_start_date) FROM creative_map)
        ) b ON (
            a.vtifa = b.vtifa 
            AND a.udw_partition_datetime <= b.udw_partition_datetime 
            AND a.udw_partition_datetime BETWEEN TO_DATE(TO_VARCHAR($report_start_date), 'yyyymmdd') AND TO_DATE(TO_VARCHAR($report_end_date), 'yyyymmdd')
        )
    -- group by 1, 2, 3, 4
);

-- select * from exposed_app_open_time limit 10;




DROP TABLE IF EXISTS exposed_app_open;
CREATE TEMP TABLE exposed_app_open AS (
    SELECT
        --partition_date as date, 
        creative_id, 
        campaign_id, 
        COUNT(DISTINCT vtifa) AS count_exposed_app_open, 
        SUM(time_spent_min) AS total_time_spent_min
    FROM exposed_app_open_time 
    GROUP BY 1, 2
);

-- select * from exposed_app_open limit 10;



DROP TABLE IF EXISTS exposed_first_time_open;
CREATE TEMP TABLE exposed_first_time_open AS (
    SELECT DISTINCT 
        creative_id, 
        campaign_id, 
        COUNT(DISTINCT vtifa) AS count_exposed_first_app_open
    FROM ( 
        SELECT DISTINCT 
            date_first_open, 
            expose_date,
            creative_id, 
            campaign_id, 
            b.vtifa, 
            ROW_NUMBER() OVER(PARTITION BY b.vtifa, date_first_open ORDER BY expose_date DESC) AS row_num
        FROM (SELECT * FROM exposure_log UNION SELECT * FROM click_log ) a
            JOIN (
                SELECT * 
                FROM first_app_open 
                WHERE date_first_open >= (SELECT MIN(campaign_start_date) FROM creative_map)
            ) b ON (
                a.vtifa = b.vtifa 
                AND a.expose_date <= b.date_first_open
            )
    )
    WHERE row_num = 1
    GROUP BY 1,2
);

-- select * FROM exposed_first_time_open limit 10;




/***************
 OUTPUT
***************/
SELECT 
    'Pluto TV' AS campaign_name, 
    m.campaign_id,
    m.placement_id,
    m.creative_id,
    creative_name, 
    placement_name,
    campaign_start_date,
    campaign_end_date,
    COALESCE(impression, 0) AS impression, 
    COALESCE(click, 0) AS click, 
    count_exposed_app_open, 
    count_exposed_first_app_open,  -- monthly_active_users,  monthly_new_users, monthly_returning_users,
    t.total_time_spent_min,    
    ROUND(t.total_time_spent_min/60, 2) total_time_spent_hour,   
    daily_downloads
FROM creative_map m
    LEFT JOIN exposure_stats ex USING (creative_id, campaign_id)
    LEFT JOIN click_stats USING (creative_id, campaign_id)
    LEFT JOIN exposed_app_open AS t USING (creative_id, campaign_id)
    LEFT JOIN exposed_first_time_open USING (creative_id, campaign_id)
    CROSS JOIN daily_downloads_table a
;


