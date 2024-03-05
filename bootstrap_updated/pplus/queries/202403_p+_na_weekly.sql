/**
    =================
    Paramount+ US and Canada Weekly
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
    mapping file: @adbiz_data.samsung_ads_data_share/analytics/custom/vaughn/paramount/20240304_p_can.csv
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
    ,graph_start_date
) = (
    (TO_CHAR(CAST($start_dt AS DATE), 'yyyymmdd') || '00')                  -- YYYYMMDDHH - report_start_date
    ,(TO_CHAR(CAST($end_dt AS DATE), 'yyyymmdd') || '23')                   -- YYYYMMDDHH - report_end_date
    ,(TO_CHAR(DATEADD('MONTH', -6, CAST($end_dt AS DATE)), 'yyyymmdd'))     -- YYYYMMDD - graph_start_date (6 months before end date)
);


-- import raw mapping file
DROP TABLE IF EXISTS raw_place_mapping;
CREATE TEMP TABLE raw_place_mapping (
    insertion_id INT
    ,line_item_name VARCHAR(556)
    ,camp_start VARCHAR(556)
    ,camp_end VARCHAR(556)
    ,campaign_name VARCHAR(556)
    ,campaign_id INT
    ,flight_name VARCHAR(556)
    ,flight_id INT
    ,creative_name VARCHAR(556)
    ,creative_id INT
    ,delete_country VARCHAR(556)
    ,line_item_id INT
    ,delete_impressions INT
);
COPY INTO raw_place_mapping 
FROM @adbiz_data.samsung_ads_data_share/analytics/custom/vaughn/paramount/20240304_p_can.csv
file_format = (format_name = adbiz_data.mycsvformat3);

SELECT * FROM raw_place_mapping;


-- update file
DROP TABLE IF EXISTS place_mapping;
CREATE TEMP TABLE place_mapping AS (
    SELECT
        insertion_id AS vao
        ,line_item_name
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


-- get creative map
DROP TABLE IF EXISTS creative_map;
CREATE TEMP TABLE creative_map AS (
    SELECT DISTINCT
        campaign_id,
        campaign_name,
        creative_id,
        creative_name,  --creative_nm AS creative_name,
        line_item_id AS placement_id,
        line_item_name AS placement_name,
        DATE_TRUNC('day', camp_start) AS campaign_start_date,
        DATE_TRUNC('day', camp_end) AS campaign_end_date
    FROM place_mapping
);

SELECT * FROM creative_map;




DROP TABLE IF EXISTS graph_table;
CREATE TEMP TABLE graph_table AS (
    SELECT DISTINCT
        ip_pii_virtual_id,
        psid_pii_virtual_id AS vpsid
    FROM (
        SELECT
            ip_pii_virtual_id,
            psid_pii_virtual_id,
            ROW_NUMBER() OVER (PARTITION BY ip_pii_virtual_id ORDER BY partition_date DESC) AS rn
        FROM graph_ip_psid_without_pii
        WHERE partition_date BETWEEN $report_end_date AND $graph_start_date -- $graph_start_date AND $report_end_date
            AND partition_country IN ($country)
    ) AS foo
    WHERE rn = 1
);



DROP TABLE IF EXISTS ip_psid_map;
CREATE TEMP TABLE ip_psid_map AS (
    SELECT DISTINCT
        vip,
        vpsid,
        vtifa
    FROM (
        SELECT
            m.vpsid,
            DEVICE_IP_PII_VIRTUAL_ID AS vip,
            vtifa,
            ROW_NUMBER() OVER (PARTITION BY DEVICE_IP_PII_VIRTUAL_ID ORDER BY event_time DESC) AS rn
        FROM data_ad_xdevice.fact_delivery_event_without_pii fact
        JOIN udw_lib.virtual_psid_tifa_mapping_v m ON GET(fact.samsung_tvids_pii_virtual_id, 0) = m.vtifa
        WHERE 
            udw_partition_datetime BETWEEN TO_TIMESTAMP($report_start_date,'YYYYMMDDHH') AND TO_TIMESTAMP($report_end_date,'YYYYMMDDHH')
            AND type = 1
            -- AND (dropped != TRUE OR dropped IS NULL)
            AND device_country = $country
    ) 
    WHERE rn = 1
);



DROP TABLE IF EXISTS cd_ccc_imp_and_click;
CREATE TEMP TABLE cd_ccc_imp_and_click AS (
    SELECT DISTINCT 
        samsung_tvid_pii_virtual_id AS vtifa,
        fact.type AS event_type, --CASE WHEN fact.type = 1 THEN 'impression' WHEN fact.type = 2 THEN 'click' END AS event_type,
        creative_id,
        campaign_id,
        c.creative_name,
        c.placement_id,
        c.placement_name,
        c.campaign_start_date,
        c.campaign_end_date,
        event_time AS timing,
        MIN(fact.event_time) OVER(PARTITION BY vtifa, fact.type) AS first_day
    FROM data_ad_xdevice.fact_delivery_event_without_pii AS fact
        JOIN creative_map c USING (campaign_id, creative_id)
    WHERE 
        DATE_TRUNC('day', fact.event_time) BETWEEN TO_TIMESTAMP($report_start_date,'YYYYMMDDHH') AND TO_TIMESTAMP($report_end_date,'YYYYMMDDHH')
        AND fact.type IN (1, 2)
        -- AND (dropped != TRUE or dropped is null)
        AND device_country IN ($country)  
        -- AND psid IS NOT NULL 
);



DROP TABLE IF EXISTS exposure_stats_imp_and_click;
CREATE TEMP TABLE exposure_stats_imp_and_click AS (
    SELECT DISTINCT
        date,
        creative_id,
        campaign_id,
        creative_name,
        placement_id,
        placement_name,
        campaign_start_date,
        campaign_end_date,
        COALESCE(SUM(impression), 0) AS impression,
        COALESCE(SUM(click), 0) AS click
    FROM (
        SELECT
            DATE_TRUNC('day', timing) AS date,
            creative_id,
            campaign_id,
            creative_name,
            placement_id,
            placement_name,
            campaign_start_date,
            campaign_end_date,
            COUNT(*) AS impression
        FROM cd_ccc_imp_and_click
        WHERE event_type = 1
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
    )
        LEFT JOIN (
            SELECT
                DATE_TRUNC('day', timing) AS date,
                creative_id,
                campaign_id,
                creative_name,
                placement_id,
                placement_name,
                campaign_start_date,
                campaign_end_date,
                COUNT(*) AS click
            FROM cd_ccc_imp_and_click
            WHERE event_type = 2
            GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
        ) AS foo USING (date, creative_id, campaign_id, placement_id, creative_name, placement_name, campaign_start_date, campaign_end_date)
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
);



DROP TABLE IF EXISTS page_visitors;
CREATE TEMP TABLE page_visitors AS (
    SELECT 
        event_time AS timing,
        segment_id, 
        c.vtifa
    FROM data_ad_xdevice.fact_delivery_event_without_pii a  
        JOIN ip_psid_map c ON a.device_ip_pii_virtual_id = c.vip
    WHERE type = 3
        AND segment_id IN (52832, 52833) 
        AND udw_partition_datetime BETWEEN TO_TIMESTAMP($report_start_date,'YYYYMMDDHH') AND TO_TIMESTAMP($report_end_date,'YYYYMMDDHH')
        AND device_country = $country
);



DROP TABLE IF EXISTS first_page_visitors;
CREATE TEMP TABLE first_page_visitors AS (
    SELECT
        vtifa,
        segment_id,
        MIN(timing) AS timing_first_open
    FROM page_visitors
    GROUP BY 1, 2
);



DROP TABLE IF EXISTS app_usage;
CREATE TEMP TABLE app_usage AS (
    SELECT
        vtifa,
        vpsid,
        start_timestamp AS timing,
        SUM(DATEDIFF('minutes', start_timestamp, end_timestamp)) AS time_spent_min
    FROM data_tv_acr.fact_app_usage_session_without_pii AS fact
        LEFT JOIN udw_prod.udw_lib.virtual_psid_tifa_mapping_v AS m ON m.vpsid = fact.psid_pii_virtual_id
    WHERE 
        app_id IN (SELECT app_id FROM meta_apps.meta_taps_sra_app_lang_l WHERE prod_nm = 'Paramount+')  -- Change Channel name
        AND fact.country IN ($country)
        AND DATEDIFF('second', start_timestamp, end_timestamp) > 60
        AND udw_partition_datetime BETWEEN DATEADD(month, -18, TO_TIMESTAMP($report_end_date, 'YYYYMMDDHH')) AND TO_TIMESTAMP($report_end_date,'YYYYMMDDHH')
    GROUP BY 1, 2, 3
);



DROP TABLE IF EXISTS first_app_open;
CREATE TEMP TABLE first_app_open AS (
    SELECT
        vtifa,
        vpsid,
        MIN(timing) AS timing_first_open
    FROM app_usage
    GROUP BY 1, 2
);



DROP TABLE IF EXISTS daily_downloads_table;
CREATE TEMP TABLE daily_downloads_table AS (
    SELECT
        DATE_TRUNC('day', timing) AS partition_date,
        COUNT(DISTINCT vtifa) AS daily_downloads
    FROM app_usage
        JOIN first_app_open USING(vpsid)
    WHERE 
        timing = timing_first_open
        -- AND partition_date BETWEEN (SELECT MIN(campaign_start_date) FROM creative_map) AND (SELECT MAX(campaign_end_date) FROM creative_map)
    GROUP BY 1
);



DROP TABLE IF EXISTS daily_visits_table;
CREATE TEMP TABLE daily_visits_table AS (
    SELECT
        DATE_TRUNC('day', timing) AS partition_date,
        segment_id,
        COUNT(DISTINCT vtifa) AS daily_visits
    FROM page_visitors
    JOIN first_page_visitors USING(vtifa, segment_id)
    WHERE 
        timing = timing_first_open
        -- AND partition_date BETWEEN (SELECT MIN(campaign_start_date) FROM creative_map) AND (SELECT MAX(campaign_end_date) FROM creative_map)
    GROUP BY 1, 2
);



DROP TABLE IF EXISTS exposed_app_open;
CREATE TEMP TABLE exposed_app_open AS (
    SELECT
        DATE_TRUNC('day', b.timing) AS date,
        creative_name,
        placement_name,
        COUNT(DISTINCT a.vtifa) AS count_exposed_app_open,
        SUM(time_spent_min) AS total_time_spent_min
    FROM cd_ccc_imp_and_click a
        JOIN (
            SELECT * 
            FROM app_usage 
            WHERE timing >= (SELECT MIN(campaign_start_date) FROM creative_map)
        ) b ON (
            a.vtifa = b.vtifa 
            AND a.timing <= b.timing
        )
    GROUP BY 1, 2, 3
);



DROP TABLE IF EXISTS daily_visits_table;
CREATE TEMP TABLE daily_visits_table AS (
    SELECT
        DATE_TRUNC('day', timing) AS partition_date,
        segment_id,
        COUNT(DISTINCT vtifa) AS daily_visits
    FROM page_visitors
        JOIN first_page_visitors USING(vtifa, segment_id)
    WHERE 
        timing = timing_first_open
        -- AND partition_date BETWEEN (SELECT MIN(campaign_start_date) FROM creative_map) AND (SELECT MAX(campaign_end_date) FROM creative_map)
    GROUP BY 1, 2
);



DROP TABLE IF EXISTS exposed_app_open;
CREATE TEMP TABLE exposed_app_open AS (
    SELECT
        DATE_TRUNC('day', b.timing) AS date,
        creative_name,
        placement_name,
        COUNT(DISTINCT a.vtifa) AS count_exposed_app_open,
        SUM(time_spent_min) AS total_time_spent_min
    FROM cd_ccc_imp_and_click a
        JOIN (
            SELECT * 
            FROM app_usage 
            WHERE timing >= (SELECT MIN(campaign_start_date) FROM creative_map)
        ) b ON (a.vtifa = b.vtifa AND a.timing <= b.timing)
    GROUP BY 1, 2, 3
);



DROP TABLE IF EXISTS exposed_first_time_open;
CREATE TEMP TABLE exposed_first_time_open AS (
    SELECT DISTINCT
        DATE_TRUNC('day', timing) AS date_exposed,
        DATE_TRUNC('day', timing_first_open) AS date_first_open,
        creative_name,
        placement_name,
        rn,
        campaign_id,
        placement_id,
        creative_id,
        COUNT(DISTINCT vtifa) AS count_exposed_first_app_open
    FROM (
        SELECT
            a.vtifa,
            a.timing,
            b.timing_first_open,
            creative_name,
            placement_name,
            campaign_id,
            placement_id,
            creative_id,
            DATEDIFF(second, a.timing, b.timing_first_open) AS diff_timing,
            ROW_NUMBER() OVER(PARTITION BY a.vtifa ORDER BY diff_timing, a.timing, b.timing_first_open) AS rn
        FROM cd_ccc_imp_and_click a
            JOIN (
                SELECT * 
                FROM first_app_open 
                WHERE timing_first_open >= (SELECT MIN(campaign_start_date) FROM creative_map)
            ) b ON (
                a.vtifa = b.vtifa 
                AND a.timing <= b.timing_first_open
            )
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
    )
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
);



DROP TABLE IF EXISTS exposed_page_visit;
CREATE TEMP TABLE exposed_page_visit AS (
    SELECT
        DATE_TRUNC('day', b.timing) AS date,
        segment_id,
        creative_name,
        placement_name,
        COUNT(DISTINCT a.vtifa) AS count_exposed_page_visit
    FROM cd_ccc_imp_and_click a
        JOIN (
            SELECT * 
            FROM page_visitors 
            WHERE timing >= (SELECT MIN(campaign_start_date) FROM creative_map)
        ) b ON (
            a.vtifa = b.vtifa 
            AND a.timing <= b.timing
        )
        -- WHERE a.creative_id IN (231373, 231374)
    GROUP BY 1, 2, 3, 4
);



DROP TABLE IF EXISTS exposed_first_time_visit;
CREATE TEMP TABLE exposed_first_time_visit AS (
    SELECT
        DATE_TRUNC('day', timing_first_open) AS date,
        segment_id,
        creative_name,
        placement_name,
        COUNT(DISTINCT a.vtifa) AS count_exposed_first_page_visit
    FROM cd_ccc_imp_and_click a
        JOIN (
            SELECT * 
            FROM first_page_visitors 
            WHERE timing_first_open >= (SELECT MIN(campaign_start_date) FROM creative_map)
        ) b ON (
            a.vtifa = b.vtifa 
            AND a.timing <= b.timing_first_open
        )
        --WHERE a.creative_id IN (231373, 231374)
    GROUP BY 1, 2, 3, 4
);



/***************
 OUTPUT
***************/

-- ---------------------
-- download table
-- ---------------------
SELECT DISTINCT
    partition_date,
    COALESCE(daily_app_downloads, 0) AS daily_app_downloads,
    COALESCE(daily_visits_SIGNUP, 0) AS daily_visits_SIGNUP,
    COALESCE(daily_visits_HOMEPAGE, 0) AS daily_visits_HOMEPAGE
FROM (
    (
        SELECT
            partition_date,
            SUM(daily_downloads) AS daily_app_downloads
        FROM daily_downloads_table
        GROUP BY 1
    ) AS a
        LEFT JOIN (
            SELECT
                partition_date,
                SUM(daily_visits) AS daily_visits_SIGNUP
            FROM daily_visits_table
            WHERE segment_id IN (52832)  -- Website Sign Up Confirmation
            GROUP BY 1
        ) AS b USING (partition_date)
        LEFT JOIN (
            SELECT
                partition_date,
                SUM(daily_visits) AS daily_visits_HOMEPAGE
            FROM daily_visits_table
            WHERE segment_id IN (52833)  -- HOMEPAGE
            GROUP BY 1
        ) AS c USING (partition_date)
)
WHERE 
    DATE_TRUNC('day', a.partition_date) BETWEEN TO_TIMESTAMP($report_start_date,'YYYYMMDDHH') AND TO_TIMESTAMP($report_end_date,'YYYYMMDDHH')
ORDER BY 1;




-- ---------------------
-- campaign table
-- ---------------------
SELECT
    'Paramount+_Q423 Initiatives' AS campaign_name,
    campaign_id,
    placement_id,
    creative_id,
    creative_name,
    placement_name,
    date AS date_of_delivery,
    campaign_start_date,
    campaign_end_date,
    impression,
    click,
    COALESCE(count_exposed_app_open, 0) AS count_exposed_app_open,
    COALESCE(count_exposed_first_app_open, 0) AS count_exposed_first_app_open,
    COALESCE(total_time_spent_min/count_exposed_app_open, 0) AS avg_min_spent_among_exposed,
    COALESCE(count_exposed_page_visit_SIGNUP, 0) AS count_exposed_page_visit_SIGNUP,
    COALESCE(count_exposed_first_page_visit_SIGNUP, 0) AS count_exposed_first_page_visit_SIGNUP,
    COALESCE(count_exposed_page_visit_HOMEPAGE, 0) AS count_exposed_page_visit_HOMEPAGE,
    COALESCE(count_exposed_first_page_visit_HOMEPAGE, 0) AS count_exposed_first_page_visit_HOMEPAGE
FROM exposure_stats_imp_and_click
    LEFT JOIN exposed_app_open USING (date, creative_name, placement_name)
    LEFT JOIN (
        SELECT
            date_first_open AS date,
            creative_name,
            placement_name,
            campaign_id,
            placement_id,
            creative_id,
            SUM(count_exposed_first_app_open) AS count_exposed_first_app_open
        FROM exposed_first_time_open
        WHERE rn = 1
        GROUP BY 1, 2, 3, 4, 5, 6
    ) AS a USING (date, creative_name, placement_name)
    LEFT JOIN daily_downloads_table b ON date = b.partition_date
    LEFT JOIN (
        SELECT
            date,
            creative_name,
            placement_name,
            SUM(CASE WHEN segment_id IN (52832) THEN count_exposed_page_visit ELSE 0 END) AS count_exposed_page_visit_SIGNUP,  -- Website Sign Up Confirmation
            SUM(CASE WHEN segment_id IN (52833) THEN count_exposed_page_visit ELSE 0 END) AS count_exposed_page_visit_HOMEPAGE  -- HOMEPAGE
        FROM exposed_page_visit
        GROUP BY 1,2,3
    ) AS c USING (date, creative_name, placement_name)
    LEFT JOIN (
        SELECT
            date,
            creative_name,
            placement_name,
            SUM(CASE WHEN segment_id IN (52832) THEN count_exposed_first_page_visit ELSE 0 END) AS count_exposed_first_page_visit_SIGNUP,  -- Website Sign Up Confirmation
            SUM(CASE WHEN segment_id IN (52833) THEN count_exposed_first_page_visit ELSE 0 END) AS count_exposed_first_page_visit_HOMEPAGE  -- HOMEPAGE
        FROM exposed_first_time_visit
        GROUP BY 1,2,3
    ) AS d USING (date, creative_name, placement_name)
;


-- ---------------------
-- time table
-- ---------------------
DROP TABLE IF EXISTS CD;
CREATE TEMP TABLE CD AS (
    SELECT DISTINCT 
        samsung_tvid_pii_virtual_id as vtifa,
        creative_id, 
        campaign_id, 
        DATE_TRUNC('day', fact.event_time) AS partition_date
    FROM data_ad_xdevice.fact_delivery_event_without_pii AS fact
    WHERE 
        DATE_TRUNC('day', fact.udw_partition_datetime) BETWEEN TO_TIMESTAMP($report_start_date,'YYYYMMDDHH') AND TO_TIMESTAMP($report_end_date,'YYYYMMDDHH')
        AND campaign_id IN (SELECT DISTINCT campaign_id FROM place_mapping)
        AND fact.type = 1
        AND (dropped != TRUE OR dropped IS NULL)
        AND device_country IN ($country)
);



DROP TABLE IF EXISTS app_usage_15s;
CREATE TEMP TABLE app_usage_15s AS (
    SELECT DISTINCT
        vpsid,
        vtifa,
        DATE_TRUNC('day', fact.start_timestamp) AS partition_date
    FROM data_tv_acr.fact_app_usage_session_without_pii AS fact
        LEFT JOIN udw_prod.udw_lib.virtual_psid_tifa_mapping_v AS m ON m.vpsid = fact.psid_pii_virtual_id
    WHERE 
        app_id IN (SELECT app_id FROM meta_apps.meta_taps_sra_app_lang_l WHERE prod_nm = 'Paramount+')
        AND fact.country IN ($country)
        AND DATE_TRUNC('day', fact.start_timestamp) BETWEEN TO_TIMESTAMP($report_start_date,'YYYYMMDDHH') AND TO_TIMESTAMP($report_end_date,'YYYYMMDDHH')
        AND DATEDIFF('second', start_timestamp, end_timestamp) > 15
);




DROP TABLE IF EXISTS app_usage_60s;
CREATE TEMP TABLE app_usage_60s AS (
    SELECT DISTINCT
        vpsid,
        vtifa,
        DATE_TRUNC('day', fact.start_timestamp) AS partition_date
    FROM data_tv_acr.fact_app_usage_session_without_pii AS fact
        LEFT JOIN udw_prod.udw_lib.virtual_psid_tifa_mapping_v AS m ON m.vpsid = fact.psid_pii_virtual_id
    WHERE 
        app_id IN (SELECT app_id FROM meta_apps.meta_taps_sra_app_lang_l WHERE prod_nm = 'Paramount+')
        AND fact.country IN ($country)
        AND DATE_TRUNC('day', fact.start_timestamp) BETWEEN TO_TIMESTAMP($report_start_date,'YYYYMMDDHH') AND TO_TIMESTAMP($report_end_date,'YYYYMMDDHH')
        AND DATEDIFF('second', start_timestamp, end_timestamp) > 60
);




DROP TABLE IF EXISTS app_usage_300s;
CREATE TEMP TABLE app_usage_300s AS (
    SELECT DISTINCT
        vpsid,
        vtifa,
        DATE_TRUNC('day', fact.start_timestamp) AS partition_date
    FROM data_tv_acr.fact_app_usage_session_without_pii AS fact
        LEFT JOIN udw_prod.udw_lib.virtual_psid_tifa_mapping_v AS m ON m.vpsid = fact.psid_pii_virtual_id
    WHERE 
        app_id IN (SELECT app_id FROM meta_apps.meta_taps_sra_app_lang_l WHERE prod_nm = 'Paramount+')
        AND fact.country IN ($country)
        AND DATE_TRUNC('day', fact.start_timestamp) BETWEEN TO_TIMESTAMP($report_start_date,'YYYYMMDDHH') AND TO_TIMESTAMP($report_end_date,'YYYYMMDDHH')
        AND DATEDIFF('second', start_timestamp, end_timestamp) > 300
);



SELECT 
    '1min time spent' AS ts, 
    line_item_name, 
    COUNT(DISTINCT a.vtifa) as count_conversion
FROM CD a
    JOIN app_usage_60s b ON (a.vtifa = b.vtifa AND a.partition_date <= b.partition_date)
    JOIN place_mapping USING(campaign_id)
    JOIN first_app_open c ON a.vtifa = c.vtifa
WHERE 
    DATE_TRUNC('day', c.timing_first_open) BETWEEN TO_TIMESTAMP($report_start_date,'YYYYMMDDHH') AND TO_TIMESTAMP($report_end_date,'YYYYMMDDHH')
GROUP BY 1, 2
UNION
SELECT 
    '5min time spent' AS ts, 
    line_item_name, 
    COUNT(DISTINCT a.vtifa) as count_conversion
FROM CD a
    JOIN app_usage_300s b ON (a.vtifa = b.vtifa AND a.partition_date <= b.partition_date)
    JOIN place_mapping USING(campaign_id)
    JOIN first_app_open c ON a.vtifa = c.vtifa
WHERE 
    DATE_TRUNC('day', c.timing_first_open) BETWEEN TO_TIMESTAMP($report_start_date,'YYYYMMDDHH') AND TO_TIMESTAMP($report_end_date,'YYYYMMDDHH')
GROUP BY 1, 2
ORDER BY 1, 2
;


-- ---------------------
-- unused
-- ---------------------
SELECT
    a.creative_name,
    a.placement_name,
    date_first_open AS date,
    SUM(CASE WHEN rn = 1 THEN a.count_exposed_first_app_open ELSE 0 END) AS downloads,
    COALESCE(SUM(b.imps), 0) AS imps
FROM exposed_first_time_open a
    LEFT JOIN (
        SELECT
            creative_name,
            placement_name,
            date_exposed,
            SUM(count_exposed_first_app_open) AS imps
        FROM exposed_first_time_open
        GROUP BY 1,2,3
    ) b ON a.creative_name = b.creative_name
        AND a.placement_name = b.placement_name
        AND a.date_first_open = b.date_exposed
WHERE 
    a.date_first_open BETWEEN TO_TIMESTAMP($report_start_date,'YYYYMMDDHH') AND TO_TIMESTAMP($report_end_date,'YYYYMMDDHH')
GROUP BY 1, 2, 3;

