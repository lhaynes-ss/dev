/**
=================
Paramount+ EU Weekly
- Run for each P+ EU country (does not include Australia)
=================
Dates: prev week.  (M - Sun)
DB: CDW EU
Instructions: https://adgear.atlassian.net/wiki/spaces/~631683943/pages/19879789397/P+and+Pluto+Coverage+Instructions

=================
FIND AND REPLACE
=================
Previous Monday: '2024-02-19' -- 'YYYY-MM-DD'
Previous Sunday: '2024-02-25' -- 'YYYY-MM-DD'
mapping file: 's3://samsung.ads.data.share/analytics/custom/vaughn/paramount/20240226_p_fr_sample.csv'
**/


-- import raw mapping file
DROP TABLE IF EXISTS raw_place_mapping;
CREATE TEMP TABLE raw_place_mapping ( 
    country VARCHAR(556),
    vao INT, -- Insertion Order ID = VAO
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
) DISTSTYLE ALL;

COPY raw_place_mapping FROM
's3://samsung.ads.data.share/analytics/custom/vaughn/paramount/20240226_p_fr_sample.csv'
iam_role 'arn:aws:iam::833376745199:role/cdw_adbiz,arn:aws:iam::571950680979:role/nyc-analytics'
removequotes DELIMITER ',' ESCAPE region AS 'us-east-1' maxerror AS 250 IGNOREHEADER 1;

--delimiter ',' escape region AS 'us-east-1' maxerror AS 250 IGNOREHEADER 1;
ANALYZE raw_place_mapping;
SELECT * FROM raw_place_mapping;


-- update file
DROP TABLE IF EXISTS place_mapping;
CREATE TEMP TABLE place_mapping AS ( 
    SELECT
        country,
        vao,
        line_item_name,
        CAST(camp_start AS TIMESTAMP) AS camp_start,
        CAST(camp_end AS TIMESTAMP) AS camp_end,
        campaign_name,
        campaign_id,
        flight_name,
        flight_id,
        creative_name,
        creative_id,
        line_item_id
    FROM raw_place_mapping
);

SELECT * FROM place_mapping;



-- get app ids here since we can't set variables in Redshift
DROP TABLE IF EXISTS app_program_id;
CREATE temp TABLE app_program_id diststyle ALL AS (
    SELECT DISTINCT
        prod_nm,
        app_id
    FROM meta_apps.meta_taps_sra_app_lang_l
    WHERE prod_nm IN ('Paramount+')
);

analyze app_program_id;
--select * from app_program_id;



-- get impressions
DROP TABLE IF EXISTS exposure_log;
CREATE TEMP TABLE exposure_log AS (
    SELECT
        device_country AS country,
        samsung_tvid AS tifa,
        DATE_TRUNC('day', fact.event_time) AS expose_date,
        creative_id,
        campaign_id
    FROM data_ad_xdevice.fact_delivery_event AS fact
    WHERE DATE_TRUNC('day', fact.event_time) BETWEEN '2024-02-19' AND '2024-02-25'
        AND fact.campaign_id IN (SELECT DISTINCT campaign_id FROM place_mapping) -- (SELECT DISTINCT campaign_id FROM campaign_meta)
        AND fact.type = 1
        AND (dropped != TRUE OR dropped IS NULL)
        AND device_country IN ('FR','DE','AT','IT')
);

--select count(*) from exposure_log;
--select * from exposure_log limit 10;



-- aggregate impression stats
DROP TABLE IF EXISTS exposure_stats;
CREATE TEMP TABLE exposure_stats AS (
    SELECT 
        country,
        expose_date AS date, 
        creative_id, 
        campaign_id, 
        COUNT(tifa) AS impression
    FROM exposure_log
    GROUP BY 1,2,3,4
);

--SELECT * FROM exposure_stats LIMIT 100;



-- get clicks
DROP TABLE IF EXISTS click_log;
CREATE TEMP TABLE click_log AS (
    SELECT
    device_country as country,
        samsung_tvid AS tifa,
        DATE_TRUNC('day', fact.event_time) AS expose_date,
        creative_id,
        campaign_id
    FROM data_ad_xdevice.fact_delivery_event AS fact
    WHERE DATE_TRUNC('day', fact.event_time) BETWEEN '2024-02-19' and '2024-02-25'
        AND fact.campaign_id in (SELECT DISTINCT campaign_id FROM place_mapping) -- (SELECT DISTINCT campaign_id FROM campaign_meta)
        AND fact.type = 2
        AND (dropped != TRUE or dropped is null)
        AND device_country IN ('FR','DE','AT','IT')
);

--SELECT * FROM click_log LIMIT 100;



-- aggregate click stats
DROP TABLE IF EXISTS click_stats;
CREATE TEMP TABLE click_stats AS (
    SELECT 
        country, 
        expose_date AS date, 
        creative_id, 
        campaign_id, 
        COUNT(tifa) AS click
    FROM click_log
    GROUP BY 1,2,3,4
);

--SELECT * FROM click_stats LIMIT 100;



-- get creative map
DROP TABLE IF EXISTS creative_map;
CREATE TEMP TABLE creative_map AS (
    SELECT DISTINCT
        country,
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

--SELECT distinct * FROM creative_map limit 100;



-- get app usage with 18 month lookback
DROP TABLE IF EXISTS app_usage;
CREATE TEMP TABLE app_usage diststyle ALL AS (
    SELECT 
        country, 
        psid_tvid(psid) as tifa, 
        DATE_TRUNC('day', fact.start_timestamp) AS partition_date,
        SUM(DATEDIFF('minutes',start_timestamp,end_timestamp)) AS time_spent_min
        FROM data_tv_acr.fact_app_usage_session AS fact
    WHERE 
        app_id IN (SELECT app_id FROM app_program_id WHERE prod_nm = 'Paramount+')  -- Change Channel name
        AND partition_datehour BETWEEN 
            (TO_CHAR(DATE_ADD('MONTH', -18, CAST('2024-02-19' AS DATE)), 'yyyymmdd') || '00') -- 18 month lookback YYYYMMDDHH
            AND (TO_CHAR(CAST('2024-02-25' AS DATE), 'yyyymmdd') || '23') -- report end date YYYYMMDDHH
        AND fact.country IN ('FR','DE','AT','IT')
        AND end_timestamp - start_timestamp > interval '60'
        --AND DATE_TRUNC('day', fact.start_timestamp) >= '2022-09-15'
    GROUP BY 1,2,3
);

--SELECT * FROM app_usage limit 100;

--select min(partition_date) from app_usage;
--SELECT DISTINCT TIZEN_APP_ID FROM APP_USAGE where tizen_app_id not in (select tizen_app_id from  lup_app_map );



-- get first app opens
DROP TABLE IF EXISTS first_app_open;
CREATE TEMP TABLE first_app_open diststyle ALL AS (
    SELECT country,tifa, MIN(partition_date) AS date_first_open
    FROM app_usage
    GROUP BY 1,2
);

Analyze first_app_open;
--SELECT * FROM first_app_open limit 100;



-- get first app opens as downloads
DROP TABLE IF EXISTS daily_downloads_table;
CREATE TEMP TABLE daily_downloads_table diststyle ALL AS (
    SELECT 
        app_usage.country, 
        partition_date, 
        COUNT(DISTINCT tifa) AS daily_downloads
    FROM app_usage
    JOIN first_app_open USING(tifa)
    WHERE 
        partition_date = date_first_open 
        AND app_usage.country = first_app_open.country
        --AND partition_date BETWEEN (SELECT MIN(campaign_start_date) FROM creative_map) AND (SELECT MAX(campaign_end_date) FROM creative_map)
    GROUP BY 1,2
);

Analyze daily_downloads_table;



-- downloads table selection 
SELECT * 
FROM daily_downloads_table 
WHERE partition_date BETWEEN '2024-02-19' AND '2024-02-25' 
ORDER BY 1,2;



-- get exposed app opens
DROP TABLE IF EXISTS exposed_app_open;
CREATE TEMP TABLE exposed_app_open diststyle ALL AS (
    SELECT 
        a.country,
        partition_date AS date, 
        creative_id, 
        campaign_id, 
        COUNT(DISTINCT a.tifa) AS count_exposed_app_open, 
        SUM(time_spent_min) AS total_time_spent_min
    FROM (SELECT * FROM exposure_log UNION SELECT * FROM click_log) a
        JOIN (SELECT * FROM app_usage WHERE partition_date >= (SELECT MIN(campaign_start_date) FROM creative_map)) b ON (
            a.tifa = b.tifa 
            AND a.expose_date <= b.partition_date 
            AND a.country = b.country
        )
    GROUP BY 1,2,3,4
);

--SELECT * FROM exposed_app_open limit 100;



-- get first time app opens
DROP TABLE IF EXISTS exposed_first_time_open;
CREATE TEMP TABLE exposed_first_time_open diststyle ALL AS (
    SELECT 
        country, 
        date_first_open AS date, 
        creative_id, 
        campaign_id, 
        COUNT(DISTINCT tifa) AS count_exposed_first_app_open
    FROM ( 
        SELECT DISTINCT 
            a.country, 
            date_first_open, 
            expose_date,
            creative_id, 
            campaign_id, 
            b.tifa, 
            ROW_NUMBER() OVER(PARTITION BY b.tifa, date_first_open ORDER BY expose_date DESC) AS row_num
        FROM (SELECT * FROM exposure_log UNION SELECT * FROM click_log) a
        JOIN (SELECT * FROM first_app_open WHERE date_first_open >= (SELECT MIN(campaign_start_date) FROM creative_map)) b ON (
            a.tifa = b.tifa 
            AND a.expose_date <= b.date_first_open 
            AND a.country = b.country
        )
    )
    WHERE row_num = 1
    GROUP BY 1,2,3,4
);

--SELECT * FROM exposed_first_time_open limit 100;



-- campaign table selection
SELECT
    -- campaign_id,
    country,
    'Paramount+' AS campaign_name,
    campaign_id,
    placement_id,
    creative_id,
    creative_name,
    placement_name,
    DATE AS date_of_delivery,
    campaign_start_date,
    campaign_end_date,
    impression,
    click,
    count_exposed_app_open,
    count_exposed_first_app_open,
    COALESCE(total_time_spent_min/count_exposed_app_open, 0) AS avg_min_spent_among_exposed
FROM exposure_stats
    LEFT JOIN click_stats USING(date, creative_id, campaign_id,country)
    JOIN creative_map USING (campaign_id,creative_id,country)
    LEFT JOIN exposed_app_open USING (date, creative_id, campaign_id, country)
    LEFT JOIN exposed_first_time_open USING (date, creative_id, campaign_id,country)
    -- LEFT JOIN daily_downloads_table a ON date = a.partition_date
ORDER BY 1,5,2,3,4;



-- app usage 60 seconds
DROP TABLE IF EXISTS app_usage_60s;
CREATE TEMP TABLE app_usage_60s diststyle ALL AS (
    SELECT distinct
        country,
        psid_tvid(psid) as tifa,
        DATE_TRUNC('day', start_timestamp) AS partition_date
    FROM data_tv_acr.fact_app_usage_session AS fact
    WHERE 
        app_id IN (SELECT app_id FROM app_program_id WHERE prod_nm = 'Paramount+')
        AND fact.country IN ('FR','DE','AT','IT')
        AND DATE_TRUNC('day', start_timestamp) BETWEEN '2024-02-19' AND '2024-02-25'
        AND end_timestamp - start_timestamp > interval '60 seconds'
);

--SELECT * FROM app_usage_60s LIMIT 100;



-- app usage 5 minutes
DROP TABLE IF EXISTS app_usage_300s;
CREATE TEMP TABLE app_usage_300s diststyle ALL AS (
    SELECT DISTINCT
        country,
        psid_tvid(psid) AS tifa,
        DATE_TRUNC('day', start_timestamp) AS partition_date
    FROM data_tv_acr.fact_app_usage_session AS fact
    WHERE 
        app_id IN (SELECT app_id FROM app_program_id WHERE prod_nm = 'Paramount+')
        AND fact.country IN ('FR','DE','AT','IT')
        AND DATE_TRUNC('day', start_timestamp) BETWEEN '2024-02-19' AND '2024-02-25'
        AND end_timestamp - start_timestamp > interval '300 seconds'
);

--SELECT * FROM app_usage_300s LIMIT 100;

    

-- time table selection
SELECT
    country,
    ts,
    line_item_name,
    COALESCE(count_conversion, 0) AS count_conversion_imps
FROM (
    SELECT
        a.country,
        '60s time spent' AS ts,
        line_item_name,
        COUNT(DISTINCT a.tifa) AS count_conversion
    FROM exposure_log a
        JOIN app_usage_60s b ON (a.tifa = b.tifa AND a.expose_date <= b.partition_date and a.country = b.country)
        JOIN place_mapping AS c ON (a.campaign_id = c.campaign_id and a.country = c.country)
    GROUP BY 1, 2,3
    UNION
    SELECT
        a.country,
        '5min time spent' AS ts,
        line_item_name,
        COUNT(DISTINCT a.tifa) AS count_conversion
    FROM exposure_log a
        JOIN app_usage_300s b ON (a.tifa = b.tifa AND a.expose_date <= b.partition_date AND a.country = b.country)
        JOIN place_mapping AS c ON (a.campaign_id = c.campaign_id AND a.country = c.country)
    GROUP BY 1, 2,3
)
ORDER BY 1,2,3
;



