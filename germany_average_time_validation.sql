/*****************************
 GERMANY VALIDATION TEST
 Runtime: approx. 10 mins
*****************************/


-- import mapping file
DROP TABLE IF EXISTS place_mapping;
CREATE TEMP TABLE place_mapping (
    COUNTRY varchar(556),
    line_item_name varchar(556),
    camp_start TIMESTAMP,
    camp_end TIMESTAMP,
    campaign_name varchar(556),
    campaign_id INT,
    flight_name varchar(556),
    flight_id INT,
    creative_name varchar(556),
    creative_id INT
) DISTSTYLE ALL;
COPY place_mapping FROM
's3://samsung.ads.data.share/analytics/custom/vaughn/pluto/20240207_pluto_de_monthly_updated.csv'
iam_role 'arn:aws:iam::833376745199:role/cdw_adbiz,arn:aws:iam::571950680979:role/nyc-analytics'
removequotes DELIMITER ',' ESCAPE region AS 'us-east-1' maxerror AS 250 IGNOREHEADER 1;

--delimiter ',' escape region AS 'us-east-1' maxerror AS 250 IGNOREHEADER 1;
ANALYZE place_mapping;
-- SELECT * FROM place_mapping;



-- get list of creatives from mapping file
DROP TABLE IF EXISTS creative_map;
CREATE TEMP TABLE creative_map AS (
    SELECT DISTINCT
        country,
        campaign_id,
        campaign_name,
        creative_id,
        creative_name,  --creative_nm AS creative_name,
        line_item_name AS placement_name,
        camp_start AS campaign_start_date,
        camp_end AS campaign_end_date
    FROM place_mapping
);

SELECT distinct * FROM creative_map limit 100;




-- get app ID as variables not available in Redshift
DROP TABLE IF EXISTS app_program_id;
CREATE temp TABLE app_program_id diststyle ALL AS (
    SELECT DISTINCT
        prod_nm,
        app_id
    FROM meta_apps.meta_taps_sra_app_lang_l
    WHERE prod_nm IN ('Pluto TV')
);

ANALYZE app_program_id;
-- SELECT * FROM app_program_id; -- 3201808016802



-- get impressions, clicks for the month where campaign id is in the mapping file
DROP TABLE IF EXISTS cd;
CREATE TEMP TABLE cd AS (
    SELECT
        device_country as country,
        samsung_tvid AS tifa,
        fact.event_time,
        DATE_TRUNC('day', fact.event_time) AS expose_date,
        creative_id,
        campaign_id
    FROM data_ad_xdevice.fact_delivery_event AS fact
    WHERE 
        DATE_TRUNC('day', fact.event_time) BETWEEN '2023-12-01' and '2023-12-31'
        AND fact.campaign_id IN (SELECT DISTINCT campaign_id FROM place_mapping) -- (SELECT DISTINCT campaign_id FROM campaign_meta)
        AND fact.type IN ( 
                1 -- impression
                ,2 -- click
        )
        AND (dropped != TRUE or dropped is null)
        AND device_country IN ('FR','ES','IT','DE','AT','GB')
        -- AND samsung_tvid = '54d5b671-2fac-09e1-4c52-d0dc13e1fe71' --- !!!!!! TESTING !!!!!!!!!!!!!!!!!!!!!!!!
);

SELECT COUNT(DISTINCT tifa) AS reach FROM cd;



-- combine mapping and impression data
DROP TABLE IF EXISTS exposed_cd;
CREATE TEMP TABLE exposed_cd AS (
    SELECT
        c.*
        ,m.creative_name
        ,m.placement_name
        ,TRUNC(m.campaign_start_date) AS campaign_start_date
        ,TRUNC(m.campaign_end_date) AS campaign_end_date
    FROM cd AS c
        JOIN creative_map m USING(country, campaign_id, creative_id)
);

SELECT creative_name, placement_name, COUNT(*) AS exposures FROM exposed_cd GROUP BY 1, 2;




-- get app usage over 1 minute for app
DROP TABLE IF EXISTS app_usage;
CREATE TEMP TABLE app_usage diststyle ALL AS (

    -- get app usage
    WITH temp_cte AS (
        SELECT DISTINCT
            country
            ,psid_tvid(psid) AS tifa
            ,fact.start_timestamp
            ,end_timestamp
        FROM data_tv_acr.fact_app_usage_session AS fact
        WHERE 
            app_id IN (SELECT app_id FROM app_program_id WHERE prod_nm = 'Pluto TV')  -- Change Channel name
            --AND partition_date
            AND fact.country IN ('FR','ES','IT','DE','AT','GB')
            AND end_timestamp - start_timestamp > interval '60'
            -- AND DATE_TRUNC('day', fact.start_timestamp) BETWEEN '2023-12-01' and '2024-01-14' -- comment out this line or first app usage will always be within report range
    )

    -- remove duplicates
    -- app_usage_id not technically necessary. Just included for debugging
    SELECT 
        ROW_NUMBER() OVER(PARTITION BY country, tifa ORDER BY start_timestamp) AS app_usage_id
        ,country
        ,tifa
        ,start_timestamp
        ,end_timestamp
        ,DATE_TRUNC('day', start_timestamp) AS partition_date
        ,SUM(DATEDIFF('minutes', start_timestamp, end_timestamp)) AS time_spent_min
        ,ROUND(time_spent_min/60, 2) AS time_spent_hour 
    FROM temp_cte
    GROUP BY 2, 3, 4, 5

);

-- SELECT * FROM app_usage WHERE tifa = '2e6b86a8-b8ba-f073-11c9-0e0607492643' AND country = 'IT' ORDER BY app_usage_id LIMIT 1000;




-- calculate time spent, only give credit to last impression before use
DROP TABLE IF EXISTS exposed_time_cd;
CREATE TEMP TABLE exposed_time_cd AS (

    WITH timing_cte AS (
        SELECT 
            c.*
            ,u.app_usage_id 
            ,time_spent_min
            ,time_spent_hour
            ,ROW_NUMBER() OVER(PARTITION BY u.app_usage_id, c.country, c.tifa ORDER BY c.event_time DESC) AS rn -- used to get last event before conversion
        FROM exposed_cd c
            JOIN app_usage u ON c.tifa = u.tifa 
            AND c.event_time <= u.start_timestamp
            AND c.country = u.country 
            AND u.partition_date BETWEEN '2023-12-01' AND '2024-01-14'
    )
    
    SELECT 
        country
        ,tifa
        ,event_time
        ,expose_date
        ,creative_id
        ,campaign_id 
        ,creative_name
        ,placement_name
        ,campaign_start_date
        ,campaign_end_date
        ,app_usage_id
        ,CASE 
            WHEN rn = 1 
            THEN time_spent_min
            ELSE 0 
        END AS time_spent_min
    FROM timing_cte t

);


-- SELECT * FROM exposed_time_cd e WHERE tifa = 'aa7585e1-4f0d-41b5-1123-29a0af63c6c0' AND country = 'DE' ORDER BY app_usage_id LIMIT 1000;
-- SELECT creative_name, placement_name, COUNT(*) AS impressions FROM exposed_cd GROUP BY 1, 2;
-- SELECT creative_name, placement_name, COUNT(DISTINCT tifa) AS reach FROM exposed_cd GROUP BY 1, 2;




WITH impressions_cte AS (
    SELECT 
        country
        ,creative_id
        ,campaign_id
        ,COUNT(*) AS impressions 
    FROM exposed_cd 
    GROUP BY 1, 2, 3
)


,reach_cte AS (
    SELECT 
        country
        ,creative_id
        ,campaign_id
        ,COUNT(DISTINCT tifa) AS reach 
    FROM exposed_cd 
    GROUP BY 1, 2, 3
)



SELECT 
    'Pluto TV' AS campaign_name
    ,e.creative_id
    ,e.creative_name
    ,e.placement_name
    ,e.campaign_start_date
    ,e.campaign_end_date
    ,i.impressions
    ,r.reach
    ,COUNT(e.*) AS exposed_app_opens_anytouch
    ,COUNT(DISTINCT e.tifa) AS exposed_app_openers
    ,SUM(e.time_spent_min) AS ts_min
    ,ROUND(ts_min/60, 2) AS ts_hour
    ,(ts_min/exposed_app_openers) AS average_ts_mins_per_user
FROM exposed_time_cd e
    JOIN impressions_cte i USING(creative_id, campaign_id, country)
    JOIN reach_cte r USING(creative_id, campaign_id, country)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8

