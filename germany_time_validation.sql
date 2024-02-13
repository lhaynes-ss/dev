-- EU Monthly
-- must be run on CDW EU
/**

Pluto - Germany | Time spenf off

Today I will try to verify the theory  by:

trying to extract and isolate a single tifa that has a reasonable number of impressions and app usage sessions over 2 weeks 
and then calculate time spent using both A and B.  
I can then try to set up a time to go over my findings with DJ or Maeve to discuss how to implement a fix.


SELECT 
    tifa
    ,date_first_open
    ,COUNT(*) AS imps
    ,MIN(expose_date) AS first_imp
    ,MAX(expose_date) AS last_imp
FROM exposure_log
join first_app_open f using (tifa, country)
GROUP BY 1, 2
HAVING DATEDIFF(DAY, first_imp, last_imp) >= 7
AND first_imp <= date_first_open
LIMIT 1000;

**/
/**
=================
FIND AND REPLACE
=================
Begining of Month: '2023-12-01' -- 'YYYY-MM-DD'
End of Month: '2023-12-31' -- 'YYYY-MM-DD'
End of Month with Attribution Window: '2024-01-14' -- 'YYYY-MM-DD'
**/


-- TEST TIFA: '54d5b671-2fac-09e1-4c52-d0dc13e1fe71'

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




-- get app ID
DROP TABLE IF EXISTS app_program_id;
CREATE temp TABLE app_program_id diststyle ALL AS (
    SELECT DISTINCT
        prod_nm,
        app_id
    FROM meta_apps.meta_taps_sra_app_lang_l
    WHERE prod_nm IN ('Pluto TV')
);

ANALYZE app_program_id;
-- SELECT * FROM app_program_id;



-- get impressions for the month where campaign id in mapping file
DROP TABLE IF EXISTS exposure_log;
CREATE TEMP TABLE exposure_log AS (
    SELECT
        device_country as country,
        samsung_tvid AS tifa,
        fact.event_time,
        DATE_TRUNC('day', fact.event_time) AS expose_date,
        creative_id,
        campaign_id
    FROM data_ad_xdevice.fact_delivery_event AS fact
    WHERE DATE_TRUNC('day', fact.event_time) BETWEEN '2023-12-01' and '2023-12-31'
        AND fact.campaign_id in (SELECT DISTINCT campaign_id FROM place_mapping) -- (SELECT DISTINCT campaign_id FROM campaign_meta)
        AND fact.type = 1 -- impression
        AND (dropped != TRUE or dropped is null)
        AND device_country IN ('FR','ES','IT','DE','AT','GB')
        -- AND samsung_tvid = '54d5b671-2fac-09e1-4c52-d0dc13e1fe71' --- !!!!!! TESTING !!!!!!!!!!!!!!!!!!!!!!!!
);

-- SELECT * FROM exposure_log LIMIT 100;

-- tifa to test with
-- 54d5b671-2fac-09e1-4c52-d0dc13e1fe71 = 12 impressions between 12/2 and 12/8
/**
SELECT 
    tifa
    ,COUNT(*) AS imps
    ,MIN(expose_date) AS first_imp
    ,MAX(expose_date) AS last_imp
FROM exposure_log
GROUP BY 1
LIMIT 1000;
**/



-- aggregate impressions by country/day; get counts
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

-- SELECT * FROM exposure_stats LIMIT 100;



-- get clicks for the month where campaign id in mapping file
DROP TABLE IF EXISTS click_log;
CREATE TEMP TABLE click_log AS (
    SELECT
    device_country as country,
        samsung_tvid AS tifa,
        fact.event_time,
        DATE_TRUNC('day', fact.event_time) AS expose_date,
        creative_id,
        campaign_id
    FROM data_ad_xdevice.fact_delivery_event AS fact
    WHERE DATE_TRUNC('day', fact.event_time) BETWEEN '2023-12-01' and '2023-12-31'
        AND fact.campaign_id in (SELECT DISTINCT campaign_id FROM place_mapping) -- (SELECT DISTINCT campaign_id FROM campaign_meta)
        AND fact.type = 2 -- click
        AND (dropped != TRUE or dropped is null)
        AND device_country IN ('FR','ES','IT','DE','AT','GB')
        -- AND samsung_tvid = '54d5b671-2fac-09e1-4c52-d0dc13e1fe71' --- !!!!!! TESTING !!!!!!!!!!!!!!!!!!!!!!!!
);

-- SELECT * FROM click_log LIMIT 100;

-- tifa to test with
-- 54d5b671-2fac-09e1-4c52-d0dc13e1fe71 = No clicks. Clicks irrelevant
/**
SELECT 
    tifa
    ,COUNT(*) AS clicks
    ,MIN(expose_date) AS first_click
    ,MAX(expose_date) AS last_click
FROM click_log
GROUP BY 1
LIMIT 1000;
**/


-- aggregate clicks by country/day; get counts
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

-- SELECT * FROM click_stats LIMIT 100;



-- get creatives from mapping file
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

-- SELECT distinct * FROM creative_map limit 100;



-- get app usage over 1 minute for app
-- original version
DROP TABLE IF EXISTS app_usage;
CREATE TEMP TABLE app_usage diststyle ALL AS (

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
      AND DATE_TRUNC('day', fact.start_timestamp) BETWEEN '2023-12-01' and '2024-01-14' -- was commented out
  )

  SELECT 
    country
    ,tifa
    ,start_timestamp
    ,end_timestamp
    ,DATE_TRUNC('day', start_timestamp) AS partition_date
    ,SUM(DATEDIFF('minutes', start_timestamp, end_timestamp)) AS time_spent_min
    ,ROUND(time_spent_min/60,2) AS time_spent_hour 
  FROM temp_cte
  GROUP BY 1, 2, 3, 4

);

/*
SELECT * FROM app_usage WHERE tifa = '54d5b671-2fac-09e1-4c52-d0dc13e1fe71' 
AND CAST(partition_date AS DATE) = CAST('2023-12-11' AS DATE) 
ORDER BY start_timestamp;
*/


-- SELECT * FROM app_usage limit 100;
-- test user time spent: 2 mins, 1 use, 12/1
/**
DROP TABLE IF EXISTS app_usage_temp;
CREATE TEMP TABLE app_usage_temp AS (
    SELECT * FROM app_usage 
    WHERE tifa = '54d5b671-2fac-09e1-4c52-d0dc13e1fe71' 
    limit 100
);

DROP TABLE IF EXISTS app_usage;
CREATE TEMP TABLE app_usage AS (
    SELECT * FROM app_usage_temp
);
**/



-- get first app open from app usage table
DROP TABLE IF EXISTS first_app_open;
CREATE TEMP TABLE first_app_open AS (
    SELECT 
        country, 
        tifa, 
        MIN(partition_date) AS date_first_open
    FROM app_usage
    GROUP BY 1,2
);
/**
-- SELECT * FROM first_app_open limit 100;
SELECT * FROM first_app_open 
-- WHERE tifa = '54d5b671-2fac-09e1-4c52-d0dc13e1fe71' 
limit 100;
**/


-- get first app usage counts by day and country
DROP TABLE IF EXISTS daily_downloads_table;
CREATE TEMP TABLE daily_downloads_table AS (
    SELECT 
        first_app_open.country, 
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

-- SELECT * FROM daily_downloads_table limit 100;




-- compare all usage to new usage; get user counts
DROP TABLE IF EXISTS app_users;
CREATE TEMP TABLE app_users AS (
    select -- all app usage for the country by month
        a.country,  
        count (distinct a.tifa) AS monthly_active_users,  
        monthly_new_users, 
        monthly_active_users-monthly_new_users AS monthly_returning_users
    from app_usage AS a
        left join ( -- get counts for new downloads (first app usage) by country for the month
            select 
                country, 
                sum(daily_downloads)  AS monthly_new_users 
            from daily_downloads_table 
            where 
                partition_date between '2023-12-01' and '2024-01-14' 
            group by 1  
        ) AS b using(country)
    where 
        a.partition_date between '2023-12-01' and '2024-01-14'
    group by 1,3
);

-- SELECT * FROM app_users limit 100;



-- compare all usage by day to new usage by day; get user counts
DROP TABLE IF EXISTS app_users_daily;
CREATE TEMP TABLE app_users_daily AS (
    select distinct  -- all app usage for the country by day
        a.country, 
        partition_date AS date,  
        count (distinct a.tifa) AS monthly_active_users,  
        daily_downloads AS monthly_new_users, 
        monthly_active_users-monthly_new_users AS monthly_returning_users
    from app_usage AS a
        left join daily_downloads_table using(country, partition_date)
    where 
        a.partition_date between '2023-12-01' and '2024-01-14'
    group by 1,2,4
);

-- select * from app_users_daily limit 100;



-- Conversions from start of campaign
-- Join impressions on app usage; sequential, within attribution window
DROP TABLE IF EXISTS exposed_app_open_;
CREATE TEMP TABLE exposed_app_open_ as (
    SELECT distinct 
        a.country,
        expose_date as date, 
        creative_id, 
        campaign_id,  
        a.tifa,
        event_time
    FROM ( -- impressions and clicks
        SELECT * FROM exposure_log 
        UNION 
        SELECT * FROM click_log
    ) a
        JOIN ( -- app usage
            SELECT * 
            FROM app_usage 
            WHERE partition_date >= (SELECT MIN(campaign_start_date) FROM creative_map)
        ) b
    ON (
        a.tifa = b.tifa 
        AND a.expose_date <= b.partition_date 
        and a.country = b.country 
        and datediff(day, expose_date, partition_date )<=14
    )
);

-- SELECT * FROM exposed_app_open_ limit 100;



-- Join impressions on app usage to get time used
DROP TABLE IF EXISTS exposed_app_open_time;
CREATE TEMP TABLE exposed_app_open_time AS (

    /**
        Objective:
        Only credit the app usage time to the last impression before the usage.
    **/

    -- get relevant app usage
    WITH usage_cte AS (
        SELECT 
            ROW_NUMBER() OVER(PARTITION BY country, tifa ORDER BY start_timestamp) AS use_id -- unique id for add sessions
            ,country
            ,tifa
            ,start_timestamp
            ,partition_date AS event_date
            ,time_spent_min
            ,time_spent_hour 
        FROM app_usage 
        WHERE 
            partition_date >= (SELECT MIN(campaign_start_date) FROM creative_map)
    )
      
    -- join impressions onto usage and sort to determine last impression event before each app usage for attribution
    ,timing_cte AS (
        SELECT DISTINCT 
            u.use_id
            ,a.country
            ,a.date
            ,a.creative_id
            ,a.campaign_id
            ,a.tifa
            ,u.time_spent_min
            ,a.event_time
            ,u.start_timestamp
            ,ROW_NUMBER() OVER(PARTITION BY u.use_id, a.country, a.tifa ORDER BY a.event_time DESC) AS rn -- used to get last event before conversion
        FROM exposed_app_open_ a
            JOIN usage_cte u ON a.tifa = u.tifa 
                AND a.event_time <= u.start_timestamp
                AND a.country = u.country 
                AND u.event_date BETWEEN '2023-12-01' AND '2024-01-14'
    )

    -- only attribute time spent to last conversion   
    SELECT 
        t.use_id
        ,t.country
        ,t.date 
        ,t.creative_id
        ,t.campaign_id
        ,t.tifa
        ,CASE 
            WHEN t.rn = 1 
            THEN t.time_spent_min 
            ELSE 0 
        END AS time_spent_min
    FROM timing_cte t
        
);



/************
TEST OUTPUT
Notes: https://adgear.atlassian.net/wiki/spaces/~71202089b033c00f994ec898e0d54bcb43fdf5/pages/20013776982/Pluto+Global+Time+Spent+Investigation
************/

-- check time spent summing
-- with test tifa, time usage matches between raw usage table and placement usage table
SELECT SUM(time_spent_min) AS time_spent_min FROM app_usage WHERE tifa = '54d5b671-2fac-09e1-4c52-d0dc13e1fe71';
SELECT SUM(a.time_spent_min) AS time_spent_min FROM exposed_app_open_time a WHERE tifa = '54d5b671-2fac-09e1-4c52-d0dc13e1fe71';


-- run time calculations
-- output matches custom report output showing the following time_spent_min for the "Past 180 Days" placements (3994687, 3965772, 3907556)
SELECT 
   c.country
   ,c.creative_name
   ,c.placement_name
   ,SUM(a.time_spent_min) AS time_spent_min
FROM exposed_app_open_time a 
   JOIN creative_map c USING(creative_id, campaign_id, country)
WHERE c.placement_name = '12/1-12/31 Native Smart TV-1st Screen (2017+): Germany - Pluto TV App Openers Past 180 Days'
group by 1, 2, 3
;

-- check clicks
-- returns 682 vs DSP 692
SELECT SUM(click) AS clicks 
FROM click_stats
   JOIN creative_map c USING(creative_id, campaign_id, country)
WHERE c.placement_name = '12/1-12/31 Native Smart TV-1st Screen (2017+): Germany - Pluto TV App Openers Past 180 Days'
;

-- check impressions
-- returns 770,137 vs DSP 776,554
select SUM(impression) AS impressions FROM exposure_stats
   JOIN creative_map c USING(creative_id, campaign_id, country)
WHERE c.placement_name = '12/1-12/31 Native Smart TV-1st Screen (2017+): Germany - Pluto TV App Openers Past 180 Days'
;


