/**

Pluto Global Monthly Report (EU Monthly)
============================
Run on 15th of the month for previous month +14 day attribution window
-- must be run on CDW EU

Original Query: 
https://github.com/SamsungAdsAnalytics/QueryBase/blob/master/UDW/MnE/Content_Partner/Pluto_TV/EMEA_CDW/2023Q4/Pluto_EU_monthly.sql

=================
FIND AND REPLACE
=================
Begining of Month: '2023-12-01' -- 'YYYY-MM-DD'
End of Month: '2023-12-31' -- 'YYYY-MM-DD'
End of Month with Attribution Window: '2024-01-14' -- 'YYYY-MM-DD'

**/




/**
TESTING
=========================================
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

-- TEST TIFA: '54d5b671-2fac-09e1-4c52-d0dc13e1fe71'
=========================================
**/




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
        AND fact.campaign_id IN (SELECT DISTINCT campaign_id FROM place_mapping) -- (SELECT DISTINCT campaign_id FROM campaign_meta)
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

SELECT * FROM exposure_stats LIMIT 100;



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

-- SELECT distinct * FROM creative_map limit 100;



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
    SELECT 
        country
        ,tifa
        ,start_timestamp
        ,end_timestamp
        ,DATE_TRUNC('day', start_timestamp) AS partition_date
        ,SUM(DATEDIFF('minutes', start_timestamp, end_timestamp)) AS time_spent_min
        ,ROUND(time_spent_min/60, 2) AS time_spent_hour 
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



-- get first app open per country/tifa from app usage table
DROP TABLE IF EXISTS first_app_open;
CREATE TEMP TABLE first_app_open AS (
    SELECT 
        country, 
        tifa, 
        MIN(partition_date) AS date_first_open
    FROM app_usage
    GROUP BY 1,2
);

-- SELECT * FROM first_app_open limit 100;
-- SELECT * FROM first_app_open WHERE tifa = '54d5b671-2fac-09e1-4c52-d0dc13e1fe71';
-- SELECT * FROM first_app_open limit 100;



-- get first app usage counts by day and country as "downloads"
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
    GROUP BY 1, 2
);

-- SELECT * FROM daily_downloads_table limit 100;




-- monthly app usage
-- ** REVIEW LOGIC
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

SELECT * FROM app_users limit 100;



-- daily app usage
-- ** REVIEW LOGIC
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
-- ** REVIEW LOGIC
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


SELECT * FROM exposed_app_open_time a LIMIT 100;



-- aggregate time used by country, creative for conversions - by day
-- ** REVIEW LOGIC
DROP TABLE IF EXISTS exposed_app_open;
CREATE TEMP TABLE exposed_app_open diststyle ALL AS (
    SELECT 
        country, 
        date, 
        creative_id, 
        campaign_id, 
        COUNT(DISTINCT tifa) AS count_exposed_app_open, 
        SUM(time_spent_min) AS total_time_spent_min
    FROM exposed_app_open_time 
    GROUP BY 1,2,3,4
);

-- SELECT * FROM exposed_app_open ORDER by total_time_spent_min DESC limit 100;



-- aggregate time used by country, creative for conversions - for the month
-- ** REVIEW LOGIC
DROP TABLE IF EXISTS exposed_app_open_monthly;
CREATE TEMP TABLE exposed_app_open_monthly as (
    SELECT 
        country, 
        creative_id, 
        campaign_id, 
        COUNT(DISTINCT tifa) AS count_exposed_app_open, 
        SUM(time_spent_min) AS total_time_spent_min
    FROM exposed_app_open_time 
    GROUP BY 1,2,3
);

--SELECT * FROM exposed_first_time_open limit 100;



-- First time Conversions from start of campaign by day 
-- Join impressions on app usage; sequential, within attribution window - last exposure
-- ** REVIEW LOGIC
DROP TABLE IF EXISTS exposed_first_time_open;
CREATE TEMP TABLE exposed_first_time_open as (
    select  distinct 
        country, 
        expose_date as date,
        creative_id, 
        campaign_id, 
        COUNT(DISTINCT tifa) AS count_exposed_first_app_open
    from ( 
        SELECT distinct 
            a.country, 
            date_first_open, 
            expose_date,
            creative_id, 
            campaign_id, 
            b.tifa, 
            row_number() over(partition by b.tifa, date_first_open order by expose_date desc) as row_num
        FROM ( -- impressions and clicks
            SELECT * FROM exposure_log 
            UNION 
            SELECT * FROM click_log 
        ) a
            JOIN ( -- first usage
                SELECT * 
                FROM first_app_open 
                WHERE 
                    date_first_open >= (SELECT MIN(campaign_start_date) FROM creative_map)
            ) b ON (
                a.tifa = b.tifa 
                AND a.expose_date <= b.date_first_open 
                and a.country=b.country 
                and datediff(day, expose_date, date_first_open )<=14
            )
    )                      
    where row_num = 1
    GROUP BY 1,2,3,4
);



-- First time Conversions from start of campaign by month
-- Join impressions on app usage; sequential, within attribution window - last exposure
-- ** REVIEW LOGIC
DROP TABLE IF EXISTS exposed_first_time_open_monthly;
CREATE TEMP TABLE exposed_first_time_open_monthly as (
    select  distinct 
    country, 
    creative_id, 
    campaign_id, 
    COUNT(DISTINCT tifa) AS count_exposed_first_app_open
    from ( -- first time by day. Same as exposed_first_time_open above
        SELECT distinct 
            a.country, 
            date_first_open, 
            expose_date,
            creative_id, 
            campaign_id, 
            b.tifa, 
            row_number()over(partition by b.tifa, date_first_open order by expose_date desc) as row_num
        FROM ( -- impressions and clicks
            SELECT * FROM exposure_log 
            UNION 
            SELECT * FROM click_log 
        ) a
            JOIN ( -- first usage
                SELECT * 
                FROM first_app_open 
                WHERE 
                    date_first_open >= (SELECT MIN(campaign_start_date) FROM creative_map)
            ) b ON (
                a.tifa = b.tifa 
                AND a.expose_date <= b.date_first_open 
                and a.country=b.country 
                and datediff(day, expose_date, date_first_open )<=14
            )
    )                       
    where row_num=1
    GROUP BY 1,2,3
);



/*********
 OUTPUT
*********/
/**
-- ----------------
-- FR - France
-- ----------------
-- ----------------------------------------------
-- By Day
SELECT  
    country, 
    'Pluto TV' AS campaign_name, 
    m.creative_id,
    creative_name, 
    placement_name,
    TRUNC(date) AS date_of_delivery,
    TRUNC(campaign_start_date) AS campaign_start_date,
    TRUNC(campaign_end_date) AS  campaign_end_date,
    coalesce(impression,0) as impression, 
    coalesce (click,0) as click,  
    count_exposed_app_open, 
    count_exposed_first_app_open,  
    t.total_time_spent_min,    
    round(t.total_time_spent_min/60,2) total_time_spent_hour --,   daily_downloads,monthly_active_users,  monthly_new_users, monthly_returning_users
FROM  creative_map m
    left join app_users_daily  using (country)
    left join exposure_stats ex USING(country,date, creative_id, campaign_id)
    left JOIN click_stats USING(country,date, creative_id, campaign_id)
    left JOIN exposed_app_open as t USING (country,date, creative_id, campaign_id)
    left JOIN exposed_first_time_open USING (country,date, creative_id, campaign_id)
    --left JOIN daily_downloads_table a
    --ON date = a.partition_date and m.country=a.country
where 
    date between '2023-12-01' and '2023-12-31'
    and m.country='FR'
order by date
;


-- MAU By Day (FR)
select 
    a.country, 
    partition_date,
    daily_downloads,
    monthly_active_users,  
    monthly_new_users, 
    monthly_returning_users
from app_users_daily t
    JOIN daily_downloads_table a ON date = a.partition_date 
        and t.country=a.country
where a.country='FR'
order by partition_date;


-- By Month
SELECT  
    'Pluto TV' AS campaign_name, 
    m.creative_id,
    creative_name, 
    placement_name,
    TRUNC(campaign_start_date) AS campaign_start_date ,
    TRUNC(campaign_end_date) AS  campaign_end_date,
    coalesce(sum(impression),0) as impression, 
    coalesce (sum(click),0) as click,   
    count_exposed_app_open,  
    sum(count_exposed_first_app_open) as count_exposed_first_app_open, 
    t.total_time_spent_min,    
    round(total_time_spent_min/60,2) total_time_spent_hour, 
    monthly_new_users as total_daily_downloads,
    monthly_active_users,  
    monthly_new_users, 
    monthly_returning_users, 
    'FR' as countrynm
FROM  creative_map m
    join app_users b using(country)
    left join exposure_stats ex USING( country,creative_id, campaign_id)
    left JOIN click_stats USING( country, date, creative_id, campaign_id)
    left JOIN exposed_app_open_monthly as t USING ( country, creative_id, campaign_id)
    left JOIN exposed_first_time_open as g USING ( country, date,creative_id, campaign_id)
where country='FR'
group by 1,2,3,4,5,6,9,11,13,14,15,16
;
-- ----------------------------------------------


-- ----------------
-- IT - Italy
-- ----------------
-- ----------------------------------------------
-- By Day
SELECT  
    'Pluto TV' AS campaign_name, 
    m.creative_id,
    creative_name, 
    placement_name,
    TRUNC(date) AS date_of_delivery,
    TRUNC(campaign_start_date) AS campaign_start_date,
    TRUNC(campaign_end_date) AS campaign_end_date,
    coalesce(impression,0) as impression, 
    coalesce (click,0) as click,  
    count_exposed_app_open, 
    count_exposed_first_app_open,  
    t.total_time_spent_min,    
    round(t.total_time_spent_min/60,2) total_time_spent_hour --,   daily_downloads,monthly_active_users,  monthly_new_users, monthly_returning_users
FROM  creative_map m
    left join app_users_daily  using (country)
    left join exposure_stats ex USING(country,date, creative_id, campaign_id)
    left JOIN click_stats USING(country,date, creative_id, campaign_id)
    left JOIN exposed_app_open as t USING (country,date, creative_id, campaign_id)
    left JOIN exposed_first_time_open USING (country,date, creative_id, campaign_id)
    --left JOIN daily_downloads_table a
    --ON date = a.partition_date and m.country=a.country
where 
    date between '2023-12-01' and '2023-12-31'
    and m.country='IT'
order by date
;


-- MAU By Day (IT)
select  
    partition_date,
    daily_downloads,
    monthly_active_users,  
    monthly_new_users, 
    monthly_returning_users
from app_users_daily t
    JOIN daily_downloads_table a ON date = a.partition_date 
        and t.country=a.country
where a.country='IT'
order by partition_date;


-- By Month
SELECT  
    'Pluto TV' AS campaign_name, 
    m.creative_id,
    creative_name, 
    placement_name,
    TRUNC(campaign_start_date) AS campaign_start_date,
    TRUNC(campaign_end_date) AS campaign_end_date,
    coalesce(sum(impression),0) as impression, 
    coalesce (sum(click),0) as click,   
    count_exposed_app_open,  
    sum(count_exposed_first_app_open) as count_exposed_first_app_open, 
    t.total_time_spent_min ,    
    round(total_time_spent_min/60,2) total_time_spent_hour, 
    monthly_new_users as total_daily_downloads,
    monthly_active_users,  
    monthly_new_users, 
    monthly_returning_users
FROM  creative_map m
    join app_users b using(country)
    left join exposure_stats ex USING( country,creative_id, campaign_id)
    left JOIN click_stats USING( country, date, creative_id, campaign_id)
    left JOIN exposed_app_open_monthly as t USING ( country, creative_id, campaign_id)
    left JOIN exposed_first_time_open as g USING ( country, date,creative_id, campaign_id)
where country='IT'
group by 1,2,3,4,5,6,9,11,13,14,15,16
;
-- ----------------------------------------------


-- ----------------
-- ES - Spain
-- ----------------
-- ----------------------------------------------
-- By Day
SELECT  
    'Pluto TV' AS campaign_name, 
    m.creative_id,
    creative_name, 
    placement_name,
    TRUNC(date) AS date_of_delivery,
    TRUNC(campaign_start_date) AS campaign_start_date,
    TRUNC(campaign_end_date) AS  campaign_end_date,
    coalesce(impression,0) as impression, 
    coalesce (click,0) as click,  
    count_exposed_app_open, 
    count_exposed_first_app_open,  
    t.total_time_spent_min,    
    round(t.total_time_spent_min/60,2) total_time_spent_hour --,   daily_downloads,monthly_active_users,  monthly_new_users, monthly_returning_users
FROM  creative_map m
    left join app_users_daily  using (country)
    left join exposure_stats ex USING(country,date, creative_id, campaign_id)
    left JOIN click_stats USING(country,date, creative_id, campaign_id)
    left JOIN exposed_app_open as t USING (country,date, creative_id, campaign_id)
    left JOIN exposed_first_time_open USING (country,date, creative_id, campaign_id)
    --left JOIN daily_downloads_table a
    --ON date = a.partition_date and m.country=a.country
where 
    date between '2023-12-01' and '2023-12-31'
    and m.country='ES'
order by date
;


-- MAU By Day (ES)
select  
    partition_date,
    daily_downloads,
    monthly_active_users,  
    monthly_new_users, 
    monthly_returning_users
from app_users_daily t
    JOIN daily_downloads_table a ON date = a.partition_date 
        and t.country=a.country
where a.country='ES'
order by partition_date;


-- By Month
SELECT  
    'Pluto TV' AS campaign_name, 
    m.creative_id,
    creative_name, 
    placement_name,
    TRUNC(campaign_start_date) AS campaign_start_date,
    TRUNC(campaign_end_date) AS campaign_end_date,
    coalesce(sum(impression),0) as impression, 
    coalesce (sum(click),0) as click,   
    count_exposed_app_open,  
    sum(count_exposed_first_app_open) as count_exposed_first_app_open, 
    t.total_time_spent_min ,    
    round(total_time_spent_min/60,2) total_time_spent_hour, 
    monthly_new_users as total_daily_downloads,
    monthly_active_users,  
    monthly_new_users, 
    monthly_returning_users
FROM  creative_map m
    join app_users b using(country)
    left join exposure_stats ex USING( country,creative_id, campaign_id)
    left JOIN click_stats USING( country, date, creative_id, campaign_id)
    left JOIN exposed_app_open_monthly as t USING ( country, creative_id, campaign_id)
    left JOIN exposed_first_time_open as g USING ( country, date,creative_id, campaign_id)
where country='ES'
group by 1,2,3,4,5,6,9,11,13,14,15,16
;
-- ----------------------------------------------


-- ----------------
-- GB - United Kingdom (UK)
-- ----------------
-- ----------------------------------------------
-- By Day
SELECT  
    'Pluto TV' AS campaign_name, 
    m.creative_id,
    creative_name, 
    placement_name,
    TRUNC(date) AS date_of_delivery,
    TRUNC(campaign_start_date) AS campaign_start_date,
    TRUNC(campaign_end_date) AS campaign_end_date,
    coalesce(impression,0) as impression, 
    coalesce (click,0) as click,  
    count_exposed_app_open, 
    count_exposed_first_app_open,  
    t.total_time_spent_min,    
    round(t.total_time_spent_min/60,2) total_time_spent_hour --,   daily_downloads,monthly_active_users,  monthly_new_users, monthly_returning_users
FROM  creative_map m
    left join app_users_daily  using (country)
    left join exposure_stats ex USING(country,date, creative_id, campaign_id)
    left JOIN click_stats USING(country,date, creative_id, campaign_id)
    left JOIN exposed_app_open as t USING (country,date, creative_id, campaign_id)
    left JOIN exposed_first_time_open USING (country,date, creative_id, campaign_id)
    --left JOIN daily_downloads_table a
    --ON date = a.partition_date and m.country=a.country
where 
    date between '2023-12-01' and '2023-12-31'
    and m.country='GB'
order by date
;


-- MAU By Day (GB)
select  
    partition_date,
    daily_downloads,
    monthly_active_users,  
    monthly_new_users, 
    monthly_returning_users
from app_users_daily t
    JOIN daily_downloads_table a ON date = a.partition_date 
        and t.country=a.country
where a.country='GB'
order by partition_date;


-- By Month
SELECT  
    'Pluto TV' AS campaign_name, 
    m.creative_id,
    creative_name, 
    placement_name,
    TRUNC(campaign_start_date) AS campaign_start_date,
    TRUNC(campaign_end_date) AS  campaign_end_date,
    coalesce(sum(impression),0) as impression, 
    coalesce (sum(click),0) as click,   
    count_exposed_app_open,  
    sum(count_exposed_first_app_open) as count_exposed_first_app_open, 
    t.total_time_spent_min ,    
    round(total_time_spent_min/60,2) total_time_spent_hour, 
    monthly_new_users as total_daily_downloads,
    monthly_active_users,  
    monthly_new_users, 
    monthly_returning_users
FROM  creative_map m
    join app_users b using(country)
    left join exposure_stats ex USING( country,creative_id, campaign_id)
    left JOIN click_stats USING( country, date, creative_id, campaign_id)
    left JOIN exposed_app_open_monthly as t USING ( country, creative_id, campaign_id)
    left JOIN exposed_first_time_open as g USING ( country, date,creative_id, campaign_id)
where country='GB'
group by 1,2,3,4,5,6,9,11,13,14,15,16
;
-- ----------------------------------------------
**/

-- ----------------
-- DE - GERMANY
-- ----------------
-- ----------------------------------------------
-- By Day
SELECT  
    'Pluto TV' AS campaign_name, 
    m.creative_id,creative_name, 
    placement_name,
    TRUNC(date) AS date_of_delivery,
    TRUNC(campaign_start_date) AS campaign_start_date,
    TRUNC(campaign_end_date) AS campaign_end_date,
    coalesce(impression,0) as impression, 
    coalesce (click,0) as click,  
    count_exposed_app_open, 
    count_exposed_first_app_open,  
    t.total_time_spent_min,    
    round(t.total_time_spent_min/60,2) total_time_spent_hour --,   daily_downloads,monthly_active_users,  monthly_new_users, monthly_returning_users
FROM creative_map m
    LEFT JOIN app_users_daily USING(country)
    LEFT JOIN exposure_stats ex USING(country, date, creative_id, campaign_id)
    LEFT JOIN click_stats USING(country, date, creative_id, campaign_id)
    LEFT JOIN exposed_app_open AS t USING (country, date, creative_id, campaign_id)
    LEFT JOIN exposed_first_time_open USING(country, date, creative_id, campaign_id)
    --left JOIN daily_downloads_table a
    --ON date = a.partition_date and m.country=a.country
WHERE 
    DATE BETWEEN '2023-12-01' AND '2023-12-31'
    AND m.country IN ('DE','AT')
ORDER BY DATE
;


-- MAU By Day (DE)
SELECT  
    partition_date,
    daily_downloads,
    monthly_active_users,  
    monthly_new_users, 
    monthly_returning_users
FROM app_users_daily t
    JOIN daily_downloads_table a ON date = a.partition_date AND t.country=a.country
WHERE 
    a.country='DE'
ORDER BY partition_date;


-- MAU By Day (AT)
SELECT  
    partition_date,
    daily_downloads,
    monthly_active_users,  
    monthly_new_users, 
    monthly_returning_users
FROM app_users_daily t
    JOIN daily_downloads_table a ON date = a.partition_date 
        and t.country=a.country
WHERE a.country='AT'
ORDER BY partition_date;


-- By Month
SELECT  
    'Pluto TV' AS campaign_name, 
    m.creative_id,
    creative_name, 
    placement_name,
    TRUNC(campaign_start_date) AS campaign_start_date,
    TRUNC(campaign_end_date) AS  campaign_end_date,
    COALESCE(SUM(impression),0) AS impression, 
    COALESCE(SUM(click),0) AS click,   
    count_exposed_app_open,  
    SUM(count_exposed_first_app_open) AS count_exposed_first_app_open, 
    t.total_time_spent_min,    
    ROUND(total_time_spent_min/60,2) total_time_spent_hour, 
    monthly_new_users AS total_daily_downloads,
    monthly_active_users,  
    monthly_new_users, 
    monthly_returning_users
FROM creative_map m
    JOIN app_users b using(country)
    LEFT JOIN exposure_stats ex USING( country,creative_id, campaign_id)
    LEFT JOIN click_stats USING( country, date, creative_id, campaign_id)
    LEFT JOIN exposed_app_open_monthly AS t USING ( country, creative_id, campaign_id)
    LEFT JOIN exposed_first_time_open AS g USING ( country, date, creative_id, campaign_id)
WHERE 
    country IN ('DE','AT')
GROUP BY 1, 2, 3, 4, 5, 6, 9, 11, 13, 14, 15, 16
;
-- ----------------------------------------------
