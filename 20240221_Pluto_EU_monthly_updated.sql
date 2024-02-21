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
SELECT * FROM place_mapping;



-- get/store app id as redshift doesn't support setting variables
DROP TABLE IF EXISTS app_program_id;
CREATE temp TABLE app_program_id diststyle ALL AS (
    SELECT DISTINCT
        prod_nm,
        app_id
    FROM meta_apps.meta_taps_sra_app_lang_l
    WHERE prod_nm IN ('Pluto TV')
);
analyze app_program_id;



--  get impressions for campaigns in mapping file
DROP TABLE IF EXISTS exposure_log;
CREATE TEMP TABLE exposure_log AS (
    SELECT
        device_country as country,
        samsung_tvid AS tifa,
        DATE_TRUNC('day', fact.event_time) AS expose_date,
        creative_id,
        campaign_id
    FROM data_ad_xdevice.fact_delivery_event AS fact
    WHERE DATE_TRUNC('day', fact.event_time) BETWEEN '2023-12-01' and '2023-12-31'
        AND fact.campaign_id in (SELECT DISTINCT campaign_id FROM place_mapping) -- (SELECT DISTINCT campaign_id FROM campaign_meta)
        AND fact.type = 1
        AND (dropped != TRUE or dropped is null)
        AND device_country IN ('FR','ES','IT','DE','AT','GB')
);

-- SELECT * FROM exposure_stats LIMIT 100;



-- aggregate impressions by date, country, creative
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

-- SELECT * FROM exposure_log LIMIT 100;


-- get clicks for campaigns in mapping file
DROP TABLE IF EXISTS click_log;
CREATE TEMP TABLE click_log AS (
    SELECT
        device_country as country,
        samsung_tvid AS tifa,
        DATE_TRUNC('day', fact.event_time) AS expose_date,
        creative_id,
        campaign_id
    FROM data_ad_xdevice.fact_delivery_event AS fact
    WHERE DATE_TRUNC('day', fact.event_time) BETWEEN '2023-12-01' and '2023-12-31'
        AND fact.campaign_id in (SELECT DISTINCT campaign_id FROM place_mapping) -- (SELECT DISTINCT campaign_id FROM campaign_meta)
        AND fact.type = 2
        AND (dropped != TRUE or dropped is null)
        AND device_country IN ('FR','ES','IT','DE','AT','GB')
);

--SELECT * FROM click_log LIMIT 100;



-- aggregate clicks by date, country, creative 
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



-- map of creatives/placements in mapping file
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

--SELECT distinct * FROM creative_map limit 100;



-- get all app usage for Pluto TV, all time
-- !!!! potential issue with duplicates !!!!
DROP TABLE IF EXISTS app_usage;
CREATE TEMP TABLE app_usage diststyle ALL AS (
    SELECT 
        country, 
        psid_tvid(psid) as tifa, 
        DATE_TRUNC('day', fact.start_timestamp) AS partition_date,
        SUM(DATEDIFF('minutes',start_timestamp,end_timestamp)) AS time_spent_min,
        round(time_spent_min/60,2) AS time_spent_hour
    FROM data_tv_acr.fact_app_usage_session AS fact
    WHERE 
        app_id IN (SELECT app_id FROM app_program_id WHERE prod_nm = 'Pluto TV')  -- Change Channel name
        --AND partition_date
        AND fact.country IN ('FR','ES','IT','DE','AT','GB')
        AND end_timestamp - start_timestamp > interval '60'
        --AND DATE_TRUNC('day', fact.start_timestamp) >= '2022-09-15'
    GROUP BY 1,2,3
);

--when
--select partition_date, count(distinct tifa) from app_usage group by 1;



-- get the first app open for each tifa/country
DROP TABLE IF EXISTS first_app_open;
CREATE TEMP TABLE first_app_open as (
    SELECT 
        country, 
        tifa, 
        MIN(partition_date) AS date_first_open
    FROM app_usage
    GROUP BY 1,2
);

--SELECT * FROM first_app_open limit 100;



--- count first app opens as "downloads"
DROP TABLE IF EXISTS daily_downloads_table;
CREATE TEMP TABLE daily_downloads_table as (
    SELECT 
        first_app_open.country, 
        partition_date, 
        COUNT(DISTINCT tifa) AS daily_downloads
    FROM app_usage
    JOIN first_app_open USING(tifa)
    WHERE partition_date = date_first_open and app_usage.country=first_app_open.country
    --AND partition_date BETWEEN (SELECT MIN(campaign_start_date) FROM creative_map) AND (SELECT MAX(campaign_end_date) FROM creative_map)
    GROUP BY 1,2
);

--SELECT * FROM daily_downloads_table limit 100;




-- join monthly downloads on monthly app usage
Drop table if exists app_users;
create temp table app_users as (
    select 
        a.country,  
        count (distinct a.tifa) as monthly_active_users,   
        monthly_new_users, 
        monthly_active_users-monthly_new_users as monthly_returning_users
    from app_usage as a
        left join ( 
            select 
                country, 
                sum(daily_downloads) as monthly_new_users from daily_downloads_table 
            where partition_date between '2023-12-01' and '2024-01-14' group by 1  
        ) as b using(country)
    where a.partition_date between '2023-12-01' and '2024-01-14'
    group by 1,3
);



-- join daily downloads on daily app usage
Drop table if exists app_users_daily;
create temp table app_users_daily as (
    select distinct 
        a.country, 
        partition_date as date,  
        count (distinct a.tifa) as monthly_active_users,  
        daily_downloads as monthly_new_users, 
        monthly_active_users-monthly_new_users as monthly_returning_users
    from app_usage as a
        left join daily_downloads_table using(country,partition_date)
    where a.partition_date between '2023-12-01' and '2024-01-14'
    group by 1,2,4
);

--select * from app_users_daily limit 100;



-- join app usage and exposure; ANYTOUCH with 14 day attribution window
DROP TABLE IF EXISTS exposed_app_open_;
CREATE TEMP TABLE exposed_app_open_ as (
    SELECT distinct 
        a.country,
        expose_date as date, 
        creative_id, 
        campaign_id,  
        a.tifa
    FROM (SELECT * FROM exposure_log UNION SELECT * FROM click_log) a
        JOIN (SELECT * FROM app_usage WHERE partition_date >= (SELECT MIN(campaign_start_date) FROM creative_map)) b
        ON (a.tifa = b.tifa AND a.expose_date <= b.partition_date and a.country=b.country and datediff(day, expose_date, partition_date )<=14)
);

-- SELECT * FROM exposed_app_open limit 100;



-- join exposed app opens on app usage; Date of Impression. Anytouch. Time will be duplicated
DROP TABLE IF EXISTS exposed_app_open_time;
CREATE TEMP TABLE exposed_app_open_time as (
    SELECT distinct  
        a.country, 
        date,  
        creative_id, 
        campaign_id, 
        a.tifa, 
        time_spent_min
    FROM exposed_app_open_ a
        JOIN (SELECT * FROM app_usage WHERE partition_date >= (SELECT MIN(campaign_start_date) FROM creative_map)) b
        ON (a.tifa = b.tifa AND a.date <= b.partition_date and a.country=b.country and partition_date between '2023-12-01' and '2024-01-14')
    --where a.tifa='0042dc16-1b87-fa2c-ff7c-dc172080376a'
    --group by 1,2,3,4
);

--SELECT * FROM exposed_app_open_ order by country, tifa,  partition_date,  creative_id, campaign_id limit 100;



-- aggregate time spent by day
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



/*

DROP TABLE IF EXISTS exposed_app_open;
CREATE TEMP TABLE exposed_app_open as (
SELECT a.country,expose_date as date, a.creative_id, a.campaign_id, COUNT(DISTINCT a.tifa) AS count_exposed_app_open, c.time_spent_min
FROM (SELECT * FROM exposure_log UNION SELECT * FROM click_log) a
JOIN (SELECT * FROM app_usage WHERE partition_date >= (SELECT MIN(campaign_start_date) FROM creative_map)) b
ON (a.tifa = b.tifa AND a.expose_date <= b.partition_date and a.country=b.country and datediff(day, expose_date, partition_date )<=14)
join  exposed_app_open_time c on a.creative_id=c.creative_id and a.campaign_id=c.campaign_id and a.expose_date=c.partition_date 
GROUP BY 1,2,3,4,6
);
--SELECT * FROM exposed_app_open limit 100;

*/



-- aggregate time spent by month
/**
-- ORIGINAL
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
**/

-- NEW
DROP TABLE IF EXISTS exposed_app_open_monthly;
CREATE TEMP TABLE exposed_app_open_monthly as (
    SELECT distinct  
        a.country, 
        creative_id, 
        campaign_id, 
        COUNT(DISTINCT a.tifa) AS count_exposed_app_open,  
        SUM(time_spent_min) AS total_time_spent_min
    FROM exposed_app_open_ a
        JOIN (SELECT * FROM app_usage WHERE partition_date >= (SELECT MIN(campaign_start_date) FROM creative_map)) b
        ON (a.tifa = b.tifa AND a.date <= b.partition_date and a.country=b.country and partition_date between '2023-12-01' and '2024-01-14')
    GROUP BY 1,2,3
);

--SELECT * FROM exposed_first_time_open limit 100;




-- join exposure on first app opens; get first opens by day; LAST TOUCH
DROP TABLE IF EXISTS exposed_first_time_open;
CREATE TEMP TABLE exposed_first_time_open as (
    select distinct 
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
            row_number()over(partition by b.tifa, date_first_open order by expose_date desc) as row_num
        FROM (
            SELECT * FROM exposure_log 
            UNION SELECT * FROM click_log 
        ) a
            JOIN (
                SELECT * FROM first_app_open 
                WHERE date_first_open >= (SELECT MIN(campaign_start_date) FROM creative_map)
            ) b ON (
                a.tifa = b.tifa 
                AND a.expose_date <= b.date_first_open 
                and a.country=b.country 
                and datediff(day, expose_date, date_first_open )<=14
            )
    )                
    where row_num=1
    GROUP BY 1,2,3,4
);




-- join exposure on first app opens; get first opens by month; LAST TOUCH
DROP TABLE IF EXISTS exposed_first_time_open_monthly;
CREATE TEMP TABLE exposed_first_time_open_monthly as (
        select distinct 
        country, 
        creative_id, 
        campaign_id, 
        COUNT(DISTINCT tifa) AS count_exposed_first_app_open
    from ( 
        SELECT distinct a.country, date_first_open, expose_date,creative_id, campaign_id, b.tifa, row_number()over(partition by b.tifa, date_first_open order by expose_date desc) as row_num
        FROM (
            SELECT * FROM exposure_log 
            UNION SELECT * FROM click_log 
        ) a
            JOIN (
                SELECT * FROM first_app_open 
                WHERE date_first_open >= (SELECT MIN(campaign_start_date) FROM creative_map)
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
    COALESCE(impression,0) AS impression, 
    COALESCE(click,0) AS click,  
    count_exposed_app_open, 
    count_exposed_first_app_open,  
    t.total_time_spent_min,    
    ROUND(t.total_time_spent_min/60,2) total_time_spent_hour --,   daily_downloads,monthly_active_users,  monthly_new_users, monthly_returning_users
FROM creative_map m
    LEFT JOIN app_users_daily USING(country)
    LEFT JOIN exposure_stats ex USING(country,date, creative_id, campaign_id)
    LEFT JOIN click_stats USING(country,date, creative_id, campaign_id)
    LEFT JOIN exposed_app_open AS t USING (country,date, creative_id, campaign_id)
    LEFT JOIN exposed_first_time_open USING (country,date, creative_id, campaign_id)
    -- LEFT JOIN daily_downloads_table a
    -- ON date = a.partition_date AND m.country = a.country
WHERE 
    date BETWEEN '2023-12-01' AND '2023-12-31'
    AND m.country = 'FR'
ORDER BY DATE
;


-- MAU By Day (FR)
SELECT 
    a.country, 
    partition_date,
    daily_downloads,
    monthly_active_users,  
    monthly_new_users, 
    monthly_returning_users
FROM app_users_daily t
    JOIN daily_downloads_table a ON date = a.partition_date 
        AND t.country = a.country
WHERE a.country = 'FR'
ORDER BY partition_date;


-- By Month
SELECT  
    'Pluto TV' AS campaign_name, 
    m.creative_id,
    creative_name, 
    placement_name,
    TRUNC(campaign_start_date) AS campaign_start_date ,
    TRUNC(campaign_end_date) AS  campaign_end_date,
    COALESCE(SUM(impression),0) AS impression, 
    COALESCE(SUM(click),0) AS click,   
    count_exposed_app_open,  
    SUM(count_exposed_first_app_open) AS count_exposed_first_app_open, 
    t.total_time_spent_min,    
    round(total_time_spent_min/60,2) total_time_spent_hour, 
    monthly_new_users AS total_daily_downloads,
    monthly_active_users,  
    monthly_new_users, 
    monthly_returning_users, 
    'FR' AS countrynm
FROM creative_map m
    JOIN app_users b using(country)
    LEFT JOIN exposure_stats ex USING( country,creative_id, campaign_id)
    LEFT JOIN click_stats USING( country, date, creative_id, campaign_id)
    LEFT JOIN exposed_app_open_monthly AS t USING ( country, creative_id, campaign_id)
    LEFT JOIN exposed_first_time_open AS g USING ( country, date,creative_id, campaign_id)
WHERE country = 'FR'
GROUP BY 1, 2, 3, 4, 5, 6, 9, 11, 13, 14, 15, 16
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
    COALESCE(impression,0) AS impression, 
    COALESCE(click,0) AS click,  
    count_exposed_app_open, 
    count_exposed_first_app_open,  
    t.total_time_spent_min,    
    ROUND(t.total_time_spent_min/60,2) total_time_spent_hour --,   daily_downloads,monthly_active_users,  monthly_new_users, monthly_returning_users
FROM  creative_map m
    LEFT JOIN app_users_daily  using (country)
    LEFT JOIN exposure_stats ex USING(country,date, creative_id, campaign_id)
    LEFT JOIN click_stats USING(country,date, creative_id, campaign_id)
    LEFT JOIN exposed_app_open AS t USING (country,date, creative_id, campaign_id)
    LEFT JOIN exposed_first_time_open USING (country,date, creative_id, campaign_id)
    -- LEFT JOIN daily_downloads_table a
    -- ON date = a.partition_date AND m.country = a.country
WHERE 
    date BETWEEN '2023-12-01' AND '2023-12-31'
    AND m.country = 'IT'
ORDER BY date
;


-- MAU By Day (IT)
SELECT  
    partition_date,
    daily_downloads,
    monthly_active_users,  
    monthly_new_users, 
    monthly_returning_users
FROM app_users_daily t
    JOIN daily_downloads_table a ON date = a.partition_date 
        AND t.country = a.country
WHERE a.country = 'IT'
ORDER BY partition_date;


-- By Month
SELECT  
    'Pluto TV' AS campaign_name, 
    m.creative_id,
    creative_name, 
    placement_name,
    TRUNC(campaign_start_date) AS campaign_start_date,
    TRUNC(campaign_end_date) AS campaign_end_date,
    COALESCE(SUM(impression),0) AS impression, 
    COALESCE(SUM(click),0) AS click,   
    count_exposed_app_open,  
    SUM(count_exposed_first_app_open) AS count_exposed_first_app_open, 
    t.total_time_spent_min ,    
    ROUND(total_time_spent_min/60,2) total_time_spent_hour, 
    monthly_new_users AS total_daily_downloads,
    monthly_active_users,  
    monthly_new_users, 
    monthly_returning_users
FROM  creative_map m
    JOIN app_users b using(country)
    LEFT JOIN exposure_stats ex USING( country,creative_id, campaign_id)
    LEFT JOIN click_stats USING( country, date, creative_id, campaign_id)
    LEFT JOIN exposed_app_open_monthly AS t USING ( country, creative_id, campaign_id)
    LEFT JOIN exposed_first_time_open AS g USING ( country, date,creative_id, campaign_id)
WHERE country = 'IT'
GROUP BY 1, 2, 3, 4, 5, 6, 9, 11, 13, 14, 15, 16
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
    COALESCE(impression,0) as impression, 
    COALESCE(click,0) as click,  
    count_exposed_app_open, 
    count_exposed_first_app_open,  
    t.total_time_spent_min,    
    ROUND(t.total_time_spent_min/60,2) total_time_spent_hour --,   daily_downloads,monthly_active_users,  monthly_new_users, monthly_returning_users
FROM  creative_map m
    LEFT JOIN app_users_daily USING(country)
    LEFT JOIN exposure_stats ex USING(country,date, creative_id, campaign_id)
    LEFT JOIN click_stats USING(country,date, creative_id, campaign_id)
    LEFT JOIN exposed_app_open as t USING (country,date, creative_id, campaign_id)
    LEFT JOIN exposed_first_time_open USING (country,date, creative_id, campaign_id)
    -- LEFT JOIN daily_downloads_table a
    -- ON date = a.partition_date AND m.country = a.country
WHERE 
    date BETWEEN '2023-12-01' AND '2023-12-31'
    AND m.country = 'ES'
ORDER BY date
;


-- MAU By Day (ES)
SELECT  
    partition_date,
    daily_downloads,
    monthly_active_users,  
    monthly_new_users, 
    monthly_returning_users
FROM app_users_daily t
    JOIN daily_downloads_table a ON date = a.partition_date 
        AND t.country = a.country
WHERE a.country = 'ES'
ORDER BY partition_date;


-- By Month
SELECT  
    'Pluto TV' AS campaign_name, 
    m.creative_id,
    creative_name, 
    placement_name,
    TRUNC(campaign_start_date) AS campaign_start_date,
    TRUNC(campaign_end_date) AS campaign_end_date,
    COALESCE(SUM(impression),0) as impression, 
    COALESCE(SUM(click),0) as click,   
    count_exposed_app_open,  
    SUM(count_exposed_first_app_open) as count_exposed_first_app_open, 
    t.total_time_spent_min ,    
    ROUND(total_time_spent_min/60,2) total_time_spent_hour, 
    monthly_new_users as total_daily_downloads,
    monthly_active_users,  
    monthly_new_users, 
    monthly_returning_users
FROM creative_map m
    JOIN app_users b using(country)
    LEFT JOIN exposure_stats ex USING( country,creative_id, campaign_id)
    LEFT JOIN click_stats USING( country, date, creative_id, campaign_id)
    LEFT JOIN exposed_app_open_monthly as t USING ( country, creative_id, campaign_id)
    LEFT JOIN exposed_first_time_open as g USING ( country, date,creative_id, campaign_id)
WHERE country = 'ES'
GROUP BY 1, 2, 3, 4, 5, 6, 9, 11, 13, 14, 15, 16
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
    COALESCE(impression,0) AS impression, 
    COALESCE(click,0) AS click,  
    count_exposed_app_open, 
    count_exposed_first_app_open,  
    t.total_time_spent_min,    
    ROUND(t.total_time_spent_min/60,2) total_time_spent_hour --,   daily_downloads,monthly_active_users,  monthly_new_users, monthly_returning_users
FROM  creative_map m
    LEFT JOIN app_users_daily  using (country)
    LEFT JOIN exposure_stats ex USING(country,date, creative_id, campaign_id)
    LEFT JOIN click_stats USING(country,date, creative_id, campaign_id)
    LEFT JOIN exposed_app_open AS t USING (country,date, creative_id, campaign_id)
    LEFT JOIN exposed_first_time_open USING (country,date, creative_id, campaign_id)
    -- LEFT JOIN daily_downloads_table a
    -- ON date = a.partition_date AND m.country = a.country
WHERE 
    date BETWEEN '2023-12-01' AND '2023-12-31'
    AND m.country = 'GB'
ORDER BY date
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
        AND t.country = a.country
WHERE a.country = 'GB'
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
    t.total_time_spent_min ,    
    ROUND(total_time_spent_min/60,2) total_time_spent_hour, 
    monthly_new_users AS total_daily_downloads,
    monthly_active_users,  
    monthly_new_users, 
    monthly_returning_users
FROM creative_map m
    JOIN app_users b using(country)
    LEFT JOIN exposure_stats ex USING( country,creative_id, campaign_id)
    LEFT JOIN click_stats USING( country, date, creative_id, campaign_id)
    LEFT JOIN exposed_app_open_monthly AS t USING( country, creative_id, campaign_id)
    LEFT JOIN exposed_first_time_open AS g USING( country, date,creative_id, campaign_id)
WHERE country = 'GB'
GROUP BY 1, 2, 3, 4, 5, 6, 9, 11, 13, 14, 15, 16
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
    COALESCE(impression,0) AS impression, 
    COALESCE(click,0) AS click,  
    count_exposed_app_open, 
    count_exposed_first_app_open,  
    t.total_time_spent_min,    
    ROUND(t.total_time_spent_min/60,2) total_time_spent_hour --,   daily_downloads,monthly_active_users,  monthly_new_users, monthly_returning_users
FROM creative_map m
    LEFT JOIN app_users_daily USING(country)
    LEFT JOIN exposure_stats ex USING(country, date, creative_id, campaign_id)
    LEFT JOIN click_stats USING(country, date, creative_id, campaign_id)
    LEFT JOIN exposed_app_open AS t USING (country, date, creative_id, campaign_id)
    LEFT JOIN exposed_first_time_open USING(country, date, creative_id, campaign_id)
    -- left JOIN daily_downloads_table a
    -- ON date = a.partition_date AND m.country = a.country
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
    JOIN daily_downloads_table a ON date = a.partition_date AND t.country = a.country
WHERE 
    a.country = 'DE'
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
        AND t.country = a.country
WHERE a.country = 'AT'
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
    JOIN app_users b USING(country)
    LEFT JOIN exposure_stats ex USING( country,creative_id, campaign_id)
    LEFT JOIN click_stats USING( country, date, creative_id, campaign_id)
    LEFT JOIN exposed_app_open_monthly AS t USING ( country, creative_id, campaign_id)
    LEFT JOIN exposed_first_time_open AS g USING ( country, date, creative_id, campaign_id)
WHERE 
    country IN ('DE','AT')
GROUP BY 1, 2, 3, 4, 5, 6, 9, 11, 13, 14, 15, 16
;
-- ----------------------------------------------