
/*******************
 WBD_Max_In-App ACR New Users & Ent vs Sports s3 Needed (approx 10 mins to run on Medium)
 'As a follow-up from our Q1 QBR with the client, they challenged us to build 2x audiences for them that we can use as a "seed" for our SMART audience Look-a-like model.'

Methodology:
   - Campaign exposure is not a qualifier
   - App open = app usage session >= 60 seconds
   - Lookback window = 18 months (18 nmonths ago - today).
   - First app open = The date at which the user has an app open but no prior app opens within the lookback window
   - Data: After users "First app open" we want to see their viewership of 6 minutes or more within 30 days of that date
   - 6 minutes: Content time does not have to be continuous within the same app session. 
      -- Spiderman meets threshold: In same app session Spiderman @3minutes, Game of Thrones @2 minutes, Spiderman @4minutes
      -- Spiderman doesn't meets threshold: In app session A Spiderman @3minutes, Game of Thrones @2 minutes; In app session B Spiderman @4minutes
      -- Adds and Games do not count
   - Sports content: Determined by content genre (ideally we'd look at sports games specifically. Not documentaries, movies or talk shows surrounding sports.)
   - Audiences
      -- Audience 1: Watched 2 or more titles for a minimum of 6 mins within 30 days of first app open
      -- Audience 2: All watched content of 6 or more mins after first app open is sports related. No 30 day qualifier

Request:
 (Audience 1) 
    New Max Users (1st time app users) 
    AND Watched 1+ different titles (series, movies, sports, etc...) for 6+minutes within 30x days of first app opens

 (Audience 2) 
    Active Max Users who watched ONLY Sports games (NBA, NHL, MLB, etc...) within the app for 6+ minutes, (aka did NOT watch any non-Sports programming for 6+ minutes). 

 Ticket:
    https://adgear.atlassian.net/browse/SAI-6509
 
 Runtime: approx 30 minutes

 Github:
    https://github.com/lhaynes-ss/dev/blob/main/max_in-app_new_users_SAI6509.sql

 References: 
    https://github.com/SamsungAdsAnalytics/QueryBase/blob/master/UDW/MnE/max_in-app_ad-tier_activity_SAI-5728.sql

Note: 
   this would be for MAX use only coreect?
   Can you check the scale and see how large would be the seed audience?
   For Lookalike we typically need at least 50k users.
   The audience name should not contain MAX, HBO MAX, or specific MAX show names

********************/

-- connection settings
USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM;
USE DATABASE UDW_PROD;



-- variables
SET (
    conversion_end_date
    ,conversion_start_date
    ,country
    ,app_id
    ,watch_threshold_seconds
) = (
    CURRENT_TIMESTAMP::DATE                           --> conversion_end_date
    ,DATEADD('month', -18, CURRENT_TIMESTAMP::DATE)   --> conversion_start_date
    ,'US'                                             --> country
    ,'3202301029760'                                  --> app_id: Max
    ,(6 * 60)                                         --> watch_threshold_seconds (n minutes * 60 seconds) -- n = 6 minutes
); 



/*****************
 Get app usage
 return app usage data as a base for additional queries.

 - app open - tells us when opened
 - app session - tells us how long opened                                         (app usage - opened more than 1 minute)
 - in app activity - tells us what happened (e.g., watch movie, served ad, etc)   (filtered_restricted_content_exposure - list of vpsid)
*****************/
-- approx. 20 mins on MEDIUM
DROP TABLE IF EXISTS app_usage;
CREATE TEMP TABLE app_usage AS (
	SELECT
		vtifa
		,vpsid
		,start_timestamp AS event_time
      ,end_timestamp AS event_end_time
		,a.udw_partition_datetime
		,SUM(DATEDIFF('second', start_timestamp, end_timestamp)) AS event_duration_seconds
	FROM data_tv_acr.fact_app_usage_session_without_pii AS a
		LEFT JOIN udw_lib.virtual_psid_tifa_mapping_v AS m ON m.vpsid = a.psid_pii_virtual_id
	WHERE 
		a.udw_partition_datetime BETWEEN $conversion_start_date AND $conversion_end_date
		AND a.country = $country
		AND a.app_id = $app_id
		AND DATEDIFF('second', start_timestamp, end_timestamp) >= 60
      -- AND vtifa = 7306295289146700299 --> TESTING !!!!!!!!!!!!!!!
	GROUP BY 
		1, 2, 3, 4, 5
);

SELECT COUNT(DISTINCT vtifa) AS users FROM app_usage;


/*****************
 Get first time app usage 

 - user should be a repeat user
 - get date 30 days from first use
 - user should have another usage session after the first
 - additional session should be 6+ minutes in length
*****************/
-- approx. 2 mins on MEDIUM
DROP TABLE IF EXISTS first_app_usage;
CREATE TEMP TABLE first_app_usage AS (

   WITH first_use_cte AS (
      SELECT 
         vtifa
         ,vpsid
         ,MIN(event_time) AS event_time
      FROM app_usage
      GROUP BY 1, 2
   )

   SELECT 
      f.vtifa
      ,f.vpsid
      ,f.event_time
      ,DATEADD('day', 30, f.event_time) AS thirty_days_out
   FROM first_use_cte f
   WHERE 
      -- user should have another usage session after the first at 6+ minutes in length
      EXISTS ( 
         SELECT 1 
         FROM app_usage a 
         WHERE 
            a.vtifa = f.vtifa
            AND a.vpsid = f.vpsid
            AND a.event_time > f.event_time
            AND a.event_duration_seconds >= $watch_threshold_seconds
      )

);

-- SELECT COUNT(*) AS cnt FROM first_app_usage;



/*****************
 Get qualified app usage sessions
 - over 6 minutes
 - any usage including and after first use
*****************/
-- approx. 1 min on MEDIUM
DROP TABLE IF EXISTS qualified_app_usage;
CREATE TEMP TABLE qualified_app_usage AS (
   SELECT DISTINCT
      a.vtifa
      ,a.vpsid
      ,a.event_time
      ,a.event_end_time
      ,a.udw_partition_datetime
      ,a.event_duration_seconds 
      ,f.thirty_days_out
   FROM app_usage a 
      JOIN first_app_usage f ON f.vtifa = a.vtifa
         AND f.vpsid = a.vpsid
         AND a.event_time >= f.event_time
         AND a.event_duration_seconds >= $watch_threshold_seconds
);

-- SELECT COUNT(*) AS cnt FROM qualified_app_usage;



-- get content usage within app sessions
-- user can be exposed to multiple different pieces of content in a single app usage session
-- Content Types: https://adgear.atlassian.net/wiki/spaces/AGILE/pages/19472811887/In-App+ACR+Datasets
/**

   |------------ APP USAGE SESSION ------------|
   |-- IN-APP CONTENT --||-- IN-APP CONTENT --|

**/
DROP TABLE IF EXISTS qualified_content_usage;
CREATE TEMP TABLE qualified_content_usage AS (
   SELECT
      a.vtifa
      ,a.vpsid
      ,a.event_time
      ,a.udw_partition_datetime AS app_usage_udw_partition_datetime
      ,a.event_duration_seconds 
      ,a.thirty_days_out
      ,c.content_id
      ,c.content_type
      ,c.start_time
      ,DATEDIFF('second', c.start_time, c.end_time) AS acr_event_duration_seconds
   FROM qualified_app_usage a 
      JOIN data_tv_acr.fact_restricted_content_exposure_without_pii c ON c.psid_pii_virtual_id = a.vpsid 
         AND c.app_id = $app_id
         AND c.partition_country = $country
         AND c.start_time >= a.event_time       --> content usage should be within qualified app usage session
         AND c.end_time <= a.event_end_time     --> content usage should be within qualified app usage session
         AND c.content_type IN (                --> content should be ...
            'PROG'                              --> linear 
            ,'VOD'                              --> movie
            ,'OTT_PROG'                         --> ott show
         )
);  

SELECT * FROM qualified_content_usage LIMIT 1000;



/*****************
 Get audience base

   get audience base to be used for both audiences
*****************/
DROP TABLE IF EXISTS audience_base;
CREATE TEMP TABLE audience_base AS (
   SELECT
      q.vtifa
      ,q.vpsid
      ,q.event_time  --> app session start =  event time
      ,q.content_id  --> content session => content id within app session
      ,q.thirty_days_out
      ,SUM(q.acr_event_duration_seconds) AS content_event_duration_seconds
   FROM qualified_content_usage q
   GROUP BY 
      1, 2, 3, 4, 5
   HAVING 
      content_event_duration_seconds >= $watch_threshold_seconds
);

-- SELECT * FROM audience_base LIMIT 1000;



/*****************
 Get audience 1

- has at least 2 content sessions over 6 minutes in 30 days
- 6 minutes does not have to be consecutive, but has to be within the same session

   |--------------- APP USAGE SESSION ----------------|
   |-- CONTENT A --||-- CONTENT B --||-- CONTENT A --|
      3 minutes         2 minutes        4 minutes

- In example above, conntent A gets attributed 7 minutes 
  because both sontent sessions are within the same app
  usage session
*****************/ 
DROP TABLE IF EXISTS audience_1;
CREATE TEMP TABLE audience_1 AS (

   WITH content_cte AS (
      SELECT
         a.vtifa
         ,a.vpsid
         ,a.event_time  
         ,a.content_id  
         ,a.content_event_duration_seconds
      FROM audience_base a
      WHERE
         a.event_time <= a.thirty_days_out
         AND a.content_event_duration_seconds >= $watch_threshold_seconds
   )

   SELECT 
      c.vtifa
      ,c.vpsid
      ,COUNT(DISTINCT c.event_time, c.content_id) AS content_session_count
   FROM content_cte c
   GROUP BY 1, 2
   HAVING 
      content_session_count >= 2
);

SELECT COUNT(*) AS audience_1_count FROM audience_1;
SELECT * FROM audience_1 LIMIT 1000;



-- ==========================================
-- ==========================================
-- ==========================================



-- get genres to determine which are sports
-- this is done by filtering out any genre that is typically not associated with sports
DROP TABLE IF EXISTS sports_content_genres;
CREATE TEMP TABLE sports_content_genres AS (
    SELECT DISTINCT 
        p.program_id
        ,a.content_id 
        ,p.program_type
        ,ARRAY_TO_STRING(ARRAY_SORT(p.genres), '|') AS genres
    FROM audience_base a
        JOIN meta_umd_src_snapshot.program p ON CAST(p.program_id AS VARCHAR) = a.content_id 
            AND p.partition_country = $country
            AND ARRAY_TO_STRING(p.genres, '|') <> ''
            AND ARRAY_TO_STRING(p.genres, '|') ILIKE ANY( --> exclude genres matching these patterns
               'Action'
               ,'%Action|%'
               ,'Art'
               ,'%Art|%'
               ,'%Affairs%'
               ,'%Adventure%'
               ,'%Aviation%'
               ,'%Agriculture%'
               ,'%Animated%'
               ,'%Anime%'
               ,'%Animal%'
               ,'%Anthology%'
               ,'%Auction%'
               ,'Auto'
               ,'%Award%'
               ,'%Biography%'
               ,'%Children%'
               ,'%Collect%'
               ,'%Comedy%'
               ,'%Computer%'
               ,'Community'
               ,'%Consumer%'
               ,'%Cook%'
               ,'%Craft%'
               ,'%Crime%'
               ,'%Drama%'
               ,'%Documentary%'
               ,'%Education%'
               ,'%Entertain%'
               ,'%Erotic%'
               ,'%Exercise%'
               ,'%Fantasy%'
               ,'%Fashion%'
               ,'%Finan%'           --> finance, financial 
               ,'%Fiction%'
               ,'%fundraiser%'
               ,'%Garden%'
               ,'%Gay%'
               ,'%Health%'
               ,'%History%'
               ,'%Hobbies%'
               ,'%Holiday%'
               ,'%Home%'
               ,'%Horror%'
               ,'%How%'
               ,'%Interview%'
               ,'%Improvement%'
               ,'%Law%'
               ,'%Medic%'           --> medicine, medical
               ,'%Mystery%'
               ,'%Nature%'
               ,'%News%'
               ,'%Music%'
               ,'%Paranormal%'
               ,'%Performing%'
               ,'%Politic%'
               ,'%Reality%'
               ,'%Religi%'          --> religious, religion
               ,'%Romance%'
               ,'%Soap%'
               ,'%Science%'
               ,'%Sitcom%'
               ,'%Shop%'
               ,'%Special%'
               ,'%Suspense%'
               ,'%Talk%'
               ,'%Technolog%'
               ,'%Travel%'
               ,'%Thriller%'
               ,'%Variety%'
               ,'%War%'
               ,'%Weather%'
               ,'%Western%'
            ) <> TRUE
);

-- SELECT DISTINCT genres FROM sports_content_genres ORDER BY genres;



-- get titles to determine which are sports (this is via manual review)
-- return ONLY the english title if available
-- added sort column. Sort column adds underscore(s) (_) to en language to give sort priority
DROP TABLE IF EXISTS sports_content_titles;
CREATE TEMP TABLE sports_content_titles AS (

   WITH titles_cte AS (
      SELECT DISTINCT 
         m.program_id
         ,g.content_id
         ,m.title 
         ,m.partition_country
         ,m.title_language
         ,CASE 
            WHEN m.title_language = 'en'       THEN '__en'
            WHEN m.title_language LIKE 'en%'   THEN '_' || m.title_language
            ELSE m.title_language
         END AS title_language_sort
         ,ROW_NUMBER() OVER(PARTITION BY m.program_id ORDER BY title_language_sort) AS row_num
      FROM meta_umd_src_snapshot.program_title m 
         JOIN sports_content_genres g ON g.program_id = m.program_id
            AND m.partition_country = $country
            AND m.title_type = 'main'
            -- AND m.title LIKE ANY ('%NBA%', '%NFL%', '%MLB%', '%NHL%', '%PGA%')
   )

   SELECT DISTINCT
      t.program_id
      ,t.content_id
      ,t.title 
      ,t.partition_country
   FROM titles_cte t 
   WHERE t.row_num = 1

);

-- SELECT * FROM sports_content_titles LIMIT 1000;



-- get qualifying sports usage
-- base audience + matches in sports genre
DROP TABLE IF EXISTS sports_content;
CREATE TEMP TABLE sports_content AS (
   SELECT DISTINCT
      a.vtifa
      ,a.vpsid
      ,a.event_time  
      ,a.content_id  
      ,t.title 
      ,g.program_type
      ,g.genres
      ,a.content_event_duration_seconds
   FROM audience_base a
      JOIN sports_content_titles t ON t.content_id = a.content_id
      JOIN sports_content_genres g ON g.content_id = a.content_id
);

-- SELECT * FROM sports_content LIMIT 1000;



/*****************
 Get audience 2 base

- has ONLY content sessions over 6 minutes that are related to sports
- 6 minutes does not have to be consecutive, but has to be within the same session

The number of total events in the base audience SHOULD match the number of events after 
being joined with sports genre if all user content watched has been sports related
*****************/ 
DROP TABLE IF EXISTS audience_2;
CREATE TEMP TABLE audience_2 AS (

   WITH content_cte AS (
      SELECT 
         a.vtifa
         ,a.vpsid
         ,COUNT(*) AS event_count
      FROM audience_base a
      GROUP BY 1, 2
   )

   ,sports_content_cte AS (
      SELECT 
         s.vtifa
         ,s.vpsid
         ,COUNT(*) AS event_count
      FROM sports_content s
      GROUP BY 1, 2
   )

   SELECT DISTINCT
      cc.vtifa
      ,cc.vpsid 
      ,cc.event_count
   FROM content_cte cc 
      JOIN sports_content_cte sc ON sc.vtifa = cc.vtifa
         AND sc.vpsid = cc.vpsid
         AND sc.event_count = cc.event_count 
);

SELECT COUNT(*) AS audience_2_count FROM audience_2;
SELECT * FROM audience_2 LIMIT 1000;

/**
-- TEST
SELECT * FROM sports_content WHERE vtifa = 7306295289146700299;
SELECT * FROM audience_base WHERE vtifa = 7306295289146700299;
**/


-- SELECT below just added to not end on a comment as this throws an error in DbVis
SELECT 'DONE';


