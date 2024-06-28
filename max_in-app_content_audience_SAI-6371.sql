
/*******************
 WBD_Max_In-App ACR Audience Needed (approx 10 mins to run on Medium)
 "Are we able to pull new acquired users who specifically watched the below programs?"

 (Audience 1) True Detective  Night Country (S4)
    - Exposed to True Detective creative ad from 1/2/24 - 1/31/24 - LI 120630 ; 119432
    - AND Watched any episode of True Detective: Night Country (Season 4) for 1+ minute from 1/14 - 2/7 in the Max app on 2016-2021 TVs

 (Audience 2) March Madness NCAA Basketball
    - Exposed to March Madness creative Ad (3/21/24 - 4/8/24) - LI 119435; 127800
    - AND Watched any NCAA basketball game for 1+ minute from 3/21 - 4/8 in the Max app on 2016-2021 TVs

 Ticket:
    https://adgear.atlassian.net/browse/SAI-6371

 Github:
    https://github.com/SamsungAdsAnalytics/QueryBase/blob/master/UDW/MnE/max_in-app_content_audience_SAI-6371.sql

 References: 
    https://github.com/SamsungAdsAnalytics/QueryBase/blob/master/UDW/MnE/max_in-app_ad-tier_activity_SAI-5728.sql

********************/

-- connection settings
USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM;
USE DATABASE UDW_PROD;


-- ===================================================
-- START SETTINGS
-- ===================================================

-- (Audience 1) True Detective  Night Country (S4)
SET (
    vao_list
    ,audience_line_items
    ,exposure_start_date
    ,exposure_end_date
    ,conversion_start_date
    ,conversion_end_date
    ,country
    ,app_id
    ,tv_model_year_start
    ,tv_model_year_end
    ,required_season
    ,title_pattern_list
) = (
    '149874'            --> vao_list 
    ,'120630, 119432'   --> audience_line_items (True Detective  Night Country)
    ,'2024-01-14'       --> exposure_start_date
    ,'2024-01-31'       --> exposure_end_date
    ,'2024-01-14'       --> conversion_start_date
    ,'2024-02-07'       --> conversion_end_date
    ,'US'               --> country
    ,'3202301029760'    --> app_id: Max
    ,2016               --> tv_model_year_start
    ,2021               --> tv_model_year_end
    ,4                  --> required_season
    ,'%True Detective: Night Country%'  --> title_pattern_list
); 



-- (Audience 2) March Madness NCAA Basketball
/***
SET (
    vao_list
    ,audience_line_items
    ,exposure_start_date
    ,exposure_end_date
    ,conversion_start_date
    ,conversion_end_date
    ,country
    ,app_id
    ,tv_model_year_start
    ,tv_model_year_end
    ,required_season
    ,title_pattern_list
) = (
    '149874, 163790'    --> vao_list 
    ,'119435, 127800'   --> audience_line_items (March Madness NCAA Basketball)
    ,'2024-03-21'       --> exposure_start_date
    ,'2024-04-08'       --> exposure_end_date
    ,'2024-03-21'       --> conversion_start_date
    ,'2024-04-08'       --> conversion_end_date
    ,'US'               --> country
    ,'3202301029760'    --> app_id: Max
    ,2016               --> tv_model_year_start
    ,2021               --> tv_model_year_end
    ,NULL               --> required_season
    ,'%NCAA%, %March Madness%'  --> title_pattern_list
); 
***/

-- ===================================================
-- END SETTINGS
-- ===================================================


-- adjust dates for script
SET (
    report_start_date
    ,report_end_date
    ,conversion_start_date
    ,conversion_end_date
) = (
    (TO_CHAR(CAST($exposure_start_date AS DATE), 'YYYYMMDD') || '00')       --> convert to string: YYMMDDHH
    ,(TO_CHAR(CAST($exposure_end_date AS DATE), 'YYYYMMDD') || '23')        --> convert to string: YYMMDDHH
    ,CAST($conversion_start_date || ' 00:00' AS TIMESTAMP)                  --> convert to timestamp
    ,CAST($conversion_end_date || ' 23:59' AS TIMESTAMP)                    --> convert to timestamp
);



/*******************
 Convert lists to tables

 Allows a single variable to be used to specify one or moultiple values to be used in the query
*******************/
-- vao list to table
DROP TABLE IF EXISTS vaos_table;
CREATE TEMP TABLE vaos_table AS (
    SELECT CAST(t.value AS INT) AS vao
    FROM TABLE(SPLIT_TO_TABLE($vao_list, ',')) AS t
);

-- SELECT * FROM vaos_table;


-- audience list to table
DROP TABLE IF EXISTS audience_table;
CREATE TEMP TABLE audience_table AS (
    SELECT CAST(t.value AS INT) AS line_item
    FROM TABLE(SPLIT_TO_TABLE($audience_line_items, ',')) AS t
);

-- SELECT * FROM audience_table;


-- title pattern list to table
DROP TABLE IF EXISTS title_pattern_table;
CREATE TEMP TABLE title_pattern_table AS (
    SELECT t.value AS program_title_pattern
    FROM TABLE(SPLIT_TO_TABLE($title_pattern_list, ',')) AS t
);

-- SELECT * FROM title_pattern_table;



/*******************
 Campaign Meta 

 This uses the base campaign meta query slightly modified to accept a list of vaos (vaos_table)
 instead of a single value, and also to filter data down to only the line itemes needed for
 the audience that we are building (JOIN audience_table)
 
 https://github.com/SamsungAdsAnalytics/QueryBase/blob/master/UDW/Basic_Queries/Campaign_Mapping/VAO%20to%20Line%20Item%20and%20Campaign
*******************/
-- campaign meta
DROP TABLE IF EXISTS campaign_meta;
CREATE TEMP TABLE campaign_meta AS (

    WITH vao_samsungCampaignID AS (
        SELECT
            vao,
            samsung_campaign_id,
            sales_order_id,
            sales_order_name
        FROM (
            SELECT
                CAST(replace(sf_opp.jira_id__c, 'VAO-', '') AS INT) AS vao,
                sf_opp.samsung_campaign_id__c AS samsung_campaign_id,
                sf_opp.operative_order_id__c AS sales_order_id,
                sf_opp.order_name__c AS sales_order_name,
                ROW_NUMBER() OVER(PARTITION BY vao ORDER BY sf_opp.lastmodifieddate DESC) AS rn
            FROM SALESFORCE.OPPORTUNITY AS sf_opp
            WHERE vao IN (SELECT DISTINCT vao FROM vaos_table)
        )
        WHERE rn = 1
    ),

    salesOrder AS (
        SELECT
            sales_order_id,
            sales_order_name,
            order_start_date,
            order_end_date,
            time_zone
        FROM (
            SELECT
                sales_order.sales_order_id,
                sales_order.sales_order_name,
                sales_order.order_start_date,
                sales_order.order_end_date,
                sales_order.time_zone,
                ROW_NUMBER() OVER(PARTITION BY sales_order.sales_order_id ORDER BY sales_order.last_modified_on DESC) AS rn
            FROM OPERATIVEONE.SALES_ORDER AS sales_order
            JOIN vao_samsungCampaignID AS vao
                USING (sales_order_id)
        ) AS foo
        WHERE foo.rn = 1
    ),

    cmpgn AS (
        SELECT DISTINCT
            sales_order_id,
            sales_order_line_item_id,
            cmpgn.id AS campaign_id,
            cmpgn.name AS campaign_name,
            rate_type,
            net_unit_cost,
            cmpgn.start_at_datetime::TIMESTAMP AS cmpgn_start_datetime_utc,
            cmpgn.end_at_datetime::TIMESTAMP AS cmpgn_end_datetime_utc
        FROM TRADER.CAMPAIGNS_LATEST AS cmpgn
        JOIN (
            SELECT DISTINCT
                cmpgn_att.campaign_id,
                cmpgn_att.rate_type,
                cmpgn_att.net_unit_cost,
                cmpgn_att.io_external_id AS sales_order_id,
                cmpgn_att.li_external_id AS sales_order_line_item_id
            FROM TRADER.CAMPAIGN_OMS_ATTRS_LATEST AS cmpgn_att
            JOIN vao_samsungCampaignID ON vao_samsungCampaignID.sales_order_id = cmpgn_att.external_id
        ) AS foo ON cmpgn.id = foo.campaign_id
    ),

    lineItem AS (
        SELECT
            sales_order_id,
            sales_order_line_item_id,
            sales_order_line_item_name,
            sales_order_line_item_start_datetime_utc,
            sales_order_line_item_end_datetime_utc
        FROM (
            SELECT
                lineItem.sales_order_id,
                lineItem.sales_order_line_item_id,
                lineItem.sales_order_line_item_name,
                TIMESTAMP_NTZ_FROM_PARTS(lineItem.sales_order_line_item_start_date::date, lineItem.start_time::time) AS sales_order_line_item_start_datetime_utc,
                TIMESTAMP_NTZ_FROM_PARTS(lineItem.sales_order_line_item_end_date::date, lineItem.end_time::time) AS sales_order_line_item_end_datetime_utc,
                ROW_NUMBER() OVER(PARTITION BY lineItem.sales_order_line_item_id ORDER BY lineItem.last_modified_on DESC) AS rn
            FROM OPERATIVEONE.SALES_ORDER_LINE_ITEMS AS lineItem
            JOIN vao_samsungCampaignID AS vao
                USING (sales_order_id)
        ) AS foo
        WHERE foo.rn = 1
    )


    -- Main query
    SELECT DISTINCT
        -- VAO info
        vao_samsungCampaignID.vao,
        vao_samsungCampaignID.samsung_campaign_id,
        vao_samsungCampaignID.sales_order_id,
        vao_samsungCampaignID.sales_order_name,
        -- Sales Order info
        salesOrder.order_start_date,
        salesOrder.order_end_date,
        -- Campaign info
        cmpgn.campaign_id,
        cmpgn.campaign_name,
        cmpgn.rate_type,
        cmpgn.net_unit_cost,
        cmpgn.cmpgn_start_datetime_utc,
        cmpgn.cmpgn_end_datetime_utc,
        -- Line Item info
        lineItem.sales_order_line_item_id,
        lineItem.sales_order_line_item_name,
        lineItem.sales_order_line_item_start_datetime_utc,
        lineItem.sales_order_line_item_end_datetime_utc,
    FROM vao_samsungCampaignID
        JOIN salesOrder USING (sales_order_id)
        JOIN cmpgn USING (sales_order_id)
        JOIN lineItem USING (sales_order_id, sales_order_line_item_id)
        JOIN audience_table a1 ON a1.line_item = lineItem.sales_order_line_item_id

);

SELECT * FROM campaign_meta;



/*******************
 Samsung Universe
 
 Samsung Universe (aka. superset) is a collection of Samsung TVs that can be found in any of following 3 data sources:
    - TV Hardware: profile_tv.fact_psid_hardware_without_pii
    - App Open: data_tv_smarthub.fact_app_opened_event_without_pii
    - O&O Samsung Ads Campaign Delivery: data_ad_xdevice.fact_delivery_event_without_pii (for exchange_id = 6 and exchange_seller_id = 86) 
 
 Any data used for attribution reports needs to be intersected with Samsung Universe
 Reference: https://adgear.atlassian.net/wiki/spaces/MAST/pages/19673186934/M+E+Analytics+-+A+I+Custom+Report+Methodology
*******************/
-- qualifier: start date = start date + 30 days if device graph resolution mechanism is used
DROP TABLE IF EXISTS qualifier; 
CREATE TEMP TABLE qualifier AS (
	SELECT 
		LISTAGG(DISTINCT '"'||EXCHANGE_SELLER_ID||'"', ',') AS exchage_seller_id_list,
		CASE 
			WHEN NOT exchage_seller_id_list LIKE ANY ('%"86"%', '%"88"%', '%"1"%', '%"256027"%', '%"237147"%', '%"escg8-6k2bc"%', '%"amgyk-mxvjr"%' ) 
			THEN 'Superset +30 days'
			ELSE 'Superset' 
		END AS qualifier, 
		CASE 
			WHEN qualifier = 'Superset +30 days' 
			THEN DATEADD(DAY, -30, TO_DATE(LEFT($REPORT_START_DATE,8),'YYYYMMDD'))::TIMESTAMP 
			ELSE TO_TIMESTAMP($REPORT_START_DATE,'YYYYMMDDHH') 
		END AS report_start_date
	FROM DATA_AD_XDEVICE.FACT_DELIVERY_EVENT_WITHOUT_PII a
		JOIN campaign_meta b ON a.campaign_id = b.campaign_id
	WHERE 
		UDW_PARTITION_DATETIME BETWEEN TO_TIMESTAMP($REPORT_START_DATE,'YYYYMMDDHH') AND TO_TIMESTAMP($REPORT_END_DATE,'YYYYMMDDHH')
		AND TYPE = 1
		AND device_country = $COUNTRY
);

SET report_start_date_qual = (SELECT report_start_date FROM qualifier);

DROP TABLE IF EXISTS samsung_ue; --5 mins IN M
CREATE TEMP TABLE samsung_ue AS (
	SELECT DISTINCT m.vtifa
	FROM PROFILE_TV.FACT_PSID_HARDWARE_WITHOUT_PII a
		JOIN UDW_LIB.VIRTUAL_PSID_TIFA_MAPPING_V m ON a.PSID_PII_VIRTUAL_ID = m.vpsid
	WHERE 
		UDW_PARTITION_DATETIME BETWEEN $REPORT_START_DATE_QUAL AND TO_TIMESTAMP($REPORT_END_DATE,'YYYYMMDDHH')
		AND partition_country = $COUNTRY	
	UNION
	SELECT DISTINCT GET(SAMSUNG_TVIDS_PII_VIRTUAL_ID , 0) AS vtifa
	FROM DATA_AD_XDEVICE.FACT_DELIVERY_EVENT_WITHOUT_PII 	
	WHERE 
		UDW_PARTITION_DATETIME BETWEEN $REPORT_START_DATE_QUAL AND TO_TIMESTAMP($REPORT_END_DATE,'YYYYMMDDHH')
		AND TYPE = 1
		AND (dropped != TRUE OR  dropped IS NULL)
		AND (EXCHANGE_ID = 6 OR EXCHANGE_SELLER_ID = 86)
		AND device_country = $COUNTRY
	UNION 
	SELECT DISTINCT m.vtifa
	FROM DATA_TV_SMARTHUB.FACT_APP_OPENED_EVENT_WITHOUT_PII a 
		JOIN UDW_LIB.VIRTUAL_PSID_TIFA_MAPPING_V m ON a.PSID_PII_VIRTUAL_ID = m.vpsid
	WHERE 
		UDW_PARTITION_DATETIME BETWEEN $REPORT_START_DATE_QUAL AND TO_TIMESTAMP($REPORT_END_DATE,'YYYYMMDDHH')
		AND partition_country = $COUNTRY
);

SELECT COUNT(*) AS cnt FROM samsung_ue;


-- variable check (validation purposes only)
SHOW VARIABLES;



/*******************
Samsung Ad Delivery Data (Exposure)

Get impressions (exposures) from the campaigns specified in campaign metadata
*******************/
-- use data_ad_xdevice.fact_delivery_event_without_pii 
DROP TABLE IF EXISTS cd;
CREATE TEMP TABLE cd AS (
	SELECT 
		GET(SAMSUNG_TVIDS_PII_VIRTUAL_ID, 0) AS vtifa,
		event_time AS imp_time
	FROM DATA_AD_XDEVICE.FACT_DELIVERY_EVENT_WITHOUT_PII a
		JOIN campaign_meta b ON a.campaign_id = b.campaign_id
		JOIN samsung_ue c ON GET(a.SAMSUNG_TVIDS_PII_VIRTUAL_ID, 0) = c.vtifa
	WHERE 
		a.UDW_PARTITION_DATETIME BETWEEN TO_TIMESTAMP($REPORT_START_DATE,'YYYYMMDDHH') AND TO_TIMESTAMP($REPORT_END_DATE,'YYYYMMDDHH')
		AND TYPE = 1 -- 1 = impression, 2 = click
		AND device_country = $country
);

-- SELECT COUNT(*) AS delivery FROM cd;



/*******************
 TV Profiles Map

 Used to map vpsid's to specific TV models
*******************/
-- TV profiles where device is between $tv_model_year_start and $tv_model_year_end
DROP TABLE IF EXISTS tv_profiles;
CREATE TEMP TABLE tv_profiles AS (
    SELECT DISTINCT 
        vpsid,
        model_yr
    FROM adbiz_data.lup_tv_profile_2023
    WHERE 
        model_yr BETWEEN $tv_model_year_start AND $tv_model_year_end
        AND COUNTRY = $country
);

-- SELECT COUNT(*) FROM tv_profiles;



/*****************
 Get app usage
 return app usage data as a base for additional queries.

 - app open - tells us when opened
 - app session - tells us how long opened                                         (app usage - opened more than 1 minute)
 - in app activity - tells us what happened (e.g., watch movie, served ad, etc)   (filtered_restricted_content_exposure - list of vpsid)
*****************/
DROP TABLE IF EXISTS app_usage;
CREATE TEMP TABLE app_usage AS (
	SELECT
		vtifa
		,vpsid
		,start_timestamp AS event_time
		,a.udw_partition_datetime
		,SUM(DATEDIFF('second', start_timestamp, end_timestamp)) AS event_duration_seconds
	FROM data_tv_acr.fact_app_usage_session_without_pii AS a
		LEFT JOIN UDW_LIB.VIRTUAL_PSID_TIFA_MAPPING_V AS m ON m.vpsid = a.psid_pii_virtual_id
	WHERE 
		a.udw_partition_datetime BETWEEN $conversion_start_date AND $conversion_end_date
		AND a.country = $country
		AND a.app_id = $app_id
		AND DATEDIFF('second', start_timestamp, end_timestamp) >= 60 
	GROUP BY 
		1, 2, 3, 4
);

-- SELECT COUNT(DISTINCT vtifa) AS users FROM app_usage;



/*******************
 In-App ACR Data

 ACR Content: data_tv_acr.fact_restricted_content_exposure_without_pii
 ACR Ads Only: data_tv_acr.filtered_restricted_content_exposure_without_pii
 ACR Content Metadata:
    - meta_umd_src_snapshot.program_title
    - meta_umd_src_snapshot.program

 https://adgear.atlassian.net/wiki/spaces/AGILE/pages/19472811887/In-App+ACR+Datasets
*******************/
-- get ACR data
-- Content metadata: the same UMD metadata that we use for Linear TV
-- fact_restricted = content, filtered_restricted = ads only
DROP TABLE IF EXISTS acr_data;
CREATE TEMP TABLE acr_data AS (
    SELECT DISTINCT
        r.psid_pii_virtual_id AS vpsid
        ,r.content_type
        ,m.program_id
        ,m.title
        ,p.season_number
    FROM data_tv_acr.fact_restricted_content_exposure_without_pii r   
        JOIN meta_umd_src_snapshot.program_title m ON CAST(m.program_id AS VARCHAR) = r.content_id
            AND m.partition_country = $country
            AND r.udw_partition_datetime BETWEEN $conversion_start_date AND $conversion_end_date
            AND EXISTS ( -- equivalent to (m.title ILIKE '%pattern1%' OR m.title ILIKE '%pattern2%', etc)
                SELECT 1
                FROM title_pattern_table t
                WHERE m.title ILIKE (t.program_title_pattern)
            )
        JOIN meta_umd_src_snapshot.program p ON p.program_id = m.program_id
            AND p.partition_country = $country
            AND 1 = ( -- if there is a season # check it. If not, return 1
                CASE 
                    WHEN $required_season IS NULL THEN 1
                    ELSE (CASE WHEN $required_season = p.season_number THEN 1 ELSE 0 END)
                END
            )
);

SELECT * FROM acr_data LIMIT 1000;
SELECT DISTINCT title FROM acr_data ORDER BY title;



/*******************
 FINAL SELECTION
*******************/
DROP TABLE IF EXISTS exposed_conversion_audience;
CREATE TEMP TABLE exposed_conversion_audience AS (
    SELECT DISTINCT 
        a.vtifa
        ,a.vpsid
    FROM cd c                                       --> exposures
        JOIN app_usage a ON a.vtifa = c.vtifa       --> exposed conversions, no attribution window specified
            AND c.imp_time <= a.event_time
        JOIN acr_data acr ON acr.vpsid = a.vpsid    --> exposed conversions filtered by content watched
        JOIN tv_profiles tv ON tv.vpsid = a.vpsid   --> filtered conversions by tv profile
);

SELECT COUNT(*) AS audience_size FROM exposed_conversion_audience;
SELECT * FROM exposed_conversion_audience LIMIT 1000;


