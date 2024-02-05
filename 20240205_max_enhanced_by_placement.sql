/*******************
 MAX User Tier
 "Are we able to pull new acquired users who specifically signed up for the ad-tier?"

 Execution Duration: Approx 7 mins
 Github: https://github.com/SamsungAdsAnalytics/QueryBase/blob/master/UDW/MnE/max_in-app_ad-tier_activity_SAI-5728.sql
 Diagram: https://github.com/SamsungAdsAnalytics/QueryBase/blob/master/UDW/MnE/max_ad-tier_diagram_enhanced.png

 Attribution Methodology below:
  - 7 day attribution window
  - Devices Exposed to Ad (11/22 - 11/27/23 EST)
  - AND with First-time opens, for 1+ min (11/22 - 12/4/23 EST)
  - AND who have NOT opened App for prior 6 months (5/23/23 - 11/21/23)

 https://adgear.atlassian.net/browse/SAI-5728

 Due to contract terms, MAX and HBO MAX In-App ACR data is available in a separate table.
 HBO Deduped: filtered_restricted_content_exposure
 https://adgear.atlassian.net/wiki/spaces/AGILE/pages/19584123665/Streaming+AI#Streaming-Data-Overview
********************/

-- connection settings
USE ROLE UDW_MARKETING_ANALYTICS_DEFAULT_CONSUMER_ROLE_PROD;
USE DATABASE UDW_PROD;
USE WAREHOUSE UDW_MARKETING_ANALYTICS_DEFAULT_WH_PROD;
USE SCHEMA PUBLIC;



/*******************
 CONFIG
 Set report variables
********************/
-- =========================================

SET report_start_date = '2023112200'; -- UTC
SET report_end_date   = '2023112823'; -- UTC
SET app_id            = '3202301029760'; -- Max
SET country           = 'US';
SET vao               = 126621;
SET attribution_window = 7; -- days

-- =========================================



/*******************
 Auto-set report variables
********************/
-- required app opens
-- date range in which consumers need to have opened app
-- converts $report_start_date and $report_end_date to timestamps, adds $attribution_window days to $report_end_date
SET start_app_required_open_window = TO_TIMESTAMP(LEFT($report_start_date, 8) || '000000', 'YYYYMMDDHHMISS'); -- '2023-11-22 00:00:00.000000000Z'
SET end_app_required_open_windwow = DATEADD(DAY, $attribution_window, TO_TIMESTAMP(LEFT($report_end_date, 8) || '235959', 'YYYYMMDDHHMISS')); -- '2023-12-04 23:59:59.000000000Z'

-- blackout - no app opens allowed
-- date range in which consumers should not have opened app
-- window ends 1 second before $report_start_date and goes back 6 months
SET end_app_blackout_window = DATEADD(SECOND, -1, $start_app_required_open_window); -- '2023-11-21 23:59:59.000000000Z'
SET start_app_blackout_window = DATEADD(MONTH, -6, $end_app_blackout_window); -- '2023-05-21 23:59:59.000000000Z'

SHOW VARIABLES;



/*******************
 Campaign Meta
 get campaign meta with vao from ticket
 https://github.com/SamsungAdsAnalytics/QueryBase/blob/master/UDW/Basic_Queries/Campaign_Mapping/VAO%20to%20Line%20Item%20and%20Campaign

********************/
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
                sf_opp.samsung_campaign_id__c AS samsung_campaign_id, --Samsung ORDER ID
                sf_opp.operative_order_id__c AS sales_order_id,
                sf_opp.order_name__c AS sales_order_name,
                ROW_NUMBER() OVER(PARTITION BY vao ORDER BY sf_opp.lastmodifieddate DESC) AS rn
            FROM SALESFORCE.OPPORTUNITY AS sf_opp
            WHERE vao = $VAO
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
                ROW_NUMBER() OVER(PARTITION BY sales_order.sales_order_id ORDER BY sales_order.last_modified_on) AS rn
            FROM OPERATIVEONE.SALES_ORDER AS sales_order
                JOIN vao_samsungCampaignID AS vao USING (sales_order_id)
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
            ) AS foo
            ON cmpgn.id = foo.campaign_id
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
                ROW_NUMBER() OVER(PARTITION BY lineItem.sales_order_line_item_id ORDER BY lineItem.last_modified_on) AS rn
            FROM OPERATIVEONE.SALES_ORDER_LINE_ITEMS AS lineItem
                JOIN vao_samsungCampaignID AS vao USING (sales_order_id)
        ) AS foo
        WHERE foo.rn = 1
    )

    /**************************************************************
    Main query          
    Remember to edit the parts you want to keep in below as well!
    **************************************************************/
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
        lineItem.sales_order_line_item_end_datetime_utc
    FROM vao_samsungCampaignID
        JOIN salesOrder USING (sales_order_id)
        JOIN cmpgn USING (sales_order_id)
        JOIN lineItem USING (sales_order_id, sales_order_line_item_id)
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
		JOIN UDW_PROD.UDW_LIB.VIRTUAL_PSID_TIFA_MAPPING_V m ON a.PSID_PII_VIRTUAL_ID = m.vpsid
	WHERE 
		UDW_PARTITION_DATETIME BETWEEN $REPORT_START_DATE_QUAL AND TO_TIMESTAMP($REPORT_END_DATE,'YYYYMMDDHH')
		AND partition_country = $COUNTRY
);

-- SELECT COUNT(*) AS cnt FROM samsung_ue;  -- 43,314,613



/*****************
 Get app usage
 return app usage data as a base for additional queries.

 - app open - tells us when opened
 - app session - tells us how long opened                                         (app usage - opened more than 1 minute)
 - in app activity - tells us what happened (e.g., watch movie, served ad, etc)   (filtered_restricted_content_exposure - list of vpsid)

 filtered_restricted_content_exposure would let us know this an ad tier account (supported by ads)
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
		LEFT JOIN udw_prod.udw_lib.virtual_psid_tifa_mapping_v AS m ON m.vpsid = a.psid_pii_virtual_id
	WHERE 
		a.udw_partition_datetime BETWEEN $start_app_blackout_window AND $end_app_required_open_windwow
		AND a.country = $country
		AND a.app_id = $app_id
		AND DATEDIFF('second', start_timestamp, end_timestamp) >= 60 
	GROUP BY 
		1, 2, 3, 4
);

-- SELECT COUNT(DISTINCT vtifa) AS users FROM app_usage; -- 8,570,021



/*****************
 Get invalid users
 return devices from app_usage where user used app during blackout. We will exclude these users from Samsung universe.
*****************/
DROP TABLE IF EXISTS app_usage_invalid;
CREATE TEMP TABLE app_usage_invalid AS (
    SELECT DISTINCT
        a.vtifa
    FROM app_usage a 
    WHERE 
        a.udw_partition_datetime BETWEEN $start_app_blackout_window AND $end_app_blackout_window
);

-- SELECT COUNT(DISTINCT vtifa) AS invalid_users FROM app_usage_invalid; -- 8,271,847



/*****************
 Miniverse
 Subset of universe that we care about. Users who did not use app during blackout.
*****************/
DROP TABLE IF EXISTS miniverse;
CREATE TEMP TABLE miniverse AS (
	SELECT 
		u.vtifa
	FROM samsung_ue u
	WHERE 
		u.vtifa NOT IN (
			SELECT i.vtifa 
			FROM app_usage_invalid i
			WHERE i.vtifa IS NOT NULL
		)
);

-- SELECT COUNT(DISTINCT vtifa) AS users FROM miniverse;



/****************
Samsung Ad Delivery Data (Exposure)
************************/
-- use data_ad_xdevice.fact_delivery_event_without_pii 
DROP TABLE IF EXISTS cd;
CREATE TEMP TABLE cd AS (
    SELECT 
        GET(SAMSUNG_TVIDS_PII_VIRTUAL_ID, 0) AS vtifa,
        event_time AS imp_time,
        b.campaign_name,
        b.sales_order_line_item_name
    FROM DATA_AD_XDEVICE.FACT_DELIVERY_EVENT_WITHOUT_PII a
        JOIN campaign_meta b ON a.campaign_id = b.campaign_id
        JOIN samsung_ue c ON GET(a.SAMSUNG_TVIDS_PII_VIRTUAL_ID, 0) = c.vtifa
    WHERE 
        a.UDW_PARTITION_DATETIME BETWEEN TO_TIMESTAMP($REPORT_START_DATE,'YYYYMMDDHH') AND TO_TIMESTAMP($REPORT_END_DATE,'YYYYMMDDHH')
        AND TYPE = 1 -- 1 = impression, 2 = click
        AND device_country = $COUNTRY
);

-- SELECT COUNT(DISTINCT vtifa) AS delivery FROM cd;



/*****************
 In-app Delivery (ACR Exposure)
 "filtered_restricted_content_exposure" would let us know this an ad tier account (supported by ads).
 "numerator_avod_creatives_us_latest" lets us know what they are watching in case we need to exclude certain ads
  See full explanation at "app_usage" table definition
*****************/
DROP TABLE IF EXISTS iad;
CREATE TEMP TABLE iad AS (
    SELECT
        r.psid_pii_virtual_id
        ,r.content_type
        ,n.advertiser_name
        ,n.k_brand_name
        ,n.k_subsidiary_name
        ,n.product_name
    FROM data_tv_acr.filtered_restricted_content_exposure_without_pii r   
        LEFT JOIN meta_ad_src_snapshot.numerator_avod_creatives_us_latest n ON n.ad_id = r.content_id
            AND n.partition_country = $country
    WHERE 
        r.udw_partition_datetime BETWEEN $start_app_required_open_window AND $end_app_required_open_windwow
);

-- SELECT * FROM iad LIMIT 1000;



/*****************
 Get conversions
 return ad tier app usage: devices where user had add supported app usage during desired window. This is a conversion.
*****************/
DROP TABLE IF EXISTS in_app;
CREATE TEMP TABLE in_app AS (
    SELECT
        a.vtifa
        ,a.event_time
    FROM app_usage a 
    WHERE 
        a.udw_partition_datetime BETWEEN $start_app_required_open_window AND $end_app_required_open_windwow
        AND a.vpsid IN (SELECT DISTINCT i.psid_pii_virtual_id FROM iad i)
);

-- SELECT * FROM in_app LIMIT 1000;



/**********************
 Attribution
 Get impressions where impression time <= converted time AND conversion within 7 days of impression, for first conversion
***********************/
DROP TABLE IF EXISTS exp_in_app; 
CREATE TEMP TABLE exp_in_app AS (

    -- get in-app conversions, sequential
    WITH conversion_cte AS (
        SELECT 
            a.vtifa
            ,a.sales_order_line_item_name AS placement
            ,a.imp_time
            ,MIN(b.event_time) AS first_conversion
        FROM cd a
            JOIN in_app b ON a.vtifa = b.vtifa 
                AND a.imp_time <= b.event_time
                AND DATEDIFF(DAY, a.imp_time, b.event_time) <= $attribution_window
            JOIN miniverse m ON m.vtifa = a.vtifa
        GROUP BY 1, 2, 3
    )

    -- get additional info
    -- determine the earliest impression and earliest converesion
    ,timing_cte AS (
        SELECT
            c.vtifa 
            ,c.placement
            ,MIN(c.imp_time) AS first_impression
            ,MIN(c.first_conversion) AS first_conversion
        FROM conversion_cte c
        GROUP BY 1, 2
    )

    -- select placements, first impression, first conversion, and number of exposures before first conversion
    SELECT
        c.vtifa 
        ,c.placement 
        ,t.first_impression
        ,t.first_conversion
        ,COUNT(DISTINCT c.imp_time) AS exposures
        ,DATEDIFF(SECOND, t.first_impression, t.first_conversion) AS seconds_til_conversion
    FROM conversion_cte c
        JOIN timing_cte t USING (vtifa, placement)
    WHERE 
        c.imp_time BETWEEN t.first_impression AND t.first_conversion
    GROUP BY 1, 2, 3, 4
    
);

/**
SELECT COUNT(*) AS exposures FROM exp_in_app;
SELECT COUNT(DISTINCT vtifa) AS exposed FROM exp_in_app;
SELECT * FROM exp_in_app LIMIT 1000;
**/



/*******************
 Data Output
*******************/
-- Placement, Exposed Conversion Data
SELECT 
    'DAY' AS view 
    ,first_impression::DATE AS first_impression
    ,placement
    ,MEDIAN(exposures) AS median_exposures_til_first_conversion
    ,(MEDIAN(seconds_til_conversion)/60)/60 AS median_hrs_til_first_conversion
    ,COUNT(DISTINCT vtifa) AS conversions
FROM exp_in_app e
GROUP BY 1, 2, 3
UNION ALL
SELECT 
    'PLACEMENT' AS view 
    ,MIN(first_impression::DATE) AS first_impression
    ,placement
    ,MEDIAN(exposures) AS median_exposures_til_first_conversion
    ,(MEDIAN(seconds_til_conversion)/60)/60 AS median_hrs_til_first_conversion
    ,COUNT(DISTINCT vtifa) AS conversions
FROM exp_in_app e
GROUP BY 1, 3
UNION ALL
SELECT 
    'TOTAL UNIQUE' AS view
    ,MIN(first_impression::DATE) AS first_impression
    ,'ALL' AS placement
    ,MEDIAN(exposures) AS median_exposures_til_first_conversion
    ,(MEDIAN(seconds_til_conversion)/60)/60 AS median_hrs_til_first_conversion
    ,COUNT(DISTINCT vtifa) AS conversions
FROM exp_in_app e
GROUP BY 1, 3
ORDER BY 1, 2, 3, 6
;





-- in-app ACR content sample
SELECT
    a.vtifa
    ,a.event_time
    ,i.*
FROM app_usage a 
    JOIN iad i ON a.vpsid = i.psid_pii_virtual_id
WHERE 
    a.udw_partition_datetime BETWEEN $start_app_required_open_window AND $end_app_required_open_windwow
LIMIT 1000;



-- Lift Report
DROP TABLE IF EXISTS data_output_1;
CREATE TEMP TABLE data_output_1 AS (

	WITH camp AS (
		SELECT 
			COUNT(DISTINCT cd.vtifa) AS reach, 
			COUNT(*) AS imp,
			CAST(imp AS FLOAT)/reach AS freq
		FROM cd
			JOIN miniverse m ON m.vtifa = cd.vtifa
	), 

	total AS (
		SELECT 
			(SELECT COUNT(DISTINCT vtifa) FROM miniverse) AS total_aud, -- use miniverse
			-- (SELECT COUNT(DISTINCT vtifa) FROM in_app) AS total_conv_aud -- inner join miniverse with all (conversions)
			( 
				SELECT COUNT(DISTINCT m.vtifa) 
				FROM miniverse m 
					INNER JOIN in_app e ON e.vtifa = m.vtifa
			) AS total_conv_aud -- inner join miniverse with all (conversions)
	) 	


	SELECT 
        ( SELECT COUNT(DISTINCT vtifa) FROM samsung_ue) AS universe,
        total_aud AS miniverse,
		imp, 
		reach, 
		freq, 
		(SELECT COUNT(DISTINCT vtifa) FROM exp_in_app) AS exp_conv_aud, 
		CAST(exp_conv_aud AS FLOAT)/reach AS exp_conv_rate, 
		total_aud - reach AS unexp_aud, 
		total_conv_aud - exp_conv_aud AS unexp_conv_aud,   				
		CAST(unexp_conv_aud AS FLOAT)/unexp_aud AS unexp_conv_rate,  	
		exp_conv_rate/unexp_conv_rate - 1 AS lift  						
	FROM camp, total
);


-- finial select
SELECT * FROM data_output_1;







/*************************
==========================
 Tests
==========================
-- in-app ACR content sample
SELECT
    a.vtifa
    ,a.event_time
    ,i.*
FROM app_usage a 
    JOIN iad i ON a.vpsid = i.psid_pii_virtual_id
WHERE 
    a.udw_partition_datetime BETWEEN $start_app_required_open_window AND $end_app_required_open_windwow
LIMIT 1000;
-- ---------------------
SELECT * 
FROM in_app a
    INNER JOIN exp_in_app e USING(vtifa)
LIMIT 1000;
-- ---------------------
SELECT 
    a.vtifa,
    a.campaign_name,
    a.sales_order_line_item_name,
    COUNT(*) AS exposed_conversions
FROM cd a
    JOIN in_app b ON a.vtifa = b.vtifa 
        AND a.imp_time <= b.event_time
        AND DATEDIFF(DAY, a.imp_time, b.event_time) <= $attribution_window
    JOIN miniverse m ON m.vtifa = a.vtifa
GROUP BY 1, 2, 3
LIMIT 1000;
 -- ---------------------       

SELECT

( -- ---------------------
	SELECT COUNT(DISTINCT vtifa) FROM samsung_ue
) AS universe
 -- ---------------------
,( -- ---------------------
	SELECT COUNT(DISTINCT vtifa) FROM miniverse
) AS miniverse
 -- ---------------------
,( -- ---------------------
	SELECT COUNT(DISTINCT m.vtifa) AS exposed_converted 
	FROM miniverse m 
		INNER JOIN exp_in_app e ON e.vtifa = m.vtifa
		INNER JOIN in_app c ON c.vtifa = m.vtifa 
) AS exposed_converted 
 -- ---------------------
,( -- ---------------------
	SELECT COUNT(DISTINCT m.vtifa) AS exposed_unconverted 
	FROM miniverse m 
		INNER JOIN exp_in_app e ON e.vtifa = m.vtifa
	WHERE
		m.vtifa NOT IN (
			SELECT c.vtifa 
			FROM in_app c
			WHERE c.vtifa IS NOT NULL
		)
) AS exposed_unconverted 
 -- ---------------------
,( -- ---------------------
	SELECT COUNT(DISTINCT m.vtifa) AS unexposed_converted 
	FROM miniverse m 
		INNER JOIN in_app c ON c.vtifa = m.vtifa 
	WHERE
		m.vtifa NOT IN (
			SELECT e.vtifa 
			FROM exp_in_app e
			WHERE e.vtifa IS NOT NULL
		)
) AS unexposed_converted 
 -- ---------------------
,( -- ---------------------
	SELECT COUNT(DISTINCT m.vtifa) AS unexposed_unconverted 
	FROM miniverse m 
	WHERE
		m.vtifa NOT IN (
			SELECT e.vtifa 
			FROM exp_in_app e
			WHERE e.vtifa IS NOT NULL
		)
		AND 
		m.vtifa NOT IN (
			SELECT c.vtifa 
			FROM in_app c
			WHERE c.vtifa IS NOT NULL
		)
) AS unexposed_unconverted
;
********************/

