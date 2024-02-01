/*****************************************
 HBO MAX IN-APP ADVERTISER TEST QUERY
 Runtime: 2 minutes
 Objective: Can we determine if in-App ACR table contains house ad (e.g. Max Ad served on Max App)
*****************************************/

-- connection settings
USE ROLE UDW_MARKETING_ANALYTICS_DEFAULT_CONSUMER_ROLE_PROD;
USE DATABASE UDW_PROD;
USE WAREHOUSE UDW_MARKETING_ANALYTICS_DEFAULT_WH_PROD;
USE SCHEMA PUBLIC;


-- set variables
SET report_start_date = '2023112600';
SET report_end_date   = '2023112623';
SET app_id            = '3202301029760'; -- Max
SET country           = 'US';
SET vao               = 126621;
SET attribution_window = 7; -- days
-- SET content_id        = '82809368';

-- required app opens
-- date range in which consumers need to have opened app
SET start_app_required_open_window = '2023-11-22 00:00:00';
SET end_app_required_open_windwow = '2023-12-04 23:59:59';

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
		a.udw_partition_datetime BETWEEN $start_app_required_open_window AND $end_app_required_open_windwow
		AND a.country = $country
		AND a.app_id = $app_id
		AND DATEDIFF('second', start_timestamp, end_timestamp) >= 60 
	GROUP BY 
		1, 2, 3, 4
);

-- SELECT COUNT(DISTINCT vtifa) AS users FROM app_usage; -- 8,570,021
-- select * from app_usage limit 100;


/*****************
 Get conversions
 return ad tier app usage: devices where user opened app during desired window. This is a conversion.
 "filtered_restricted_content_exposure" would let us know this an ad tier account (supported by ads).
  See full explanation at "app_usage" table definition

 refactor todo: We can mabye extract filtered_restricted table IDs instead of JOIN on whole table?
*****************/
DROP TABLE IF EXISTS in_app;
CREATE TEMP TABLE in_app AS (
    SELECT
		a.vtifa
		,a.event_time
		,r.app_id
		,r.channel_id
		,r.content_type
		,r.content_id
		,r.provider_id
		,r.acr_source
		,r.udw_batch_id
		,r.udw_partition_datetime
    FROM app_usage a 
		JOIN data_tv_acr.filtered_restricted_content_exposure_without_pii r ON r.psid_pii_virtual_id = a.vpsid 
			AND r.app_id = $app_id
    WHERE 
        a.udw_partition_datetime BETWEEN $start_app_required_open_window AND $end_app_required_open_windwow
		AND r.udw_partition_datetime BETWEEN $start_app_required_open_window AND $end_app_required_open_windwow
		-- AND r.content_type IN ('AD', 'OTT_AD', 'POLAD')
		-- AND r.content_id IN ('15258152743', '15217944449')
);


/**
Note: 
-------
Join "data_tv_acr.filtered_restricted_content_exposure_without_pii.content_id" ON "meta_ad_src_snapshot.numerator_avod_creatives_us_latest.ad_id".

Meta Data will be on OTT_AD. AD types have alphanumeric content ID (e.g., "ADS:US:provider:Competitrack:SHFYCO-1827") which
can't be joined to the numeric numerator_avod ad_id.

Content_type "AD":
--------------------
	Linear Ads creatives and metadata from 3P vendors: Vivvix in the US and Ebiquity in ESBO. 
	AD content type can match in-app if the ad creative was viewed in one of the In-App ACR supported apps.

Content_type "OTT_AD":
----------------------
	AVOD Ads creatives and metadata for 10 AVOD apps: 
	Disney+, Tubi TV, Roku, Pluto TV, Hulu, Paramount+, Peacock, Discovery+, 
	HBO Max, Prende TV. OTT_AD content type is only available in the US.

https://adgear.atlassian.net/wiki/spaces/AGILE/pages/19472811887/In-App+ACR+Datasets#Metadata
**/



/**
--=========================
-- count rows from in_app
--=========================
SELECT count(*) 
FROM in_app
WHERE 
	content_type <> 'AD';
**/



/**
--===========================================
-- count rows from in_app joined on metadata 
--===========================================
SELECT COUNT(*) 
FROM in_app a
LEFT JOIN meta_ad_src_snapshot.numerator_avod_creatives_us_latest n ON a.content_id = n.ad_id
WHERE
	a.content_type <> 'AD'
	n.partition_country = $country
LIMIT 1000;
**/



/**
--===============================
-- get a sample of Max Metadata
--===============================
SELECT DISTINCT
	n.advertiser_name
	,n.k_subsidiary_name
	,n.product_name
FROM in_app a
	LEFT JOIN meta_ad_src_snapshot.numerator_avod_creatives_us_latest n ON a.content_id = n.ad_id
WHERE
	n.partition_country = $country
	and n.advertiser_name = 'Max'
LIMIT 1000;
**/



--===============================
-- get a sample ACR Metadata
--===============================
SELECT
	a.vtifa
	,a.content_type
	,a.content_id
	,n.advertiser_name
	,n.k_brand_name
	,n.k_subsidiary_name
	,n.product_name
FROM in_app a
	LEFT JOIN meta_ad_src_snapshot.numerator_avod_creatives_us_latest n ON a.content_id = n.ad_id
WHERE
	n.partition_country = $country
LIMIT 1000;



