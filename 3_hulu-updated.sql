/********************
 Template: Hulu - LiveRamp & App Pixel Attribution Report
 Wiki: https://adgear.atlassian.net/wiki/spaces/MAST/pages/19409273050/Hulu+-+LiveRamp+App+Pixel+Attribution+Report

 Ticket:
 https://adgear.atlassian.net/browse/SAI-5826
 ID: 1160536249
********************/

-- connection settings
USE ROLE UDW_MARKETING_ANALYTICS_DEFAULT_CONSUMER_ROLE_PROD;
USE DATABASE UDW_PROD;
USE WAREHOUSE UDW_MARKETING_ANALYTICS_DEFAULT_WH_PROD;
USE SCHEMA PUBLIC;



-- set variables 
-- change country, app name, vao
SET reporting_country           = 'US';
SET app_name                    = 'Hulu';
SET reporting_vao               = 136761;
SET reporting_vao2              = NULL;
SET attribution_window_unit     = 'DAY';
SET attribution_window_liveramp = 0;



/********************
 Audience S3 Intake

 Import Hulu DMP audience from s3
 1. copy to location below
 2. import to temp table "input_audience_s3"
********************/
DROP TABLE IF EXISTS input_audience_s3;
CREATE temp TABLE input_audience_s3 (psid VARCHAR(512));
COPY INTO input_audience_s3
FROM @adbiz_data.SAMSUNG_ADS_DATA_SHARE/analytics/custom/vaughn/hulu/test/274913_sai5826_20240118.csv
FILE_FORMAT = (format_name = adbiz_data.analytics_csv);


DROP TABLE IF EXISTS input_audience;
CREATE TEMP TABLE input_audience AS (
    SELECT DISTINCT
        vtifa
    FROM input_audience_s3 a
        LEFT JOIN udw_prod.udw_lib.virtual_psid_tifa_mapping_v m ON LOWER(m.psid) = a.psid
);

-- SELECT COUNT(*) FROM input_audience;


/********************
 Campaign Mapping

 https://github.com/SamsungAdsAnalytics/QueryBase/blob/master/UDW/Basic_Queries/Campaign_Mapping/VAO%20to%20Line%20Item%20and%20Campaign%2BFlight%2BCreative
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
                sf_opp.samsung_campaign_id__c AS samsung_campaign_id,
                sf_opp.operative_order_id__c AS sales_order_id,
                sf_opp.order_name__c AS sales_order_name,
                ROW_NUMBER() OVER(PARTITION BY vao ORDER BY sf_opp.lastmodifieddate DESC) AS rn
            FROM SALESFORCE.OPPORTUNITY AS sf_opp
            WHERE vao IN (
                $reporting_vao
                -- ,$reporting_vao2
            )
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
                    cmpgn_att.external_id AS sales_order_id,
                    cmpgn_att.li_external_id AS sales_order_line_item_id
                FROM TRADER.CAMPAIGN_OMS_ATTRS_LATEST AS cmpgn_att
                    JOIN vao_samsungCampaignID ON vao_samsungCampaignID.sales_order_id = cmpgn_att.external_id
            ) AS foo ON cmpgn.id = foo.campaign_id
    ),

    flight AS (
        SELECT DISTINCT
            cmpgn.sales_order_id,
            flight.id AS flight_id,
            flight.name AS flight_name,
            flight.start_at_datetime::TIMESTAMP AS flight_start_datetime_utc,
            flight.end_at_datetime::TIMESTAMP AS flight_end_datetime_utc
        FROM TRADER.FLIGHTS_LATEST AS flight
            JOIN cmpgn USING (campaign_id)
    ),

    cmpgn_flight_creative AS (
        SELECT DISTINCT
            cmpgn.sales_order_id,
            campaign_id,
            flight_id,
            creative_id
        FROM DATA_AD_XDEVICE.FACT_DELIVERY_EVENT_WITHOUT_PII AS fact
            JOIN cmpgn USING (campaign_id)
        WHERE fact.udw_partition_datetime BETWEEN (SELECT MIN(cmpgn_start_datetime_utc) FROM cmpgn) AND (SELECT MAX(cmpgn_end_datetime_utc) FROM cmpgn)
    ),

    creative AS (
        SELECT DISTINCT 
            cmpgn_flight_creative.sales_order_id,
            creative.id AS creative_id,
            creative.name AS creative_name
        FROM TRADER.CREATIVES_LATEST AS creative
            JOIN cmpgn_flight_creative ON cmpgn_flight_creative.creative_id = creative.id
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


    -- Main query  (Remember to edit the parts you want to keep in below as well!)
    SELECT DISTINCT
        -- VAO info
        'combined' as vao, -- vao_samsungCampaignID.vao,
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
        -- Flight info
        flight.flight_id,
        flight.flight_name,
        flight.flight_start_datetime_utc,
        flight.flight_end_datetime_utc,
        -- Creative info
        creative.creative_id,
        creative.creative_name,
        -- Line Item info
        lineItem.sales_order_line_item_id,
        lineItem.sales_order_line_item_name,
        lineItem.sales_order_line_item_start_datetime_utc,
        lineItem.sales_order_line_item_end_datetime_utc
    FROM vao_samsungCampaignID
        JOIN salesOrder USING (sales_order_id)
        JOIN cmpgn USING (sales_order_id)
        JOIN flight USING (sales_order_id)
        JOIN cmpgn_flight_creative USING (sales_order_id)
        JOIN creative USING (sales_order_id)
        JOIN lineItem USING (sales_order_id, sales_order_line_item_id)
);


SET campaign_start = (SELECT MIN(cmpgn_start_datetime_utc) FROM campaign_meta)::TIMESTAMP;
SET campaign_end = (SELECT MAX(cmpgn_end_datetime_utc) FROM campaign_meta)::TIMESTAMP;
SHOW VARIABLES;

SELECT $reporting_country, $app_name, $reporting_vao, $campaign_start, $campaign_end;
SELECT * FROM campaign_meta;



/********************
 Samsung Universe

 https://github.com/SamsungAdsAnalytics/QueryBase/blob/master/UDW/Basic_Queries/Samsung%20Universe%20Update.sql
 
 Samsung Universe (aka. superset) is a collection of Samsung TVs that can be found in any of following 3 data sources:
    - TV Hardware: profile_tv.fact_psid_hardware_without_pii
    - App Open: data_tv_smarthub.fact_app_opened_event_without_pii
    - O&O Samsung Ads Campaign Delivery: data_ad_xdevice.fact_delivery_event_without_pii (for exchange_id = 6 and exchange_seller_id = 86) 
 
 Any data used for attribution reports needs to be intersected with Samsung Universe
 Reference: https://adgear.atlassian.net/wiki/spaces/MAST/pages/19673186934/M+E+Analytics+-+A+I+Custom+Report+Methodology
********************/
-- qualifier: start date = start date + 30 days if device graph resolution mechanism is used
SET report_start_date = TO_CHAR($campaign_start, 'YYYYMMDDHH');
SET report_end_date = TO_CHAR($campaign_end, 'YYYYMMDDHH');
SET country = $reporting_country;

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

-- SELECT COUNT(*) AS cnt FROM samsung_ue;  
          


/********************
 Common Tables
********************/
-- get content delivery (conversion) data
DROP TABLE IF EXISTS cd;
CREATE TEMP TABLE cd AS (
    SELECT
        fact.device_country AS country,
        GET(SAMSUNG_TVIDS_PII_VIRTUAL_ID, 0) AS vtifa, 
        event_time AS timing,
        cm.vao,
        cm.sales_order_line_item_id as line_item_id,
        cm.sales_order_line_item_name as line_item_name,
        cm.creative_id,
        cm.creative_name,
        cm.flight_id,
        cm.flight_name,
        COUNT(*) AS imps
    FROM data_ad_xdevice.fact_delivery_event_without_pii AS fact
        JOIN campaign_meta AS cm USING (campaign_id, flight_id, creative_id)
    WHERE udw_partition_datetime BETWEEN $campaign_start AND DATEADD($attribution_window_unit, $attribution_window_liveramp, $campaign_end)
        AND fact.type IN (1)
        AND fact.device_country = $reporting_country
    GROUP BY 1,2,3,4,5,6,7,8,9,10
);


-- get app usage
DROP TABLE IF EXISTS app_usg;
CREATE TEMP TABLE app_usg AS (

    WITH fact AS (
        SELECT
            fact.country,
            vtifa,
            fact.start_timestamp AS timing,
            fact.start_timestamp,
            fact.end_timestamp
        FROM data_tv_acr.fact_app_usage_session_without_pii AS fact
            JOIN adbiz_data.lup_app_cat_genre_2023 AS am USING (app_id)
            LEFT JOIN udw_prod.udw_lib.virtual_psid_tifa_mapping_v AS pii_map ON pii_map.vpsid = fact.psid_pii_virtual_id
        WHERE DATEDIFF('second', fact.start_timestamp, fact.end_timestamp) > 60
            AND udw_partition_datetime BETWEEN $campaign_start AND DATEADD($attribution_window_unit, $attribution_window_liveramp, $campaign_end)
            AND fact.country = $reporting_country
            AND am.prod_nm = $app_name
    )

    SELECT DISTINCT
        country,
        vtifa,
        timing,
        COUNT(*) AS app_opens,
        SUM(DATEDIFF('hours',start_timestamp,end_timestamp)) AS hour_spent
    FROM fact
        JOIN samsung_ue USING (country, vtifa)
    GROUP BY 1,2,3

);



/********************
 Liveramp
********************/
-- get intersection of 3P audience and samsung universe as "matched"
DROP TABLE IF EXISTS liveramp_matched_audience;
    CREATE TEMP TABLE liveramp_matched_audience AS (
    SELECT DISTINCT
        country,
        vtifa
    FROM samsung_ue AS superset
        JOIN input_audience AS s3 USING (vtifa)
);



-- get app usage for "matched"
DROP TABLE IF EXISTS liveramp_matched_app_usg;
CREATE TEMP TABLE liveramp_matched_app_usg AS (
    SELECT DISTINCT
        CONCAT('Samsung matched ', $app_name, ' subscribed app users') AS auds_cate,  -- ' app users' -> ' subscribed app users'
        country,
        timing,
        vtifa,
        SUM(app_opens) AS app_opens_conv,
        SUM(hour_spent) AS hour_spent_conv
    FROM app_usg
        JOIN liveramp_matched_audience USING (country, vtifa)
    GROUP BY 1,2,3,4
);



/********************
 Outputs
********************/
------------------------------------------------------------------------------------------------------------------------------------------------------
-- Liveramp
----------------
-- General Info
SELECT DISTINCT
    auds_cate,                                                          
    SUM(cnt_liveramp_raw) AS cnt_liveramp_raw,                          
    SUM(cnt_liveramp_samsung_matched) AS cnt_liveramp_samsung_matched 
FROM ( -- get liveramp audience counts
    SELECT
        CONCAT('Samsung matched ', $app_name, ' subscribed app users') AS auds_cate,
        COUNT(DISTINCT vtifa) AS cnt_liveramp_raw
    FROM input_audience
)
    JOIN ( -- join "matched" app usage data
        SELECT
            auds_cate,
            COUNT(DISTINCT vtifa) AS cnt_liveramp_samsung_matched
        FROM liveramp_matched_app_usg
        GROUP BY 1
    ) AS a USING (auds_cate)
GROUP BY 1
;


------------------------------------------------------------------------------------------------------------------------------------------------------
-- Overall Lift
----------------
SELECT DISTINCT
    vao,
    auds_cate,
    campaign_id,
    campaign_name,
    expd_liveramp_registered_app_user,
    expd_hr_spent_liveramp_registered_app_user,
    reach,
    imps,
    CAST(imps AS FLOAT)/reach AS freq,
    ttl_liveramp_registered_app_user,
    ttl_hr_spent_liveramp_registered_app_user,
    total_uni_superset,
    total_uni_superset - reach AS total_unexpd_uni_superset,
    ttl_liveramp_registered_app_user - expd_liveramp_registered_app_user AS unexpd_liveramp_registered_app_user,
    ttl_hr_spent_liveramp_registered_app_user - expd_hr_spent_liveramp_registered_app_user AS unexpd_hr_spent_liveramp_registered_app_user,
    CAST(expd_liveramp_registered_app_user AS FLOAT) / reach AS expd_conv_rate,
    CAST(unexpd_liveramp_registered_app_user AS FLOAT) / total_unexpd_uni_superset AS unexpd_conv_rate,
    CAST(unexpd_hr_spent_liveramp_registered_app_user AS FLOAT) / unexpd_liveramp_registered_app_user AS avg_unexpd_hour_spent,
    CAST(expd_hr_spent_liveramp_registered_app_user AS FLOAT) / expd_liveramp_registered_app_user AS avg_expd_hour_spent,
    expd_conv_rate / unexpd_conv_rate - 1  AS lift,
    avg_expd_hour_spent / avg_unexpd_hour_spent - 1 AS lift_time_spent
FROM (
    SELECT
        country,
        auds_cate,
        'overall' AS campaign_id,
        'overall' AS campaign_name,
        COUNT(DISTINCT vtifa) AS expd_liveramp_registered_app_user,
        SUM(hour_spent_conv) AS expd_hr_spent_liveramp_registered_app_user
    FROM liveramp_matched_app_usg
        JOIN ( -- content delivery
            SELECT DISTINCT
                country,
                vao,
                vtifa
            FROM cd
        ) AS x USING (country, vtifa)
    GROUP BY 1,2,3,4
) AS a
    JOIN (
        SELECT
            country,
            vao,
            'overall' AS campaign_id,
            'overall' AS campaign_name,
            COUNT(DISTINCT vtifa) AS reach,
            SUM(imps) AS imps
        FROM cd
        GROUP BY 1,2,3,4
    ) AS b USING (country, campaign_id, campaign_name)
    JOIN (
        SELECT
            country,
            COUNT(DISTINCT vtifa) AS ttl_liveramp_registered_app_user,
            SUM(hour_spent_conv) AS ttl_hr_spent_liveramp_registered_app_user
        FROM liveramp_matched_app_usg
        GROUP BY 1
    ) AS c USING (country)
    JOIN (
        SELECT
            country,
            COUNT(DISTINCT vtifa) AS total_uni_superset
        FROM samsung_ue
        GROUP BY 1
    ) AS d USING (country)
;


------------------------------------------------------------------------------------------------------------------------------------------------------
-- Lift by Impression
----------------------
SELECT DISTINCT
    vao,
    auds_cate,
    expd_imps,
    expd_liveramp_registered_app_user,
    reach,
    ttl_liveramp_registered_app_user,
    total_uni_superset,
    total_uni_superset - reach AS total_unexpd_uni_superset,
    ttl_liveramp_registered_app_user - expd_liveramp_registered_app_user AS unexpd_liveramp_registered_app_user,
    CAST(expd_liveramp_registered_app_user AS FLOAT) / reach AS expd_conv_rate,
    CAST(unexpd_liveramp_registered_app_user AS FLOAT) / total_unexpd_uni_superset AS unexpd_conv_rate,
    expd_conv_rate / unexpd_conv_rate - 1  AS lift
FROM (
    SELECT
        country,
        auds_cate,
        CASE
            WHEN imps > 19 THEN 20
            ELSE imps
            END AS expd_imps,
        COUNT(DISTINCT vtifa) AS expd_liveramp_registered_app_user
    FROM liveramp_matched_app_usg
        JOIN (
            SELECT
                country,
                vao,
                vtifa,
                SUM(imps) AS imps
            FROM cd
            GROUP BY 1,2,3
        ) AS x USING (country, vtifa)
    GROUP BY 1,2,3
) AS a
    JOIN (
        SELECT
            country,
            vao,
            CASE
                WHEN imps > 19 THEN 20
                ELSE imps
                END AS expd_imps,
            COUNT(DISTINCT vtifa) AS reach
        FROM (
            SELECT
                country,
                vao,
                vtifa,
                SUM(imps) AS imps
            FROM cd
            GROUP BY 1,2,3
        ) AS x
        GROUP BY 1,2,3
    ) AS b USING (country, expd_imps)
    JOIN (
        SELECT
            country,
            COUNT(DISTINCT vtifa) AS ttl_liveramp_registered_app_user
        FROM liveramp_matched_app_usg
        GROUP BY 1
    ) AS c USING (country)
    JOIN (
        SELECT
            country,
            COUNT(DISTINCT vtifa) AS total_uni_superset
        FROM samsung_ue
        GROUP BY 1
    ) AS d USING (country)
ORDER BY expd_imps ASC
;


------------------------------------------------------------------------------------------------------------------------------------------------------
-- Lift by Line item / Placement
---------------------------------
SELECT DISTINCT
    vao,
    auds_cate,
    campaign_id,
    campaign_name,
    expd_liveramp_registered_app_user,
    expd_hr_spent_liveramp_registered_app_user,
    reach,
    imps,
    CAST(imps AS FLOAT)/reach AS freq,
    ttl_liveramp_registered_app_user,
    ttl_hr_spent_liveramp_registered_app_user,
    total_uni_superset,
    total_uni_superset - reach AS total_unexpd_uni_superset,
    ttl_liveramp_registered_app_user - expd_liveramp_registered_app_user AS unexpd_liveramp_registered_app_user,
    ttl_hr_spent_liveramp_registered_app_user - expd_hr_spent_liveramp_registered_app_user AS unexpd_hr_spent_liveramp_registered_app_user,
    CAST(expd_liveramp_registered_app_user AS FLOAT) / reach AS expd_conv_rate,
    CAST(unexpd_liveramp_registered_app_user AS FLOAT) / total_unexpd_uni_superset AS unexpd_conv_rate,
    CAST(unexpd_hr_spent_liveramp_registered_app_user AS float) / unexpd_liveramp_registered_app_user AS avg_unexpd_hour_spent,
    CAST(expd_hr_spent_liveramp_registered_app_user AS float) / expd_liveramp_registered_app_user AS avg_expd_hour_spent,
    expd_conv_rate / unexpd_conv_rate - 1  AS lift,
    avg_expd_hour_spent / avg_unexpd_hour_spent - 1 AS lift_time_spent
FROM (
    SELECT
        country,
        auds_cate,
        line_item_id AS campaign_id,
        line_item_name AS campaign_name,
        COUNT(DISTINCT vtifa) AS expd_liveramp_registered_app_user,
        SUM(hour_spent_conv) AS expd_hr_spent_liveramp_registered_app_user
    FROM liveramp_matched_app_usg
        JOIN (
            SELECT DISTINCT
                country,
                vao,
                vtifa,
                line_item_id,
                line_item_name
            FROM cd
        ) AS x USING (country, vtifa)
    GROUP BY 1,2,3,4
) AS a
    JOIN (
        SELECT
            country,
            vao,
            line_item_id AS campaign_id,
            line_item_name AS campaign_name,
            COUNT(DISTINCT vtifa) AS reach,
            SUM(imps) AS imps
        FROM cd
        GROUP BY 1,2,3,4
    ) AS b USING (country, campaign_id, campaign_name)
    JOIN (
        SELECT
            country,
            COUNT(DISTINCT vtifa) AS ttl_liveramp_registered_app_user,
            SUM(hour_spent_conv) AS ttl_hr_spent_liveramp_registered_app_user
        FROM liveramp_matched_app_usg
        GROUP BY 1
    ) AS c USING (country)
    JOIN (
        SELECT
            country,
            COUNT(DISTINCT vtifa) AS total_uni_superset
        FROM samsung_ue
        GROUP BY 1
    ) AS d USING (country)
ORDER BY campaign_name, campaign_id
;


------------------------------------------------------------------------------------------------------------------------------------------------------
-- Lift by Creative
--------------------
SELECT DISTINCT
    vao,
    auds_cate,
    campaign_id,
    campaign_name,
    expd_liveramp_registered_app_user,
    expd_hr_spent_liveramp_registered_app_user,
    reach,
    imps,
    CAST(imps AS FLOAT)/reach AS freq,
    ttl_liveramp_registered_app_user,
    ttl_hr_spent_liveramp_registered_app_user,
    total_uni_superset,
    total_uni_superset - reach AS total_unexpd_uni_superset,
    ttl_liveramp_registered_app_user - expd_liveramp_registered_app_user AS unexpd_liveramp_registered_app_user,
    ttl_hr_spent_liveramp_registered_app_user - expd_hr_spent_liveramp_registered_app_user AS unexpd_hr_spent_liveramp_registered_app_user,
    CAST(expd_liveramp_registered_app_user AS FLOAT) / reach AS expd_conv_rate,
    CAST(unexpd_liveramp_registered_app_user AS FLOAT) / total_unexpd_uni_superset AS unexpd_conv_rate,
    cast(unexpd_hr_spent_liveramp_registered_app_user AS float) / unexpd_liveramp_registered_app_user AS avg_unexpd_hour_spent,
    cast(expd_hr_spent_liveramp_registered_app_user AS float) / expd_liveramp_registered_app_user AS avg_expd_hour_spent,
    expd_conv_rate / unexpd_conv_rate - 1  AS lift,
    avg_expd_hour_spent / avg_unexpd_hour_spent - 1 AS lift_time_spent
FROM (
    SELECT
        country,
        auds_cate,
        creative_id AS campaign_id,
        creative_name AS campaign_name,
        COUNT(DISTINCT vtifa) AS expd_liveramp_registered_app_user,
        SUM(hour_spent_conv) AS expd_hr_spent_liveramp_registered_app_user
    FROM liveramp_matched_app_usg
        JOIN (
            SELECT DISTINCT
                country,
                vao,
                vtifa,
                creative_id,
                creative_name
            FROM cd
        ) AS x USING (country, vtifa)
    GROUP BY 1,2,3,4
) AS a
    JOIN (
        SELECT
            country,
            vao,
            creative_id AS campaign_id,
            creative_name AS campaign_name,
            COUNT(DISTINCT vtifa) AS reach,
            SUM(imps) AS imps
        FROM cd
        GROUP BY 1,2,3,4
    ) AS b USING (country, campaign_id, campaign_name)
    JOIN (
        SELECT
            country,
            COUNT(DISTINCT vtifa) AS ttl_liveramp_registered_app_user,
            SUM(hour_spent_conv) AS ttl_hr_spent_liveramp_registered_app_user
        FROM liveramp_matched_app_usg
        GROUP BY 1
    ) AS c USING (country)
    JOIN (
        SELECT
            country,
            COUNT(DISTINCT vtifa) AS total_uni_superset
        FROM samsung_ue
        GROUP BY 1
    ) AS d USING (country)
ORDER BY campaign_name, campaign_id
;


------------------------------------------------------------------------------------------------------------------------------------------------------
-- By Flight
--------------
SELECT DISTINCT
    vao,
    auds_cate,
    flight_id,
    flight_name,
    expd_liveramp_registered_app_user,
    expd_hr_spent_liveramp_registered_app_user,
    reach,
    imps,
    CAST(imps AS FLOAT)/reach AS freq,
    ttl_liveramp_registered_app_user,
    ttl_hr_spent_liveramp_registered_app_user,
    total_uni_superset,
    total_uni_superset - reach AS total_unexpd_uni_superset,
    ttl_liveramp_registered_app_user - expd_liveramp_registered_app_user AS unexpd_liveramp_registered_app_user,
    ttl_hr_spent_liveramp_registered_app_user - expd_hr_spent_liveramp_registered_app_user AS unexpd_hr_spent_liveramp_registered_app_user,
    CAST(expd_liveramp_registered_app_user AS FLOAT) / reach AS expd_conv_rate,
    CAST(unexpd_liveramp_registered_app_user AS FLOAT) / total_unexpd_uni_superset AS unexpd_conv_rate,
    cast(unexpd_hr_spent_liveramp_registered_app_user AS float) / unexpd_liveramp_registered_app_user AS avg_unexpd_hour_spent,
    cast(expd_hr_spent_liveramp_registered_app_user AS float) / expd_liveramp_registered_app_user AS avg_expd_hour_spent,
    expd_conv_rate / unexpd_conv_rate - 1  AS lift,
    avg_expd_hour_spent / avg_unexpd_hour_spent - 1 AS lift_time_spent
FROM (
    SELECT
        country,
        auds_cate,
        flight_id,
        flight_name,
        COUNT(DISTINCT vtifa) AS expd_liveramp_registered_app_user,
        SUM(hour_spent_conv) AS expd_hr_spent_liveramp_registered_app_user
    FROM liveramp_matched_app_usg
        JOIN (
            SELECT DISTINCT
                country,
                vao,
                vtifa,
                flight_id,
                flight_name
            FROM cd
        ) AS x USING (country, vtifa)
    GROUP BY 1,2,3,4
) AS a
    JOIN (
        SELECT
            country,
            vao,
            flight_id,
            flight_name,
            COUNT(DISTINCT vtifa) AS reach,
            SUM(imps) AS imps
        FROM cd
        GROUP BY 1,2,3,4
    ) AS b USING (country, flight_id, flight_name)
    JOIN (
        SELECT
            country,
            COUNT(DISTINCT vtifa) AS ttl_liveramp_registered_app_user,
            SUM(hour_spent_conv) AS ttl_hr_spent_liveramp_registered_app_user
        FROM liveramp_matched_app_usg
        GROUP BY 1
    ) AS c USING (country)
    JOIN (
        SELECT
            country,
            COUNT(DISTINCT vtifa) AS total_uni_superset
        FROM samsung_ue
        GROUP BY 1
    ) AS d USING (country)
ORDER BY flight_name, flight_id
;


------------------------------------------------------------------------------------------------------------------------------------------------------
--- Daily Exposure Output
--------------------------
WITH cte_liveramp_registered AS (
    SELECT
        country,
        auds_cate,
        DATE_TRUNC('day', timing) AS timing_date,
        COUNT(DISTINCT vtifa) AS ttl_liveramp_registered_app_user
    FROM liveramp_matched_app_usg
    GROUP BY 1,2,3
),

cte_cd AS (
    SELECT
        country,
        vao,
        DATE_TRUNC('day', timing) AS timing_date,
        COUNT(DISTINCT vtifa) AS reach,
        SUM(imps) AS imps
    FROM cd
    GROUP BY 1,2,3
),

cte_expd_liveramp_registered AS (
    SELECT
        lr.country,
        DATE_TRUNC('day', lr.timing) AS timing_date,
        COUNT(DISTINCT lr.vtifa) AS expd_liveramp_registered_app_user
    FROM liveramp_matched_app_usg AS lr
    JOIN cd
        ON lr.country = cd.country
            AND lr.vtifa = cd.vtifa
            AND DATE_TRUNC('day', lr.timing) = DATE_TRUNC('day', cd.timing)
    GROUP BY 1,2
),

cte_superset AS (
    SELECT
        country,
        COUNT(DISTINCT vtifa) AS total_uni_superset
    FROM samsung_ue
    GROUP BY 1
)

SELECT DISTINCT
    vao,
    timing_date,
    auds_cate,
    ttl_liveramp_registered_app_user,
    expd_liveramp_registered_app_user,
    reach,
    imps,
    total_uni_superset,
    total_uni_superset - reach AS total_unexpd_uni_superset,
    ttl_liveramp_registered_app_user - expd_liveramp_registered_app_user AS unexpd_liveramp_registered_app_user
FROM cte_liveramp_registered
    JOIN cte_cd USING (country, timing_date)
    JOIN cte_expd_liveramp_registered USING (country, timing_date)
    JOIN cte_superset USING (country)
ORDER BY timing_date
;

