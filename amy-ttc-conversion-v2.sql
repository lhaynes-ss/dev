/*******************
Paramount+ US Conversion Time

Ticket:
https://adgear.atlassian.net/browse/SAI-5964

********************/
-- connection hooks
USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_MEDIUM; -- _MEDIUM
USE DATABASE UDW_PROD;



/*****************
config
*****************/
SET start_date = '2023-10-01';
SET end_date = '2023-12-31';

SET (
    report_end_date
    ,report_end_date_minus_3_months
    ,report_start_date
    ,app_name_string
    ,country
    ,vao_list
    ,attribution_window
) = (
    TO_TIMESTAMP($end_date || ' 23:59:59')                              -- report_end_date
    ,TO_TIMESTAMP(DATEADD('day', 1, DATEADD('months', -3, $end_date)))  -- report_end_date_minus_3_months
    ,TO_TIMESTAMP($start_date || ' 00:00:00')                           -- report_start_date
    ,'paramount'                                                        -- app_name_string
    ,'US'                                                               -- country
    ,'107448, 113062, 85586, 84817'                                     -- vao_list '107448, 113062, 85586, 84817'
    ,30                                                                 -- attribution_window (days)
);

SHOW VARIABLES;



-- app id table
-- get app ids for app usage conversions based on app name string
DROP TABLE IF EXISTS app_id;
CREATE TEMP TABLE app_id AS (
    SELECT DISTINCT 
        a.app_id AS app_id
    FROM adbiz_data.lup_app_cat_genre_2023 a 
    WHERE 
        LOWER(a.prod_nm) LIKE '%' || $app_name_string || '%'
);

-- SELECT * FROM app_id;



-- vao list table
-- convert vao list variable into a table
DROP TABLE IF EXISTS vaos;
CREATE TEMP TABLE vaos AS (
    SELECT DISTINCT t.value AS vao
        FROM TABLE(SPLIT_TO_TABLE($vao_list, ',')) AS t
);

-- SELECT * FROM vaos;



/*****************
Campaign Meta
*****************/
-- get campaign meta with vao from ticket
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
            WHERE vao IN (SELECT v.vao FROM vaos v)
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

    -- added product info to lineitem
    lineItem AS (
        SELECT
            sales_order_id,
            sales_order_line_item_id,
            sales_order_line_item_name,
            sales_order_line_item_start_datetime_utc,
            sales_order_line_item_end_datetime_utc,
            product.product_id,
            SPLIT_PART(REGEXP_REPLACE(product.product_name, '–', '-'), '-', 1) AS product,
            product.product_name
        FROM (
            SELECT
                lineItem.sales_order_id,
                lineItem.sales_order_line_item_id,
                lineItem.sales_order_line_item_name,
                lineItem.product_id,
                TIMESTAMP_NTZ_FROM_PARTS(lineItem.sales_order_line_item_start_date::date, lineItem.start_time::time) AS sales_order_line_item_start_datetime_utc,
                TIMESTAMP_NTZ_FROM_PARTS(lineItem.sales_order_line_item_end_date::date, lineItem.end_time::time) AS sales_order_line_item_end_datetime_utc,
                ROW_NUMBER() OVER(PARTITION BY lineItem.sales_order_line_item_id ORDER BY lineItem.last_modified_on) AS rn
            FROM OPERATIVEONE.SALES_ORDER_LINE_ITEMS AS lineItem
                JOIN vao_samsungCampaignID AS vao USING (sales_order_id)
        ) AS foo
        LEFT JOIN OPERATIVEONE.PRODUCTS AS product USING(product_id)
        WHERE foo.rn = 1
    )

    -- --------------
    -- Main query 
    -- --------------         
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
        lineItem.product_id,
        lineItem.product,
        lineItem.product_name
    FROM vao_samsungCampaignID
        JOIN salesOrder USING (sales_order_id)
        JOIN cmpgn USING (sales_order_id)
        JOIN lineItem USING (sales_order_id, sales_order_line_item_id)
    WHERE 
        -- only include campaign data where lines were active in reporting window
        lineItem.sales_order_line_item_start_datetime_utc <= $report_end_date
        AND lineItem.sales_order_line_item_end_datetime_utc >= $report_start_date
);

SELECT * FROM campaign_meta;



/*****************
samsung universe
*****************/
/**
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
            THEN DATEADD(DAY, -30, TO_DATE($report_start_date))::TIMESTAMP 
            ELSE $report_start_date 
        END AS report_start_date
    FROM DATA_AD_XDEVICE.FACT_DELIVERY_EVENT_WITHOUT_PII a
        JOIN campaign_meta b ON a.campaign_id = b.campaign_id
    WHERE 
        udw_partition_datetime BETWEEN $report_start_date AND $report_end_date
        AND TYPE = 1
        AND device_country = $country
);

-- SELECT * FROM qualifier;
SET report_start_date_qual = (SELECT report_start_date FROM qualifier);
**/

-- save time for testing
SET report_start_date_qual = $report_start_date;



DROP TABLE IF EXISTS samsung_ue; --5 mins IN M
CREATE TEMP TABLE samsung_ue AS (
    SELECT DISTINCT m.vtifa
    FROM PROFILE_TV.FACT_PSID_HARDWARE_WITHOUT_PII a
        JOIN UDW_LIB.VIRTUAL_PSID_TIFA_MAPPING_V m ON a.PSID_PII_VIRTUAL_ID = m.vpsid
    WHERE 
        udw_partition_datetime BETWEEN $report_start_date_qual AND $report_end_date
        AND partition_country = $country	
    UNION
    SELECT DISTINCT GET(SAMSUNG_TVIDS_PII_VIRTUAL_ID , 0) AS vtifa
    FROM DATA_AD_XDEVICE.FACT_DELIVERY_EVENT_WITHOUT_PII 	
    WHERE 
        udw_partition_datetime BETWEEN $report_start_date_qual AND $report_end_date
        AND TYPE = 1
        AND (dropped != TRUE OR  dropped IS NULL)
        AND (EXCHANGE_ID = 6 OR EXCHANGE_SELLER_ID = 86)
        AND device_country = $country
    UNION 
    SELECT DISTINCT m.vtifa
    FROM DATA_TV_SMARTHUB.FACT_APP_OPENED_EVENT_WITHOUT_PII a 
        JOIN UDW_PROD.UDW_LIB.VIRTUAL_PSID_TIFA_MAPPING_V m ON a.PSID_PII_VIRTUAL_ID = m.vpsid
    WHERE 
        udw_partition_datetime BETWEEN $report_start_date_qual AND $report_end_date
        AND partition_country = $country
);

-- SELECT COUNT(*) AS universe_size FROM samsung_ue; 



/*****************
app usage
    app usage wuth attribution window. Filter by country, app, and must be 60 seconds+.
    row number is added to get first app usage later.
*****************/
DROP TABLE IF EXISTS app_usage;
CREATE TEMP TABLE app_usage AS (
    SELECT
        vtifa
        ,vpsid
        ,start_timestamp AS app_use_event_time
        ,a.udw_partition_datetime
        ,SUM(DATEDIFF('second', start_timestamp, end_timestamp)) AS app_use_event_duration_seconds
        ,ROW_NUMBER() OVER(PARTITION BY vtifa ORDER BY start_timestamp ASC) AS rn
    FROM data_tv_acr.fact_app_usage_session_without_pii AS a
        LEFT JOIN udw_prod.udw_lib.virtual_psid_tifa_mapping_v AS m ON m.vpsid = a.psid_pii_virtual_id
    WHERE 
        a.udw_partition_datetime BETWEEN $report_start_date AND DATEADD('day', $attribution_window, $report_end_date)
        AND a.country = $country
        AND a.app_id IN (SELECT DISTINCT ai.app_id FROM app_id ai)
        AND DATEDIFF('second', start_timestamp, end_timestamp) >= 60 
    GROUP BY 
        1, 2, 3, 4
);

-- SELECT COUNT(DISTINCT vtifa) AS app_users FROM app_usage; 



/*****************
exposures (impressions)
*****************/
DROP TABLE IF EXISTS cd;
CREATE TEMP TABLE cd AS (
	SELECT 
		GET(SAMSUNG_TVIDS_PII_VIRTUAL_ID, 0) AS vtifa
		,event_time AS imp_time
        ,b.product
	FROM DATA_AD_XDEVICE.FACT_DELIVERY_EVENT_WITHOUT_PII a
		JOIN campaign_meta b ON a.campaign_id = b.campaign_id
		JOIN samsung_ue c ON GET(a.SAMSUNG_TVIDS_PII_VIRTUAL_ID, 0) = c.vtifa
	WHERE 
		a.UDW_PARTITION_DATETIME BETWEEN $report_start_date AND $report_end_date
		AND TYPE = 1 -- 1 = impression, 2 = click
		AND device_country = $country
);



/*****************
first app usage
    get first app usage for each vtifa
*****************/
DROP TABLE IF EXISTS first_app_usage;
CREATE TEMP TABLE first_app_usage AS (
    SELECT
        vtifa
        ,vpsid
        ,app_use_event_time
        ,udw_partition_datetime
    FROM app_usage
    WHERE 
        rn = 1
);

-- SELECT COUNT(*) FROM first_app_usage;


/*****************
exposed conversions
*****************/
-- get first touch, first app usage
DROP TABLE IF EXISTS exp_first_app_use_ft; 
CREATE TEMP TABLE exp_first_app_use_ft AS (
    WITH cte AS (
        SELECT 
            a.vtifa
            ,a.imp_time
            ,a.product
            ,b.app_use_event_time
            ,b.udw_partition_datetime
            ,ROW_NUMBER() OVER(PARTITION BY a.vtifa, b.app_use_event_time ORDER BY a.imp_time ASC) AS rn
        FROM cd a
            JOIN first_app_usage b ON a.vtifa = b.vtifa 
                AND a.imp_time <= b.app_use_event_time
                AND DATEDIFF('day', a.imp_time, b.app_use_event_time) <= $attribution_window
    )

    SELECT
        c.vtifa
        ,c.imp_time
        ,c.product
        ,c.app_use_event_time
        ,c.udw_partition_datetime
        ,DATEDIFF('day', c.imp_time, c.app_use_event_time) AS total_days_to_convert
    FROM cte c
    WHERE
        c.rn = 1
);

-- SELECT COUNT(DISTINCT vtifa) FROM exp_first_app_use_ft;



-- get last touch, first app usage
DROP TABLE IF EXISTS exp_first_app_use_lt; 
CREATE TEMP TABLE exp_first_app_use_lt AS (
    WITH cte AS (
        SELECT 
            a.vtifa
            ,a.imp_time
            ,a.product
            ,b.app_use_event_time
            ,b.udw_partition_datetime
            ,ROW_NUMBER() OVER(PARTITION BY a.vtifa, b.app_use_event_time ORDER BY a.imp_time DESC) AS rn
        FROM cd a
            JOIN first_app_usage b ON a.vtifa = b.vtifa 
                AND a.imp_time <= b.app_use_event_time
                AND DATEDIFF('day', a.imp_time, b.app_use_event_time) <= $attribution_window
    )

    SELECT
        c.vtifa
        ,c.imp_time
        ,c.product
        ,c.app_use_event_time
        ,c.udw_partition_datetime
        ,DATEDIFF('day', c.imp_time, c.app_use_event_time) AS total_days_to_convert
    FROM cte c
    WHERE
        c.rn = 1
);

-- SELECT COUNT(DISTINCT vtifa) FROM exp_first_app_use_lt;



-- get first touch, all app usage
DROP TABLE IF EXISTS exp_app_use_ft; 
CREATE TEMP TABLE exp_app_use_ft AS (
    WITH cte AS (
        SELECT 
            a.vtifa
            ,a.imp_time
            ,a.product
            ,b.app_use_event_time
            ,b.udw_partition_datetime
            ,ROW_NUMBER() OVER(PARTITION BY a.vtifa, b.app_use_event_time ORDER BY a.imp_time ASC) AS rn
        FROM cd a
            JOIN app_usage b ON a.vtifa = b.vtifa 
                AND a.imp_time <= b.app_use_event_time
                AND DATEDIFF('day', a.imp_time, b.app_use_event_time) <= $attribution_window
    )

    SELECT
        c.vtifa
        ,c.imp_time
        ,c.product
        ,c.app_use_event_time
        ,c.udw_partition_datetime
        ,DATEDIFF('day', c.imp_time, c.app_use_event_time) AS total_days_to_convert
    FROM cte c
    WHERE
        c.rn = 1
);

-- SELECT COUNT(DISTINCT vtifa) FROM exp_app_use_ft;



-- get last touch, all app usage
DROP TABLE IF EXISTS exp_app_use_lt; 
CREATE TEMP TABLE exp_app_use_lt AS (
    WITH cte AS (
        SELECT 
            a.vtifa
            ,a.imp_time
            ,a.product
            ,b.app_use_event_time
            ,b.udw_partition_datetime
            ,ROW_NUMBER() OVER(PARTITION BY a.vtifa, b.app_use_event_time ORDER BY a.imp_time DESC) AS rn
        FROM cd a
            JOIN app_usage b ON a.vtifa = b.vtifa 
                AND a.imp_time <= b.app_use_event_time
                AND DATEDIFF('day', a.imp_time, b.app_use_event_time) <= $attribution_window
    )

    SELECT
        c.vtifa
        ,c.imp_time
        ,c.product
        ,c.app_use_event_time
        ,c.udw_partition_datetime
        ,DATEDIFF('day', c.imp_time, c.app_use_event_time) AS total_days_to_convert
    FROM cte c
    WHERE
        c.rn = 1
);

-- SELECT COUNT(DISTINCT vtifa) FROM exp_app_use_lt;


/*****************
OUTPUT
*****************/
/*****************
time to conversion: first touch
*****************/
WITH time_to_conversion AS (
    SELECT 
        e.product
        ,CASE
            WHEN e.total_days_to_convert >= 15
            THEN '15+'
            ELSE RIGHT('0' || e.total_days_to_convert::string, 2)
        END AS days_to_convert
        ,COUNT(DISTINCT e.vtifa) AS unique_count_first_time
        ,SUM(unique_count_first_time) OVER(PARTITION BY e.product ORDER BY e.product, days_to_convert) AS cumulative_count_first_time
    FROM exp_first_app_use_ft e
    GROUP BY 1, 2
)

,app_opens_cte AS (
    SELECT 
        e.product
        ,CASE
            WHEN e.total_days_to_convert >= 15
            THEN '15+'
            ELSE RIGHT('0' || e.total_days_to_convert::string, 2)
        END AS days_to_convert
        ,COUNT(e.vtifa) AS unique_count_app_opens -- no DISTINCT because we want App opens not app openers. 
    FROM exp_app_use_ft e
    GROUP BY 1, 2
)


SELECT 
    INITCAP($app_name_string) || ' | ' || $country || ' | Attribution Days: ' || 
        $attribution_window || ' | Time to conversion: First touch' AS methodology
    ,$report_start_date AS start_date
    ,$report_end_date AS end_date
    ,ttc.product
    ,ttc.days_to_convert
    ,ttc.unique_count_first_time
    ,ttc.cumulative_count_first_time
    ,a.unique_count_app_opens
    ,SUM(a.unique_count_app_opens) OVER(PARTITION BY ttc.product ORDER BY ttc.product, ttc.days_to_convert) AS cumulative_count_app_opens
FROM time_to_conversion ttc 
    JOIN app_opens_cte a ON a.product = ttc.product
        AND a.days_to_convert = ttc.days_to_convert
GROUP BY 
    ttc.product
    ,ttc.days_to_convert
    ,ttc.unique_count_first_time
    ,ttc.cumulative_count_first_time
    ,a.unique_count_app_opens
ORDER BY 
    ttc.product
    ,ttc.days_to_convert
;



/*****************
time to conversion: Last touch
*****************/
WITH time_to_conversion AS (
    SELECT 
        e.product
        ,CASE
            WHEN e.total_days_to_convert >= 15
            THEN '15+'
            ELSE RIGHT('0' || e.total_days_to_convert::string, 2)
        END AS days_to_convert
        ,COUNT(DISTINCT e.vtifa) AS unique_count_first_time
        ,SUM(unique_count_first_time) OVER(PARTITION BY e.product ORDER BY e.product, days_to_convert) AS cumulative_count_first_time
    FROM exp_first_app_use_lt e
    GROUP BY 1, 2
)

,app_opens_cte AS (
    SELECT 
        e.product
        ,CASE
            WHEN e.total_days_to_convert >= 15
            THEN '15+'
            ELSE RIGHT('0' || e.total_days_to_convert::string, 2)
        END AS days_to_convert
        ,COUNT(e.vtifa) AS unique_count_app_opens -- no DISTINCT because we want App opens not app openers. 
    FROM exp_app_use_lt e
    GROUP BY 1, 2
)


SELECT 
    INITCAP($app_name_string) || ' | ' || $country || ' | Attribution Days: ' || 
        $attribution_window || ' | Time to conversion: Last touch' AS methodology
    ,$report_start_date AS start_date
    ,$report_end_date AS end_date
    ,ttc.product
    ,ttc.days_to_convert
    ,ttc.unique_count_first_time
    ,ttc.cumulative_count_first_time
    ,a.unique_count_app_opens
    ,SUM(a.unique_count_app_opens) OVER(PARTITION BY ttc.product ORDER BY ttc.product, ttc.days_to_convert) AS cumulative_count_app_opens
FROM time_to_conversion ttc 
    JOIN app_opens_cte a ON a.product = ttc.product
        AND a.days_to_convert = ttc.days_to_convert
GROUP BY 
    ttc.product
    ,ttc.days_to_convert
    ,ttc.unique_count_first_time
    ,ttc.cumulative_count_first_time
    ,a.unique_count_app_opens
ORDER BY 
    ttc.product
    ,ttc.days_to_convert
;

