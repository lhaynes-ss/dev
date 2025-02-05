-- GENERATE MAPPING TABLES
-- runtime: approx 30 mins
/**
Calling:

----------
PARAMOUNT+
----------
-- connection settings
USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD_GLOBAL;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_GLOBAL_LARGE;
USE DATABASE UDW_PROD_GLOBAL;
USE SCHEMA UDW_CLIENTSOLUTIONS_CS;


-- paramount map
CALL udw_clientsolutions_cs.sp_update_custom_creative_mapping_global(
    advertiser_list => '13186, 13187'
    ,destination_table => 'udw_clientsolutions_cs.paramount_custom_creative_mapping'
);

-- verify
SELECT * FROM udw_clientsolutions_cs.paramount_custom_creative_mapping;


-------
PLUTO
-------
-- connection settings
USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD_GLOBAL;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_GLOBAL_LARGE;
USE DATABASE UDW_PROD_GLOBAL;
USE SCHEMA UDW_CLIENTSOLUTIONS_CS;


-- pluto map
CALL udw_clientsolutions_cs.sp_update_custom_creative_mapping_global(
    advertiser_list => '13191, 13190'
    ,destination_table => 'udw_clientsolutions_cs.pluto_custom_creative_mapping'
);

-- verify
SELECT * FROM udw_clientsolutions_cs.pluto_custom_creative_mapping;


**/


-- connection settings
USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD_GLOBAL;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_GLOBAL;
USE DATABASE UDW_PROD_GLOBAL;
USE SCHEMA UDW_CLIENTSOLUTIONS_CS;



CREATE OR REPLACE PROCEDURE udw_clientsolutions_cs.sp_update_custom_creative_mapping_global(
    advertiser_list         VARCHAR     -- list of advertiser ids for mapping (e.g., 'x, y, z')
    ,destination_table      VARCHAR     -- full table name to write map to (db.schema.table)
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE 
    current_ts          TIMESTAMP;

BEGIN

    -- get current date/time
    current_ts := CURRENT_TIMESTAMP();

    -- set dates; objective: pull reporting for past -x days or past quarter, whichever is earlier
    -- script updated to go back 3 months instead of x days
    -- LET reporting_start     := (SELECT DATEADD('day', -49, CURRENT_DATE)::TIMESTAMP);
    LET reporting_start     := (SELECT DATEADD('month', -3, CURRENT_DATE)::TIMESTAMP);
    LET reporting_end       := (SELECT (DATEADD('day', -1, CURRENT_DATE)::VARCHAR || ' 23:59:59')::TIMESTAMP);
    LET quarter_start       := (SELECT DATE_TRUNC('quarter', CURRENT_DATE)::TIMESTAMP);


    -- get creative data
    DROP TABLE IF EXISTS creative_name_info;
    CREATE TEMP TABLE creative_name_info AS (
        SELECT DISTINCT
            c.id AS creative_id
            ,c.name 
        FROM trader.creatives_latest c
    );


    -- split the list of advertiser ids into separate values and store in a temp table
    DROP TABLE IF EXISTS advertiser_list;
    CREATE TEMP TABLE advertiser_list AS (
        SELECT a.value AS advertiser_id
        FROM TABLE(SPLIT_TO_TABLE(:advertiser_list, ',')) AS a
    );


    -- get sales orders for advertiser
    DROP TABLE IF EXISTS sales_order_source_table;
    CREATE TEMP TABLE sales_order_source_table AS (
        SELECT s.*
        FROM sales_order_meta s
        WHERE
            s.advertiser_id IN (
                SELECT DISTINCT a.advertiser_id
                FROM advertiser_list a
            )
    );


    -- get campaign data for active campaigns
    DROP TABLE IF EXISTS cmpgn;
    CREATE TEMP TABLE cmpgn AS (

        WITH vao_cte AS (
            SELECT 
                t.sales_order_id
                ,t.vao 
            FROM sales_order_source_table t
            GROUP BY 1, 2
        )

        SELECT DISTINCT
            v.vao
            ,oms_att.sales_order_id
            ,cmpgn.id AS campaign_id
            ,cmpgn.name AS campaign_name
            ,c.flight_id
            ,c.creative_id
            ,oms_att.package_sales_order_line_item_id
        FROM trader.campaigns_latest AS cmpgn
            JOIN (
                SELECT DISTINCT
                    cmpgn_att.campaign_id
                    ,cmpgn_att.io_external_id AS sales_order_id
                    ,cmpgn_att.li_external_id AS package_sales_order_line_item_id
                FROM trader.campaign_oms_attrs_latest AS cmpgn_att
            ) AS oms_att ON cmpgn.id = oms_att.campaign_id
            JOIN (
                SELECT DISTINCT 
                    campaign_id
                    ,flight_id
                    ,creative_id
                FROM udw_clientsolutions_cs.campaign_flight_creative
            ) AS c ON cmpgn.id = c.campaign_id
            JOIN vao_cte v ON v.sales_order_id = oms_att.sales_order_id
        WHERE 
            cmpgn.state != 'archived'
            AND oms_att.sales_order_id IN (SELECT DISTINCT t.sales_order_id FROM sales_order_source_table t)
    );


    -- get exposure data
    DROP TABLE IF EXISTS cd;
    CREATE TEMP TABLE cd AS (
        SELECT
            c.vao
            ,ld.country
            ,GET(ld.samsung_tvids_pii_virtual_id, 0) AS vtifa
            ,ld.campaign_id
            ,ld.flight_id
            ,ld.creative_id
        FROM trader.log_delivery_raw_anonymized ld
            JOIN cmpgn c ON c.campaign_id = ld.campaign_id
                AND c.flight_id = ld.flight_id
                AND c.creative_id = ld.creative_id
        WHERE 
            ld.event IN (
                'impression'    -- 1 impression-
                ,'click'        -- 2 click
                ,'tracker'      -- 7 web
            )
            AND ld.country IS NOT NULL
            AND (ld.dropped != TRUE OR ld.dropped IS NULL)
            AND ld.udw_partition_datetime >= LEAST(:reporting_start, :quarter_start)
            AND ld.udw_partition_datetime <= :reporting_end
    );


    -- get date/time
    LET last_update_ts  := (SELECT CURRENT_TIMESTAMP);


    -- compose mapping data
    DROP TABLE IF EXISTS creative_map;
    CREATE TEMP TABLE creative_map AS (

        WITH global_cte AS (
            SELECT DISTINCT
                so.advertiser_name
                ,so.product_country_targeting
                ,cd.country
                ,CASE 
                    WHEN so.product_country_targeting = 'USA'
                    THEN 'US'
                    ELSE r.country_code_iso_3166_alpha_2
                END AS product_country_code_targeting
                ,COALESCE(r.region, '') AS region
                ,so.vao
                ,c.campaign_id
                ,c.campaign_name 
                ,cd.flight_id
                ,f.name AS flight_name
                ,f.start_at_datetime AS flight_start_date
                ,f.end_at_datetime AS flight_end_date
                ,so.package_sales_order_line_item_id AS line_item_id
                ,so.package_sales_order_line_item_name AS line_item_name
                ,cn.name AS creative_name 
                ,cn.creative_id
                ,so.package_sales_order_line_item_start_at AS line_item_start_ts
                ,so.package_sales_order_line_item_end_at AS line_item_end_ts
                ,so.advertiser_id AS advertiser_id
                ,so.sales_order_name AS insertion_order_name
                ,so.order_start_date AS campaign_start_date
                ,so.order_end_date AS campaign_end_date
                ,so.package_cost_type AS rate_type
                ,so.package_net_unit_cost AS rate
                ,CASE 
                    WHEN so.package_is_added_value = 1 
                    THEN so.package_added_value_amount 
                    ELSE so.package_net_cost 
                END AS booked_budget
                ,COALESCE(so.package_production_quantity, 0) AS placement_impressions_booked
                ,0 AS budget_delivered
                ,so.package_product_name AS product_name
                ,:last_update_ts AS last_update_ts
            FROM sales_order_source_table so
                LEFT JOIN cmpgn c ON c.package_sales_order_line_item_id = so.package_sales_order_line_item_id
                LEFT JOIN cd ON cd.vao = so.vao 
                    AND cd.campaign_id = c.campaign_id
                LEFT JOIN creative_name_info cn ON cn.creative_id = cd.creative_id
                LEFT JOIN udw_lib.country_region_mapping_v r ON r.country_name = so.product_country_targeting
                LEFT JOIN trader.flights_latest f ON f.id = cd.flight_id
                    AND f.campaign_id = cd.campaign_id
            WHERE 
                1 = 1
                AND so.vao IS NOT NULL
                AND so.sales_order_name IS NOT NULL
                AND so.sales_order_name != ''
                AND c.campaign_ID IS NOT NULL
                AND line_item_end_ts >= LEAST(:reporting_start, :quarter_start)
                AND line_item_start_ts <= :reporting_end
        )

        -- join regions
        SELECT * FROM global_cte 
    );

    -- SELECT * FROM creative_map LIMIT 1000;


    -- make table if not exixts
    LET stmt0 VARCHAR := 'CREATE TABLE IF NOT EXISTS ' || :destination_table || ' (
        ADVERTISER_NAME                     VARCHAR
        ,PRODUCT_COUNTRY_TARGETING          VARCHAR
        ,COUNTRY                            VARCHAR
        ,PRODUCT_COUNTRY_CODE_TARGETING     VARCHAR
        ,REGION                             VARCHAR
        ,VAO                                INT
        ,CAMPAIGN_ID                        INT
        ,CAMPAIGN_NAME                      VARCHAR
        ,FLIGHT_ID                          INT
        ,FLIGHT_NAME                        VARCHAR
        ,FLIGHT_START_DATE                  TIMESTAMP
        ,FLIGHT_END_DATE                    TIMESTAMP
        ,LINE_ITEM_ID                       INT
        ,LINE_ITEM_NAME                     VARCHAR
        ,CREATIVE_NAME                      VARCHAR
        ,CREATIVE_ID                        INT
        ,LINE_ITEM_START_TS                 TIMESTAMP
        ,LINE_ITEM_END_TS                   TIMESTAMP
        ,ADVERTISER_ID                      INT
        ,INSERTION_ORDER_NAME               VARCHAR
        ,CAMPAIGN_START_DATE                DATE
        ,CAMPAIGN_END_DATE                  DATE
        ,RATE_TYPE                          VARCHAR
        ,RATE                               DOUBLE
        ,BOOKED_BUDGET                      DOUBLE
        ,PLACEMENT_IMPRESSIONS_BOOKED       INT
        ,BUDGET_DELIVERED                   DOUBLE
        ,PRODUCT_NAME                       VARCHAR
        ,LAST_UPDATE_TS                     TIMESTAMP
    )';
    EXECUTE IMMEDIATE stmt0;


    -- Remove all old data in the table
    LET stmt1 VARCHAR := 'DELETE FROM ' || :destination_table;
    EXECUTE IMMEDIATE stmt1;


    -- Insert new data
    LET stmt2 VARCHAR := 'INSERT INTO ' || :destination_table || '(SELECT * FROM creative_map)';
    EXECUTE IMMEDIATE stmt2;


    RETURN 'SUCCESS';


-- handle exception
-- prod: https://hooks.slack.com/triggers/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
-- dev: https://hooks.slack.com/triggers/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
EXCEPTION
    WHEN OTHER THEN
        SELECT udw_clientsolutions_cs.udf_submit_slack_notification_simple(
            slack_webhook_url => 'https://hooks.slack.com/triggers/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'
            ,date_string => :current_ts::VARCHAR
            ,name_string => 'Snowflake Task Monitor (GLOBAL)'
            ,message_string => 'Procedure "udw_clientsolutions_cs.sp_update_custom_creative_mapping_global" failed.' || 
                ' Error: (' || :SQLCODE || ', ' || :SQLERRM || ')'
        );

        RETURN 'FAILED WITH ERROR(' || :SQLCODE || ', ' || :SQLERRM || ')';

END;
$$;

