/**
 Pull reporting and dump to s3 buckets for all regions

**/
    -- TODO: Add ACL for stages => stage creation  STORAGE_AWS_OBJECT_ACL = 'bucket-owner-full-control'
/**

--------------
EXAMPLE 1
--------------
CALL udw_clientsolutions_cs.sp_partner_get_weekly_reports_global(
    partner                     => 'paramount'
    ,region                     => 'cdw_na'
    ,report_interval            => 'weekly'
    ,start_date                 => ''
    ,end_date                   => ''
    ,countries                  => 'MX'
    ,max_rows                   => 999999
    ,attribution_window         => 7
    ,us_stage                   => '@clientsolutions_internal/analytics/custom/lhaynes/unified/paramount_plus_external_us/'
    ,int_stage                  => '@clientsolutions_internal/analytics/custom/lhaynes/unified/paramount_plus_external_international/'
    ,us_stage_receipt           => '@clientsolutions_internal/analytics/custom/lhaynes/unified_receipts/paramount_plus_external_us/'
    ,int_stage_receipt          => '@clientsolutions_internal/analytics/custom/lhaynes/unified_receipts/paramount_plus_external_international/'
    ,save_receipt_YorN          => 'Y'
    ,file_name_prefix           => 'paramount_plus_'
    ,attribution_window_days    => 7
    ,lookback_window_months     => 12
    ,page_visit_lookback_days   => 30
    ,operative_table            => 'udw_clientsolutions_cs.paramount_operative_sales_orders'
    ,mapping_table              => 'udw_clientsolutions_cs.paramount_custom_creative_mapping'
    ,app_name                   => 'Paramount+'
    ,signup_segment             => '52832'
    ,homepage_segment           => '52833'
);

--------------
EXAMPLE 2
--------------
CALL udw_clientsolutions_cs.sp_partner_get_weekly_reports_global(
    partner                     => 'pluto'
    ,region                     => 'cdw_eu'
    ,report_interval            => 'weekly'
    ,start_date                 => ''
    ,end_date                   => ''
    ,countries                  => 'GB'
    ,max_rows                   => 999999
    ,attribution_window         => 7
    ,us_stage                   => '@clientsolutions_internal/analytics/custom/lhaynes/unified/pluto_external_us/'
    ,int_stage                  => '@clientsolutions_internal/analytics/custom/lhaynes/unified/pluto_external_international/'
    ,us_stage_receipt           => '@clientsolutions_internal/analytics/custom/lhaynes/unified_receipts/pluto_external_us/'
    ,int_stage_receipt          => '@clientsolutions_internal/analytics/custom/lhaynes/unified_receipts/pluto_external_international/'
    ,save_receipt_YorN          => 'Y'
    ,file_name_prefix           => 'pluto_'
    ,attribution_window_days    => 7
    ,lookback_window_months     => 12
    ,page_visit_lookback_days   => 30
    ,operative_table            => 'udw_clientsolutions_cs.pluto_operative_sales_orders'
    ,mapping_table              => 'udw_clientsolutions_cs.pluto_custom_creative_mapping'
    ,app_name                   => 'Pluto TV'
    ,signup_segment             => ''
    ,homepage_segment           => ''
);

**/

-- connection settings
USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD_GLOBAL;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_GLOBAL_LARGE;
USE DATABASE UDW_PROD_GLOBAL;
USE SCHEMA UDW_CLIENTSOLUTIONS_CS;


CREATE OR REPLACE PROCEDURE udw_clientsolutions_cs.sp_partner_get_weekly_reports_global(
    -- specify parameters for procedure
    partner                     VARCHAR     --> paramount | pluto
    ,region                     VARCHAR     --> location of data (e.g., cdw_eu, cdw_apac, cdw_sa, udw_na) 
    ,report_interval            VARCHAR     --> weekly | monthly
    ,start_date                 VARCHAR     --> start of reporting window;
    ,end_date                   VARCHAR     --> end of reporting window; Not including attribution window
    ,countries                  VARCHAR     --> list of countries for report (e.g., US, CA, MX)
    ,max_rows                   INT         --> max # of rows per report. Point at which report is split into additional file.
    ,attribution_window         INT         --> max days after exposure for attribution credit
    ,us_stage                   VARCHAR     --> stage to use for US reports
    ,int_stage                  VARCHAR     --> stage to use for international reports
    ,us_stage_receipt           VARCHAR     --> stage to use for our copy of US reports
    ,int_stage_receipt          VARCHAR     --> stage to use for our copy of international reports
    ,save_receipt_YorN          VARCHAR     --> 'Y' or 'N'. Indicates whether to save a copy of the report for us
    ,file_name_prefix           VARCHAR     --> paramount_plus_ | pluto_
    ,attribution_window_days    INT         --> number of days for conversion attribution
    ,lookback_window_months     INT         --> number of months for lookback window
    ,page_visit_lookback_days   INT         --> number of days for web pixel lookback
    ,operative_table            VARCHAR     --> operative table
    ,mapping_table              VARCHAR     --> mapping table
    ,app_name                   VARCHAR     --> app name
    ,signup_segment             VARCHAR     --> segment id for signup pixel
    ,homepage_segment           VARCHAR     --> segment id for homepage pixel
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE 
    -- specify variables used in this stored procedure
    output_stage                            VARCHAR;    --> stage usaed for specific report instance
    output_stage_receipt                    VARCHAR;    --> stage usaed for our copy of a specific report instance
    file_path                               VARCHAR;    --> s3 path for specific report instance
    country                                 VARCHAR;    --> country for specific report instance
    output_file                             VARCHAR;    --> output_stage + file_path
    output_file_receipt                     VARCHAR;    --> output_stage_receipt + file_path
    stmt_begin                              VARCHAR;    --> query string used to write report to s3
    stmt_begin_receipt                      VARCHAR;    --> query string used to write report to our internal stage
    stmt_end                                VARCHAR;    --> query string used to write report to s3 + our internal stage
    stmt_begin_plus_stmt_end                VARCHAR;    --> stmt_begin + stmt_end
    stmt_begin_receipt_plus_stmt_end        VARCHAR;    --> stmt_begin_receipt + stmt_end
    file_partitions                         INT;        --> total number of files needed given 'max_rows' argument above
    sequence_id                             VARCHAR;    --> sequential file number given number of file partitions
    report_start_datetime                   TIMESTAMP;  --> start of reporting window
    report_end_datetime                     TIMESTAMP;  --> end of reporting window
    lookback_datetime                       TIMESTAMP;  --> lookback window for finding new users
    page_visit_lookback_datetime            TIMESTAMP;  --> lookback window for page visits
    report_end_datetime_with_attribution    TIMESTAMP;  --> end of reporting window + attribution window
    current_date_dt                         TIMESTAMP;  --> today
    days_since_last_mapping_update          INT;        --> date diff between last time mapping was updated and report date
    STALE_MAP_EXCEPTION                     EXCEPTION;  --> will be thrown if mapping file is out of date
    foo                                     VARCHAR;    --> test variable. RETURN foo if needed with value to check
BEGIN

    -- get current date
    current_date_dt := CURRENT_DATE();

    -- use date arguments if available or the defaults:
    IF (TRIM(:start_date) = '' OR TRIM(:end_date) = '') THEN 
        end_date                := CURRENT_DATE()::VARCHAR;
        start_date              := DATEADD('DAY', -30, CURRENT_DATE())::VARCHAR;
    END IF;

    -- clear signup if not set
    IF (TRIM(:signup_segment) = '') THEN 
        LET signup_segment := '';
        signup_segment := NULL;
    END IF;

    -- clear homepage if not set
    IF (TRIM(:homepage_segment) = '') THEN 
        LET homepage_segment := '';
        homepage_segment := NULL;
    END IF;

    -- auto set vars
    report_start_datetime                   := (:start_date::VARCHAR    || ' 00:00:00')::TIMESTAMP;
    report_end_datetime                     := (:end_date::VARCHAR      || ' 23:59:59')::TIMESTAMP;
    lookback_datetime                       := DATEADD('MONTH',  -ABS(:lookback_window_months),      :report_start_datetime);    --> lookback_datetime (12 months before start date)
    page_visit_lookback_datetime            := DATEADD('DAYS',   -ABS(:page_visit_lookback_days),    :report_start_datetime);    --> shorter lookback for page visits as querying this data is expensive
    report_end_datetime_with_attribution    := DATEADD(DAY,      ABS(:attribution_window_days),      :report_end_datetime);

    -- split the list of countries into separate values and store in a temp table
    DROP TABLE IF EXISTS countries;
    CREATE TEMP TABLE countries AS (
        SELECT c.value AS country
        FROM TABLE(SPLIT_TO_TABLE(:countries, ',')) AS c
    );

    -- create a cursor for the countries table so that we can loop through the values
    LET res RESULTSET := (SELECT DISTINCT country FROM countries);
    LET cur CURSOR FOR res;


    -- loop through each country in the countries table
    OPEN cur;
        FOR record IN cur DO

            -- get the country field in the country variable in lowercase
            country := TRIM(LOWER(record.country));


            -- =============================================================================
            -- =============================================================================
            -- START REPORT
            -- =============================================================================
            -- =============================================================================
            /**
                To extract and run this REPORT separately as a stand-alone query in dbvis, do the following steps:
                --------------------------------------------------------------------------------------------------
                1. Extract query between START REPORT and END REPORT except for this comment block
                2. Regex find and replace. Swap the x's in the "replace" value with $ signs:
                    Find:       "([^:])(:)([a-zA-Z])" 
                    Replace:    "x1xxx3" 
                3. Add header below to the top of the extracted query.
                4. Add this select to the bottom of the file: "SELECT * FROM output_table ORDER BY row_num;"
                5. Remove the "mapping check" block below

                -- start header
                USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD_GLOBAL;
                USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_GLOBAL_LARGE;
                USE DATABASE UDW_PROD_GLOBAL;
                USE SCHEMA UDW_CLIENTSOLUTIONS_CS;

                SET current_date_dt                = CURRENT_DATE;
                SET start_date                  = '2024-08-26';
                SET end_date                    = '2024-09-01';
                SET attribution_window_days     = 7;
                SET lookback_window_months      = 12;
                SET page_visit_lookback_days    = 30;
                SET operative_table             = 'udw_clientsolutions_cs.paramount_operative_sales_orders';
                SET mapping_table               = 'udw_clientsolutions_cs.paramount_custom_creative_mapping';
                SET app_name                    = 'Paramount+';

                SET signup_segment              = '52832';
                SET homepage_segment            = '52833';

                SET max_rows                    = 999999;
                SET country                     = 'US';


                -- auto set vars
                SET report_start_datetime                   = (CAST($start_date    || ' 00:00:00' AS VARCHAR))::TIMESTAMP;
                SET report_end_datetime                     = (CAST($end_date      || ' 23:59:59' AS VARCHAR))::TIMESTAMP;
                SET lookback_datetime                       = DATEADD('MONTH',  -ABS($lookback_window_months),      $report_start_datetime);    --> lookback_datetime (12 months before start date)
                SET page_visit_lookback_datetime            = DATEADD('DAYS',   -ABS($page_visit_lookback_days),    $report_start_datetime);    --> shorter lookback for page visits as querying this data is expensive
                SET report_end_datetime_with_attribution    = DATEADD(DAY,      ABS($attribution_window_days),      $report_end_datetime);
                -- end header
            **/


            -- =====================================================
            -- MAPPING DATA
            -- =====================================================


            /**
            Get mapping table data.
            -------------------------
            This table will contain data for multiple regions where the campaign
            had active lines within this quarter.
            **/
            DROP TABLE IF EXISTS mapping_data;
            CREATE TEMP TABLE mapping_data AS (

                SELECT
                    s.package_is_added_value AS is_added_value
                    ,m.*
                FROM TABLE(:mapping_table) m
                LEFT JOIN sales_order_meta s ON s.vao = m.vao
                    AND s.package_sales_order_line_item_id = m.line_item_id

            );

            -- SELECT * FROM mapping_data;


            -- start mapping check.
            -- check to see how many days ago the mapping file was updated
            LET days_since_last_mapping_update    := 0;
            days_since_last_mapping_update        := (SELECT DATEDIFF('DAY', MAX(m.last_update_ts), :current_date_dt) FROM mapping_data m);

            -- if map was last updated 7 or more days ago then throw an exception to fail the query.
            -- This keeps the report from auto-delivering stale info. Map is stale
            IF (:days_since_last_mapping_update >= 7) THEN
                RAISE STALE_MAP_EXCEPTION;
            END IF;
            -- end mapping check


            /**
            Report base as "campaign meta"
            --------------
            We can filter mapping data to just data with line items active within
            the reporting range and use the field "product_country_code_targeting"
            to isolate regions for data export.

            product_country_code_targeting =    the 2 digit country code that was targeted by
                                                the campaign/line/creative.
            **/
            DROP TABLE IF EXISTS campaign_meta;
            CREATE TEMP TABLE campaign_meta AS (

                SELECT DISTINCT
                    m.product_country_code_targeting AS mapping_country
                    ,m.country
                    ,m.vao
                    ,:current_date_dt::DATE AS report_execution_date
                    ,m.advertiser_id
                    ,m.advertiser_name
                    ,m.insertion_order_name
                    ,m.campaign_id
                    ,m.campaign_name
                    ,m.campaign_start_date
                    ,m.campaign_end_date
                    ,m.flight_id
                    ,m.flight_name
                    ,m.flight_start_date
                    ,m.flight_end_date
                    ,m.line_item_id
                    ,m.line_item_name
                    ,m.creative_id
                    ,m.creative_name
                    ,m.rate_type
                    ,m.rate
                    ,m.line_item_start_ts
                    ,m.line_item_end_ts
                    ,m.placement_impressions_booked
                    ,m.booked_budget
                    ,m.budget_delivered
                    ,m.product_name
                    ,m.is_added_value
                FROM mapping_data m
                WHERE
                    m.line_item_start_ts < :report_end_datetime
                    AND m.line_item_end_ts >= :report_start_datetime
                    AND LOWER(m.product_country_code_targeting) IN (LOWER(:country))

            );

            SELECT * FROM campaign_meta;


            -- foo := (SELECT COUNT(*) FROM campaign_meta);
            -- RETURN foo::VARCHAR;


            /**
            Get creative map
            ------------------
            The creative map should contain distinct campaign/flight/creative
            combinations for each country in the mapping data.
            **/
            DROP TABLE IF EXISTS creative_map;
            CREATE TEMP TABLE creative_map AS (

                SELECT DISTINCT
                    c.mapping_country
                    ,c.campaign_id
                    ,c.campaign_name
                    ,c.creative_id
                    ,c.creative_name
                    ,c.flight_id
                    ,c.line_item_id
                    ,c.line_item_name
                    ,c.product_name
                FROM campaign_meta c

            );

            SELECT * FROM creative_map;


            -- map vtifas to IP address. This will be used later to reduce pixel activity down to 1 vtifa per IP address
            DROP TABLE IF EXISTS ip_psid_map;
            CREATE TEMP TABLE ip_psid_map AS (

                -- get last vtifa associated with IP address as virtual IP (vip)
                WITH map_cte AS (
                    SELECT
                        m.vpsid
                        ,m.vtifa
                        ,IFNULL(l.ipv6_pii_virtual_id, l.ip_pii_virtual_id) AS vip
                        ,ROW_NUMBER() OVER (PARTITION BY IFNULL(l.ipv6_pii_virtual_id, l.ip_pii_virtual_id) ORDER BY TO_TIMESTAMP(FLOOR(l.timestamp)::INT) DESC) AS row_num
                    FROM creative_map cm
                        JOIN trader.log_delivery_raw_anonymized l ON l.country = cm.mapping_country
                        JOIN udw_lib.virtual_psid_tifa_mapping_v m ON GET(l.samsung_tvids_pii_virtual_id, 0) = m.vtifa
                    WHERE
                        l.country IS NOT NULL
                        AND (l.dropped != TRUE OR l.dropped IS NULL)
                        AND l.campaign_id = cm.campaign_id
                        AND l.flight_id = cm.flight_id
                        AND l.creative_id = cm.creative_id
                        AND l.event IN (
                            'impression'    -- 1 impression
                            ,'click'        -- 2 click
                        )
                        AND l.udw_partition_datetime BETWEEN :page_visit_lookback_datetime::DATE AND :report_end_datetime::DATE
                )

                SELECT DISTINCT
                    m.vip
                    ,m.vpsid
                    ,m.vtifa
                FROM map_cte m
                WHERE
                    m.row_num = 1

            );


            -- =====================================================
            -- EXPOSURES
            -- =====================================================

            /**
            Get delivery (typically named "cd")
            ---------------------------------------
            delivery = Impressions, Clicks
            ip_psid_map_vtifa = reduced vtifa (the last vtifa used from that IP address) for web data
            **/
            DROP TABLE IF EXISTS delivery;
            CREATE TEMP TABLE delivery AS (

                SELECT
                    GET(ld.samsung_tvids_pii_virtual_id, 0) AS vtifa
                    ,TO_TIMESTAMP(FLOOR(ld.timestamp)::INT) AS exposure_datetime
                    ,ld.country
                    ,cm.campaign_id
                    ,cm.line_item_id
                    ,cm.creative_id
                    ,cm.flight_id
                    ,ld.event
                    ,IFNULL(ld.ipv6_pii_virtual_id, ld.ip_pii_virtual_id) AS vip
                FROM creative_map cm
                    LEFT JOIN trader.log_delivery_raw_anonymized ld ON ld.country = cm.mapping_country
                        AND ld.country IS NOT NULL
                        AND (ld.dropped != TRUE OR ld.dropped IS NULL)
                        AND ld.campaign_id = cm.campaign_id
                        AND ld.flight_id = cm.flight_id
                        AND ld.creative_id = cm.creative_id
                        AND ld.event IN (
                            'impression'    -- 1 impression
                            ,'click'        -- 2 click
                        )
                        AND ld.udw_partition_datetime BETWEEN :report_start_datetime AND :report_end_datetime

            );

            -- SELECT COUNT(*) AS exposure_count FROM delivery;


            /**
            split delivery up into sub tables "impressions" and "clicks"
            **/
            DROP TABLE IF EXISTS impressions;
            CREATE TEMP TABLE impressions AS (

                SELECT d.*
                FROM delivery d
                WHERE d.event = 'impression'

            );


            DROP TABLE IF EXISTS clicks;
            CREATE TEMP TABLE clicks AS (

                SELECT d.*
                FROM delivery d
                WHERE d.event = 'click'

            );


            -- get video tracker data
            DROP TABLE IF EXISTS video;
            CREATE TEMP TABLE video AS (

                SELECT
                    GET(ld.samsung_tvids_pii_virtual_id, 0) AS vtifa
                    ,TO_TIMESTAMP(FLOOR(ld.timestamp)::INT) AS exposure_datetime
                    ,ld.country
                    ,cm.campaign_id
                    ,cm.line_item_id
                    ,cm.creative_id
                    ,cm.flight_id
                    ,ld.event
                    ,ld.tracker                     --> String that indicates how much of the video played for video type (first_quartile, midpoint, etc)
                FROM creative_map cm
                    JOIN trader.log_delivery_raw_anonymized ld ON ld.country = cm.mapping_country
                        AND ld.country IS NOT NULL
                        AND (ld.dropped != TRUE OR ld.dropped IS NULL)
                        AND ld.campaign_id = cm.campaign_id
                        AND ld.flight_id = cm.flight_id
                        AND ld.creative_id = cm.creative_id
                        AND ld.event = 'tracker'    --> video
                        -- AND ld.udw_partition_datetime BETWEEN :report_start_datetime AND :report_end_datetime_with_attribution
                        AND ld.udw_partition_datetime BETWEEN :report_start_datetime AND :report_end_datetime

            );

            -- SELECT COUNT(*) AS video_count FROM video LIMIT 1000;


            -- get pixel exposure data
            DROP TABLE IF EXISTS pixels;
            CREATE TEMP TABLE pixels AS (

                SELECT
                    -- m.vtifa
                    TO_TIMESTAMP(FLOOR(ld.timestamp)::INT) AS exposure_datetime
                    ,ld.country
                    ,ld.event
                    ,ld.segment_id              --> Integer that indicates segment used for when pixel type
                    ,m.vip
                FROM trader.log_delivery_raw_anonymized ld
                    JOIN ip_psid_map m ON IFNULL(ld.ipv6_pii_virtual_id, ld.ip_pii_virtual_id) = m.vip
                WHERE
                    ld.country IN (SELECT DISTINCT cm.mapping_country FROM creative_map cm)
                    AND ld.country IS NOT NULL
                    AND (ld.dropped != TRUE OR ld.dropped IS NULL)
                    AND ld.event = 'segment'    --> web pixel
                    AND ld.segment_id IS NOT NULL
                    AND ld.segment_id::VARCHAR IN (:signup_segment::VARCHAR, :homepage_segment::VARCHAR)  --> convert to varchar as the segments can be ""
                    -- AND ld.udw_partition_datetime BETWEEN :page_visit_lookback_datetime AND :report_end_datetime_with_attribution
                    AND ld.udw_partition_datetime BETWEEN :page_visit_lookback_datetime AND :report_end_datetime

            );

            -- SELECT COUNT(*) AS pixel_count FROM pixels LIMIT 1000;


            -- get first app usage for report window + attribution window for "exposed" first use calculations
            DROP TABLE IF EXISTS first_pixels_in_report_window;
            CREATE TEMP TABLE first_pixels_in_report_window AS (

                SELECT
                    -- p.vtifa
                    p.segment_id
                    ,p.country
                    ,p.vip
                    ,MIN(p.exposure_datetime) AS exposure_datetime
                FROM pixels p
                GROUP BY 1, 2, 3
                HAVING
                    -- MIN(p.exposure_datetime) BETWEEN :report_start_datetime AND :report_end_datetime_with_attribution
                    MIN(p.exposure_datetime) BETWEEN :report_start_datetime AND :report_end_datetime

            );


            -- =====================================================
            -- APP USAGE
            -- =====================================================


            /**
            Get app usage
            --------------------------
                - min time threshold is 60 seconds
                - includes lookback + report window + attribution window extension
                - technically this is more than the lookback, as lookback should be based on each day in the report window not just the first day
            **/
            DROP TABLE IF EXISTS app_usage;
            CREATE TEMP TABLE app_usage AS (

                SELECT
                    m.vtifa
                    ,m.vpsid
                    ,f.start_timestamp AS app_usage_datetime
                    ,f.country
                    ,SUM(DATEDIFF('minutes', f.start_timestamp, f.end_timestamp)) AS time_spent_min
                FROM data_tv_acr.fact_app_usage_session_anonymized f
                    LEFT JOIN udw_lib.virtual_psid_tifa_mapping_v m ON m.vpsid = f.psid_pii_virtual_id
                WHERE
                    f.app_id IN ( -- app should be equal to the desired app
                        SELECT DISTINCT m.app_id
                        FROM meta_apps.meta_taps_sra_app_stat_d_anonymized m
                        WHERE m.app_nm = :app_name
                    )
                    AND f.country IN (SELECT DISTINCT cm.mapping_country FROM creative_map cm)
                    AND DATEDIFF('second', f.start_timestamp, f.end_timestamp) >= 60
                    -- AND f.udw_partition_datetime BETWEEN :lookback_datetime AND :report_end_datetime_with_attribution
                    AND f.udw_partition_datetime BETWEEN :lookback_datetime AND :report_end_datetime
                GROUP BY 1, 2, 3, 4

            );

            -- SELECT COUNT(*) AS app_usage_count FROM app_usage;


            -- restrict app usage to report window + attribution window for "exposed" use calculations
            DROP TABLE IF EXISTS app_usage_in_report_window;
            CREATE TEMP TABLE app_usage_in_report_window AS (

                SELECT
                    a.vtifa
                    ,a.vpsid
                    ,a.country
                    ,a.app_usage_datetime
                    ,a.time_spent_min
                FROM app_usage a
                WHERE
                    -- a.app_usage_datetime BETWEEN :report_start_datetime AND :report_end_datetime_with_attribution
                    a.app_usage_datetime BETWEEN :report_start_datetime AND :report_end_datetime

            );


            -- get first app usage for report window + attribution window for "exposed" first use calculations
            DROP TABLE IF EXISTS first_app_usage_in_report_window;
            CREATE TEMP TABLE first_app_usage_in_report_window AS (

                SELECT
                    a.vtifa
                    ,a.vpsid
                    ,a.country
                    ,MIN(a.app_usage_datetime) AS app_first_open_time
                FROM app_usage a
                GROUP BY 1, 2, 3
                HAVING
                    -- app_first_open_time BETWEEN :report_start_datetime AND :report_end_datetime_with_attribution
                    app_first_open_time BETWEEN :report_start_datetime AND :report_end_datetime

            );


            -- =====================================================
            -- EXPOSED CONVERSIONS (EXPOSURES + APP USAGE)
            -- =====================================================

            /**
            Get exposed conversions
            --------------------------
                - sequential
                - last touch, impressions
                - time of exposure
            **/
            -- exposed app opens
            DROP TABLE IF EXISTS exposed_app_opens;
            CREATE TEMP TABLE exposed_app_opens AS (

                WITH exposed_app_usage_cte AS (
                    SELECT
                        i.exposure_datetime --> time of exposure
                        ,i.campaign_id
                        ,i.flight_id
                        ,i.creative_id
                        ,i.country
                        ,i.vtifa
                        ,u.time_spent_min
                        ,ROW_NUMBER() OVER(PARTITION BY u.vtifa, u.vpsid, u.app_usage_datetime ORDER BY i.exposure_datetime DESC) AS row_num --> last touch
                    FROM impressions i
                        JOIN app_usage_in_report_window u ON u.vtifa = i.vtifa --> app usage
                            AND u.country = i.country
                            AND i.exposure_datetime <= u.app_usage_datetime
                            AND DATEDIFF('DAY', i.exposure_datetime, u.app_usage_datetime) <= :attribution_window_days
                )

                SELECT
                    e.*
                FROM exposed_app_usage_cte e
                WHERE
                    e.row_num = 1

            );

            -- SELECT * FROM exposed_app_opens LIMIT 1000;


            -- exposed web pixel impressions
            DROP TABLE IF EXISTS exposed_visits;
            CREATE TEMP TABLE exposed_visits AS (

                WITH exposed_visits_cte AS (
                    SELECT
                        i.exposure_datetime --> time of exposure
                        ,i.campaign_id
                        ,i.flight_id
                        ,i.creative_id
                        ,i.country
                        ,p.segment_id       --> Integer that indicates segment used for when pixel type
                        ,p.vip
                        ,ROW_NUMBER() OVER(PARTITION BY p.vip, p.segment_id, p.country, p.exposure_datetime ORDER BY i.exposure_datetime DESC) AS row_num --> last touch
                    FROM impressions i
                        JOIN pixels p ON p.vip = i.vip --> conversion
                            AND p.country = i.country
                            AND i.exposure_datetime <= p.exposure_datetime
                            AND DATEDIFF('DAY', i.exposure_datetime, p.exposure_datetime) <= :attribution_window_days
                )

                SELECT
                    e.*
                FROM exposed_visits_cte e
                WHERE
                    e.row_num = 1

            );

            -- SELECT * FROM exposed_visits LIMIT 1000;


            /**
            Get exposed first time conversions
            --------------------------
                - sequential
                - last touch, impressions
                - time of exposure
            **/
            -- exposed installs (first app opens)
            DROP TABLE IF EXISTS exposed_installs;
            CREATE TEMP TABLE exposed_installs AS (

                WITH exposed_installs_cte AS (
                    SELECT
                        i.exposure_datetime --> time of exposure
                        ,i.campaign_id
                        ,i.flight_id
                        ,i.creative_id
                        ,i.country
                        ,i.vtifa            --> identifier
                        ,ROW_NUMBER() OVER(PARTITION BY u.vtifa, u.vpsid, u.app_first_open_time ORDER BY i.exposure_datetime DESC) AS row_num --> last touch
                    FROM impressions i
                        JOIN first_app_usage_in_report_window u ON u.vtifa = i.vtifa --> conversion
                            AND u.country = i.country
                            AND i.exposure_datetime <= u.app_first_open_time
                            AND DATEDIFF('DAY', i.exposure_datetime, u.app_first_open_time) <= :attribution_window_days
                )

                SELECT
                    e.*
                FROM exposed_installs_cte e
                WHERE
                    e.row_num = 1

            );

            -- SELECT * FROM exposed_installs LIMIT 1000;


            -- first time exposed web pixel impressions
            DROP TABLE IF EXISTS exposed_first_time_visits;
            CREATE TEMP TABLE exposed_first_time_visits AS (

                WITH exposed_first_time_visits_cte AS (
                    SELECT
                        i.exposure_datetime --> time of exposure
                        ,i.campaign_id
                        ,i.flight_id
                        ,i.creative_id
                        ,i.country
                        ,p.segment_id       --> Integer that indicates segment used for when pixel type
                        ,p.vip
                        ,ROW_NUMBER() OVER(PARTITION BY p.vip, p.segment_id, p.country, p.exposure_datetime ORDER BY i.exposure_datetime DESC) AS row_num --> last touch
                    FROM impressions i
                        JOIN first_pixels_in_report_window p ON p.vip = i.vip --> conversion
                            AND p.country = i.country
                            AND i.exposure_datetime <= p.exposure_datetime
                            AND DATEDIFF('DAY', i.exposure_datetime, p.exposure_datetime) <= :attribution_window_days
                )

                SELECT
                    e.*
                FROM exposed_first_time_visits_cte e
                WHERE
                    e.row_num = 1

            );

            -- SELECT * FROM exposed_first_time_visits LIMIT 1000;


            -- =====================================================
            -- OUTPUT PREP
            -- =====================================================


            -- this is the foundation for the report. The remaining data will be appended to this.
            DROP TABLE IF EXISTS core_data;
            CREATE TEMP TABLE core_data AS (

                -- count all impressions
                WITH impressions_cte AS (
                    SELECT
                        i.exposure_datetime::DATE AS date_of_exposure
                        ,i.campaign_id
                        ,i.flight_id
                        ,i.creative_id
                        ,i.country
                        ,COUNT(*) AS impressions
                    FROM impressions i
                    WHERE
                        date_of_exposure BETWEEN :report_start_datetime::DATE AND :report_end_datetime::DATE
                    GROUP BY 1, 2, 3, 4, 5
                )

                -- count all clicks
                ,clicks_cte AS (
                    SELECT
                        c.exposure_datetime::DATE AS date_of_exposure
                        ,c.campaign_id
                        ,c.flight_id
                        ,c.creative_id
                        ,c.country
                        ,COUNT(*) AS clicks
                    FROM clicks c
                    WHERE
                        date_of_exposure BETWEEN :report_start_datetime::DATE AND :report_end_datetime::DATE
                    GROUP BY 1, 2, 3, 4, 5
                )

                -- count unique vtifa's among impressions
                ,reach_cte AS (
                    SELECT
                        i.exposure_datetime::DATE AS date_of_exposure
                        ,i.campaign_id
                        ,i.flight_id
                        ,i.creative_id
                        ,i.country
                        ,COUNT(DISTINCT i.vtifa) AS reach
                    FROM impressions i
                    WHERE
                        date_of_exposure BETWEEN :report_start_datetime::DATE AND :report_end_datetime::DATE
                    GROUP BY 1, 2, 3, 4, 5
                )

                -- merge in core data for final selection
                SELECT DISTINCT
                    c.mapping_country
                    ,c.report_execution_date
                    ,i.date_of_exposure
                    ,c.advertiser_id
                    ,c.advertiser_name AS advertiser
                    ,c.insertion_order_name
                    ,c.campaign_id
                    ,c.campaign_name
                    ,c.campaign_start_date
                    ,c.campaign_end_date
                    ,c.flight_id
                    ,c.flight_name
                    ,c.flight_start_date
                    ,c.flight_end_date
                    ,c.line_item_id
                    ,c.line_item_name
                    ,c.creative_id
                    ,c.creative_name
                    ,c.rate_type
                    ,c.rate
                    ,c.placement_impressions_booked
                    ,c.booked_budget
                    ,COALESCE(i.impressions, 0) AS impressions_delivered
                    ,CASE
                        WHEN LOWER(c.line_item_name) LIKE '%make_good%'
                        THEN
                            -- No charge for makegoods ('makegood' or 'make good')
                            0.00
                        WHEN c.is_added_value = 1
                        THEN
                            -- ADDED VALUE Placements are free and incur no cost
                            0.00
                        WHEN c.rate_type = 'CPM'
                        THEN
                            -- for CPM, rate is per 1000 impressions so...
                            -- budget delivered = impressions delivered/1000 * rate
                            (CAST(impressions_delivered AS FLOAT)/1000) * c.rate
                        WHEN c.rate_type = 'Flat Rate'
                        THEN
                            -- for Flat Rate, just return the flat booked budget.
                            c.booked_budget
                        ELSE NULL
                    END AS budget_delivered
                    ,COALESCE(cl.clicks, 0) AS clicks
                    ,COALESCE(r.reach, 0) AS reach
                    ,c.line_item_start_ts
                    ,c.line_item_end_ts
                    ,c.product_name
                FROM campaign_meta c
                    LEFT JOIN impressions_cte i ON i.campaign_id = c.campaign_id
                        AND i.flight_id = c.flight_id
                        AND i.creative_id = c.creative_id
                        AND i.country = c.mapping_country
                    LEFT JOIN clicks_cte cl ON cl.campaign_id = c.campaign_id
                        AND cl.flight_id = c.flight_id
                        AND cl.creative_id = c.creative_id
                        AND cl.country = c.mapping_country
                        AND cl.date_of_exposure = i.date_of_exposure
                    LEFT JOIN reach_cte r ON r.campaign_id = c.campaign_id
                        AND r.flight_id = c.flight_id
                        AND r.creative_id = c.creative_id
                        AND r.country = c.mapping_country
                        AND r.date_of_exposure = i.date_of_exposure
                WHERE
                    i.date_of_exposure BETWEEN :report_start_datetime AND :report_end_datetime

            );

            -- SELECT * FROM core_data;


            -- =====================================================
            -- OUTPUT
            -- =====================================================


            -- final selection
            DROP TABLE IF EXISTS output_table;
            CREATE TEMP TABLE output_table AS (

                -- get app opens and average time use by campaign, flight, creative
                WITH exposure_stats_cte AS (
                    SELECT
                        e.exposure_datetime::DATE AS date_of_exposure
                        ,e.campaign_id
                        ,e.flight_id
                        ,e.creative_id
                        ,e.country
                        ,COUNT(*) AS exposed_app_open_count
                        ,AVG(e.time_spent_min) AS average_time_spent_min
                    FROM exposed_app_opens e
                    WHERE
                        e.row_num = 1
                        AND date_of_exposure BETWEEN :report_start_datetime::DATE AND :report_end_datetime::DATE
                    GROUP BY
                        1, 2, 3, 4, 5
                )

                -- get unique vtifa's as distinct "openers" by campaign, flight, creative
                ,exposed_openers_cte AS (
                    SELECT
                        e.exposure_datetime::DATE AS date_of_exposure
                        ,e.campaign_id
                        ,e.flight_id
                        ,e.creative_id
                        ,e.country
                        ,COUNT(DISTINCT e.vtifa) AS exposed_app_openers
                    FROM exposed_app_opens e
                    WHERE
                        e.row_num = 1
                        AND date_of_exposure BETWEEN :report_start_datetime::DATE AND :report_end_datetime::DATE
                    GROUP BY
                        1, 2, 3, 4, 5
                )

                -- get all installs by country. Unexposed don't have campaign, flight, creative data
                ,installs_cte AS (
                    SELECT
                        f.app_first_open_time::DATE AS app_first_open_date
                        ,f.country
                        ,COUNT(DISTINCT f.vtifa) AS exposed_and_unexposed_install_count
                    FROM first_app_usage_in_report_window f
                    WHERE
                        app_first_open_date BETWEEN :report_start_datetime::DATE AND :report_end_datetime::DATE
                    GROUP BY
                        1, 2
                )

                -- get unique vtifa's as distinct "installs" by campaign, flight, creative
                ,exposed_installs_cte AS (
                    SELECT
                        e.exposure_datetime::DATE AS date_of_exposure
                        ,e.campaign_id
                        ,e.flight_id
                        ,e.creative_id
                        ,e.country
                        ,COUNT(DISTINCT e.vtifa) AS exposed_install_count
                    FROM exposed_installs e
                    WHERE
                        e.row_num = 1
                        AND date_of_exposure BETWEEN :report_start_datetime::DATE AND :report_end_datetime::DATE
                    GROUP BY
                        1, 2, 3, 4, 5
                )

                -- translate tracker data to tile counts by campaign, flight, creative
                ,video_tile_cte AS (
                    SELECT
                        v.exposure_datetime::DATE AS date_of_exposure
                        ,v.campaign_id
                        ,v.flight_id
                        ,v.creative_id
                        ,v.country
                        ,SUM(CASE WHEN v.tracker IN ('autoplay_start', 'start') THEN 1 END) AS tile_start_count
                        ,SUM(CASE WHEN v.tracker IN ('autoplay_first_quartile', 'first_quartile') THEN 1 END) AS tile_25_count
                        ,SUM(CASE WHEN v.tracker IN ('autoplay_midpoint_quartile', 'midpoint') THEN 1 END) AS tile_50_count
                        ,SUM(CASE WHEN v.tracker IN ('autoplay_third_quartile', 'third_quartile') THEN 1 END) AS tile_75_count
                        ,SUM(CASE WHEN v.tracker IN ('autoplay_complete', 'complete') THEN 1 END) AS tile_complete_count
                    FROM video v
                    WHERE
                        date_of_exposure BETWEEN :report_start_datetime::DATE AND :report_end_datetime::DATE
                    GROUP BY
                        1, 2, 3, 4, 5
                )

                -- get all visits by country. Unexposed don't have campaign, flight, creative data
                ,total_visits_cte AS (
                    -- SELECT
                    --     p.exposure_datetime::DATE AS date_of_exposure
                    --     ,p.country
                    --     ,SUM(CASE WHEN p.segment_id = :homepage_segment THEN 1 END) AS total_homepage_page_visits
                    --     ,SUM(CASE WHEN p.segment_id = :signup_segment THEN 1 END) AS total_signup_page_visits
                    -- FROM pixels p
                    -- WHERE
                    --     date_of_exposure BETWEEN :report_start_datetime AND :report_end_datetime
                    -- GROUP BY 1, 2

                    SELECT
                        p.exposure_datetime::DATE AS date_of_exposure
                        ,p.country
                        ,COUNT(DISTINCT CASE WHEN p.segment_id = :homepage_segment THEN p.vip ELSE NULL END) AS total_homepage_page_visits
                        ,COUNT(DISTINCT CASE WHEN p.segment_id = :signup_segment THEN p.vip ELSE NULL END) AS total_signup_page_visits
                    FROM pixels p
                    WHERE
                        date_of_exposure BETWEEN :report_start_datetime AND :report_end_datetime
                    GROUP BY 1, 2

                )

                -- get exposed visit counts by segment, campaign, flight, creative
                ,exposed_visits_cte AS (
                    SELECT
                        e.exposure_datetime::DATE AS date_of_exposure
                        ,e.country
                        ,e.campaign_id
                        ,e.flight_id
                        ,e.creative_id
                        ,COUNT(DISTINCT CASE WHEN e.segment_id = :homepage_segment THEN e.vip ELSE NULL END) AS exposed_homepage_page_visits
                        ,COUNT(DISTINCT CASE WHEN e.segment_id = :signup_segment THEN e.vip ELSE NULL END) AS exposed_signup_page_visits
                    FROM exposed_visits e
                    WHERE
                        date_of_exposure BETWEEN :report_start_datetime::DATE AND :report_end_datetime::DATE
                    GROUP BY 1, 2, 3, 4, 5
                )

                -- get first time exposed visit counts by segment, campaign, flight, creative
                ,exposed_first_time_visits_cte AS (
                    SELECT
                        e.exposure_datetime::DATE AS date_of_exposure
                        ,e.country
                        ,e.campaign_id
                        ,e.flight_id
                        ,e.creative_id
                        ,COUNT(DISTINCT CASE WHEN e.segment_id = :homepage_segment THEN e.vip ELSE NULL END) AS exposed_first_time_homepage_page_visits
                        ,COUNT(DISTINCT CASE WHEN e.segment_id = :signup_segment THEN e.vip ELSE NULL END) AS exposed_first_time_signup_page_visits
                    FROM exposed_first_time_visits e
                    WHERE
                        date_of_exposure BETWEEN :report_start_datetime::DATE AND :report_end_datetime::DATE
                    GROUP BY 1, 2, 3, 4, 5
                )

                -- prep data
                ,report_prep_cte AS (
                    SELECT
                        c.mapping_country
                        -- DON'T INCLUDE ROWS ABOVE IN OUTPUT
                        ,c.report_execution_date
                        ,c.date_of_exposure
                        ,CASE WHEN DATEDIFF('DAY', c.date_of_exposure, c.report_execution_date) <= :attribution_window_days
                            THEN 'No'
                            ELSE 'Yes'
                        END AS is_fully_attributed
                        ,c.advertiser_id
                        ,REPLACE(c.advertiser, ',', '') AS advertiser
                        ,REPLACE(c.insertion_order_name, ',', '') AS insertion_order_name
                        ,c.campaign_id
                        ,REPLACE(c.campaign_name, ',', '') AS campaign_name
                        ,c.campaign_start_date
                        ,c.campaign_end_date
                        ,c.flight_id
                        ,REPLACE(c.flight_name, ',', '') AS flight_name
                        ,c.flight_start_date
                        ,c.flight_end_date
                        ,c.line_item_id
                        ,REPLACE(c.line_item_name, ',', '') AS line_item_name
                        ,c.creative_id
                        ,REPLACE(c.creative_name, ',', '') AS creative_name
                        ,c.product_name
                        ,c.rate_type
                        ,c.rate
                        ,c.placement_impressions_booked
                        ,c.booked_budget
                        ,c.impressions_delivered
                        ,c.budget_delivered
                        ,c.clicks
                        ,CASE
                            WHEN c.impressions_delivered = 0 OR c.clicks = 0
                            THEN 0 ELSE CAST(c.clicks AS FLOAT)/c.impressions_delivered
                        END AS ctr                                                                  --> ctr = clicks/impressions delivered
                        ,CASE
                            WHEN clicks = 0
                            THEN 0
                            ELSE CAST(budget_delivered AS FLOAT)/clicks
                        END AS cpc                                                                  --> cpc = budget delivered/clicks
                        ,c.reach
                        ,CASE
                            WHEN impressions_delivered = 0
                            THEN 0
                            ELSE CAST(impressions_delivered AS FLOAT)/reach
                        END AS frequency                                                            --> frequency = impressions delivered/reach
                        ,i.exposed_and_unexposed_install_count AS installs
                        ,e.average_time_spent_min AS average_exposed_time_spent_minutes
                        ,COALESCE(v.tile_start_count, 0) AS video_start
                        ,COALESCE(v.tile_25_count, 0) AS video_25
                        ,COALESCE(v.tile_50_count, 0) AS video_50
                        ,COALESCE(v.tile_75_count, 0) AS video_75
                        ,COALESCE(v.tile_complete_count, 0) AS video_completions
                        ,eo.exposed_app_openers AS total_exposed_app_openers
                        ,e.exposed_app_open_count AS total_exposed_app_opens
                        ,ei.exposed_install_count AS first_time_exposed_app_opens
                        ,tv.total_signup_page_visits AS total_signup_page_visit
                        ,ev.exposed_signup_page_visits AS total_exposed_signup_page_visits
                        ,efv.exposed_first_time_signup_page_visits AS total_first_time_exposed_signup_page_visits
                        ,tv.total_homepage_page_visits AS total_homepage_page_visits
                        ,ev.exposed_homepage_page_visits AS total_exposed_homepage_page_visits
                        ,efv.exposed_first_time_homepage_page_visits AS total_first_time_exposed_homepage_page_visits
                        -- DON'T INCLUDE ROWS BELOW IN OUTPUT
                        ,c.line_item_start_ts
                        ,c.line_item_end_ts
                        ,ROW_NUMBER() OVER(ORDER BY c.advertiser, c.mapping_country, c.date_of_exposure::DATE, c.campaign_id, c.flight_id, c.line_item_id, c.creative_id, impressions_delivered) AS row_num
                    FROM core_data c
                        LEFT JOIN exposure_stats_cte e ON e.campaign_id = c.campaign_id
                            AND e.flight_id = c.flight_id
                            AND e.creative_id = c.creative_id
                            AND e.country = c.mapping_country
                            AND e.date_of_exposure = c.date_of_exposure
                        LEFT JOIN exposed_openers_cte eo ON eo.campaign_id = c.campaign_id
                            AND eo.flight_id = c.flight_id
                            AND eo.creative_id = c.creative_id
                            AND eo.country = c.mapping_country
                            AND eo.date_of_exposure = c.date_of_exposure
                        LEFT JOIN exposed_installs_cte ei ON ei.campaign_id = c.campaign_id
                            AND ei.flight_id = c.flight_id
                            AND ei.creative_id = c.creative_id
                            AND ei.country = c.mapping_country
                            AND ei.date_of_exposure = c.date_of_exposure
                        LEFT JOIN installs_cte i ON i.country = c.mapping_country
                            AND i.app_first_open_date = c.date_of_exposure
                        LEFT JOIN video_tile_cte v ON v.campaign_id = c.campaign_id
                            AND v.flight_id = c.flight_id
                            AND v.creative_id = c.creative_id
                            AND v.country = c.mapping_country
                            AND v.date_of_exposure = c.date_of_exposure
                        LEFT JOIN total_visits_cte tv ON tv.country = c.mapping_country
                            AND tv.date_of_exposure = c.date_of_exposure
                        LEFT JOIN exposed_visits_cte ev ON ev.country = c.mapping_country
                            AND ev.campaign_id = c.campaign_id
                            AND ev.flight_id = c.flight_id
                            AND ev.creative_id = c.creative_id
                            AND ev.date_of_exposure = c.date_of_exposure
                        LEFT JOIN exposed_first_time_visits_cte efv ON efv.country = c.mapping_country
                            AND efv.campaign_id = c.campaign_id
                            AND efv.flight_id = c.flight_id
                            AND efv.creative_id = c.creative_id
                            AND efv.date_of_exposure = c.date_of_exposure
                )

                -- final selection
                SELECT
                    r.*
                    ,CEIL(r.row_num/:max_rows) AS row_partition  --> used for setting max number of rows per file
                FROM report_prep_cte r
                WHERE
                    r.date_of_exposure IS NOT NULL
                ORDER BY
                    r.row_num

            );

            
            -- =============================================================================
            -- =============================================================================
            -- END REPORT
            -- =============================================================================
            -- =============================================================================


            -- determine the number of partitions needed. At least 1
            file_partitions := (SELECT GREATEST(CEIL(COUNT(*)/:max_rows), 1) FROM output_table);

            -- create a dynamic query to push the output to appropriate file/bucket
            -- NOTE: COPY INTO has PARTITION BY command that would automatically split file up, but this would prepend "data_" to the file names which  
            -- would violate the naming convention requested, so we will have to do this manually
            -- with num as a counter, loop from 1 to count of file_partitions and extract report data to files based on row_partition field
            FOR num IN 1 TO file_partitions DO 

                -- save num as sequence ID (e.g., 001, 002, 003)
                sequence_id := RIGHT('000' || num::VARCHAR, 3);


                -- if for the US then use the US buckets, else use the international buckets
                IF (:country = 'us') THEN 
                    output_stage            := us_stage;
                    output_stage_receipt    := us_stage_receipt;
                ELSE
                    output_stage            := int_stage;
                    output_stage_receipt    := int_stage_receipt;
                END IF;


                -- e.g., "weekly/paramount_plus_eu_weekly_campaign_2024-10-21_2024-10-27_001.csv"
                file_path   := :report_interval || '/' || :file_name_prefix || :country || '_' || :report_interval || '_campaign_' || :start_date || '_' || :end_date || '_' || :sequence_id || '.csv';

                -- e.g., "@paramount_plus_external_us/weekly/paramount_plus_eu_weekly_campaign_2024-10-21_2024-10-27_001.csv"
                -- e.g., "@clientsolutions_internal/analytics/custom/lhaynes/unified/paramount_plus_external_international/weekly/paramount_plus_eu_weekly_campaign_2024-10-21_2024-10-27_001.csv"
                output_file         := output_stage         || file_path;
                output_file_receipt := output_stage_receipt || file_path;


                -- output queries
                -- select all fields except row_partition
                -- this LEFT JOIN ensures that if no data is available that a blank file will be sent
                stmt_begin          := 'COPY INTO ' || :output_file;
                stmt_begin_receipt  := 'COPY INTO ' || :output_file_receipt;

                stmt_end := '
                FROM (
                    SELECT 
                        r.report_execution_date
                        ,o.date_of_exposure
                        ,o.is_fully_attributed
                        ,o.advertiser_id
                        ,o.advertiser
                        ,o.insertion_order_name
                        ,o.campaign_id
                        ,o.campaign_name
                        ,o.campaign_start_date
                        ,o.campaign_end_date
                        ,o.flight_id
                        ,o.flight_name
                        ,o.flight_start_date
                        ,o.flight_end_date
                        ,o.line_item_id
                        ,o.line_item_name
                        ,o.creative_id
                        ,o.creative_name
                        ,o.product_name
                        ,o.rate_type
                        ,o.rate
                        ,o.placement_impressions_booked
                        ,o.booked_budget
                        ,o.impressions_delivered
                        ,o.budget_delivered
                        ,o.clicks
                        ,o.ctr                                                       
                        ,o.cpc                                           
                        ,o.reach
                        ,o.frequency                                                         
                        ,o.installs
                        ,o.average_exposed_time_spent_minutes
                        ,o.video_start
                        ,o.video_25
                        ,o.video_50
                        ,o.video_75
                        ,o.video_completions
                        ,o.total_exposed_app_openers
                        ,o.total_exposed_app_opens
                        ,o.first_time_exposed_app_opens
                        ,o.total_signup_page_visit
                        ,o.total_exposed_signup_page_visits
                        ,o.total_first_time_exposed_signup_page_visits
                        ,o.total_homepage_page_visits
                        ,o.total_exposed_homepage_page_visits
                        ,o.total_first_time_exposed_homepage_page_visits
                    FROM (SELECT ''' || :current_date_dt::DATE || ''' AS report_execution_date) r
                        LEFT JOIN output_table o ON 1 = 1
                            AND o.row_partition = ''' || num::VARCHAR || '''
                    ORDER BY o.row_num
                )
                FILE_FORMAT     = (
                    FORMAT_NAME                     = udw_clientsolutions_cs.analytics_csv
                    COMPRESSION                     = ''none''
                    NULL_IF                         = ()
                    FIELD_OPTIONALLY_ENCLOSED_BY    = ''"''
                    SKIP_HEADER                     = 0
                    DATE_FORMAT                     = ''YYYY-MM-DD''
                    TIME_FORMAT                     = ''HH24:MI:SS''
                    TIMESTAMP_FORMAT                = ''YYYY-MM-DD HH24:MI:SS''
                )
                SINGLE          = TRUE
                HEADER          = TRUE
                MAX_FILE_SIZE   = 4900000000
                OVERWRITE       = TRUE;
                ';


                -- make dynamic query strings for the s3 file and our copy of the s3 file
                stmt_begin_plus_stmt_end            := stmt_begin           || stmt_end;
                stmt_begin_receipt_plus_stmt_end    := stmt_begin_receipt   || stmt_end;


                -- execute queries
                -- RO ("RECEIPT ONLY") option added for testing
                IF (:save_receipt_YorN = 'RO') THEN
                    -- only save receipt reports
                    EXECUTE IMMEDIATE stmt_begin_receipt_plus_stmt_end;
                ELSEIF (:save_receipt_YorN = 'Y') THEN
                    -- deliver reports and save receipts
                    EXECUTE IMMEDIATE stmt_begin_plus_stmt_end;
                    EXECUTE IMMEDIATE stmt_begin_receipt_plus_stmt_end;
                ELSE
                    -- only deliver reports. Assumed N as default
                    EXECUTE IMMEDIATE stmt_begin_plus_stmt_end;
                END IF;


            END FOR;


        END FOR;
    CLOSE cur;


    RETURN 'SUCCESS';


-- handle exception
-- prod: https://hooks.slack.com/triggers/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
-- dev: https://hooks.slack.com/triggers/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
EXCEPTION
    WHEN OTHER THEN

        -- Procedure xyz for partner failed. Error: (0, error message) || LOG: (message 1 => message 2 => message 3)
        SELECT udw_clientsolutions_cs.udf_submit_slack_notification_simple(
            slack_webhook_url => 'https://hooks.slack.com/triggers/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'
            ,date_string => :current_date_dt::VARCHAR
            ,name_string => 'Snowflake Task Monitor (GLOBAL)'
            ,message_string => 'Procedure "udw_clientsolutions_cs.sp_partner_get_weekly_reports_global" failed.' || 
                ' Error: (' || :SQLCODE || ', ' || :SQLERRM || ')'
        );

        RETURN 'FAILED WITH ERROR(' || :SQLCODE || ', ' || :SQLERRM || ')';

END;
$$;
