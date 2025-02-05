/**

Execute Pluto reports
---------------------------

USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD_GLOBAL;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_GLOBAL_LARGE;
USE DATABASE UDW_PROD_GLOBAL;
USE SCHEMA UDW_CLIENTSOLUTIONS_CS;

CALL udw_clientsolutions_cs.sp_pluto_get_weekly_reports_global(
    save_receipt_YorN => 'Y' -- Y | N | RO (Receipt Only)
    ,start_date => ''
    ,end_date => ''
);

**/

-- connection settings
USE ROLE UDW_CLIENTSOLUTIONS_DEFAULT_CONSUMER_ROLE_PROD_GLOBAL;
USE WAREHOUSE UDW_CLIENTSOLUTIONS_DEFAULT_WH_PROD_GLOBAL_LARGE;
USE DATABASE UDW_PROD_GLOBAL;
USE SCHEMA UDW_CLIENTSOLUTIONS_CS;


CREATE OR REPLACE PROCEDURE udw_clientsolutions_cs.sp_pluto_get_weekly_reports_global(
    -- specify parameters for procedure
    save_receipt_YorN          VARCHAR      --> 'Y' or 'N'. Indicates whether to save a copy of the report for us
    ,start_date                VARCHAR      --> 'YYYY-MM-DD' or if '' then dates will be auto-set by the stored proc
    ,end_date                  VARCHAR      --> 'YYYY-MM-DD' or if '' then dates will be auto-set by the stored proc
)
RETURNS STRING
LANGUAGE SQL
AS
$$
DECLARE 
    -- specify variables used in this stored procedure
    partner                     VARCHAR;    --> paramount | pluto
    interval                    VARCHAR;    --> weekly | monthly
    regions                     OBJECT;     --> countries by connection. database_region where database in (udw, cdw) and region in (na, eu, nordics, apac, sa)
    max_rows                    INT;        --> max # of rows per report. Point at which report is split into additional file. 1 million - 1 for header
    attribution_window          INT;        --> max days after exposure for attribution credit
    us_stage                    VARCHAR;    --> stage for US reports s3 bucket
    int_stage                   VARCHAR;    --> stage for international reports s3 bucket
    us_stage_receipt            VARCHAR;    --> stage for our copy of US reports
    int_stage_receipt           VARCHAR;    --> stage for our copy of international reports
    file_name_prefix            VARCHAR;    --> string prepended to file name. Usually advertiser name
    attribution_window_days     INT;        --> number of days for conversion attribution
    lookback_window_months      INT;        --> number of months for lookback window
    page_visit_lookback_days    INT;        --> number of days for web pixel lookback
    operative_table             VARCHAR;    --> schema.table for advertiser custom operative one data
    mapping_table               VARCHAR;    --> schema.table for advertiser custom mapping data
    app_name                    VARCHAR;    --> app name
    signup_segment              VARCHAR;    --> segment id for signup pixel or ''
    homepage_segment            VARCHAR;    --> segment id for homepage pixel or ''
    current_date_dt             TIMESTAMP;  --> today
    region_keys                 ARRAY;      --> holds region keys for looping (e.g., ['udw_na', 'cdw_eu', ...])
    region_key                  VARCHAR;    --> holds a specific region, used for looping (e.g., 'udw_na')
    region_countries            VARCHAR;    --> holds the corresponding value for the region_key (e.g., 'US, CA')
    sp                          VARCHAR;    --> name of the stored procedure to use
    get_reports_query           VARCHAR;    --> the dynamic query string used to generate the report
    task_name                   VARCHAR;    --> the name of the task that will trigger this stored procedure
    log                         ARRAY;      --> array to store log messages for debugging
    log_message                 VARCHAR;    --> message to add to the log array. For debugging.

BEGIN

    -- get current date
    current_date_dt := CURRENT_DATE();

    -- ==================================================================================
    -- start config 
    -- ==================================================================================
    partner                     := 'pluto';
    interval                    := 'weekly';
    regions                     := OBJECT_CONSTRUCT(
                                    'udw_na'        , 'US, CA'
                                    ,'cdw_eu'       , 'AT, DE, ES, FR, GB, IT'
                                    ,'cdw_nordics'  , 'DK, NO, SE'
                                    -- ,'cdw_apac'     , 'AU'
                                    ,'cdw_sa'       , 'BR'
                                );
    max_rows                    := 999999;
    attribution_window          := 7;

    -- us_stage                    := '@udw_marketing_analytics_reports.pluto_external_us/';
    -- int_stage                   := '@udw_marketing_analytics_reports.pluto_external_international/';
    -- file_name_prefix            := 'pluto_';

    us_stage                    := '@clientsolutions_internal/analytics/custom/lhaynes/unified/pluto_external_us/';
    int_stage                   := '@clientsolutions_internal/analytics/custom/lhaynes/unified/pluto_external_international/';
    file_name_prefix            := 'pluto_';

    us_stage_receipt            := '@clientsolutions_internal/analytics/custom/lhaynes/unified_receipts/pluto_external_us/';
    int_stage_receipt           := '@clientsolutions_internal/analytics/custom/lhaynes/unified_receipts/pluto_external_international/';

    IF (TRIM(LOWER(:save_receipt_YorN)) = 'y') THEN
        save_receipt_YorN       := 'Y';
    ELSEIF (TRIM(LOWER(:save_receipt_YorN)) = 'ro') THEN
        save_receipt_YorN       := 'RO';
    ELSE
        save_receipt_YorN       := 'N';
    END IF;

    attribution_window_days     := 7;
    lookback_window_months      := 12;
    page_visit_lookback_days    := 30;
    operative_table             := 'udw_clientsolutions_cs.pluto_operative_sales_orders';
    mapping_table               := 'udw_clientsolutions_cs.pluto_custom_creative_mapping';

    app_name                    := 'Pluto TV';
    signup_segment              := '';
    homepage_segment            := '';

    task_name                   := 'tsk_pluto_get_weekly_reports_global';

    -- ==================================================================================
    -- end config 
    -- ==================================================================================

    -- init logging
    log := ARRAY_CONSTRUCT();
    log_message := '';

    -- get an array of all of the keys from the regions object
    -- for regions = { "a": "regions a", "b": "regions b", "c": "regions c"}
    -- region_keys = ['a', 'b', 'c']
    region_keys := OBJECT_KEYS(:regions);
    
    -- loop through the keys array... (e.g., ['udw_na', 'cdw_eu', ...])
    FOR num IN 0 TO ARRAY_SIZE(:region_keys) - 1 DO

        -- log message (e.g., Region udw_na started.)
        log_message := 'Region ' || :region_key || ' started.';
        log := (SELECT ARRAY_APPEND(:log, :log_message));
        
        -- get the region key (e.g., 'udw_na')
        region_key := GET(:region_keys, num);

        -- get the countries list (e.g., 'US, CA')
        region_countries := GET(:regions, :region_key);

        -- specify stored procedure to use depending on region
        sp := 'sp_partner_get_weekly_reports_global';

        -- build dynamic query
        get_reports_query := '
            CALL udw_clientsolutions_cs.' || :sp || '(
                partner                     => ''' || :partner || '''
                ,region                     => ''' || :region_key || '''
                ,report_interval            => ''' || :interval || '''
                ,start_date                 => ''' || :start_date || '''
                ,end_date                   => ''' || :end_date || '''
                ,countries                  => ''' || :region_countries || '''
                ,max_rows                   => '   || :max_rows || '
                ,attribution_window         => '   || :attribution_window || '
                ,us_stage                   => ''' || :us_stage || '''
                ,int_stage                  => ''' || :int_stage || '''
                ,file_name_prefix           => ''' || :file_name_prefix || '''
                ,us_stage_receipt           => ''' || :us_stage_receipt || '''
                ,int_stage_receipt          => ''' || :int_stage_receipt || '''
                ,save_receipt_YorN          => ''' || :save_receipt_YorN || '''
                ,attribution_window_days    => '   || :attribution_window_days || '  
                ,lookback_window_months     => '   || :lookback_window_months || '   
                ,page_visit_lookback_days   => '   || :page_visit_lookback_days || ' 
                ,operative_table            => ''' || :operative_table || '''        
                ,mapping_table              => ''' || :mapping_table || '''        
                ,app_name                   => ''' || :app_name || '''               
                ,signup_segment             => ''' || :signup_segment || '''         
                ,homepage_segment           => ''' || :homepage_segment || '''       
            );
        ';


        -- RETURN :get_reports_query;              --> uncomment this line for testing

        -- execute dynamic query
        EXECUTE IMMEDIATE :get_reports_query;   --> production

        -- log message (e.g., Region udw_na completed.)
        log_message := 'Region ' || :region_key || ' completed.';
        log := (SELECT ARRAY_APPEND(:log, :log_message));


    END FOR;

    RETURN 'SUCCESS';


-- handle exception
-- prod: https://hooks.slack.com/triggers/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
-- dev: https://hooks.slack.com/triggers/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
EXCEPTION
    WHEN OTHER THEN

        -- Task x failed. Error: (0, error message) || LOG: (log message 1 => log message 2)
        SELECT udw_clientsolutions_cs.udf_submit_slack_notification_simple(
            slack_webhook_url => 'https://hooks.slack.com/triggers/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'
            ,date_string => :current_date_dt::VARCHAR
            ,name_string => 'Snowflake Task Monitor (GLOBAL)'
            ,message_string => 'Task "' || :task_name || '" failed.' || 
                ' Error: (' || :SQLCODE || ', ' || :SQLERRM || ')' ||
                ' || LOG: (' || ARRAY_TO_STRING(:log, ' => ') || ')'
        );

        RETURN 'FAILED WITH ERROR(' || :SQLCODE || ', ' || :SQLERRM || ')';

END;
$$;
