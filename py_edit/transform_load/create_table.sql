-- LIST @adbiz_data.samsung_ads_data_share/analytics/custom/vaughn/test/foo/;
-- aws --profile nyc s3 cp john_doe.csv s3://samsung.ads.data.share/analytics/custom/vaughn/test/foo/
-- @adbiz_data.samsung_ads_data_share/analytics/custom/vaughn/test/foo/john_doe.csv 
-- udw_prod.udw_clientsolutions_cs.org_time_tracking



DROP TABLE IF EXISTS org_time_tracking_raw;
CREATE TEMP TABLE org_time_tracking_raw (
    name VARCHAR(556)
    ,role VARCHAR(556)
    ,region VARCHAR(556)
    ,vts VARCHAR(556)
    ,vertical VARCHAR(556)
    ,team VARCHAR(556)
    ,sales_group VARCHAR(556)
    ,category VARCHAR(556)
    ,task VARCHAR(556)
    ,date_15_apr VARCHAR(556)
    ,date_16_apr VARCHAR(556)
    ,date_17_apr VARCHAR(556)
    ,date_18_apr VARCHAR(556)
    ,date_19_apr VARCHAR(556)
    ,date_22_apr VARCHAR(556)
    ,date_23_apr VARCHAR(556)
    ,date_24_apr VARCHAR(556)
    ,date_25_apr VARCHAR(556)
    ,date_26_apr VARCHAR(556)
    ,date_29_apr VARCHAR(556)
    ,date_30_apr VARCHAR(556)
    ,date_1_may VARCHAR(556)
    ,date_2_may VARCHAR(556)
    ,date_3_may VARCHAR(556)
    ,date_6_may VARCHAR(556)
    ,date_7_may VARCHAR(556)
    ,date_8_may VARCHAR(556)
    ,date_9_may VARCHAR(556)
    ,date_10_may VARCHAR(556)
    ,date_13_may VARCHAR(556)
    ,date_14_may VARCHAR(556)
    ,date_15_may VARCHAR(556)
    ,date_16_may VARCHAR(556)
    ,date_17_may VARCHAR(556)
    ,total VARCHAR(556)
);

COPY INTO org_time_tracking_raw
  FROM @adbiz_data.samsung_ads_data_share/analytics/custom/vaughn/test/foo/john_doe.csv
  FILE_FORMAT = (FORMAT_NAME = adbiz_data.mycsvformat3);

-- SELECT * FROM org_time_tracking_raw LIMIT 100;


DROP TABLE IF EXISTS org_time_tracking;
CREATE TEMP TABLE org_time_tracking (
    SELECT 
        name
        ,role
        ,region
        ,vertical
        ,team
        ,sales_group
        ,category
        ,task
        ,date_15_apr
        ,date_16_apr
        ,date_17_apr
        ,date_18_apr
        ,date_19_apr
        ,date_22_apr
        ,date_23_apr
        ,date_24_apr
        ,date_25_apr
        ,date_26_apr
        ,date_29_apr
        ,date_30_apr
        ,date_1_may
        ,date_2_may
        ,date_3_may
        ,date_6_may
        ,date_7_may
        ,date_8_may
        ,date_9_may
        ,date_10_may
        ,date_13_may
        ,date_14_may
        ,date_15_may
        ,date_16_may
        ,date_17_may
    FROM org_time_tracking_raw
);
