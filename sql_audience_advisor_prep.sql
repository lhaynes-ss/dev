/**
 ------------------
 AUDIENCE ADVISOR
 ------------------
 Import CSV from s3 to UDW temp table

 Find and Replace template values below
 =====================================
 {{source_file_1}}      (e.g., s3://samsung-dm-data-share-analytics/export/20240110/psid/123456.csv)
 {{source_file_2}}      (e.g., s3://samsung-dm-data-share-analytics/export/20240110/psid/123456.csv)
 {{source_file_3}}      (e.g., s3://samsung-dm-data-share-analytics/export/20240110/psid/123456.csv)
 {{user_name}}          (e.g., vaughn) the name of your dir in "/analytics/custom/"
 {{audience_1_id}}      (e.g., 1234)
 {{audience_2_id}}      (e.g., 1234)
 {{audience_3_id}}      (e.g., 1234)
 {{ticket_num}}         (e.g., SAI9876)
 {{date}}               (e.g., 20240131)


 AWS
 =====================================
 -----------------
 List Directories
 -----------------
 aws --profile nyc s3 ls s3://samsung.ads.data.share/analytics/custom/{{user_name}}/

 ---------
 Copy 
 --------
 aws --profile nyc s3 cp {{source_file_1}} s3://samsung.ads.data.share/analytics/custom/{{user_name}}/audiencetransfer/{{ticket_num}}/{{date}}/{{audience_1_id}}.csv 
 aws --profile nyc s3 cp {{source_file_2}} s3://samsung.ads.data.share/analytics/custom/{{user_name}}/audiencetransfer/{{ticket_num}}/{{date}}/{{audience_2_id}}.csv 
 aws --profile nyc s3 cp {{source_file_3}} s3://samsung.ads.data.share/analytics/custom/{{user_name}}/audiencetransfer/{{ticket_num}}/{{date}}/{{audience_3_id}}.csv 
 
 Wiki: https://adgear.atlassian.net/wiki/spaces/MAST/pages/19055676278/Audience+Advisor+Next+Gen+CAPI+replacement
 Github: https://github.com/SamsungAdsAnalytics/QueryBase/tree/master/UDW/Standardized_Reports/Audience_Advisor
 **/

-- import audience 1
DROP TABLE IF EXISTS capi_audience_{{audience_1_id}};
CREATE TEMP TABLE capi_audience_{{audience_1_id}} (psid VARCHAR(100));
COPY INTO capi_audience_{{audience_1_id}} 
FROM @adbiz_data.SAMSUNG_ADS_DATA_SHARE/analytics/custom/{{user_name}}/{{ticket_num}}/{{date}}/{{audience_1_id}}.csv
file_format = (format_name = adbiz_data.analytics_csv);

-- import audience 2
DROP TABLE IF EXISTS capi_audience_{{audience_2_id}};
CREATE TEMP TABLE capi_audience_{{audience_2_id}} (psid VARCHAR(100));
COPY INTO capi_audience_{{audience_2_id}} 
FROM @adbiz_data.SAMSUNG_ADS_DATA_SHARE/analytics/custom/{{user_name}}/{{ticket_num}}/{{date}}/{{audience_2_id}}.csv
file_format = (format_name = adbiz_data.analytics_csv);

-- import audience 3
DROP TABLE IF EXISTS capi_audience_{{audience_3_id}};
CREATE TEMP TABLE capi_audience_{{audience_3_id}} (psid VARCHAR(100));
COPY INTO capi_audience_{{audience_3_id}} 
FROM @adbiz_data.SAMSUNG_ADS_DATA_SHARE/analytics/custom/{{user_name}}/{{ticket_num}}/{{date}}/{{audience_3_id}}.csv
file_format = (format_name = adbiz_data.analytics_csv);


-- =====================================
-- MERGE AUDIENCES (per ticket request)
-- =====================================
-- OPTION 1: MERGE psid's FROM 3 audiences - Using "OR" logic (UNION)
DROP TABLE IF EXISTS capi_audience_or;
CREATE temp TABLE capi_audience_or AS
SELECT DISTINCT psid
FROM (
    SELECT * FROM capi_audience_{{audience_1_id}}
    UNION 
    SELECT * FROM capi_audience_{{audience_2_id}}
    UNION 
    SELECT * FROM capi_audience_{{audience_3_id}}
);


-- OPTION 2: MERGE psid's FROM 3 audiences - Using "AND" logic (JOIN)
DROP TABLE IF EXISTS capi_audience_and;
CREATE temp TABLE capi_audience_and AS
SELECT DISTINCT psid
FROM (
    SELECT DISTINCT * 
    FROM capi_audience_{{audience_1_id}}
        INNER JOIN capi_audience_{{audience_2_id}} USING(psid)
        INNER JOIN capi_audience_{{audience_3_id}} USING(psid)
);

