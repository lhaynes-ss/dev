-- https://adgear.atlassian.net/browse/SAI-6351



-- raw data preview
SELECT * 
FROM udw_prod.udw_clientsolutions_cs.org_time_tracking 
LIMIT 1000;


-- spreadsheet output
SELECT 
    log_date
    ,name
    ,role
    ,vertical
    ,region
    ,category
    ,SUM(minutes_active/60) AS hours_active
FROM udw_prod.udw_clientsolutions_cs.org_time_tracking 
GROUP BY 
    log_date
    ,name
    ,role
    ,vertical
    ,region
    ,category    
;
