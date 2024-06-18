-- https://adgear.atlassian.net/browse/SAI-6351



-- raw data preview
SELECT * 
FROM udw_prod.udw_clientsolutions_cs.org_time_tracking 
LIMIT 1000;


-- spreadsheet output
SELECT 
    name
    ,role
    ,region
    ,vertical
    ,team
    ,sales_group
    ,department
    ,category
    ,task
    ,sales_cycle
    ,log_date
    ,minutes_active
    ,week_num
FROM udw_prod.udw_clientsolutions_cs.org_time_tracking 
WHERE 
    log_date BETWEEN CAST('2024-04-22' AS DATE) AND CAST('2024-05-17' AS DATE)   
;
