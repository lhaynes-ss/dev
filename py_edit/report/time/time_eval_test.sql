
/****

Time Evaluation Halfway Test

1) Overall percentage by each category 
2) Percentage break out of Campaign launch by task 
3) Percentage break out of media planning by task
4) Total hours collected across these slides 
a. What are the top 10 tasks that take up the most time (in order) and % from total hours collected?
5) % of Pre-sales vs. Post Sales
6) What % of time does upselling task take (how many hours over total hours). 

7) can you update the excel above to also include tasks break out by:
    Reporting
    Internal Meetings
    Client Communication
    Audience Creation/Management
    Campaign Management

****/


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
    ,log_date
    ,minutes_active 
FROM 
    udw_clientsolutions_cs.org_time_tracking
LIMIT 100;



-- 1) Overall percentage by each category 
-- 4) Total hours collected across these slides 
WITH time_cte AS ( 
    SELECT 
        category
        ,SUM(minutes_active) AS total_minutes_active
    FROM udw_prod.udw_clientsolutions_cs.org_time_tracking
    GROUP BY category
)

SELECT
    category
    ,total_minutes_active
    ,RATIO_TO_REPORT(total_minutes_active) OVER () AS percent_minutes_active
    ,(SUM(total_minutes_active) OVER(PARTITION BY category))/60 AS total_hours
    ,(SUM(total_minutes_active) OVER())/60 AS grand_total_hours
FROM time_cte
ORDER BY 3 DESC
;



-- 2) Percentage break out of Campaign launch by task 
-- 3) Percentage break out of media planning by task
-- 7) can you update the excel above to also include...
-- ====================================================
WITH time_cte AS ( 
    SELECT 
        category
        ,task
        ,SUM(minutes_active) AS total_minutes_active
    FROM udw_prod.udw_clientsolutions_cs.org_time_tracking
    WHERE
        category = 'Campaign Launch'
    GROUP BY category, task
)

SELECT
    category
    ,task
    ,total_minutes_active
    ,RATIO_TO_REPORT(total_minutes_active) OVER () AS percent_minutes_active
FROM time_cte
ORDER BY 4 DESC
;

------
WITH time_cte AS ( 
    SELECT 
        category
        ,task
        ,SUM(minutes_active) AS total_minutes_active
    FROM udw_prod.udw_clientsolutions_cs.org_time_tracking
    WHERE
        category = 'Media Planning'
    GROUP BY category, task
)

SELECT
    category
    ,task
    ,total_minutes_active
    ,RATIO_TO_REPORT(total_minutes_active) OVER () AS percent_minutes_active
FROM time_cte
ORDER BY 4 DESC
;

------
WITH time_cte AS ( 
    SELECT 
        category
        ,task
        ,SUM(minutes_active) AS total_minutes_active
    FROM udw_prod.udw_clientsolutions_cs.org_time_tracking
    WHERE
        category = 'Reporting'
    GROUP BY category, task
)

SELECT
    category
    ,task
    ,total_minutes_active
    ,RATIO_TO_REPORT(total_minutes_active) OVER () AS percent_minutes_active
FROM time_cte
ORDER BY 4 DESC
;

------
WITH time_cte AS ( 
    SELECT 
        category
        ,task
        ,SUM(minutes_active) AS total_minutes_active
    FROM udw_prod.udw_clientsolutions_cs.org_time_tracking
    WHERE
        category = 'Internal Meetings'
    GROUP BY category, task
)

SELECT
    category
    ,task
    ,total_minutes_active
    ,RATIO_TO_REPORT(total_minutes_active) OVER () AS percent_minutes_active
FROM time_cte
ORDER BY 4 DESC
;

------
WITH time_cte AS ( 
    SELECT 
        category
        ,task
        ,SUM(minutes_active) AS total_minutes_active
    FROM udw_prod.udw_clientsolutions_cs.org_time_tracking
    WHERE
        category = 'Client Communication'
    GROUP BY category, task
)

SELECT
    category
    ,task
    ,total_minutes_active
    ,RATIO_TO_REPORT(total_minutes_active) OVER () AS percent_minutes_active
FROM time_cte
ORDER BY 4 DESC
;

------
WITH time_cte AS ( 
    SELECT 
        category
        ,task
        ,SUM(minutes_active) AS total_minutes_active
    FROM udw_prod.udw_clientsolutions_cs.org_time_tracking
    WHERE
        category = 'Audience Creation/Management'
    GROUP BY category, task
)

SELECT
    category
    ,task
    ,total_minutes_active
    ,RATIO_TO_REPORT(total_minutes_active) OVER () AS percent_minutes_active
FROM time_cte
ORDER BY 4 DESC
;

------
WITH time_cte AS ( 
    SELECT 
        category
        ,task
        ,SUM(minutes_active) AS total_minutes_active
    FROM udw_prod.udw_clientsolutions_cs.org_time_tracking
    WHERE
        category = 'Campaign Management'
    GROUP BY category, task
)

SELECT
    category
    ,task
    ,total_minutes_active
    ,RATIO_TO_REPORT(total_minutes_active) OVER () AS percent_minutes_active
FROM time_cte
ORDER BY 4 DESC
;
-- ====================================================



-- a. What are the top 10 tasks that take up the most time (in order) and % from total hours collected?
-- 6) What % of time does upselling task take (how many hours over total hours). 
WITH time_cte AS ( 
    SELECT 
        category
        ,task
        ,SUM(minutes_active) AS total_minutes_active
    FROM udw_prod.udw_clientsolutions_cs.org_time_tracking
    GROUP BY category, task
)

SELECT
    ROW_NUMBER() OVER(ORDER BY total_minutes_active DESC) AS rank_by_time
    ,task
    ,category
    ,total_minutes_active
    ,RATIO_TO_REPORT(total_minutes_active) OVER () AS percent_minutes_active
FROM time_cte
ORDER BY 
    rank_by_time
    ,category
    ,task
;



-- 5) % of Pre-sales vs. Post Sales
WITH cte AS ( 
    SELECT 
        sales_cycle
        ,SUM(minutes_active) AS total_minutes_active
    FROM udw_prod.udw_clientsolutions_cs.org_time_tracking
    -- WHERE sales_cycle IN ('Pre Sales', 'Post Sales')
    GROUP BY 1
)

SELECT
    sales_cycle
    ,total_minutes_active
    ,RATIO_TO_REPORT(total_minutes_active) OVER () AS percent_minutes_active
FROM cte
ORDER BY 1
;


