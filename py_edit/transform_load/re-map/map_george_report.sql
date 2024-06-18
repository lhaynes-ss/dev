/**
upload maps
-------------
aws --profile scop s3 cp client_facing_map.csv s3://samsung.ads.data.share/analytics/custom/vaughn/test/
aws --profile scop s3 cp leadership_map.csv s3://samsung.ads.data.share/analytics/custom/vaughn/test/
aws --profile scop s3 cp map.csv s3://samsung.ads.data.share/analytics/custom/vaughn/test/
**/


--===================
-- CLIENT FACING DERPARTMENT MAP
--===================
DROP TABLE IF EXISTS client_facing_tab;
CREATE TEMP TABLE client_facing_tab (
    label VARCHAR(500)
    ,department VARCHAR(500)	
);

COPY INTO client_facing_tab
FROM @adbiz_data.samsung_ads_data_share/analytics/custom/vaughn/test/client_facing_map.csv
file_format = (format_name = adbiz_data.mycsvformat3);


DROP TABLE IF EXISTS client_facing_table;
CREATE TEMP TABLE client_facing_table AS (
    SELECT DISTINCT 
        TRIM(label) AS label
        ,TRIM(department) AS department
    FROM client_facing_tab
);



--===================
-- LEADERSHIP ROLES MAP
--===================
DROP TABLE IF EXISTS leadership_tab;
CREATE TEMP TABLE leadership_tab (
    label VARCHAR(500)
    ,role VARCHAR(500)	
);

COPY INTO leadership_tab
FROM @adbiz_data.samsung_ads_data_share/analytics/custom/vaughn/test/leadership_map.csv
file_format = (format_name = adbiz_data.mycsvformat3);


DROP TABLE IF EXISTS leadership_table;
CREATE TEMP TABLE leadership_table AS (
    SELECT DISTINCT 
        TRIM(label) AS label
        ,TRIM(role) AS role
    FROM leadership_tab
);


--===================
-- DATA MAP
--===================
DROP TABLE IF EXISTS map_tab;
CREATE TEMP TABLE map_tab (
    department VARCHAR(500)
    ,sales_cycle VARCHAR(500)	
    ,category VARCHAR(500)
    ,change_category VARCHAR(500)
    ,task VARCHAR(500)

);

COPY INTO map_tab
FROM @adbiz_data.samsung_ads_data_share/analytics/custom/vaughn/test/map.csv
file_format = (format_name = adbiz_data.mycsvformat3);



-- SELECT trim(department) AS department, trim(task), row_number() OVER(PARTITION BY trim(department), trim(task) ORDER BY department, task) as rn  FROM map_tab; 



-- SELECT * FROM map_tab WHERE task LIKE 'Ad-hoc%';
-- select * from UDW_CLIENTSOLUTIONS_CS.ORG_TIME_TRACKING;


DROP TABLE IF EXISTS map_table;
CREATE TEMP TABLE map_table AS (
    SELECT DISTINCT 
        TRIM(department) AS department
        ,TRIM(sales_cycle) AS sales_cycle
        ,TRIM(category) AS category
        ,TRIM(change_category) AS change_category
        ,CASE 
            WHEN TRIM(task) LIKE 'Town Hall%NPS%'
            THEN 'Town Hall/Sales +/ NPS Live etc…'
            WHEN TRIM(task) LIKE 'Ad-hoc%Slack%Phone%'
            THEN 'Ad-hoc Support questions and solve issues - Slack  Email  Phone Call etc.'
            ELSE TRIM(task)
        END AS task
    FROM map_tab
);


SELECT 
    t.LOG_DATE, 
    t.NAME, 
    t.ROLE, 
    t.REGION, 
    t.VERTICAL, 
    t.TEAM, 
    t.SALES_GROUP, 
    t.DEPARTMENT, 
    t.CATEGORY, 
    t.TASK, 
    t.SALES_CYCLE, 
    t.MINUTES_ACTIVE, 
    t.WEEK_NUM,
    TRIM(m.department) AS new_department,
    TRIM(m.sales_cycle) AS new_sales_cycle,
    TRIM(m.category) AS new_category,
    TRIM(m.task) AS new_task,
    l.label AS leadership_status,
    c.label AS client_facing_status
FROM UDW_CLIENTSOLUTIONS_CS.ORG_TIME_TRACKING t
    LEFT JOIN map_table m ON TRIM(m.DEPARTMENT) = TRIM(t.DEPARTMENT)
        AND TRIM(m.task) = TRIM(t.task)
        AND TRIM(m.category) = TRIM(t.category)
    LEFT JOIN leadership_table l ON l.role = t.role 
    LEFT JOIN client_facing_table c ON c.department = t.department
WHERE m.task IS NULL
;  





--===================
-- VERIFY COUNTS STILL MATCH
--===================

SELECT 
    COUNT(*)
FROM UDW_CLIENTSOLUTIONS_CS.ORG_TIME_TRACKING t;

SELECT 
    COUNT(*)
FROM UDW_CLIENTSOLUTIONS_CS.ORG_TIME_TRACKING t
    LEFT JOIN map_table m ON TRIM(m.DEPARTMENT) = TRIM(t.DEPARTMENT)
        AND TRIM(m.task) = TRIM(t.task)
        AND TRIM(m.category) = TRIM(t.category);



--===================
-- FINAL SELECTION
--===================

-- Report with re-mapping applied
DROP TABLE IF EXISTS report;
CREATE TEMP TABLE report AS (
    SELECT 
        t.NAME, 
        t.ROLE, 
        t.REGION, 
        t.VERTICAL, 
        t.TEAM, 
        t.SALES_GROUP, 
        t.DEPARTMENT, 
        CASE 
            WHEN COALESCE(m.change_category, '') <> '' 
            THEN TRIM(m.change_category) 
            ELSE t.category 
        END AS category, 
        t.TASK, 
        TRIM(m.sales_cycle) AS sales_cycle, 
        t.LOG_DATE, 
        t.MINUTES_ACTIVE, 
        t.WEEK_NUM,
        l.label AS leadership_status,
        c.label AS client_facing_status
    FROM UDW_CLIENTSOLUTIONS_CS.ORG_TIME_TRACKING t
        LEFT JOIN map_table m ON TRIM(m.DEPARTMENT) = TRIM(t.DEPARTMENT)
            AND TRIM(m.task) = TRIM(t.task)
            AND TRIM(m.category) = TRIM(t.category)
        LEFT JOIN leadership_table l ON l.role = t.role 
        LEFT JOIN client_facing_table c ON c.department = t.department
    WHERE 
        t.log_date BETWEEN CAST('2024-04-22' AS DATE) AND CAST('2024-05-17' AS DATE)  
);


WITH team_name_cte AS (
    SELECT 
        r.TEAM 
        ,COUNT(DISTINCT r.NAME || ' - ' || r.ROLE) AS team_size
        ,CASE WHEN COALESCE(r.TEAM, '') = '' THEN 'No Team' ELSE r.TEAM END || ' (' || team_size::VARCHAR || ')' AS team_size_name
    FROM report r
    GROUP BY 1
)

SELECT 
    r.*
    ,t.team_size_name
FROM report r
    LEFT JOIN team_name_cte t ON r.team = t.team
;



/************************************
START Story Report for George
************************************/

-- filter report data to sales cycle ONLY
DROP TABLE IF EXISTS report_filtered;
CREATE TEMP TABLE report_filtered AS (
    WITH percents_cte AS (
        SELECT 
            r.sales_cycle
            ,r.category
            ,SUM(r.minutes_active)/60 AS hours_active
        FROM report r
        WHERE   
            r.sales_cycle 
            -- NOT -- toggle (comment in/out) this "NOT" for alt report
            IN (
                'Pre and Post Sales'
                ,'Post Sales'
                ,'Pre Sales'
            )
            AND r.category <> 'PTO'
            -- AND r.client_facing_status = 'Client Facing' -- toggle (comment in/out) this value for alt report
        GROUP BY 1, 2
    )

    SELECT 
        p.sales_cycle
        ,p.category
        ,p.hours_active
        ,ROW_NUMBER() OVER(PARTITION BY p.sales_cycle, p.category ORDER BY p.hours_active DESC) AS row_number 
    FROM percents_cte p
);


-- get sales cycle hour and percent totals
DROP TABLE IF EXISTS report_row_header;
CREATE TEMP TABLE report_row_header AS (
    WITH percents_cte AS (
        SELECT 
            r.sales_cycle
            ,SUM(r.hours_active) AS hours_active
        FROM report_filtered r
        GROUP BY 1
    )

    SELECT 
        p.sales_cycle
        ,p.hours_active
        ,100 * RATIO_TO_REPORT(p.hours_active) OVER () AS percent_hours 
    FROM percents_cte p
);


-- get category data
DROP TABLE IF EXISTS category_data;
CREATE TEMP TABLE category_data AS (
    WITH categories_cte AS (
        SELECT 
            * 
            ,SUM(hours_active) OVER (PARTITION BY sales_cycle ORDER BY sales_cycle, hours_active DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_total
            ,SUM(hours_active) OVER (PARTITION BY sales_cycle) AS cycle_grand_total
            ,100 * (cumulative_total/cycle_grand_total) AS cumulative_percent
            ,CASE 
                WHEN cumulative_percent < 99 
                THEN category 
                ELSE 'All Other' 
            END AS report_category
        FROM report_filtered 
        ORDER BY
            sales_cycle
            ,hours_active DESC
    )
    
    -- adds 0 values for all sales_cycle and category combinations to avoid NULL values in pivot table
    ,zero_matrix_cte AS (
        SELECT DISTINCT 
            h.sales_cycle
            ,NULL AS category
            ,0 AS hours_active
            ,1 AS row_number
            ,0 AS cumulative_total
            ,0 AS cycle_grand_total
            ,0 AS cumulative_percent
            ,c.report_category  
        FROM categories_cte c
            CROSS JOIN report_row_header h
    )
    
   ,adjusted_categories_cte AS (
        SELECT * FROM categories_cte
        UNION 
        SELECT * FROM zero_matrix_cte
    )

    SELECT 
        *
        ,SUM(hours_active) OVER(PARTITION BY report_category) AS category_sum 
    FROM adjusted_categories_cte
 
);


-- get category headers and table data
DROP TABLE IF EXISTS category_headers;
CREATE TEMP TABLE category_headers AS (

    WITH category_headers_cte AS (
        SELECT 
            c.sales_cycle
            ,c.report_category
            ,c.cycle_grand_total
            ,c.category_sum
            ,SUM(c.hours_active) AS total_hours_active
        FROM category_data c
        GROUP BY 1, 2, 3, 4
    )

    ,totals_cte AS (
        SELECT 
            c.sales_cycle
            ,c.report_category
            ,c.total_hours_active
            ,SUM(c.total_hours_active) OVER() AS grand_total_hours
        FROM category_headers_cte c
        GROUP BY 1, 2, 3
    )
    
    ,category_sum_cte AS (
        SELECT DISTINCT
            h.report_category
            ,h.category_sum
        FROM category_headers_cte h
    )
    
    ,category_sort_cte AS(
        SELECT DISTINCT
            n.report_category
            ,n.category_sum
            ,ROW_NUMBER() OVER(ORDER BY n.category_sum DESC, n.report_category) AS category_sort_num
        FROM category_sum_cte n
    )
    
    SELECT 
        t.sales_cycle
        ,RIGHT('000' || s.category_sort_num::VARCHAR, 2) || ' - % ' || t.report_category AS report_category
        ,t.total_hours_active
        ,t.grand_total_hours
        ,100 * (t.total_hours_active/t.grand_total_hours) AS category_cycle_percent_hours
    FROM totals_cte t
    JOIN category_sort_cte s ON s.report_category = t.report_category
);


-- prep build of report data for output
DROP TABLE IF EXISTS report_output;
CREATE TEMP TABLE report_output AS (
    SELECT 
        r.sales_cycle
        ,r.percent_hours AS cycle_percent_hours
        ,c.report_category
        ,c.category_cycle_percent_hours
        ,r.hours_active AS cycle_hours_active
        ,c.total_hours_active AS category_cycle_hours_active
        ,c.grand_total_hours
    FROM report_row_header r
        JOIN category_headers c ON r.sales_cycle = c.sales_cycle
);


-- prep output for pivot
DROP TABLE IF EXISTS report_output_prep;
CREATE TEMP TABLE report_output_prep AS (
    SELECT 
        r.sales_cycle
        ,r.cycle_percent_hours
        ,r.report_category
        ,r.category_cycle_percent_hours
    FROM report_output r
);


-- pivot columns
DROP TABLE IF EXISTS report_output_pivot;
CREATE TEMP TABLE report_output_pivot AS (
    SELECT 
        *
    FROM report_output_prep
        PIVOT(
            SUM(category_cycle_percent_hours)
            FOR report_category IN (
                SELECT DISTINCT report_category 
                FROM report_output_prep
                ORDER BY report_category
            )
        )
);

-- final selection
SELECT * 
FROM report_output_pivot
ORDER BY cycle_percent_hours DESC;



/**
-- alt display with totals row
with total_cte AS ( 
select 'Total' AS sales_cycle,(SELECT SUM(percent_hours) from report_row_header) AS cycle_percent_hours, report_category, category_cycle_percent_hours  from report_output_prep group by 3, 4 --category_headers
)

SELECT * 
FROM report_output_pivot
union
 SELECT 
        *
    FROM total_cte
        PIVOT(
            SUM(category_cycle_percent_hours)
            FOR report_category IN (
                SELECT DISTINCT report_category 
                FROM report_output_prep
                ORDER BY report_category
            )
        )
       ORDER BY cycle_percent_hours ASC
;
**/