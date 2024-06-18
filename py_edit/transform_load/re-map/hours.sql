



DROP TABLE IF EXISTS report_time;
CREATE TEMP TABLE report_time AS (
    WITH department_name_cte AS (
        SELECT 
            r.department
            ,COUNT(DISTINCT r.NAME || ' - ' || r.ROLE) AS department_size
            ,CASE WHEN COALESCE(r.department, '') = '' THEN 'No Department' ELSE r.department END || ' (' || department_size::VARCHAR || ')' AS department_size_name
        FROM report r
        GROUP BY 1
    )

    ,base_cte AS (
        SELECT 
            r.department
            ,t.department_size
            ,SUM(r.MINUTES_ACTIVE)/60.00 AS department_total_hours
            ,department_total_hours/t.department_size AS department_average_hours_per_member
        FROM report r
            LEFT JOIN department_name_cte t ON r.department = t.department
        WHERE 
            r.category <> 'PTO'
        GROUP BY 1, 2   
    )


    SELECT 
        b.* 
        ,(SELECT SUM(MINUTES_ACTIVE)/60 FROM report r WHERE r.department = b.department AND r.category <> 'PTO' AND r.sales_cycle = 'Pre Sales') AS department_presales_cycle_hours
        ,department_presales_cycle_hours/b.department_size AS avg_member_presales_cycle_hours
        ,department_presales_cycle_hours/4 AS department_presales_cycle_hours_weekly
        ,avg_member_presales_cycle_hours/4 AS avg_member_presales_cycle_hours_weekly
        ,department_presales_cycle_hours/20 AS department_presales_cycle_hours_daily
        ,avg_member_presales_cycle_hours/20 AS avg_member_presales_cycle_hours_daily


        ,(SELECT SUM(MINUTES_ACTIVE)/60 FROM report r WHERE r.department = b.department AND r.category <> 'PTO' AND r.sales_cycle = 'Pre and Post Sales') AS department_pre_and_post_sales_cycle_hours
        ,department_pre_and_post_sales_cycle_hours/b.department_size AS avg_member_pre_and_post_sales_cycle_hours
        ,department_pre_and_post_sales_cycle_hours/4 AS department_pre_and_post_sales_cycle_hours_weekly
        ,avg_member_pre_and_post_sales_cycle_hours/4 AS avg_member_pre_and_post_sales_cycle_hours_weekly
        ,department_pre_and_post_sales_cycle_hours/20 AS department_pre_and_post_sales_cycle_hours_daily
        ,avg_member_pre_and_post_sales_cycle_hours/20 AS avg_member_pre_and_post_sales_cycle_hours_daily


        ,(SELECT SUM(MINUTES_ACTIVE)/60 FROM report r WHERE r.department = b.department AND r.category <> 'PTO' AND r.sales_cycle = 'Post Sales') AS department_postsales_cycle_hours
        ,department_postsales_cycle_hours/b.department_size AS avg_member_postsales_cycle_hours
        ,department_postsales_cycle_hours/4 AS department_postsales_cycle_hours_weekly
        ,avg_member_postsales_cycle_hours/4 AS avg_member_postsales_cycle_hours_weekly
        ,department_postsales_cycle_hours/20 AS department_postsales_cycle_hours_daily
        ,avg_member_postsales_cycle_hours/20 AS avg_member_postsales_cycle_hours_daily


        ,(SELECT SUM(MINUTES_ACTIVE)/60 FROM report r WHERE r.department = b.department AND r.category <> 'PTO' AND r.sales_cycle NOT IN('Pre Sales', 'Pre and Post Sales', 'Post Sales')) AS department_na_cycle_hours
        ,department_na_cycle_hours/b.department_size AS avg_member_na_cycle_hours
        ,department_na_cycle_hours/4 AS department_na_cycle_hours_weekly
        ,avg_member_na_cycle_hours/4 AS avg_member_na_cycle_hours_weekly
        ,department_na_cycle_hours/20 AS department_na_cycle_hours_daily
        ,avg_member_na_cycle_hours/20 AS avg_member_na_cycle_hours_daily

    FROM base_cte b
    ORDER BY department_size DESC

);



SELECT 
    DEPARTMENT
    ,DEPARTMENT_SIZE
    ,DEPARTMENT_TOTAL_HOURS
    ,DEPARTMENT_AVERAGE_HOURS_PER_MEMBER

    ,DEPARTMENT_PRESALES_CYCLE_HOURS
    ,DEPARTMENT_PRE_AND_POST_SALES_CYCLE_HOURS
    ,DEPARTMENT_POSTSALES_CYCLE_HOURS
    ,DEPARTMENT_NA_CYCLE_HOURS

    ,DEPARTMENT_PRESALES_CYCLE_HOURS_WEEKLY
    ,DEPARTMENT_PRE_AND_POST_SALES_CYCLE_HOURS_WEEKLY
    ,DEPARTMENT_POSTSALES_CYCLE_HOURS_WEEKLY
    ,DEPARTMENT_NA_CYCLE_HOURS_WEEKLY

    ,DEPARTMENT_PRESALES_CYCLE_HOURS_DAILY
    ,DEPARTMENT_PRE_AND_POST_SALES_CYCLE_HOURS_DAILY
    ,DEPARTMENT_POSTSALES_CYCLE_HOURS_DAILY
    ,DEPARTMENT_NA_CYCLE_HOURS_DAILY
FROM report_time;



SELECT 
    DEPARTMENT
    ,DEPARTMENT_SIZE
    ,DEPARTMENT_TOTAL_HOURS
    ,DEPARTMENT_AVERAGE_HOURS_PER_MEMBER

    ,AVG_MEMBER_PRESALES_CYCLE_HOURS
    ,AVG_MEMBER_PRE_AND_POST_SALES_CYCLE_HOURS
    ,AVG_MEMBER_POSTSALES_CYCLE_HOURS
    ,AVG_MEMBER_NA_CYCLE_HOURS

    ,AVG_MEMBER_PRESALES_CYCLE_HOURS_WEEKLY
    ,AVG_MEMBER_PRE_AND_POST_SALES_CYCLE_HOURS_WEEKLY
    ,AVG_MEMBER_POSTSALES_CYCLE_HOURS_WEEKLY
    ,AVG_MEMBER_NA_CYCLE_HOURS_WEEKLY

    ,AVG_MEMBER_PRESALES_CYCLE_HOURS_DAILY
    ,AVG_MEMBER_PRE_AND_POST_SALES_CYCLE_HOURS_DAILY
    ,AVG_MEMBER_POSTSALES_CYCLE_HOURS_DAILY
    ,AVG_MEMBER_NA_CYCLE_HOURS_DAILY
FROM report_time;



