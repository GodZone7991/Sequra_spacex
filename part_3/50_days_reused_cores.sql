WITH core_launches AS (
    SELECT
        core,
        c.launch_id,
        CAST(date_utc AS TIMESTAMP) AS date_utc,  
        -- itirate over the cores with the same id to get the order of the launches
        ROW_NUMBER() OVER (PARTITION BY core ORDER BY date_utc) AS launch_order
    FROM
        "dev"."public"."cores" c
        JOIN "dev"."public"."launches" l ON c.launch_id = l.launch_id
),
-- select the cores with the current and previous launch dates
core_launch_diffs AS (
    SELECT
        cl1.core,
        cl1.date_utc AS current_launch_date,
        cl2.date_utc AS previous_launch_date,
        DATEDIFF('day', cl2.date_utc, cl1.date_utc) AS days_between
    FROM
        core_launches cl1
        -- ensure we join on records where launch is joined with the one immidiately before it 
        JOIN core_launches cl2 ON cl1.core = cl2.core AND cl1.launch_order = cl2.launch_order + 1
)
SELECT
    core,
    current_launch_date,
    previous_launch_date,
    days_between
FROM
    core_launch_diffs
WHERE
    days_between < 50;