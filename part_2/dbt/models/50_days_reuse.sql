# this configuration allows us to rewrite the file when the querry is executed 

{{ config(
    materialized='table',
    post_hook="DROP TABLE IF EXISTS {{ this }}"
) }}


WITH core_launches AS (
    SELECT
        core,
        c.launch_id,
        CAST(l.date_utc AS TIMESTAMP) AS date_utc,
        ROW_NUMBER() OVER (PARTITION BY core ORDER BY l.date_utc) AS launch_order,
        LAG(CAST(l.date_utc AS TIMESTAMP)) OVER (PARTITION BY core ORDER BY l.date_utc) AS previous_launch_date
    FROM
        {{ source('dev', 'cores') }} c
        JOIN {{ source('dev', 'launches') }} l ON c.launch_id = l.launch_id
)
SELECT
    core,
    date_utc AS current_launch_date,
    previous_launch_date,
    DATEDIFF('day', previous_launch_date, date_utc) AS days_between
FROM
    core_launches
WHERE
    DATEDIFF('day', previous_launch_date, date_utc) < 50;