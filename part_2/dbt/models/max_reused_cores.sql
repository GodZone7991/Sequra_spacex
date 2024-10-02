# this configuration allows us to rewrite the file when the querry is executed 


{{ config(
    materialized='table',
    post_hook="DROP TABLE IF EXISTS {{ this }}"
) }}


SELECT
    core,
    COUNT(*) AS reuse_count
FROM
    {{ source('dev', 'cores') }}
GROUP BY
    core
ORDER BY
    reuse_count DESC
LIMIT 1;