SELECT
    core,
    COUNT(*) AS reuse_count
FROM
    "dev"."public"."cores"
GROUP BY
    core
ORDER BY
    reuse_count DESC
LIMIT 1;