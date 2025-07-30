
-- Add the Hive SerDe library
ADD JAR /usr/lib/hive/lib/hive-serde-0.13.1-cdh5.2.0.jar;

-- Add the required OpenCSV dependency library
ADD JAR /usr/lib/hive/lib/opencsv-2.3.jar;

SELECT
    country,
    COUNT(1) AS comedy_count
FROM
    netflix_data
WHERE
    type = 'Movie'
    AND listed_in LIKE '%Comedies%'
    AND country IS NOT NULL
GROUP BY
    country
ORDER BY
    comedy_count DESC
LIMIT 1;
