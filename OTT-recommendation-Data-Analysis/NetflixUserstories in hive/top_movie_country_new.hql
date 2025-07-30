
-- Add the Hive SerDe library
ADD JAR /usr/lib/hive/lib/hive-serde-0.13.1-cdh5.2.0.jar;

-- Add the required OpenCSV dependency library
ADD JAR /usr/lib/hive/lib/opencsv-2.3.jar;

SELECT
    -- Trim the country name directly in the SELECT statement
    TRIM(single_country) AS country_name,
    COUNT(1) AS movie_count
FROM
    netflix_data
-- Use one LATERAL VIEW to explode the countries
LATERAL VIEW
    explode(split(country, ',')) country_list AS single_country
WHERE
    type = 'Movie' AND TRIM(single_country) != ''
-- Group by the trimmed country name
GROUP BY
    TRIM(single_country)
ORDER BY
    movie_count DESC
LIMIT 1;
