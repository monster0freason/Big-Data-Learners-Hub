
-- Add the Hive SerDe library
ADD JAR /usr/lib/hive/lib/hive-serde-0.13.1-cdh5.2.0.jar;

-- Add the required OpenCSV dependency library
ADD JAR /usr/lib/hive/lib/opencsv-2.3.jar;

SELECT
    genre,
    COUNT(1) AS genre_count
FROM
    netflix_data
LATERAL VIEW
    explode(split(listed_in, ', ')) genre_list AS genre
WHERE
    type = 'Movie'
    AND duration LIKE '% min'
    AND CAST(split(duration, ' ')[0] AS INT) > 60
GROUP BY
    genre
ORDER BY
    genre_count DESC
LIMIT 1;