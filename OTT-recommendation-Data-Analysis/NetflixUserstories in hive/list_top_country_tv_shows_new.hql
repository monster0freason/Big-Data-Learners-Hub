
-- Add the Hive SerDe library
ADD JAR /usr/lib/hive/lib/hive-serde-0.13.1-cdh5.2.0.jar;

-- Add the required OpenCSV dependency library
ADD JAR /usr/lib/hive/lib/opencsv-2.3.jar;

-- Step 1: Create a CTE to first get the count of shows for each country
WITH CountryCounts AS (
  SELECT
    country,
    COUNT(DISTINCT show_id) AS show_count
  FROM
    netflix_data
  WHERE
    type = 'TV Show' AND country IS NOT NULL
  GROUP BY
    country
),
-- Step 2: Use a second CTE to rank the results from the first one
RankedCountries AS (
  SELECT
    country,
    ROW_NUMBER() OVER (ORDER BY show_count DESC) as rn
  FROM
    CountryCounts
)
-- Step 3: Now, join the original table with the final ranked list
SELECT
  t.show_id,
  t.title,
  t.description
FROM
  netflix_data t
JOIN
  (SELECT country FROM RankedCountries WHERE rn = 1) top_country
ON
  t.country = top_country.country
WHERE
  t.type = 'TV Show';
