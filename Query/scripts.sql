## BASIC VALIDATION AND DATA checksum table
# 1- Total rows per table 
SELECT 'titles' , COUNT(*) FROM titles
UNION ALL
SELECT 'credits',COUNT(*) FROM credits
UNION ALL
SELECT 'movies', COUNT(*) FROM movies
UNION ALL
SELECT 'shows',COUNT(*) FROM shows; 
# 2- NULL/MISSING count per cols
SELECT 
    SUM(CASE WHEN title IS NULL OR TRIM(title)='' THEN 1 ELSE 0 END),
    SUM(CASE WHEN imdb_score IS NULL THEN 1 ELSE 0 END) AS missing_score,
    SUM(CASE WHEN imdb_votes IS NULL THEN 1 ELSE 0 END) AS missing_votes
FROM titles;
### KPI /AGGREGATION queries
# 3- Total titles ,avg score ,aveg votes
SELECT 
COUNT(*) AS total_titles,
ROUND(AVG(imdb_score),2) AS AVG_SCORE,
ROUND(AVG(imdb_votes),0) AS AVG_VOTES
FROM titles;
# Movies/Shows per production country (top 10)
SELECT MAIN_PRODUCTION AS country,COUNT(*) AS CNT
FROM (
   SELECT MAIN_PRODUCTION FROM movies
   UNION ALL
   SELECT MAIN_PRODUCTION FROM shows
) t
GROUP BY MAIN_PRODUCTION
ORDER BY CNT DESC
LIMIT 10;
# Average score by genre
SELECT genres AS MAIN_GENRE,ROUND(AVG(imdb_score),2) AS avg_score,COUNT(*) AS CNT
FROM titles 
GROUP BY MAIN_GENRE
HAVING CNT>10
ORDER BY avg_score DESC;
#Releases per year (trend)
SELECT release_year,COUNT(*) AS total_releases
FROM titles
GROUP BY release_year
ORDER BY release_year;
###JOIN BASED ANALYSIS
#7 Top actors by number of credits(actors only)
SELECT c.name,
       COUNT(*) AS roles
FROM credits c
WHERE LOWER(c.role) LIKE '%actor%' OR LOWER(c.role)='actor'
GROUP BY c.name
ORDER BY roles DESC
LIMIT 20;
#Top titles by average cast size (how many unique people)
SELECT t.title, COUNT(DISTINCT c.person_id) AS cast_size
FROM titles t
JOIN credits c ON t.id = c.title_id
GROUP BY t.title
ORDER BY cast_size DESC
LIMIT 20;
#Country vs avg rating using credits volume
SELECT t.production_countries AS country, 
       ROUND(AVG(t.imdb_score),2) AS avg_rating,
       COUNT(c.person_id) AS total_credits
FROM titles t
LEFT JOIN credits c ON t.id = c.title_id
GROUP BY t.production_countries
ORDER BY avg_rating DESC;
###Titles with highest avg actor rating (example using actors' film counts)
WITH actor_counts AS (
  SELECT name, COUNT(*) AS cnt FROM credits WHERE LOWER(role) LIKE '%actor%' GROUP BY name
)
SELECT t.title, ROUND(AVG(t.imdb_score),2) as avg_score, COUNT(c.person_id) as cast_count
FROM titles t
JOIN credits c ON t.id = c.title_id
GROUP BY t.title
ORDER BY avg_score DESC
LIMIT 20;
####Advanced analytics, window functions, ranking
# 11. Top N per genre (useful for Top lists)
WITH ranked AS (
  SELECT title, genres, imdb_score,
         ROW_NUMBER() OVER (PARTITION BY genres ORDER BY imdb_score DESC) AS rn
  FROM titles
)
SELECT * FROM ranked WHERE rn <= 5 ORDER BY genres, imdb_score DESC;
# 12.Rolling average score per year (3-year window)
SELECT release_year,
       ROUND(AVG(avg_score) OVER (ORDER BY release_year ROWS BETWEEN 2 PRECEDING AND CURRENT ROW),2) AS rolling_avg
FROM (
  SELECT release_year, AVG(imdb_score) AS avg_score FROM titles GROUP BY release_year
) t
ORDER BY release_year;
# 13.Growth % year-on-year in release counts
WITH yearly AS (
  SELECT release_year, COUNT(*) AS cnt FROM titles GROUP BY release_year
)
SELECT y1.release_year,
       y1.cnt,
       ROUND(((y1.cnt - COALESCE(y2.cnt,0)) / NULLIF(COALESCE(y2.cnt,0),0)) * 100,2) AS pct_change
FROM yearly y1
LEFT JOIN yearly y2 ON y2.release_year = y1.release_year - 1
ORDER BY y1.release_year;
# Data quality & staging queries (useful to demonstrate ETL rigour)
# 14.Find titles in credits but missing in titles table (data integrity)
SELECT DISTINCT c.title_id
FROM credits c
LEFT JOIN titles t ON c.title_id = t.id
WHERE t.id IS NULL
LIMIT 10;
#15.Duplicate names or suspicious duplicates in titles
SELECT title, COUNT(*) AS cnt
FROM titles
GROUP BY title
HAVING cnt > 1
ORDER BY cnt DESC;
###  Business KPIs / metric queries for dashboard cards
# 16. % of high-rated titles (score >= 8)
SELECT 
  SUM(CASE WHEN imdb_score >= 8 THEN 1 ELSE 0 END) AS high_rated,
  COUNT(*) AS total,
  ROUND(SUM(CASE WHEN imdb_score >= 8 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pct_high
FROM titles;
# Distribution across score buckets (Good / Avg / Low)
SELECT CASE 
         WHEN imdb_score >= 8 THEN 'High (>=8)'
         WHEN imdb_score >= 6 THEN 'Average (6-7.9)'
         ELSE 'Low (<6)'
       END AS bucket, COUNT(*) AS cnt
FROM titles
GROUP BY bucket;
### Recommendation / user-style analytics
# Frequently co-appearing actors (simple pair counting)
SELECT a.person_id AS actor1, b.person_id AS actor2, COUNT(*) AS common_titles
FROM credits a
JOIN credits b ON a.id = b.id AND a.person_id < b.person_id
WHERE LOWER(a.role) LIKE '%actor%' AND LOWER(b.role) LIKE '%actor%'
GROUP BY actor1, actor2
HAVING common_titles >= 1
ORDER BY common_titles DESC
LIMIT 4;
###Performance & production practices (Views, Indexes, Materialized)
# 19.Create a view for dashboard-ready titles summary
CREATE OR REPLACE VIEW vw_titles_summary AS
SELECT id, title, release_year, imdb_score, imdb_votes, genres, production_countries
FROM titles;
# 20.Create index recommendations (run once)
CREATE INDEX idx_titles_release_year ON titles(release_year);
CREATE INDEX idx_titles_genre ON titles(geners);
CREATE INDEX idx_credits_id ON credits(title_id);
CREATE INDEX idx_credits_person ON credits(person_id);
# 21.Materialized-like table (pre-aggregated) — create a summary table for heavy metrics
CREATE TABLE agg_year_genre AS
SELECT release_year, genres, COUNT(*) AS cnt, ROUND(AVG(imdb_score),2) AS avg_score
FROM titles
GROUP BY release_year, genres;
### Example stored procedure for common repeated refresh
# 22.Simple stored procedure to refresh aggregated table
DELIMITER //
CREATE PROCEDURE refresh_agg_year_genre()
BEGIN
  TRUNCATE TABLE agg_year_genre;
  INSERT INTO agg_year_genre
  SELECT release_year, MAIN_GENRE, COUNT(*) AS cnt, ROUND(AVG(imdb_score),2) AS avg_score
  FROM titles
  GROUP BY release_year, MAIN_GENRE;
END //
DELIMITER ;
###DVANCED PRODUCTION-LEVEL QUERIES
# 23.Top 10 directors with most high-rated titles
WITH expanded AS (
  SELECT 
    TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(production_countries, ',', n.n), ',', -1)) AS country,
    TRIM(SUBSTRING_INDEX(SUBSTRING_INDEX(genres, ',', m.n), ',', -1)) AS genre
  FROM titles
  JOIN (
    SELECT 1 n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
  ) n
  JOIN (
    SELECT 1 n UNION SELECT 2 UNION SELECT 3 UNION SELECT 4 UNION SELECT 5
  ) m
  ON 1
  WHERE production_countries IS NOT NULL AND genres IS NOT NULL
),
genre_counts AS (
  SELECT 
    country,
    genre,
    COUNT(*) AS total_titles,
    RANK() OVER (PARTITION BY country ORDER BY COUNT(*) DESC) AS rnk
  FROM expanded
  WHERE country <> '' AND genre <> ''
  GROUP BY country, genre
)
SELECT * 
FROM genre_counts 
WHERE rnk = 1
ORDER BY country;
# 25. Longest movies (duration > 150 mins)
SELECT TITLE, DURATION, SCORE
FROM movies
WHERE DURATION > 150
ORDER BY DURATION DESC
LIMIT 15;
#  Shortest shows (≤1 season)
SELECT 	TITLE, NUMBER_OF_SEASONS, SCORE
FROM shows
WHERE NUMBER_OF_SEASONS <= 1
ORDER BY SCORE DESC;
# 27. Country with highest average IMDb score
SELECT production_countries, ROUND(AVG(imdb_score),2) AS avg_score
FROM titles
GROUP BY production_countries
ORDER BY avg_score DESC
LIMIT 5;
#28. Correlation check: Duration vs IMDb score (for movies)
SELECT ROUND(AVG(DURATION),2) AS avg_duration,
       ROUND(AVG(SCORE),2) AS avg_score,
       ROUND(AVG(DURATION * SCORE) - AVG(DURATION)*AVG(SCORE),2) AS covar
FROM movies;
#29. Total releases by decade
SELECT CONCAT(FLOOR(release_year/10)*10, 's') AS decade, COUNT(*) AS total
FROM titles
GROUP BY decade
ORDER BY decade;
##30. Genres popular over decades
SELECT CONCAT(FLOOR(release_year/10)*10, 's') AS decade,
       genres,
       COUNT(*) AS total
FROM titles
GROUP BY decade, genres
ORDER BY decade, total DESC;
#31. Average IMDb score per decade
SELECT CONCAT(FLOOR(release_year/10)*10, 's') AS decade,
       ROUND(AVG(imdb_score),2) AS avg_score
FROM titles
GROUP BY decade
ORDER BY decade;
#32. Actor-country collaboration count
SELECT c.name AS actor, t.production_countries, COUNT(*) AS appearances
FROM credits c
JOIN titles t ON c.title_id = t.id
WHERE LOWER(c.role) LIKE '%actor%'
GROUP BY c.name, t.production_countries
ORDER BY appearances DESC
LIMIT 20;
#33. Total number of unique people per title
SELECT t.title, COUNT(DISTINCT c.person_id) AS total_people
FROM titles t
JOIN credits c ON t.id = c.title_id
GROUP BY t.title
ORDER BY total_people DESC
LIMIT 20;
#34. Detect potential outliers in IMDb votes
SELECT title, imdb_votes, imdb_score
FROM titles
WHERE imdb_votes > (SELECT AVG(imdb_votes)*3 FROM titles);
#35. Genre share percentage by year
SELECT release_year, genres,
       ROUND((COUNT(*)*100.0 / SUM(COUNT(*)) OVER (PARTITION BY release_year)),2) AS genre_share
FROM titles
GROUP BY release_year, genres
ORDER BY release_year, genre_share DESC;
#36. Rating bucket by genre
SELECT genres,
       SUM(CASE WHEN imdb_score >= 8 THEN 1 ELSE 0 END) AS high,
       SUM(CASE WHEN imdb_score BETWEEN 6 AND 8 THEN 1 ELSE 0 END) AS medium,
       SUM(CASE WHEN imdb_score < 6 THEN 1 ELSE 0 END) AS low
FROM titles
GROUP BY genres;
## 37. Identify genres with declining scores
WITH yearly AS (
  SELECT genres, release_year, ROUND(AVG(imdb_score),2) AS avg_score
  FROM titles
  GROUP BY genres, release_year
)
SELECT genres, MIN(avg_score) AS min_score, MAX(avg_score) AS max_score,
       (MAX(avg_score) - MIN(avg_score)) AS score_change
FROM yearly
GROUP BY genres
ORDER BY score_change ASC;
#38. Movies vs Shows — average score comparison
SELECT 'Movies' AS category, ROUND(AVG(SCORE),2) AS avg_score FROM movies
UNION ALL
SELECT 'Shows', ROUND(AVG(SCORE),2) FROM shows;
#39. Most active production houses
SELECT production_countries, COUNT(*) AS total_titles
FROM titles
GROUP BY production_countries
ORDER BY total_titles DESC
LIMIT 15;
#40. Most common pair of actor–director
SELECT a.name AS actor, d.name AS director, COUNT(*) AS count_together
FROM credits a
JOIN credits d ON a.title_id = d.title_id
WHERE LOWER(a.role) LIKE '%actor%' AND LOWER(d.role) LIKE '%director%'
GROUP BY a.name, d.name
HAVING COUNT(*) > 2
ORDER BY count_together DESC
LIMIT 15;
#41. Highest-rated title per country
SELECT t1.*
FROM titles t1
JOIN (
  SELECT production_countries, MAX(imdb_score) AS max_score
  FROM titles
  GROUP BY production_countries
) t2 ON t1.production_countries = t2.production_countries AND t1.imdb_score = t2.max_score;
# 42. Average votes by score bucket
SELECT CASE 
         WHEN imdb_score >= 8 THEN 'High'
         WHEN imdb_score BETWEEN 6 AND 8 THEN 'Medium'
         ELSE 'Low' END AS bucket,
       ROUND(AVG(imdb_votes),0) AS avg_votes
FROM titles
GROUP BY bucket;
#44. Average cast size by genre
SELECT t.genres, AVG(cast_count) AS avg_cast
FROM (
  SELECT t.id, COUNT(DISTINCT c.person_id) AS cast_count
  FROM titles t
  JOIN credits c ON t.id = c.title_id
  GROUP BY t.id
) sub
JOIN titles t ON t.id = sub.id
GROUP BY t.genres;
#45. Identify countries producing most high-rated shows
#SELECT MAIN_PRODUCTION, COUNT(*) AS high_rated_shows
#FROM shows
#WHERE SCORE >= 8
#GROUP BY MAIN_PRODUCTION
#ORDER BY high_rated_shows DESC;
#46. Yearly IMDb votes total
SELECT release_year, SUM(imdb_votes) AS total_votes
FROM titles
GROUP BY release_year
ORDER BY release_year;
#47. Actor with highest average title score
SELECT c.name, ROUND(AVG(t.imdb_score),2) AS avg_actor_score
FROM credits c
JOIN titles t ON c.title_id = t.id
WHERE LOWER(c.role) LIKE '%actor%'
GROUP BY c.name
HAVING COUNT(*) >= 3
ORDER BY avg_actor_score DESC
LIMIT 10;
#48. Compare votes and score (outlier detection)
#SELECT title, imdb_score, imdb_votes
#FROM titles
#WHERE imdb_votes > 1000 AND imdb_score < 6
#ORDER BY imdb_votes DESC;
#49. Number of multi-country collaborations
SELECT COUNT(*) AS multi_country_projects
FROM titles
WHERE production_countries LIKE '%,%';
#50. Identify inactive genres (no release in last 5 years)
SELECT DISTINCT genres
FROM titles
WHERE genres NOT IN (
  SELECT DISTINCT MAIN_GENRE FROM titles WHERE release_year >= 2020
);

