-- CREATE OR REPLACE TABLE `x-marketing.3x.db_consolidation_content_analytics` AS
TRUNCATE TABLE `x-marketing.3x.db_consolidation_content_analytics`;
INSERT INTO `x-marketing.3x.db_consolidation_content_analytics`
(
  _contentitem,
  _contenttype,
  _gatingstrategy,
  _homeurl,
  _summary,
  _status,
  _buyerstage,
  _vertical,
  _persona,
  _engagement
)
WITH content_inventory AS (
  SELECT
    _contentitem,
    _contenttype,
    _gatingstrategy,
    _homeurl,
    _summary,
    _status,
    _buyerstage,
    _vertical,
    _persona,
  FROM `x-marketing.x_mysql.db_airtable_3x_content_inventory` 
),
email AS (
  SELECT
    _homeurl,
    CONCAT("Email ", INITCAP(_engagement)) AS _engagement
  FROM `x-marketing.3x.email_content_analytics` 
),
-- ads AS (
--   SELECT
--     _homeurl,
--     'Ads' AS _engagement
--   FROM `x-marketing.3x.db_ads_content_analytics`
-- ),
web AS (
  SELECT
    _homeurl,
    'Web' AS _engagement
  FROM `x-marketing.3x.db_web_content_analytics`
)

SELECT 
  content_inventory.*,
  CASE
    WHEN _engagement != ''
    THEN _engagement
    ELSE CAST(NULL AS STRING)
  END AS _engagement
FROM content_inventory
LEFT JOIN (
  SELECT
    *
  FROM email
  UNION ALL
  -- SELECT
  --   *
  -- FROM ads
  -- UNION ALL
  SELECT
    *
  FROM web 
) engagement
ON engagement._homeurl = content_inventory._homeurl



















