-- Keyword Related
CREATE OR REPLACE TABLE `x-marketing.blend360.db_email_alert_keywords` AS
WITH split_strings_to_arrays AS (
SELECT  
    CASE
        WHEN _extractdate = "" THEN NULL
        WHEN _extractdate LIKE '%/%'
        THEN PARSE_DATE('%m/%e/%Y', _extractdate) 
        ELSE PARSE_DATE('%F', _extractdate) 
    END AS extract_date,
    _timeframe AS timeframe,
    _segmentname AS segment_name,
    SPLIT(_accountname, ';')[ORDINAL(1)] AS account_name,
    IF(_accountname LIKE '%6QA%', 'Yes', 'No') AS has_6QA,
    _country AS country,
    _domain AS domain,
    _accountreach AS account_reach,
    _buyingstage AS buying_stage,
    _profilefit AS profile_fit,
    SPLIT(TRIM(_bomboracompanysurgetopics), ',') AS bomboracompanysurgetopics ,
    _keywords AS _keywords,
FROM `x-marketing.blend360_mysql.db_6sense_impact` 
),

unnest_arrays AS (
SELECT 
    * EXCEPT (bomboracompanysurgetopics, _bomboracompanysurgetopics), 
    TRIM (_bomboracompanysurgetopics) AS _bomboracompanysurgetopics 
FROM split_strings_to_arrays 
CROSS JOIN UNNEST(bomboracompanysurgetopics) AS _bomboracompanysurgetopics
),

nest_bombora AS(  
SELECT
    * EXCEPT(_bomboracompanysurgetopics),
    ARRAY_AGG(_bomboracompanysurgetopics) AS _bomboracompanysurgetopics 
FROM unnest_arrays
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
), 

keyword_array AS (
SELECT 
    * EXCEPT (_keywords),
    SPLIT(_keywords, ', ') AS _keywords,
FROM nest_bombora
), 

keyword_unnest_array AS (
SELECT
    * EXCEPT(_keywords ),
FROM keyword_array,
UNNEST(_keywords) AS keywords
),

split_keyword_and_score AS (
SELECT
    * EXCEPT(keywords),
    SPLIT(INITCAP(REGEXP_REPLACE(keywords, r'\s+', ' ')) , ' (')[ORDINAL(1)] AS keyword,
    CAST(REGEXP_EXTRACT(keywords, r'\((\d+)\)') AS INT64) AS keyword_count
FROM keyword_unnest_array 
)
SELECT 
    * EXCEPT (keyword),
    REPLACE(keyword, '"', '') AS keyword
FROM split_keyword_and_score;

CREATE OR REPLACE TABLE `x-marketing.blend360.db_email_alert_bombora` AS
WITH split_strings_to_arrays AS (
SELECT  
    CASE
        WHEN _extractdate = "" THEN NULL
        WHEN _extractdate LIKE '%/%'
        THEN PARSE_DATE('%m/%e/%Y', _extractdate) 
        ELSE PARSE_DATE('%F', _extractdate) 
    END AS extract_date,
    _timeframe AS timeframe,
    _segmentname AS segment_name,
    SPLIT(_accountname, ';')[ORDINAL(1)] AS account_name,
    IF(_accountname LIKE '%6QA%', 'Yes', 'No') AS has_6QA,
    _country AS country,
    _domain AS domain,
    _accountreach AS account_reach,
    _buyingstage AS buying_stage,
    _profilefit AS profile_fit,
    _bomboracompanysurgetopics,
    SPLIT(TRIM(_bomboracompanysurgetopics), ',') AS bomboracompanysurgetopics ,
    _keywords AS _keywords,
FROM `x-marketing.blend360_mysql.db_6sense_impact` 
)
SELECT 
    * EXCEPT (bomboracompanysurgetopics,_bomboracompanysurgetopics ), 
    TRIM (_bomboracompanysurgetopics ) AS _bomboracompanysurgetopics 
FROM split_strings_to_arrays,
UNNEST(bomboracompanysurgetopics ) AS _bomboracompanysurgetopics;

-- Website URLs Related
CREATE OR REPLACE TABLE `x-marketing.blend360.db_email_alert_landingpages` AS
WITH split_strings_to_arrays AS (
SELECT  
    CASE
        WHEN _extractdate LIKE '%/%'
        THEN PARSE_DATE('%m/%e/%Y', _extractdate) 
        ELSE PARSE_DATE('%F', _extractdate) 
    END AS extract_date,
    _timeframe AS timeframe,
    _segmentname AS segment_name,
    SPLIT(_accountname, ';')[ORDINAL(1)] AS account_name,
    IF(_accountname LIKE '%6QA%', 'Yes', 'No') AS has_6QA,
    _country AS country,
    _domain AS domain,
    _accountreach AS account_reach,
    _buyingstage AS buying_stage,
    _profilefit AS profile_fit,
    CAST(REGEXP_EXTRACT(_weburls, r'\((\d+)\)') AS INT64)  AS _web_urls_count,
    CASE
        WHEN _webvisitcount != '' THEN CAST(_webvisitcount AS INT) 
        ELSE NULL
    END AS web_visit_count,
    CASE
        WHEN _webvisitknowncontactcount != '' THEN CAST(_webvisitknowncontactcount AS INT) 
        ELSE NULL
    END AS known_contact_count,
    CASE
        WHEN _keywordanonymoususerscount != '' THEN CAST(_keywordanonymoususerscount AS INT) 
        ELSE NULL
    END AS anonymous_count,
    REGEXP_REPLACE(_weburls, r' \(\d+\)', '')_weburls,
    CASE
        WHEN _weburls IS NOT NULL THEN SPLIT( _weburls, ', ') 
        ELSE SPLIT( _weburls, ', ') 
    END AS _web,
    CASE
        WHEN _weburls IS NOT NULL THEN SPLIT( REGEXP_REPLACE(_weburls, r' \(\d+\)', ''), ', ') 
        ELSE SPLIT( REGEXP_REPLACE(_weburls, r' \(\d+\)', ''), ', ') 
    END AS _webpages,
    CASE
        WHEN _weburls IS NOT NULL THEN 'Raw Web URLs'
        ELSE 'Clean Web URLs' 
    END AS _url_source
FROM `x-marketing.blend360_mysql.db_6sense_impact`
WHERE _weburls != ''

),

unnest_arrays AS (
SELECT
    * EXCEPT(_webpages),
FROM split_strings_to_arrays,
UNNEST(_webpages) AS webpages
),

clean_urls AS (
SELECT
    * EXCEPT(webpages, _url_source),
    CASE 
        WHEN _url_source = 'Raw Web URLs' 
        THEN
            SPLIT(
                SPLIT(
                    REPLACE(
                        REPLACE(
                            REPLACE(
                                REPLACE(
                                    REPLACE(
                                        REPLACE(
                                            webpages, 
                                            '%3D', '='
                                        ), 
                                        '%3A', ':'
                                    ), 
                                    '%2F', '/'
                                ),
                                '%25', '%'
                            ),
                            '%20', ' '
                        ),
                        '%26', '&'
                    ),
                    'redirect='
                )[ORDINAL(1)], 
                '&event='
            )[ORDINAL(1)] 
        ELSE SPLIT(webpages, ' (')[ORDINAL(1)] 
    END AS page_url
FROM unnest_arrays
)
SELECT * FROM clean_urls;