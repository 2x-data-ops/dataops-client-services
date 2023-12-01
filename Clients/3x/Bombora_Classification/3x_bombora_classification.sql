CREATE OR REPLACE TABLE `3x.db_bombora_classification` AS 
WITH bombora_data AS (

    SELECT DISTINCT

        EXTRACT(DATE FROM _timestamp) AS _date,
        _domain,
        _companyname,
        _companysize,
        _companyrevenue,
        _industry,
        _compositescore,
        _topicname,
        _topicid,
        CASE WHEN _compositescore IS NULL THEN 0 ELSE CAST(_compositescore AS INT64) END AS _compositescore_num,

    FROM 
        `x_mysql.db_3x_everyone_under_the_sun`
)
,

-- Get all classification fields
classification_info AS (

    SELECT

        bubble AS _bubble,
        category AS _category,
        niche_bubble AS _niche,
        topic AS _topic,
        CAST(topic_id AS STRING) AS topicid

    FROM 
        `x_google_sheets.Bombora_EUTS`

), ---combine row data with topic classification 
all_data_combine AS (
SELECT bombora_data.*,
classification_info.* EXCEPT (topicid,_topic)
 FROM bombora_data 
LEFT JOIN classification_info ON bombora_data._topicid = classification_info.topicid
), -- Combine all information together
combined_data AS (
SELECT *,
COUNT(
            DISTINCT 
                CASE 
                    WHEN _bubble = 'Everyone Under the Sun' 
                    THEN _topicname
                    ELSE NULL 
                END 
        ) OVER(
            PARTITION BY 
                _date,
                _domain
        )
        AS _euts_count,
                COUNT(
            DISTINCT 
                CASE 
                    WHEN _bubble != 'Everyone Under the Sun' 
                    THEN _topicname
                    ELSE NULL 
                END 
        ) OVER(
            PARTITION BY 
                _date,
                _domain,
                _bubble
        )
        AS _bubble_count,

        COUNT(DISTINCT _topicname) OVER(
            PARTITION BY 
                _date,
                _domain,
                _bubble,
                _category
        )
        AS _category_count,

        CASE 
            WHEN _niche IS NULL THEN NULL 
            ELSE 
                COUNT(DISTINCT _topicname) OVER(
                    PARTITION BY 
                        _date,
                        _domain,
                        _niche
                )
        END 
        AS _niche_count,

FROM all_data_combine
--WHERE _domain = '8451.com' AND _date = '2023-09-13'
),
-- Label accounts that qualify as EUTS
evaluate_euts AS (
SELECT
        *,

        CASE  
            WHEN _bubble = 'Everyone Under the Sun' 
            THEN 
                true 
            ELSE 
                false 
        END 
        AS _in_euts , 
        FROM 
         combined_data
)
,

-- Label accounts that qualify as bubble
evaluate_bubble AS (

    SELECT
        *,

        CASE 
            WHEN 
                _in_euts = true  
            AND 
                _bubble_count > 0
            THEN 
                true 
            ELSE 
                false 
        END 
        AS _in_bubble 

    FROM 
        evaluate_euts
), alll_data AS ( SELECT
        *,

        CASE 
            WHEN 
                _in_euts = true  
            AND 
                _in_bubble = true
            AND 
                _category_count >= 1
            AND 
                _niche_count > 0
            THEN 
                true 
            ELSE 
                false 
        END 
        AS _in_niche

    FROM 
        evaluate_bubble
), _euts_all AS ( 
    SELECT DISTINCT _domain, _date ,
       
        STRING_AGG(CONCAT (_topicname, ' (', _compositescore, ')'), ", ") OVER (

                PARTITION BY 
                    _date,
                    _domain,_bubble
                ORDER BY 
                    _compositescore DESC

            )
            AS _topicnames_euts,
       
        FROM alll_data 
        WHERE  _bubble = 'Everyone Under the Sun'
), _topic_all AS (
      SELECT DISTINCT _domain, _date , 
        STRING_AGG(CONCAT (_topicname, ' (', _compositescore, ')'), ", ") OVER (

                PARTITION BY 
                    _date,
                    _domain
                ORDER BY 
                    _compositescore DESC

            ) 
            AS _topicnames
             FROM alll_data 
             WHERE  _bubble != 'Everyone Under the Sun'
), _topic_Strategy AS (
      SELECT DISTINCT _domain, _date , 
        STRING_AGG(CONCAT (_topicname, ' (', _compositescore, ')'), ", ") OVER (

                PARTITION BY 
                    _date,
                    _domain
                ORDER BY 
                    _compositescore DESC

            ) 
            AS _topicnames_Strategy
             FROM alll_data 
             WHERE  _bubble != 'Strategy'
), _topic_Campaign AS (
      SELECT DISTINCT _domain, _date , 
        STRING_AGG(CONCAT (_topicname, ' (', _compositescore, ')'), ", ") OVER (

                PARTITION BY 
                    _date,
                    _domain
                ORDER BY 
                    _compositescore DESC

            ) 
            AS _topicnames_Campaign
             FROM alll_data 
             WHERE  _bubble != 'Campaign'
), _topic_Content AS (
      SELECT DISTINCT _domain, _date , 
        STRING_AGG(CONCAT (_topicname, ' (', _compositescore, ')'), ", ") OVER (

                PARTITION BY 
                    _date,
                    _domain
                ORDER BY 
                    _compositescore DESC

            ) 
            AS _topicnames_Content
             FROM alll_data 
             WHERE  _bubble != 'Content'
), _topic_MarOps AS (
      SELECT DISTINCT _domain, _date , 
        STRING_AGG(CONCAT (_topicname, ' (', _compositescore, ')'), ", ") OVER (

                PARTITION BY 
                    _date,
                    _domain
                ORDER BY 
                    _compositescore DESC

            ) 
            AS _topicnames_MarOps
             FROM alll_data 
             WHERE  _bubble != 'MarOps'
), _topic_all_data AS (
     SELECT alll_data.* ,
_euts_all._topicnames_euts,
_topic_all._topicnames,
        ROW_NUMBER() OVER(
                PARTITION BY 
                    alll_data._date,
                    alll_data._domain
                ORDER BY _topicnames DESC , _euts_all._topicnames_euts DESC 
            )
            AS _rownum
FROM alll_data
LEFT JOIN _euts_all ON alll_data._domain = _euts_all._domain AND alll_data._date = _euts_all._date
LEFT JOIN _topic_all ON alll_data._domain = _topic_all._domain AND alll_data._date = _topic_all._date
) SELECT _topic_all_data.*,
-- _topicnames_Strategy,_topicnames_Content,_topicnames_MarOps,_topicnames_Campaign,
 FROM _topic_all_data
-- LEFT JOIN _topic_Strategy ON _topic_all_data._domain = _topic_Strategy._domain AND _topic_all_data._date = _topic_Strategy._date
-- LEFT JOIN _topic_Campaign ON _topic_all_data._domain = _topic_Campaign._domain AND _topic_all_data._date = _topic_Campaign._date
-- LEFT JOIN _topic_Content ON _topic_all_data._domain = _topic_Content._domain AND _topic_all_data._date = _topic_Content._date
-- LEFT JOIN _topic_MarOps ON _topic_all_data._domain = _topic_MarOps._domain AND _topic_all_data._date = _topic_MarOps._date
-- --WHERE alll_data._domain = 'sungard.com' AND alll_data._date = '2023-11-27'

    