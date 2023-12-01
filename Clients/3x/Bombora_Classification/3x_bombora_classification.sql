TRUNCATE TABLE `3x.db_bombora_classification`;

INSERT INTO `3x.db_bombora_classification`(
    _date, 
    _domain, 
    _companyname, 
    _companysize, 
    _companyrevenue, 
    _industry, 
    _compositescore, 
    _topicname, 
    _topicid, 
    _compositescore_num, 
    _bubble, 
    _category, 
    _niche, 
    _euts_count, 
    _bubble_count, 
    _category_count, 
    _niche_count, 
    _in_euts, 
    _in_bubble, 
    _in_niche, 
    _topicnames_euts, 
    _topicnames,
    _topicnames_Strategy,
    _topicnames_Content,
    _topicnames_MarOps,
    _topicnames_Campaign,
    _rownum
)WITH bombora_data AS (

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
), all_data_segement AS (
  WITH _euts_all AS ( 
    SELECT DISTINCT _domain, _date ,
       
        STRING_AGG(CONCAT (_topicname, ' (', _compositescore, ')'), ", ") OVER (

                PARTITION BY 
                    _date,
                    _domain,_bubble
                ORDER BY 
                    _compositescore DESC

            )
            AS _topicnames_euts,
       
        FROM evaluate_bubble

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
             FROM evaluate_bubble

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
             FROM evaluate_bubble

             WHERE  _bubble = 'Strategy'
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
             FROM evaluate_bubble
 
             WHERE  _bubble = 'Campaign'
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
             FROM evaluate_bubble

             WHERE  _bubble = 'Content'
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
             FROM evaluate_bubble

             WHERE  _bubble = 'MarOps'
) SELECT * EXCEPT (_rownum) 
FROM (
SELECT DISTINCT alll_data._domain, 
alll_data._date ,
_euts_all._topicnames_euts,
_topic_all._topicnames,
_topicnames_Strategy,
_topicnames_Content,_topicnames_MarOps,_topicnames_Campaign,
ROW_NUMBER() OVER(
                PARTITION BY 
                    alll_data._date,
                    alll_data._domain
                ORDER BY _compositescore_num DESC, _topicnames DESC , _topicnames_euts DESC , _topicnames_MarOps DESC ,_topicnames_Strategy DESC,
_topicnames_Content DESC,_topicnames_Campaign DESC, _in_euts DESC
            )
            AS _rownum
FROM evaluate_bubble alll_data
LEFT JOIN _euts_all ON alll_data._domain = _euts_all._domain AND alll_data._date = _euts_all._date
LEFT JOIN _topic_all ON alll_data._domain = _topic_all._domain AND alll_data._date = _topic_all._date
LEFT JOIN _topic_Strategy ON alll_data._domain = _topic_Strategy._domain AND alll_data._date = _topic_Strategy._date
LEFT JOIN _topic_Campaign ON alll_data._domain = _topic_Campaign._domain AND alll_data._date = _topic_Campaign._date
LEFT JOIN _topic_MarOps ON alll_data._domain = _topic_MarOps._domain AND alll_data._date = _topic_MarOps._date
LEFT JOIN _topic_Content ON alll_data._domain = _topic_Content._domain AND alll_data._date = _topic_Content._date
WHERE _in_euts = true
) WHERE _rownum = 1
), alll_data AS (
  SELECT
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
) SELECT alll_data.* ,
all_data_segement.* EXCEPT (_domain, _date),
        ROW_NUMBER() OVER(
                PARTITION BY 
                    alll_data._date,
                    alll_data._domain
                ORDER BY   _topicnames_euts DESC ,_topicnames DESC , _in_euts DESC, _topicnames_MarOps DESC ,_topicnames_Strategy DESC,
_topicnames_Content DESC,_topicnames_Campaign DESC
            )
            AS _rownum
FROM alll_data
LEFT OUTER JOIN all_data_segement  ON alll_data._domain = all_data_segement ._domain AND alll_data._date = all_data_segement ._date
--WHERE alll_data._domain = 'houstonisd.org' AND alll_data._date = '2023-11-27'