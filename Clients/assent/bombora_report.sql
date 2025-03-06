CREATE OR REPLACE TABLE `assent.topic_clustering` AS
WITH accounts AS (
  SELECT 
    DISTINCT LOWER(_domain) AS _domain, 
    _companyname AS _company, "Target" AS _source, 
    _salesforceaccountid AS _accountid, 
    _industry AS _industry,  
    INITCAP('') AS _hqcity,
    _hqstate AS _hqstate,
    _hqcountry AS _hqcountry,
    _sic4code1 AS _hqzipcode,
    _type AS _clienttype,
    CONCAT('https://assentcompliance.lightning.force.com/lightning/r/Account/', _salesforceaccountid,'/view') AS _salesforcelink
    FROM `x-marketing.assent_mysql.db_assent_target_company_list` 
    ORDER BY 1
)
,target_bombora AS (
  SELECT DISTINCT _domain, 
    CASE WHEN mainAcc._company IS NOT NULL THEN mainAcc._company ELSE bomboraAcc._companyname END AS _company,
    CASE WHEN mainAcc._industry IS NOT NULL THEN mainAcc._industry ELSE bomboraAcc._industry END AS _industry, 
    --bomboraAcc._industry,
    CASE WHEN mainAcc._hqcity IS NOT NULL THEN mainAcc._hqcity ELSE bomboraAcc._hqcity END AS _hqcity, 
    CASE WHEN mainAcc._hqstate IS NOT NULL THEN mainAcc._hqstate ELSE bomboraAcc._hqstate END AS _hqstate, 
    CASE WHEN mainAcc._hqcountry IS NOT NULL THEN mainAcc._hqcountry ELSE bomboraAcc._hqcountry END AS _hqcountry, 
    CASE WHEN mainAcc._hqzipcode IS NOT NULL THEN mainAcc._hqzipcode ELSE bomboraAcc._hqzip END AS _hqzip, 
    CASE WHEN mainAcc._domain IS NOT NULL THEN mainAcc._source ELSE "Bombora" END AS _source,
    _compositescore, 
    _topicid, 
    _topicname,
    _date,
    _clienttype,
    _compositescoredelta,
    _salesforcelink,
    _accountid
  FROM accounts mainAcc
  FULL JOIN (
    SELECT DISTINCT  _domain, 
          _companyname, 
          _industry, 
          _hqcity,
          _hqstate, 
          _hqcountry, 
          _hqzip, 
          _compositescore,
          _compositescoredelta,
          _topicid, 
          _topicname,
          PARSE_DATE('%F',_extractdate)  _date,
    FROM ( SELECT * FROM `x-marketing.assent_mysql.db_bombora_reports`
    WHERE _sdc_deleted_at IS NULL AND _extractdate != ''
    )) bomboraAcc USING(_domain)
  ORDER BY _domain 
),
_all AS ( 
SELECT *
FROM target_bombora
),
total_date AS (
  SELECT * EXCEPT (_compositescore),
  CASE WHEN _compositescore IS NULL THEN 0 ELSE CAST(_compositescore AS INT64) END AS _compositescore,
  COUNT(DISTINCT _topicname) OVER (PARTITION BY _date,_domain) AS topic_count
  FROM _all
),
average_surge_score AS (
  SELECT *, 
    CASE WHEN topic_count > 0 THEN _compositescore / topic_count ELSE 0 END AS avg_surge_score
  FROM total_date
),clustering AS (
  SELECT 
  CAST(topic_id AS STRING) AS _topicid, 
  clusters 
  FROM `x-marketing.assent_googlesheet.Sheet1`
), all_data AS (
SELECT average_surge_score.*,
clusters 
 FROM average_surge_score
LEFT JOIN clustering USING(_topicid)
), all_bombora_topic_data AS ( 
  SELECT *,
  COUNT(DISTINCT clusters) OVER (PARTITION BY _date,_domain) AS cluster_count
 FROM all_data 
) , bombora_summary_report AS (
   SELECT * EXCEPT (_topiccountdelta ), CAST(_topiccountdelta AS NUMERIC) AS _topiccountdelta 
 FROM (
  SELECT _domain, CAST(_topiccount AS INT64) AS _topiccount, CAST(_averagescore AS INT64) AS _averagescore, _topiccountdelta,
PARSE_DATE('%F',_extractdate) AS _date 
  FROM `x-marketing.assent_mysql.db_bombora_summary`
  WHERE _topiccountdelta <> 'New' AND  _sdc_deleted_at IS NULL AND _extractdate != ''
 ) 
), all_data_combine AS (
   SELECT 
all_bombora_topic_data.* ,
bombora_summary_report.* EXCEPT (_domain,_date)
FROM all_bombora_topic_data
LEFT JOIN bombora_summary_report ON all_bombora_topic_data._domain = bombora_summary_report._domain 
AND all_bombora_topic_data._date = bombora_summary_report._date 
), alll_being_combine AS (
   SELECT *,CASE WHEN (_topiccountdelta >= 12) OR ((_compositescoredelta = '10' OR _compositescoredelta = '+10' )AND _topiccount >= 6 ) or (_topiccount>=6 AND _averagescore >= 70) OR (_averagescore>=67 AND cluster_count = 4) THEN "Intent to Buy"
WHEN (_topiccountdelta >= 8 AND _topiccountdelta < 12 ) OR ((_compositescoredelta = '10' OR _compositescoredelta = '+10' ) AND _topiccount >= 4 and _topiccount < 6) OR ( _topiccount >= 6 AND _averagescore >= 67 AND _averagescore < 70) OR (_averagescore >=62 and _averagescore < 67 and cluster_count = 4) THEN "Intent to Engage"
WHEN (_topiccountdelta >= 6 AND _topiccountdelta < 8) OR ((_compositescoredelta = '10' OR _compositescoredelta = '+10' ) AND _topiccount >= 2 and _topiccount < 4) OR ( _topiccount >= 6 AND _averagescore >= 63 AND _averagescore < 67) OR (_averagescore >= 60 and _averagescore < 62 and cluster_count =4) THEN "Intent to Research" END AS _account_categories,
CASE WHEN (_topiccountdelta >= 12) OR ((_compositescoredelta = '10' OR _compositescoredelta = '+10' )AND _topiccount >= 6 ) or (_topiccount>=6 AND _averagescore >= 70) OR (_averagescore>=67 AND cluster_count = 4) THEN "Intent to Buy"  
 END AS _intent_to_buy,
 CASE WHEN (_topiccountdelta >= 8 AND _topiccountdelta < 12 ) OR ((_compositescoredelta = '10' OR _compositescoredelta = '+10' ) AND _topiccount >= 4 and _topiccount < 6) OR ( _topiccount >= 6 AND _averagescore >= 67 AND _averagescore < 70) OR (_averagescore >=62 and _averagescore < 67 and cluster_count = 4) THEN "Intent to Engage" END AS _intent_to_engaged,
 CASE WHEN (_topiccountdelta >= 6 AND _topiccountdelta < 8) OR ((_compositescoredelta = '10' OR _compositescoredelta = '+10' ) AND _topiccount >= 2 and _topiccount < 4) OR ( _topiccount >= 6 AND _averagescore >= 63 AND _averagescore < 67) OR (_averagescore >= 60 and _averagescore < 62 and cluster_count =4) THEN "Intent to Research" END AS _intent_to_research,
CASE WHEN (_compositescoredelta = '10' OR _compositescoredelta = '+10' ) THEN TRUE ELSE FALSE END _Composite_score_delta_of_10,CAST(_topicid AS INT64) AS _topic_int ,
CASE WHEN CAST(_topicid AS INT64) >= 6 THEN TRUE ELSE FALSE END AS _Min_6_topics,
CASE WHEN CAST(_topicid AS INT64) >= 4 AND CAST(_topicid AS INT64) < 6 THEN TRUE ELSE FALSE END AS  _Between_4_5_topics,
CASE WHEN CAST(_topicid AS INT64) >= 2 AND CAST(_topicid AS INT64) < 4 THEN TRUE ELSE FALSE END AS _Between_2_3_topics,
CASE WHEN _topiccountdelta >= 8 AND _topiccountdelta < 12 THEN TRUE ELSE FALSE END AS  _topic_count_delta_8_11,
CASE WHEN _topiccountdelta >= 6 AND _topiccountdelta < 8 THEN TRUE ELSE FALSE END AS _topic_count_delta_6_7,
CASE WHEN _averagescore >= 67 AND _averagescore < 70 THEN TRUE ELSE FALSE END  AS _average_score_67_69,
CASE WHEN _averagescore >= 63 AND _averagescore < 67 THEN TRUE ELSE FALSE END AS  _average_score_63_66,
CASE WHEN _averagescore >= 62 AND _averagescore < 67 THEN TRUE ELSE FALSE END AS  _average_score_62_66,
CASE WHEN _averagescore >= 60 AND _averagescore < 62 THEN TRUE ELSE FALSE END  AS _average_score_60_61,
CASE WHEN (_compositescoredelta = '10' OR _compositescoredelta = '+10' ) THEN _topicid END AS _compositescoredelta_10
FROM all_data_combine
) SELECT *
  -- alll_being_combine._domain,alll_being_combine._date,COUNT(DISTINCT CASE WHEN (_compositescoredelta = '10' OR _compositescoredelta = '+10' ) THEN _topicid END ), COUNT(DISTINCT _topicid),CASE WHEN COUNT(DISTINCT _topicid) >= 6 THEN TRUE ELSE FALSE END
FROM alll_being_combine;
--WHERE _domain = 'accenture.com'

--where _domain = 'bmcf.com'

CREATE OR REPLACE TABLE `assent.topic_clustering_segment` AS

WITH accounts AS (
  SELECT 
    DISTINCT LOWER(_domain) AS _domain, 
    _companyname AS _company, "Target" AS _source, 
    _salesforceaccountid AS _accountid, 
    _industry AS _industry,  
    INITCAP('') AS _hqcity,
    _hqstate AS _hqstate,
    _hqcountry AS _hqcountry,
    _sic4code1 AS _hqzipcode,
    _type AS _clienttype,
    CONCAT('https://assentcompliance.lightning.force.com/lightning/r/Account/', _salesforceaccountid,'/view') AS _salesforcelink
    FROM `x-marketing.assent_mysql.db_assent_target_company_list` 
    ORDER BY 1
)
,target_bombora AS (
  SELECT DISTINCT _domain, 
    CASE WHEN mainAcc._company IS NOT NULL THEN mainAcc._company ELSE bomboraAcc._companyname END AS _company,
    CASE WHEN mainAcc._industry IS NOT NULL THEN mainAcc._industry ELSE bomboraAcc._industry END AS _industry, 
    --bomboraAcc._industry,
    CASE WHEN mainAcc._hqcity IS NOT NULL THEN mainAcc._hqcity ELSE bomboraAcc._hqcity END AS _hqcity, 
    CASE WHEN mainAcc._hqstate IS NOT NULL THEN mainAcc._hqstate ELSE bomboraAcc._hqstate END AS _hqstate, 
    CASE WHEN mainAcc._hqcountry IS NOT NULL THEN mainAcc._hqcountry ELSE bomboraAcc._hqcountry END AS _hqcountry, 
    CASE WHEN mainAcc._hqzipcode IS NOT NULL THEN mainAcc._hqzipcode ELSE bomboraAcc._hqzip END AS _hqzip, 
    CASE WHEN mainAcc._domain IS NOT NULL THEN mainAcc._source ELSE "Bombora" END AS _source,
    _compositescore, 
    _topicid, 
    _topicname,
    _date,
    _clienttype,
    _compositescoredelta,
    _salesforcelink,
    _accountid
  FROM accounts mainAcc
  FULL JOIN (
    SELECT DISTINCT  _domain, 
          _companyname, 
          _industry, 
          _hqcity,
          _hqstate, 
          _hqcountry, 
          _hqzip, 
          _compositescore,
          _compositescoredelta,
          _topicid, 
          _topicname,
          PARSE_DATE('%F',_extractdate)  _date,
    FROM ( SELECT * FROM `x-marketing.assent_mysql.db_bombora_reports`
    WHERE _sdc_deleted_at IS NULL AND _extractdate != ''
    )) bomboraAcc USING(_domain)
  ORDER BY _domain 
),
_all AS ( 
SELECT *
FROM target_bombora
),
total_date AS (
  SELECT * EXCEPT (_compositescore),
  CASE WHEN _compositescore IS NULL THEN 0 ELSE CAST(_compositescore AS INT64) END AS _compositescore,
  COUNT(DISTINCT _topicname) OVER (PARTITION BY _date,_domain) AS topic_count
  FROM _all
),
average_surge_score AS (
  SELECT *, 
    CASE WHEN topic_count > 0 THEN _compositescore / topic_count ELSE 0 END AS avg_surge_score
  FROM total_date
),clustering AS (
  SELECT 
  CAST(topic_id AS STRING) AS _topicid, 
  clusters 
  FROM `x-marketing.assent_googlesheet.Sheet1`
), all_data AS (
SELECT average_surge_score.*,
clusters 
 FROM average_surge_score
LEFT JOIN clustering USING(_topicid)
), all_bombora_topic_data AS ( 
  SELECT *,
  COUNT(DISTINCT clusters) OVER (PARTITION BY _date,_domain) AS cluster_count
 FROM all_data 
) , bombora_summary_report AS (
   SELECT * EXCEPT (_topiccountdelta ), CAST(_topiccountdelta AS NUMERIC) AS _topiccountdelta 
 FROM (
  SELECT _domain, CAST(_topiccount AS INT64) AS _topiccount, CAST(_averagescore AS INT64) AS _averagescore, _topiccountdelta,
PARSE_DATE('%F',_extractdate) AS _date 
  FROM `x-marketing.assent_mysql.db_bombora_summary`
  WHERE _topiccountdelta <> 'New' AND  _sdc_deleted_at IS NULL AND _extractdate != ''
 ) 
), all_data_combine AS (
   SELECT 
all_bombora_topic_data.* ,
bombora_summary_report.* EXCEPT (_domain,_date)
FROM all_bombora_topic_data
LEFT JOIN bombora_summary_report ON all_bombora_topic_data._domain = bombora_summary_report._domain 
AND all_bombora_topic_data._date = bombora_summary_report._date 
), alll_being_combine AS (
   SELECT *
FROM all_data_combine
), topic_clusteric AS (
   SELECT *
  -- alll_being_combine._domain,alll_being_combine._date,COUNT(DISTINCT CASE WHEN (_compositescoredelta = '10' OR _compositescoredelta = '+10' ) THEN _topicid END ), COUNT(DISTINCT _topicid),CASE WHEN COUNT(DISTINCT _topicid) >= 6 THEN TRUE ELSE FALSE END
FROM alll_being_combine
),  topic_delta_12 AS (
SELECT  _domain,
_date, 
STRING_AGG(_topicname, ',' ORDER BY _compositescore DESC) AS _topic_agg_topic_delta_12, 
COUNT(DISTINCT _topicid) AS _topic_distict_topic_delta_12,
 TRUE  _topiccountdelta_12,
FROM (  SELECT DISTINCT _domain,
_date,_topicname,_topicid,_compositescore  
FROM topic_clusteric
WHERE  IFNULL(CAST(_topiccountdelta AS NUMERIC), 0) >= 12 )
GROUP BY 1,2
ORDER BY _date DESC
), topic_delta_8_12 AS (
SELECT _domain,
_date, 
STRING_AGG(_topicname, ',' ORDER BY _compositescore DESC) AS _topic_agg_topic_delta_8_12,
 COUNT(DISTINCT _topicid) AS _topic_distict_topic_delta_8_12,
 TRUE  AS _topiccountdelta_8_12, 
FROM (  SELECT DISTINCT _domain,
_date,_topicname,_topicid,_compositescore  
FROM topic_clusteric
WHERE IFNULL(CAST(_topiccountdelta AS NUMERIC), 0) >= 8 AND IFNULL(CAST(_topiccountdelta AS NUMERIC), 0) < 12 )
GROUP BY 1,2
ORDER BY _date DESC
), topic_delta_6_8 AS (
SELECT _domain,
_date, 
STRING_AGG(_topicname, ',' ORDER BY _compositescore DESC) AS _topic_agg_topic_delta_6_8, COUNT(DISTINCT _topicid) AS _topic_distict_topic_delta_6_8,
 TRUE  AS _topiccountdelta_6_8,
FROM  (  SELECT DISTINCT _domain,
_date,_topicname,_topicid,_compositescore  
FROM topic_clusteric
WHERE IFNULL(CAST(_topiccountdelta AS NUMERIC), 0) >= 6 AND IFNULL(CAST(_topiccountdelta AS NUMERIC), 0) < 8 )
GROUP BY 1,2
ORDER BY _date DESC
), composite_score AS (
   SELECT DISTINCT _domain,
_date, 
STRING_AGG(_topicname, ',' ORDER BY _compositescore DESC) AS _topic_agg_composite_score,
COUNT(DISTINCT _topicid ) AS _topic_distict_composite_score,
CASE WHEN COUNT(DISTINCT _topicid ) >= 6 THEN TRUE ELSE FALSE END AS _comscore_more6,
CASE WHEN COUNT(DISTINCT _topicid ) >= 4 AND COUNT(DISTINCT _topicid ) < 6 THEN TRUE ELSE FALSE END AS _comscore_more4_6,
CASE WHEN COUNT(DISTINCT _topicid ) >= 2 AND COUNT(DISTINCT _topicid ) < 4 THEN TRUE ELSE FALSE END AS _comscore_more2_4,
FROM ( SELECT DISTINCT _domain,
_date, _topicname,_topicid,_compositescore FROM topic_clusteric
WHERE (_compositescoredelta = '10' OR _compositescoredelta = '+10' ) )
GROUP BY 1,2
), topic_avg_topic_70 AS  (
 SELECT *,TRUE AS topic_avg_topic_70 
FROM (
  SELECT _domain,_date, 
  STRING_AGG(_topicname, ',' ORDER BY _compositescore DESC) AS _topic_agg_avg_topic_70 ,
  COUNT(DISTINCT _topicid ) AS _topic_count_avg_topic_70 
  FROM (SELECT DISTINCT _domain,
_date, _topicname,_topicid,_compositescore FROM topic_clusteric  
  WHERE  _averagescore >= 70 )
  GROUP BY 1,2
  ) WHERE _topic_count_avg_topic_70  >= 6 
), topic_avg_67 AS (
  SELECT * , TRUE AS topic_avg_67 
FROM (
SELECT _domain,_date,  
STRING_AGG(_topicname, ',' ORDER BY _compositescore DESC) AS _topic_agg_topic_avg_67,
COUNT(DISTINCT _topicid ) AS _topic_count_topic_avg_67
FROM (SELECT DISTINCT _domain,
_date, _topicname,_topicid,_compositescore FROM topic_clusteric  
WHERE  _averagescore >= 67 AND _averagescore < 70 )
GROUP BY 1,2
) WHERE _topic_count_topic_avg_67 >= 6
), topic_avg_count63 AS (
  SELECT * , TRUE AS topic_avg_count63
FROM (
SELECT _domain,_date, 
STRING_AGG(_topicname, ',' ORDER BY _compositescore DESC) AS _topic_agg_topic_avg_count63,
COUNT(DISTINCT _topicid ) AS _topic_count_topic_avg_count63
FROM (SELECT DISTINCT _domain,
_date, _topicname,_topicid,_compositescore FROM topic_clusteric   
WHERE  _averagescore >= 63 AND _averagescore < 66)
GROUP BY 1,2
) WHERE _topic_count_topic_avg_count63 >= 6 
), avg_topic_4cluster_67 AS (
  SELECT *, TRUE AS  avg_topic_4cluster_67
FROM (
SELECT _domain,_date, 
STRING_AGG(_topicname, ',' ORDER BY _compositescore DESC) AS _topic_agg_avg_topic_4cluster_67,
COUNT(DISTINCT clusters ) AS _topic_count_avg_topic_4cluster_67
FROM (SELECT DISTINCT _domain,
_date, clusters,_topicname,_topicid,_compositescore FROM topic_clusteric  
WHERE  _averagescore >= 67 )
GROUP BY 1,2
) WHERE _topic_count_avg_topic_4cluster_67 >= 4 
), avg_topic_4cluster_62 AS (
  SELECT *, TRUE AS avg_topic_4cluster_62 
FROM (
SELECT _domain,_date, 
STRING_AGG(_topicname, ',' ORDER BY _compositescore DESC) AS _topic_agg_avg_topic_4cluster_62,
COUNT(DISTINCT clusters ) AS _topic_count_avg_topic_4cluster_62
FROM (SELECT DISTINCT _domain,
_date,clusters, _topicname,_topicid,_compositescore FROM topic_clusteric    
WHERE  _averagescore >= 62 AND  _averagescore < 67)
GROUP BY 1,2
) WHERE _topic_count_avg_topic_4cluster_62 >= 4 
), avg_topic_4cluster_count_60 AS (
  SELECT * , TRUE AS avg_topic_4cluster_count_60
FROM (
SELECT _domain,_date, 
STRING_AGG(_topicname, ',' ORDER BY _compositescore DESC) AS _topic_agg_avg_topic_4cluster_count_60,
COUNT(DISTINCT clusters ) AS _topic_count_avg_topic_4cluster_count_60
FROM (SELECT DISTINCT _domain,
_date, clusters,_topicname,_topicid,_compositescore FROM topic_clusteric    
WHERE  _averagescore >= 60 AND  _averagescore < 62)
GROUP BY 1,2
) WHERE _topic_count_avg_topic_4cluster_count_60 >= 4 
) , avg_score AS (
  SELECT _domain, 
_date,avg(_averagescore) AS _averagescores, AVG( CASE WHEN _topicid IS NOT NULL THEN 1 ELSE 0 END ) AS _avg_topic
FROM topic_clusteric 
--WHERE _domain = 'edmundoptics.com'
GROUP BY 1,2
ORDER BY _date DESC
), all_citeria AS (
   SELECT DISTINCT 
c. _domain,
c._date,
a._averagescores,
_avg_topic,
_topic_agg_topic_delta_12,
_topic_agg_topic_delta_8_12,
_topic_agg_topic_delta_6_8,
COALESCE(_topic_agg_topic_delta_12,
_topic_agg_topic_delta_8_12,
_topic_agg_topic_delta_6_8) AS _topic_agg_topic_delta,
_topic_distict_topic_delta_12,
_topic_distict_topic_delta_8_12,
_topic_distict_topic_delta_6_8, --- topic aggregation 
_topiccountdelta_12,
_topiccountdelta_8_12,
_topiccountdelta_6_8,
_topic_agg_composite_score, ---topic aggregation_com
_topic_distict_composite_score,
_comscore_more6,
_comscore_more4_6,
_comscore_more2_4,
_topic_agg_avg_topic_70 ,
_topic_count_avg_topic_70 ,
topic_avg_topic_70,
_topic_agg_topic_avg_67,
_topic_count_topic_avg_67,
topic_avg_67,
_topic_agg_topic_avg_count63,
_topic_count_topic_avg_count63,
topic_avg_count63,
COALESCE(_topic_agg_avg_topic_70 ,_topic_agg_topic_avg_67,_topic_agg_topic_avg_count63) AS _topics_aggr_avg_topic6,
_topic_agg_avg_topic_4cluster_62,
_topic_count_avg_topic_4cluster_62,
avg_topic_4cluster_62,
_topic_agg_avg_topic_4cluster_count_60,
_topic_count_avg_topic_4cluster_count_60,
avg_topic_4cluster_count_60,
_topic_agg_avg_topic_4cluster_67,
_topic_count_avg_topic_4cluster_67,
avg_topic_4cluster_67,
COALESCE(_topic_agg_avg_topic_4cluster_67,_topic_agg_avg_topic_4cluster_62,_topic_agg_avg_topic_4cluster_count_60) AS _topics_aggr_4cluster
FROM topic_clusteric c
LEFT JOIN avg_score a ON c._domain = a._domain AND c._date = a._date
LEFT JOIN topic_delta_12 d ON c._domain = d._domain AND c._date = d._date
LEFT JOIN topic_delta_8_12 z  ON c._domain = z._domain AND c._date = z._date
LEFT JOIN topic_delta_6_8 y ON c._domain = y._domain AND c._date = y._date
LEFT JOIN composite_score  e ON c._domain = e._domain AND c._date = e._date
LEFT JOIN  topic_avg_topic_70  f ON c._domain = f._domain AND c._date = f._date
LEFT JOIN  topic_avg_67 g ON c._domain = g._domain AND c._date = g._date
LEFT JOIN  topic_avg_count63 u ON c._domain = u._domain AND c._date = u._date
LEFT JOIN  avg_topic_4cluster_67  r ON c._domain = r._domain AND  c._date  = r._date 
LEFT JOIN  avg_topic_4cluster_62 t ON c._domain = t._domain AND c._date = t._date
LEFT JOIN  avg_topic_4cluster_count_60  v ON c._domain = v._domain AND c._date = v._date
) , account_categories AS (
  SELECT *, 
CASE WHEN _topiccountdelta_12 = TRUE OR _comscore_more6 = TRUE OR topic_avg_topic_70 = TRUE OR avg_topic_4cluster_67  = TRUE THEN "Intent to Buy"
WHEN _topiccountdelta_8_12 = TRUE OR _comscore_more4_6 = TRUE OR topic_avg_67 = TRUE OR avg_topic_4cluster_62 = TRUE THEN "Intent to Engage"
WHEN _topiccountdelta_6_8 = TRUE OR _comscore_more2_4 = TRUE OR topic_avg_count63 = TRUE OR avg_topic_4cluster_count_60 = TRUE THEN "Intent to Research" END AS _account_categoriess
FROM  all_citeria
) SELECT topic_clusteric.*,
account_categories.* EXCEPT (_domain,_date)
 FROM topic_clusteric
LEFT JOIN account_categories ON topic_clusteric._domain = account_categories._domain 
AND topic_clusteric._date = account_categories._date;



CREATE OR REPLACE TABLE `assent.topic_clustering_aggregate` AS
WITH accounts AS (
  SELECT 
    DISTINCT LOWER(_domain) AS _domain, 
    _companyname AS _company, "Target" AS _source, 
    _salesforceaccountid AS _accountid, 
    _industry AS _industry,  
    INITCAP('') AS _hqcity,
    _hqstate AS _hqstate,
    _hqcountry AS _hqcountry,
    _sic4code1 AS _hqzipcode,
    _type AS _clienttype,
    CONCAT('https://assentcompliance.lightning.force.com/lightning/r/Account/', _salesforceaccountid,'/view') AS _salesforcelink
    FROM `x-marketing.assent_mysql.db_assent_target_company_list` 
    ORDER BY 1
)
,target_bombora AS (
  SELECT DISTINCT _domain, 
    CASE WHEN mainAcc._company IS NOT NULL THEN mainAcc._company ELSE bomboraAcc._companyname END AS _company,
    CASE WHEN mainAcc._industry IS NOT NULL THEN mainAcc._industry ELSE bomboraAcc._industry END AS _industry, 
    --bomboraAcc._industry,
    CASE WHEN mainAcc._hqcity IS NOT NULL THEN mainAcc._hqcity ELSE bomboraAcc._hqcity END AS _hqcity, 
    CASE WHEN mainAcc._hqstate IS NOT NULL THEN mainAcc._hqstate ELSE bomboraAcc._hqstate END AS _hqstate, 
    CASE WHEN mainAcc._hqcountry IS NOT NULL THEN mainAcc._hqcountry ELSE bomboraAcc._hqcountry END AS _hqcountry, 
    CASE WHEN mainAcc._hqzipcode IS NOT NULL THEN mainAcc._hqzipcode ELSE bomboraAcc._hqzip END AS _hqzip, 
    CASE WHEN mainAcc._domain IS NOT NULL THEN mainAcc._source ELSE "Bombora" END AS _source,
    _compositescore, 
    _topicid, 
    _topicname,
    _date,
    _clienttype,
    _compositescoredelta,
    _salesforcelink,
    _accountid
  FROM accounts mainAcc
  FULL JOIN (
    SELECT DISTINCT  _domain, 
          _companyname, 
          _industry, 
          _hqcity,
          _hqstate, 
          _hqcountry, 
          _hqzip, 
          _compositescore,
          _compositescoredelta,
          _topicid, 
          _topicname,
          PARSE_DATE('%F',_extractdate)  _date,
    FROM ( SELECT * FROM `x-marketing.assent_mysql.db_bombora_reports`
    WHERE _sdc_deleted_at IS NULL AND _extractdate != ''
    )) bomboraAcc USING(_domain)
  ORDER BY _domain 
),
_all AS ( 
SELECT *
FROM target_bombora
),
total_date AS (
  SELECT * EXCEPT (_compositescore),
  CASE WHEN _compositescore IS NULL THEN 0 ELSE CAST(_compositescore AS INT64) END AS _compositescore,
  COUNT(DISTINCT _topicname) OVER (PARTITION BY _date,_domain) AS topic_count
  FROM _all
),
average_surge_score AS (
  SELECT *, 
    CASE WHEN topic_count > 0 THEN _compositescore / topic_count ELSE 0 END AS avg_surge_score
  FROM total_date
),clustering AS (
  SELECT 
  CAST(topic_id AS STRING) AS _topicid, 
  clusters 
  FROM `x-marketing.assent_googlesheet.Sheet1`
), all_data AS (
SELECT average_surge_score.*,
clusters 
 FROM average_surge_score
LEFT JOIN clustering USING(_topicid)
), all_bombora_topic_data AS ( 
  SELECT *,
  COUNT(DISTINCT clusters) OVER (PARTITION BY _date,_domain) AS cluster_count
 FROM all_data 
) , bombora_summary_report AS (
   SELECT * EXCEPT (_topiccountdelta ), CAST(_topiccountdelta AS NUMERIC) AS _topiccountdelta 
 FROM (
  SELECT _domain, CAST(_topiccount AS INT64) AS _topiccount, CAST(_averagescore AS INT64) AS _averagescore, _topiccountdelta,
PARSE_DATE('%F',_extractdate) AS _date 
  FROM `x-marketing.assent_mysql.db_bombora_summary`
  WHERE _topiccountdelta <> 'New' AND  _sdc_deleted_at IS NULL AND _extractdate != ''
 ) 
), all_data_combine AS (
   SELECT 
all_bombora_topic_data.* ,
bombora_summary_report.* EXCEPT (_domain,_date)
FROM all_bombora_topic_data
LEFT JOIN bombora_summary_report ON all_bombora_topic_data._domain = bombora_summary_report._domain 
AND all_bombora_topic_data._date = bombora_summary_report._date 
), alll_being_combine AS (
   SELECT *
FROM all_data_combine
), topic_clusteric AS (
   SELECT *,
   ROW_NUMBER() OVER (PARTITION BY _domain,_date ORDER BY _compositescore DESC) row_num
  -- alll_being_combine._domain,alll_being_combine._date,COUNT(DISTINCT CASE WHEN (_compositescoredelta = '10' OR _compositescoredelta = '+10' ) THEN _topicid END ), COUNT(DISTINCT _topicid),CASE WHEN COUNT(DISTINCT _topicid) >= 6 THEN TRUE ELSE FALSE END
FROM alll_being_combine
),  topic_delta_12 AS (
SELECT  _domain,
_date, 
STRING_AGG(_topicname, ',' ORDER BY _compositescore DESC) AS _topic_agg_topic_delta_12, 
COUNT(DISTINCT _topicid) AS _topic_distict_topic_delta_12,
 TRUE  _topiccountdelta_12,
FROM (  SELECT DISTINCT _domain,
_date,_topicname,_topicid,_compositescore  
FROM topic_clusteric
WHERE  IFNULL(CAST(_topiccountdelta AS NUMERIC), 0) >= 12 )
GROUP BY 1,2
ORDER BY _date DESC
), topic_delta_8_12 AS (
SELECT _domain,
_date, 
STRING_AGG(_topicname, ',' ORDER BY _compositescore DESC) AS _topic_agg_topic_delta_8_12,
 COUNT(DISTINCT _topicid) AS _topic_distict_topic_delta_8_12,
 TRUE  AS _topiccountdelta_8_12, 
FROM (  SELECT DISTINCT _domain,
_date,_topicname,_topicid,_compositescore  
FROM topic_clusteric
WHERE IFNULL(CAST(_topiccountdelta AS NUMERIC), 0) >= 8 AND IFNULL(CAST(_topiccountdelta AS NUMERIC), 0) < 12 )
GROUP BY 1,2
ORDER BY _date DESC
), topic_delta_6_8 AS (
SELECT _domain,
_date, 
STRING_AGG(_topicname, ',' ORDER BY _compositescore DESC) AS _topic_agg_topic_delta_6_8, COUNT(DISTINCT _topicid) AS _topic_distict_topic_delta_6_8,
 TRUE  AS _topiccountdelta_6_8,
FROM  (  SELECT DISTINCT _domain,
_date,_topicname,_topicid,_compositescore  
FROM topic_clusteric
WHERE IFNULL(CAST(_topiccountdelta AS NUMERIC), 0) >= 6 AND IFNULL(CAST(_topiccountdelta AS NUMERIC), 0) < 8 )
GROUP BY 1,2
ORDER BY _date DESC
), composite_score AS (
   SELECT DISTINCT _domain,
_date, 
STRING_AGG(_topicname, ',' ORDER BY _compositescore DESC) AS _topic_agg_composite_score,
COUNT(DISTINCT _topicid ) AS _topic_distict_composite_score,
CASE WHEN COUNT(DISTINCT _topicid ) >= 6 THEN TRUE ELSE FALSE END AS _comscore_more6,
CASE WHEN COUNT(DISTINCT _topicid ) >= 4 AND COUNT(DISTINCT _topicid ) < 6 THEN TRUE ELSE FALSE END AS _comscore_more4_6,
CASE WHEN COUNT(DISTINCT _topicid ) >= 2 AND COUNT(DISTINCT _topicid ) < 4 THEN TRUE ELSE FALSE END AS _comscore_more2_4,
FROM ( SELECT DISTINCT _domain,
_date, _topicname,_topicid,_compositescore FROM topic_clusteric
WHERE (_compositescoredelta = '10' OR _compositescoredelta = '+10' ) )
GROUP BY 1,2
), topic_avg_topic_70 AS  (
 SELECT *,TRUE AS topic_avg_topic_70 
FROM (
  SELECT _domain,_date, 
  STRING_AGG(_topicname, ',' ORDER BY _compositescore DESC) AS _topic_agg_avg_topic_70 ,
  COUNT(DISTINCT _topicid ) AS _topic_count_avg_topic_70 
  FROM (SELECT DISTINCT _domain,
_date, _topicname,_topicid,_compositescore FROM topic_clusteric  
  WHERE  _averagescore >= 70 )
  GROUP BY 1,2
  ) WHERE _topic_count_avg_topic_70  >= 6 
), topic_avg_67 AS (
  SELECT * , TRUE AS topic_avg_67 
FROM (
SELECT _domain,_date,  
STRING_AGG(_topicname, ',' ORDER BY _compositescore DESC) AS _topic_agg_topic_avg_67,
COUNT(DISTINCT _topicid ) AS _topic_count_topic_avg_67
FROM (SELECT DISTINCT _domain,
_date, _topicname,_topicid,_compositescore FROM topic_clusteric  
WHERE  _averagescore >= 67 AND _averagescore < 70 )
GROUP BY 1,2
) WHERE _topic_count_topic_avg_67 >= 6
), topic_avg_count63 AS (
  SELECT * , TRUE AS topic_avg_count63
FROM (
SELECT _domain,_date, 
STRING_AGG(_topicname, ',' ORDER BY _compositescore DESC) AS _topic_agg_topic_avg_count63,
COUNT(DISTINCT _topicid ) AS _topic_count_topic_avg_count63
FROM (SELECT DISTINCT _domain,
_date, _topicname,_topicid,_compositescore FROM topic_clusteric   
WHERE  _averagescore >= 63 AND _averagescore < 66)
GROUP BY 1,2
) WHERE _topic_count_topic_avg_count63 >= 6 
), avg_topic_4cluster_67 AS (
  SELECT *, TRUE AS  avg_topic_4cluster_67
FROM (
SELECT _domain,_date, 
STRING_AGG(_topicname, ',' ORDER BY _compositescore DESC) AS _topic_agg_avg_topic_4cluster_67,
COUNT(DISTINCT clusters ) AS _topic_count_avg_topic_4cluster_67
FROM (SELECT DISTINCT _domain,
_date,clusters , _topicname,_topicid,_compositescore FROM topic_clusteric  
WHERE  _averagescore >= 67 )
GROUP BY 1,2
) WHERE _topic_count_avg_topic_4cluster_67 >= 4 
), avg_topic_4cluster_62 AS (
  SELECT *, TRUE AS avg_topic_4cluster_62 
FROM (
SELECT _domain,_date, 
STRING_AGG(_topicname, ',' ORDER BY _compositescore DESC) AS _topic_agg_avg_topic_4cluster_62,
COUNT(DISTINCT clusters ) AS _topic_count_avg_topic_4cluster_62
FROM (SELECT DISTINCT _domain,
_date, clusters ,_topicname,_topicid,_compositescore FROM topic_clusteric    
WHERE  _averagescore >= 62 AND  _averagescore < 67)
GROUP BY 1,2
) WHERE _topic_count_avg_topic_4cluster_62 >= 4 
), avg_topic_4cluster_count_60 AS (
  SELECT * , TRUE AS avg_topic_4cluster_count_60
FROM (
SELECT _domain,_date, 
STRING_AGG(_topicname, ',' ORDER BY _compositescore DESC) AS _topic_agg_avg_topic_4cluster_count_60,
COUNT(DISTINCT clusters ) AS _topic_count_avg_topic_4cluster_count_60
FROM (SELECT DISTINCT _domain,
_date, clusters ,_topicname,_topicid,_compositescore FROM topic_clusteric    
WHERE  _averagescore >= 60 AND  _averagescore < 62)
GROUP BY 1,2
) WHERE _topic_count_avg_topic_4cluster_count_60 >= 4 
) , avg_score AS (
  SELECT _domain, 
_date,avg(_averagescore) AS _averagescores, AVG( CASE WHEN _topicid IS NOT NULL THEN 1 ELSE 0 END ) AS _avg_topic
FROM topic_clusteric 
--WHERE _domain = 'edmundoptics.com'
GROUP BY 1,2
ORDER BY _date DESC
), all_citeria AS (
   SELECT DISTINCT 
c. _domain,
c._date,
a._averagescores,
_avg_topic,
_topic_agg_topic_delta_12,
_topic_agg_topic_delta_8_12,
_topic_agg_topic_delta_6_8,
COALESCE(_topic_agg_topic_delta_12,
_topic_agg_topic_delta_8_12,
_topic_agg_topic_delta_6_8) AS _topic_agg_topic_delta,
_topic_distict_topic_delta_12,
_topic_distict_topic_delta_8_12,
_topic_distict_topic_delta_6_8, --- topic aggregation 
_topiccountdelta_12,
_topiccountdelta_8_12,
_topiccountdelta_6_8,
_topic_agg_composite_score, ---topic aggregation_com
_topic_distict_composite_score,
_comscore_more6,
_comscore_more4_6,
_comscore_more2_4,
_topic_agg_avg_topic_70 ,
_topic_count_avg_topic_70 ,
topic_avg_topic_70,
_topic_agg_topic_avg_67,
_topic_count_topic_avg_67,
topic_avg_67,
_topic_agg_topic_avg_count63,
_topic_count_topic_avg_count63,
topic_avg_count63,
COALESCE(_topic_agg_avg_topic_70 ,_topic_agg_topic_avg_67,_topic_agg_topic_avg_count63) AS _topics_aggr_avg_topic6,
_topic_agg_avg_topic_4cluster_62,
_topic_count_avg_topic_4cluster_62,
avg_topic_4cluster_62,
_topic_agg_avg_topic_4cluster_count_60,
_topic_count_avg_topic_4cluster_count_60,
avg_topic_4cluster_count_60,
_topic_agg_avg_topic_4cluster_67,
_topic_count_avg_topic_4cluster_67,
avg_topic_4cluster_67,
COALESCE(_topic_agg_avg_topic_4cluster_67,_topic_agg_avg_topic_4cluster_62,_topic_agg_avg_topic_4cluster_count_60) AS _topics_aggr_4cluster
FROM topic_clusteric c
LEFT JOIN avg_score a ON c._domain = a._domain AND c._date = a._date
LEFT JOIN topic_delta_12 d ON c._domain = d._domain AND c._date = d._date
LEFT JOIN topic_delta_8_12 z  ON c._domain = z._domain AND c._date = z._date
LEFT JOIN topic_delta_6_8 y ON c._domain = y._domain AND c._date = y._date
LEFT JOIN composite_score  e ON c._domain = e._domain AND c._date = e._date
LEFT JOIN  topic_avg_topic_70  f ON c._domain = f._domain AND c._date = f._date
LEFT JOIN  topic_avg_67 g ON c._domain = g._domain AND c._date = g._date
LEFT JOIN  topic_avg_count63 u ON c._domain = u._domain AND c._date = u._date
LEFT JOIN  avg_topic_4cluster_67  r ON c._domain = r._domain AND  c._date  = r._date 
LEFT JOIN  avg_topic_4cluster_62 t ON c._domain = t._domain AND c._date = t._date
LEFT JOIN  avg_topic_4cluster_count_60  v ON c._domain = v._domain AND c._date = v._date
) , account_categories AS (
  SELECT *, 
CASE WHEN _topiccountdelta_12 = TRUE OR _comscore_more6 = TRUE OR topic_avg_topic_70 = TRUE OR avg_topic_4cluster_67  = TRUE THEN "Intent to Buy"
WHEN _topiccountdelta_8_12 = TRUE OR _comscore_more4_6 = TRUE OR topic_avg_67 = TRUE OR avg_topic_4cluster_62 = TRUE THEN "Intent to Engage"
WHEN _topiccountdelta_6_8 = TRUE OR _comscore_more2_4 = TRUE OR topic_avg_count63 = TRUE OR avg_topic_4cluster_count_60 = TRUE THEN "Intent to Research" END AS _account_categoriess
FROM  all_citeria
),_topic_aggregate AS (
  SELECT  _domain,
_date, _topicname,_topicid,_compositescore,
ROW_NUMBER() OVER (PARTITION BY _domain,_date ORDER BY _compositescore DESC) row_num
 FROM ( SELECT DISTINCT _domain,
_date, _topicname,_topicid,_compositescore FROM topic_clusteric )
 --WHERE _domain = 'dell.com' 

)
, topic_aggregate AS (
SELECT
  _domain,
  _date,
  --row_num,
  MAX(CASE WHEN row_num = 1 THEN _topicname END) AS topic_1,
  MAX(CASE WHEN row_num = 2 THEN _topicname END) AS topic_2,
  MAX(CASE WHEN row_num = 3 THEN _topicname END) AS topic_3,
  MAX(CASE WHEN row_num = 4 THEN _topicname END) AS topic_4,
  MAX(CASE WHEN row_num = 5 THEN _topicname END) AS topic_5,
  MAX(CASE WHEN row_num = 6 THEN _topicname END) AS topic_6,
  MAX(CASE WHEN row_num = 7 THEN _topicname END) AS topic_7,
  MAX(CASE WHEN row_num = 8 THEN _topicname END) AS topic_8,
  MAX(CASE WHEN row_num = 9 THEN _topicname END) AS topic_9,
  MAX(CASE WHEN row_num = 10 THEN _topicname END) AS topic_10,
  MAX(CASE WHEN row_num = 11 THEN _topicname END) AS topic_11,
  MAX(CASE WHEN row_num = 12 THEN _topicname END) AS topic_12,
  MAX(CASE WHEN row_num = 13 THEN _topicname END) AS topic_13,
  MAX(CASE WHEN row_num = 14 THEN _topicname END) AS topic_14,
  MAX(CASE WHEN row_num = 15 THEN _topicname END) AS topic_15,
  MAX(CASE WHEN row_num = 16 THEN _topicname END) AS topic_16,
  MAX(CASE WHEN row_num = 17 THEN _topicname END) AS topic_17,
  MAX(CASE WHEN row_num = 18 THEN _topicname END) AS topic_18,
  MAX(CASE WHEN row_num = 19 THEN _topicname END) AS topic_19,
  MAX(CASE WHEN row_num = 20 THEN _topicname END) AS topic_20,
  MAX(CASE WHEN row_num = 21 THEN _topicname END) AS topic_21,
  MAX(CASE WHEN row_num = 22 THEN _topicname END) AS topic_22,
  MAX(CASE WHEN row_num = 23 THEN _topicname END) AS topic_23,
  MAX(CASE WHEN row_num = 24 THEN _topicname END) AS topic_24,
  MAX(CASE WHEN row_num = 25 THEN _topicname END) AS topic_25,
  MAX(CASE WHEN row_num = 26 THEN _topicname END) AS topic_26,
  MAX(CASE WHEN row_num = 27 THEN _topicname END) AS topic_27,
  MAX(CASE WHEN row_num = 28 THEN _topicname END) AS topic_28,
  MAX(CASE WHEN row_num = 29 THEN _topicname END) AS topic_29,
  MAX(CASE WHEN row_num = 30 THEN _topicname END) AS topic_30,
  MAX(CASE WHEN row_num = 31 THEN _topicname END) AS topic_31,
  MAX(CASE WHEN row_num = 32 THEN _topicname END) AS topic_32,
  MAX(CASE WHEN row_num = 33 THEN _topicname END) AS topic_33,
  MAX(CASE WHEN row_num = 34 THEN _topicname END) AS topic_34,
  MAX(CASE WHEN row_num = 35 THEN _topicname END) AS topic_35,
  MAX(CASE WHEN row_num = 36 THEN _topicname END) AS topic_36,
  MAX(CASE WHEN row_num = 37 THEN _topicname END) AS topic_37,
  MAX(CASE WHEN row_num = 38 THEN _topicname END) AS topic_38,
  MAX(CASE WHEN row_num = 39 THEN _topicname END) AS topic_39,
  MAX(CASE WHEN row_num = 40 THEN _topicname END) AS topic_40,
  MAX(CASE WHEN row_num = 41 THEN _topicname END) AS topic_41,
  MAX(CASE WHEN row_num = 42 THEN _topicname END) AS topic_42,
  MAX(CASE WHEN row_num = 43 THEN _topicname END) AS topic_43,
  MAX(CASE WHEN row_num = 44 THEN _topicname END) AS topic_44,
  MAX(CASE WHEN row_num = 45 THEN _topicname END) AS topic_45,
  MAX(CASE WHEN row_num = 46 THEN _topicname END) AS topic_46,
  MAX(CASE WHEN row_num = 47 THEN _topicname END) AS topic_47,
  MAX(CASE WHEN row_num = 48 THEN _topicname END) AS topic_48,
  MAX(CASE WHEN row_num = 49 THEN _topicname END) AS topic_49,
  MAX(CASE WHEN row_num = 50 THEN _topicname END) AS topic_50,
  FROM _topic_aggregate
  GROUP BY 1,2
)SELECT topic_clusteric.*,
account_categories.* EXCEPT (_domain,_date),
topic_aggregate.* EXCEPT (_domain,_date)
 FROM topic_clusteric
LEFT JOIN account_categories ON topic_clusteric._domain = account_categories._domain 
AND topic_clusteric._date = account_categories._date
LEFT JOIN topic_aggregate ON topic_clusteric._domain = topic_aggregate._domain 
AND topic_clusteric._date = topic_aggregate._date;

