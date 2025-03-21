#Bombora Topic Clustering 
CREATE OR REPLACE TABLE ems.topic_clustering AS
WITH ems_accounts AS (
  SELECT 
    DISTINCT _shortenedwebsite AS _domain, 
    _accountname AS _company, "Target" AS _source, 
    _accountid, 
    industry__c AS _industry,  
    INITCAP(fp_city__c) AS _hqcity,
    fp_state__c AS _hqstate,
    fp_country__c AS _hqcountry,
    fp_zip__c AS _hqzipcode,
    _clienttype
  FROM `x-marketing.ems_mysql.db_target_account` 
  JOIN `x-marketing.ems_salesforce.Account` ON account_id__c = _accountid
  ORDER BY 1
)
,target_bombora AS (
  SELECT DISTINCT _domain, 
    CASE WHEN mainAcc._company IS NOT NULL THEN mainAcc._company ELSE bomboraAcc._companyname END AS _company,
    --CASE WHEN mainAcc._industry IS NOT NULL THEN mainAcc._industry ELSE bomboraAcc._industry END AS _industry, 
    bomboraAcc._industry,
    CASE WHEN mainAcc._hqcity IS NOT NULL THEN mainAcc._hqcity ELSE bomboraAcc._hqcity END AS _hqcity, 
    CASE WHEN mainAcc._hqstate IS NOT NULL THEN mainAcc._hqstate ELSE bomboraAcc._hqstate END AS _hqstate, 
    CASE WHEN mainAcc._hqcountry IS NOT NULL THEN mainAcc._hqcountry ELSE bomboraAcc._hqcountry END AS _hqcountry, 
    CASE WHEN mainAcc._hqzipcode IS NOT NULL THEN mainAcc._hqzipcode ELSE bomboraAcc._hqzip END AS _hqzip, 
    CASE WHEN mainAcc._domain IS NOT NULL THEN mainAcc._source ELSE "Bombora" END AS _source,
    _compositescore, 
    _topicid, 
    _topicname,
    _date,
    _clienttype
  FROM ems_accounts mainAcc
  FULL JOIN (
    SELECT DISTINCT _domain, 
          _companyname, 
          _industry, 
          _hqcity,
          _hqstate, 
          _hqcountry, 
          _hqzip, 
          _compositescore,
          _topicid, 
          _topicname,
          PARSE_DATE('%F',_reportdate) AS _date,
    --ROW_NUMBER() OVER(PARTITION BY _domain, EXTRACT(DATE FROM _date),_topicid,_segment,account_suppression ORDER BY _compositescore DESC) AS rownum  
    FROM ( SELECT * FROM `x-marketing.ems_mysql.db_ems_bombora_account`
    )) bomboraAcc USING(_domain)
  ORDER BY _domain 
) 
SELECT * FROM target_bombora