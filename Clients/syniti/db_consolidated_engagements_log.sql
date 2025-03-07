CREATE OR REPLACE TABLE `x-marketing.syniti.db_consolidated_engagements_log`
PARTITION BY DATE(_timestamp)
CLUSTER BY _engagement, _domain

AS
SELECT 
        *
      FROM ( 
        SELECT DISTINCT _email, 
        RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) AS _domain, 
        TIMESTAMP(FORMAT_TIMESTAMP('%F %I:%M:%S %Z', _timestamp)) AS _timestamp,
        DATE_TRUNC(DATE(_timestamp), MONTH) AS _month,
        DATE_TRUNC(DATE(_timestamp), QUARTER) AS _quater,
        EXTRACT(WEEK FROM _timestamp) AS _week,  
        EXTRACT(YEAR FROM _timestamp) AS _year,
        -- _utmcampaign AS _contentTitle, 
        CONCAT("Email ", INITCAP(_engagement)) AS _engagement,
        _description,
        -- _email_id AS _campaignID,
        _campaignid
        FROM 
          (SELECT * FROM `syniti.db_email_engagements_log`)
        WHERE 
          /* (EXTRACT(DATE FROM _timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY ) AND CURRENT_DATE() )
          AND */
          LOWER(_engagement) NOT IN ('sent', 'downloaded',  'bounced' , 'unsubscribed', /* 'processed', 'deffered', 'spam', 'suppressed', */ 'dropped')
      ) 
      WHERE 
        NOT REGEXP_CONTAINS(_domain,'2x.marketing|syniti') 
        AND _domain IS NOT NULL 
      -- ORDER BY 
      --   1, 3 DESC, 2 DESC