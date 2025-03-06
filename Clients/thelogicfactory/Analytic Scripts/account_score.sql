#TLF bombora account score to calculate all the score for each account from bombora, target and non target account. Each account could only reach to 60 the max score 
#for all channel. The score come from web,paid_social and organic social. 
CREATE OR REPLACE TABLE `x-marketing.thelogicfactory.account_score` AS
WITH all_accounts AS ( #Pulling all account details
        SELECT * EXCEPT (rownum)
    FROM (
    SELECT *,
    ROW_NUMBER() OVER(
            PARTITION BY _domain
             ORDER BY _tier DESC
          ) 
          AS rownum 
    FROM (
      SELECT 
      DISTINCT 
      _domain, 
      _companyname AS _company, 
      CASE WHEN _domain = 'refresco.com' THEN 'Food & Beverage'
      WHEN _domain = 'diageo.com' THEN 'Food & Beverage'
      WHEN _domain = 'cocacolacompany.com' THEN 'Food & Beverage'
      WHEN _domain = 'cocacola.com' THEN 'Food & Beverage'
         WHEN _domain = 'ejgallo.com' THEN 'Food & Beverage' 
         WHEN _domain = 'cokecce.com' THEN 'Food & Beverage'
         WHEN _domain = 'pernodricard.com' THEN 'Food & Beverage'
         WHEN _domain = 'pernod-ricard.com' THEN 'Food & Beverage' ELSE _segment END AS _industry, 
      CAST(_tier AS INT64) AS _tier, 
      "Target" AS _source,
      _linkedinurl, 
       _suppressed
    FROM `thelogicfactory_mysql.db_account_list` 
    WHERE  _domain IS NOT NULL AND  _domain NOT IN ('.', '(blank)') AND _sdc_deleted_at IS NULL 
    UNION ALL 
       SELECT * 
    FROM (
    SELECT 
      DISTINCT 
      CASE WHEN _accountdomain LIKE '%ferrero%' THEN 'ferrero.com'
      WHEN _accountdomain LIKE '%www.%' THEN RIGHT(LOWER(_accountdomain),LENGTH(LOWER(_accountdomain))-STRPOS(LOWER(_accountdomain),'.')) 
      WHEN _accountdomain LIKE '%kraftheinzcompany.com/%' THEN 'kraftheinzcompany.com' 
      ELSE LOWER(_accountdomain) END AS _accountdomain,
      _accountname, 
      '' AS _industry,
      0 AS _tier,
      "Non Target",
      '' AS Linkedinurl,
      '' AS _suppressed 
    FROM `x-marketing.thelogicfactory_mysql.db_airtable_hip` ) 
    WHERE  
      (_accountdomain NOT IN (SELECT DISTINCT _domain FROM (SELECT _domain FROM `x-marketing.thelogicfactory_mysql.db_bombora_accounts`
      UNION ALL 
      SELECT _domain FROM `x-marketing.thelogicfactory_mysql.db_bombora_account_report` )) AND _accountdomain <> "")
        AND 
      (_accountdomain NOT IN (SELECT DISTINCT _domain FROM `x-marketing.thelogicfactory_mysql.db_account_list`
      WHERE  _sdc_deleted_at IS NOT NULL) AND _accountdomain <> "")
    )
     ) WHERE rownum = 1 
     

)
,target_bombora AS ( #Combining the list of accounts - Target & Bombora
  
  SELECT 
  DISTINCT 
    _domain, 
    CASE WHEN mainAcc._company IS NOT NULL THEN mainAcc._company ELSE bomboraAcc._companyname END AS _company,
    CASE WHEN mainAcc._industry IS NOT NULL THEN mainAcc._industry ELSE bomboraAcc._industry END AS _industry,
    CASE WHEN mainAcc._tier IS NOT NULL THEN mainAcc._tier ELSE CAST(NULL AS INT64) END AS _tier,
    CASE WHEN mainAcc._domain IS NOT NULL THEN mainAcc._source ELSE "Bombora" END AS _source,
    mainAcc._linkedinurl AS _linkedinurl,
    mainAcc._suppressed  AS _suppressed,
    CASE WHEN _source = 'Target' THEN "Active" ELSE account_suppression END AS account_suppression , 
    FROM all_accounts mainAcc
      FULL JOIN ( SELECT * FROM (
        SELECT _companyrevenue, _hqzip, _countrycompositescoredelta, _metrocompositescoredelta, _countrycompositescore, _clustername, _address2, _intentcountry, _linkedinurl, _companysize, _companyname, _topicid, _metrocompositescore, CASE WHEN _domain = 'refresco.com' THEN 'Food & Beverage'
      WHEN _domain = 'diageo.com' THEN 'Food & Beverage'
      WHEN _domain = 'cocacolacompany.com' THEN 'Food & Beverage'
      WHEN _domain = 'cocacola.com' THEN 'Food & Beverage'
      WHEN _domain = 'ejgallo.com' THEN 'Food & Beverage' 
      WHEN _domain = 'cokecce.com' THEN 'Food & Beverage'
      WHEN _domain = 'pernodricard.com' THEN 'Food & Beverage'
      WHEN _domain = 'pernod-ricard.com' THEN 'Food & Beverage'
      WHEN _domain = 'coors.com' THEN 'Manufacturing > Food & Beverage'  ELSE _industry END AS _industry, _address1, _hqcountry, _hqcity, _metroarea, _state, _hqstate, _compositescore, _statecompositescoredelta, _topicname,CASE WHEN _domain = "sapagroup.com" THEN "hydro.com" ELSE _domain END AS _domain, _date, _domainorigin, _compositescoredelta, _statecompositescore,_segment,CASE WHEN _Segment IS NOT NULL THEN "Active" ELSE 'Suppressed' END AS account_suppression 
    FROM (
          SELECT *,
            CASE WHEN  _intentcountry IN ('United States','Canada','United Kingdom (Great Britain)','Ireland',
          'Netherlands','Belgium','Sweden','Denmark','Finland','Norway','Switzerland') AND _clustername = 'Food & Beverages' AND _industry = 'Manufacturing > Food & Beverage' AND _companyrevenue IN ('XLarge ($200MM-$1B)','XXLarge ($1B+)') AND _companysize IN('Medium (200 - 499 Employees)','Medium-Large (500 - 999 Employees)','Large (1,000 - 4,999 Employees)','XLarge (5,000 - 10,000 Employees)','XXLarge (10,000+ Employees)') THEN 'Food & Beverage'
          WHEN  _intentcountry IN ('United States','Canada','United Kingdom (Great Britain)','France','Netherlands','Spain','Belgium','Sweden','Switzerland') AND _clustername = 'Retail Logistics' AND _industry IN ('Retail','Retail > Consumer Electronics','Retail > Department Stores & Super Stores','Retail > Drug Stores & Pharmacies','Retail > Furniture','Retail > Grocery','Retail > Home Improvement & Hardware','Retail > Motor Vehicles','Retail > Office Products') AND _companyrevenue IN ('XXLarge ($1B+)')AND _companysize IN('Medium (200 - 499 Employees)','Medium-Large (500 - 999 Employees)','Large (1,000 - 4,999 Employees)','XLarge (5,000 - 10,000 Employees)','XXLarge (10,000+ Employees)') THEN 'Retail Logistics' 
          WHEN  _intentcountry IN ('Austria' , 'Belgium' , 'Brazil' , 'Canada' , 'Finland' , 'France' , 'Germany' ,'Italy' , 'Luxembourg' , 'Mexico' ,'Netherlands' , 'Norway' , 'Poland' , 'Romania' , 'Slovenia' , 'Spain' , 'Sweden' , 'Switzerland' , 'United Kingdom (Great Britain)' , 'United States') AND _clustername = 'Metals' AND (_industry LIKE  '%Metals%' ) AND _companyrevenue IN ('XXLarge ($1B+)','XLarge ($200MM-$1B)') AND _companysize IN('Large (1,000 - 4,999 Employees)','XLarge (5,000 - 10,000 Employees)','XXLarge (10,000+ Employees)') THEN 'Metals' END AS _Segment 
          FROM `x-marketing.thelogicfactory_mysql.db_bombora_account_report` 
          WHERE _sdc_deleted_at IS NULL 
          )
          UNION ALL
            SELECT _companyrevenue, _hqzip, _countrycompositescoredelta, _metrocompositescoredelta, _countrycompositescore,'' AS _clustername, _address2, _intentcountry, _linkedinurl, _companysize, _companyname, _topicid, _metrocompositescore, _industry, _address1, _hqcountry, _hqcity, _metroarea, _state, _hqstate, _compositescore, _statecompositescoredelta, _topicname, _domain, _date, _domainorigin, _compositescoredelta, _statecompositescore, _segment,"Active" AS account_suppression 
          FROM (
          SELECT *, 'Food & Beverage'AS _Segment 
          FROM `x-marketing.thelogicfactory_mysql.db_bombora_accounts` 
          WHERE _sdc_deleted_at IS NULL 
    ))WHERE  account_suppression = 'Active') bomboraAcc USING(_domain)
  
   
  ORDER BY _domain 
  )
,email_engagements AS ( #pulling all the email engagements associated with the contacts
    SELECT _domain,_emailOpentotal,_emailClickedtotal,
    SUM(DISTINCT(CASE WHEN _emailOpentotal >= 7 THEN 1 * 5 ELSE 0 END)) AS _emailopenscore_more,
    SUM(DISTINCT(CASE WHEN _emailOpentotal >= 3 THEN  1 * 3 ELSE 0 END)) AS _emailopenscore,
    SUM(DISTINCT(CASE WHEN _emailClickedtotal >= 7 THEN 1 * 10 ELSE 0 END)) AS _emailclickscore_more,
    SUM(DISTINCT(CASE WHEN _emailClickedtotal >= 3 THEN  1 * 5 ELSE 0 END)) AS _emailclickscore,
    ((CASE WHEN _emailOpentotal >= 7 THEN 1 * 5 ELSE 0 END)+(CASE WHEN _emailOpentotal >= 3 THEN  1 * 3 ELSE 0 END)+(CASE WHEN _emailClickedtotal >= 7 THEN 1 * 10 ELSE 0 END) + (CASE WHEN _emailClickedtotal >= 3 THEN  1 * 5 ELSE 0 END)) AS _email_score
    FROM
    (
      SELECT  
     _domain,
     SUM(_emailOpened) AS _emailOpentotal, 
    SUM(_emailClicked) AS _emailClickedtotal, 

  FROM (
    SELECT CASE WHEN _domain = 'effem.com' THEN 'mars.com' 
    --WHEN _domain = 'pepsico.com' THEN 'pepsi.com' 
    WHEN _domain = 'coca-cola.com' THEN 'coca-colacompany.com'
    ELSE 
    _domain END AS _domain,
     
      SUM(CASE WHEN _engagement = 'Opened' THEN 1 ELSE 0 END ) AS _emailOpened,  
     SUM( CASE WHEN _engagement = 'Clicked' THEN 1 ELSE 0 END) AS _emailClicked,
     SUM( CASE WHEN _engagement = 'Downloaded' THEN 1 ELSE 0 END) AS _formFilled,
     
      -- SUM(CASE WHEN _engagement = 'Web Visits' THEN 1 END) AS _webVisit,
      -- EXTRACT(WEEK FROM _timestamp) AS _week, EXTRACT(YEAR FROM _timestamp) AS _year
      
    FROM ( SELECT * EXCEPT (_domain), COALESCE(_domain, RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@'))) AS _domain FROM `thelogicfactory.db_campaign_analysis`
    WHERE _timestamp >= '2022-01-01' AND _utm_campaign NOT LIKE '%Survey%' AND _isBot is NULL AND _utm_source LIKE '%email%'
    )
    --WHERE _domain = 'fedex.com'
    GROUP BY 1
    
  ) a

  --WHERE _domain = 'foodtravelexperts.com'
  GROUP BY 1
  ORDER BY 1, 3 DESC, 2 DESC)
  GROUP BY 1,2,3
  ORDER BY _emailOpentotal DESC
) ,email_last_engagementdate AS (
  SELECT email_engagements.*,
 _last_engagement._last_engagement_TS AS _last_engagement_email,
             EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS _email_week,
           EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS _email_year
  FROM 
  (
      SELECT       
      CASE WHEN _domain = 'effem.com' THEN 'mars.com' 
      --WHEN _domain = 'pepsico.com' THEN 'pepsi.com'
      WHEN _domain = 'coca-cola.com' THEN 'coca-colacompany.com' ELSE 
    _domain END  AS _domain,
        MAX(_timestamp) OVER(PARTITION BY CASE WHEN _domain = 'effem.com' THEN 'mars.com' 
      --WHEN _domain = 'pepsico.com' THEN 'pepsi.com'
      WHEN _domain = 'coca-cola.com' THEN 'coca-colacompany.com' ELSE 
    _domain END) AS _last_engagement_TS,
        ROW_NUMBER() OVER(PARTITION BY CASE WHEN _domain = 'effem.com' THEN 'mars.com' 
      --WHEN _domain = 'pepsico.com' THEN 'pepsi.com'
      WHEN _domain = 'coca-cola.com' THEN 'coca-colacompany.com' ELSE 
    _domain END ORDER BY _timestamp DESC) AS rownum 
    FROM `thelogicfactory.db_campaign_analysis`  email
    WHERE _engagement IN ( 'Opened','Clicked','Downloaded')
    AND _timestamp >= '2022-01-01' AND _utm_campaign NOT LIKE '%Survey%' AND _isBot is NULL AND _utm_source LIKE '%email%'
    
  ) _last_engagement 
  RIGHT JOIN email_engagements ON  email_engagements._domain = _last_engagement._domain
  WHERE rownum = 1 
)
,paid_social_ads  AS ( #get all paid social score 
  SELECT *, 
    (CASE WHEN _paidClicks >= 1 THEN _paidClicks * 3 ELSE 0 END) AS _paid_ads_clicks_score,
    (CASE WHEN _paidComment >= 1 THEN _paidComment * 10 ELSE 0 END ) AS _paid_adComment_score,
    (CASE WHEN _paidShare >= 1 THEN _paidShare * 15 ELSE 0 END) AS _paid_adShare_score ,
    ((CASE WHEN _paidClicks >= 1 THEN _paidClicks * 3 ELSE 0 END) +
    (CASE WHEN _paidComment >= 1 THEN _paidComment * 10 ELSE 0 END ) +
    (CASE WHEN _paidShare >= 1 THEN _paidShare * 15 ELSE 0 END)) AS _paid_social_score 
    FROM (
  SELECT _accountdomain AS _domain,
SUM( DISTINCT CASE WHEN ( _medium = 'Paid Social' AND _engagementtype = 'Click') OR   (_medium = 'Paid Social' AND _engagementtype = 'Like') THEN 1 ELSE 0 END)  AS _paidClicks,
SUM( DISTINCT CASE WHEN _medium = 'Paid Social' AND _engagementtype = 'Follow' THEN 1 ELSE 0 END)  AS _paidFollow,
SUM( DISTINCT CASE WHEN _medium = 'Paid Social' AND _engagementtype = 'Comment' THEN 1 ELSE 0 END)  AS _paidComment,
SUM( DISTINCT CASE WHEN _medium = 'Paid Social' AND _engagementtype = 'Share' THEN 1 ELSE 0 END)  AS _paidShare
FROM `x-marketing.thelogicfactory_mysql.db_airtable_hip` 
WHERE  _medium = 'Paid Social' 
GROUP BY 1) 
)
,paid_social_ads_last_engagementdate AS ( #get the last engagement date of the paid social from the last date the account enggage to paid social
  SELECT 
  paid_social_ads.*,
  _last_engagement._last_engagement_TS AS _last_engagement_paid_social,
  EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS week,
  EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS year
  FROM(
    SELECT *,
    MAX(_timestamp) OVER(PARTITION BY _domain) AS _last_engagement_TS,
    ROW_NUMBER() OVER(PARTITION BY _domain ORDER BY _timestamp DESC) AS rownum
    FROM (
      SELECT _accountdomain AS _domain,
CAST(_date AS TIMESTAMP) AS _timestamp
FROM `x-marketing.thelogicfactory_mysql.db_airtable_hip` 
WHERE _medium = 'Paid Social'
      )
      )_last_engagement
      RIGHT JOIN paid_social_ads ON  paid_social_ads._domain = _last_engagement._domain 
    WHERE rownum = 1 
)
,organic_social_ads  AS ( #get all organic social ads score for account 
  SELECT *, 
  (CASE WHEN _adClicks >= 1 THEN _adClicks * 3 ELSE 0 END) AS _ads_clicks_score,
  (CASE WHEN _adComment >= 1 THEN _adComment * 10 ELSE 0 END ) AS _adComment_score,
  (CASE WHEN _adShare >= 1 THEN _adShare * 15 ELSE 0 END) AS _adShare_score ,
  (CASE WHEN _adFollow >= 1 THEN _adFollow * 4 ELSE 0 END) AS _organicfollow_score,
  ((CASE WHEN _adClicks >= 1 THEN _adClicks * 3 ELSE 0 END) +
  (CASE WHEN _adComment >= 1 THEN _adComment * 10 ELSE 0 END ) +
  (CASE WHEN _adShare >= 1 THEN _adShare * 15 ELSE 0 END)+
  (CASE WHEN _adFollow >= 1 THEN _adFollow * 4 ELSE 0 END)) AS _organic_social_score 
  FROM (

  SELECT _accountdomain AS _domain,
SUM( DISTINCT CASE WHEN ( _medium = 'Organic Social' AND _engagementtype = 'Click') OR   (_medium = 'Organic Social' AND _engagementtype = 'Like') THEN 1 ELSE 0 END)  AS _adClicks,
SUM( DISTINCT CASE WHEN _medium = 'Organic Social' AND _engagementtype = 'Follow' THEN 1 ELSE 0 END)  AS _adFollow,
SUM( DISTINCT CASE WHEN _medium = 'Organic Social' AND _engagementtype = 'Comment' THEN 1 ELSE 0 END)  AS _adComment,
SUM( DISTINCT CASE WHEN _medium = 'Organic Social' AND _engagementtype = 'Share' THEN 1 ELSE 0 END)  AS _adShare
FROM `x-marketing.thelogicfactory_mysql.db_airtable_hip` 
WHERE  _medium = 'Organic Social'
GROUP BY 1 ) 

)
,organic_social_ads_last_engagementdate AS ( #getting the last engagement date
  SELECT 
  organic_social_ads.*,
  _last_engagement._last_engagement_TS AS _last_engagement_organic_social,
  EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS week_organic,
  EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS year_organic
  FROM(
    SELECT *,
    MAX(_timestamp) OVER(PARTITION BY _domain) AS _last_engagement_TS,
    ROW_NUMBER()OVER(PARTITION BY _domain ORDER BY _timestamp DESC) AS rownum
    FROM (
      SELECT _accountdomain AS _domain ,
CAST(_date AS TIMESTAMP) AS _timestamp 
FROM `x-marketing.thelogicfactory_mysql.db_airtable_hip` 
WHERE _medium = 'Organic Social'
      )
      ) _last_engagement
      RIGHT JOIN organic_social_ads ON  organic_social_ads._domain = _last_engagement._domain 
      WHERE rownum = 1 
) 
,search_ads  AS ( #get all organic social ads score for account 
  SELECT *, 
  (CASE WHEN _searchads >= 1 THEN 20 ELSE 0 END) AS _searchads_score,
  ((CASE WHEN _searchads >= 1 THEN 20 ELSE 0 END)) AS _searchads_score 
  FROM (
  
SELECT 
 _accountdomain AS _domain ,
SUM( DISTINCT CASE WHEN  _medium IN( 'Organic Search','Paid Search') THEN 1 ELSE 0 END)  AS _searchads
FROM `x-marketing.thelogicfactory_mysql.db_airtable_hip` 
WHERE _medium IN( 'Organic Search','Paid Search')
GROUP BY 1) 

)
,search_ads_ads_last_engagementdate AS ( #getting the last engagement date
  SELECT 
  search_ads.*,
  _last_engagement._last_engagement_TS AS _last_engagement_search_ads,
  EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS week_organic,
  EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS year_organic
  FROM(
    SELECT *,
    MAX(_timestamp) OVER(PARTITION BY _domain) AS _last_engagement_TS,
    ROW_NUMBER()OVER(PARTITION BY _domain ORDER BY _timestamp DESC) AS rownum
    FROM (
      SELECT _accountdomain AS _domain ,
CAST(_date AS TIMESTAMP) AS _timestamp 
FROM `x-marketing.thelogicfactory_mysql.db_airtable_hip` 
WHERE _medium IN( 'Organic Search','Paid Search')
      )
      ) _last_engagement
      RIGHT JOIN search_ads ON  search_ads._domain = _last_engagement._domain 
      WHERE rownum = 1 
)
,web_data AS ( #get all the web data score
  SELECT
    CASE WHEN _domain LIKE '%ferrero%' THEN 'ferrero.com'
            WHEN _domain LIKE '%www.%' THEN RIGHT(LOWER(_domain),LENGTH(LOWER(_domain))-STRPOS(LOWER(_domain),'.')) 
            WHEN _domain LIKE '%kraftheinzcompany.com/%' THEN 'kraftheinzcompany.com'
            WHEN _domain LIKE '%olymel.com%' THEN 'olymel.ca' ELSE LOWER(_domain)
             END AS _domain,* EXCEPT (_domain),
  CASE WHEN (/* newsletter_subscription_score */ + website_time_spent_score + website_page_view_score + website_visitor_count_score + visited_website_score) > 50 THEN 50
  ELSE (/* newsletter_subscription_score */ + website_time_spent_score + website_page_view_score + website_visitor_count_score + visited_website_score)
  END AS total_web_score
  FROM (
    SELECT
    *,
    /* CASE WHEN newsletter_subscription > 0 THEN 20 ELSE 0 END AS newsletter_subscription_score, */
    (CASE WHEN website_time_spent_total >= 2 THEN 20 ELSE 0 END ) + 
    (CASE WHEN website_time_spent_total <= 2 THEN 10
    ELSE 0 END ) AS website_time_spent_score,
    (CASE 
    WHEN website_page_view_total >= 5 THEN 15 ELSE 0 END) + 
    (CASE WHEN website_page_view_total <= 5 THEN 10
    ELSE 0 END ) AS website_page_view_score,
    (CASE 
    WHEN website_visitor_count_total >= 3 THEN 10 ELSE 0 END)+ 
    (CASE WHEN website_visitor_count_total <= 3 THEN 5
    ELSE 0
    END )AS website_visitor_count_score,
    5 AS visited_website_score
    FROM (
  SELECT _domain,
SUM(website_time_spent) AS website_time_spent_total,
SUM(website_page_view) AS website_page_view_total,
SUM(website_visitor_count) AS website_visitor_count_total,
true AS visited_website_total,
SUM(_adsClicks) AS _adsClicks_total
FROM (
SELECT 
        /* _website, */ REGEXP_REPLACE(RIGHT(_website,LENGTH(_website)-STRPOS(_website,'.')), '/','') AS _domain,
        -- SUM(newsletter_subscription) AS newsletter_subscription,
        ROUND((SUM(website_time_spent)/60), 2) AS website_time_spent,
        SUM(website_page_view) AS website_page_view,
        SUM(website_visitor_count) AS website_visitor_count,
        true AS visited_website,
        0 _adsClicks,
    FROM (
        SELECT /* DISTINCT _source */
            accounts._website,
            accounts._companyname ,
            -- SUM(CASE WHEN opt_in__c = true AND unsubscribed = false THEN 1 END) AS newsletter_subscription,
            SUM(_timeonpage) AS website_time_spent,
            COUNT(main._page) AS website_page_view,
            COUNT(DISTINCT main._visitid) AS website_visitor_count,
            
        FROM `x-marketing.thelogicfactory_mysql.leadfeeder_visits` main 
        LEFT JOIN `thelogicfactory_mysql.leadfeeder_accounts` accounts USING(_accountid)
        WHERE main._timestamp >= '2022-01-01'
        GROUP BY 1,2
        )
    -- WHERE REGEXP_REPLACE(RIGHT(_website,LENGTH(_website)-STRPOS(_website,'.')), '/','') = 'opcw.org'
 GROUP BY 1
 UNION All 
  SELECT 
        CASE WHEN _accountdomain LIKE '%ferrero%' THEN 'ferrero.com'
            WHEN _accountdomain LIKE '%www.%' THEN RIGHT(LOWER(_accountdomain),LENGTH(LOWER(_accountdomain))-STRPOS(LOWER(_accountdomain),'.')) 
            WHEN _accountdomain LIKE '%kraftheinzcompany.com/%' THEN 'kraftheinzcompany.com'
            WHEN _accountdomain LIKE '%olymel.com%' THEN 'olymel.ca' ELSE LOWER(_accountdomain)
             END AS _domain, 
            0.0 AS website_time_spent,
            1 AS website_page_view,
            1 AS website_visitor_count,
            true AS visited_website,
        SUM( DISTINCT CASE WHEN (_medium = 'Paid Social' AND _engagementtype = 'Click')  THEN 1 ELSE 0 END)  AS _paidClicks /* COALESCE(CAST(NULL AS INT64), 0) AS _adClicks*/,      
  FROM `x-marketing.thelogicfactory_mysql.db_airtable_hip` high_intent
  WHERE _accountdomain <> ""   AND _engagementtype = 'Click' 
  GROUP BY 1
) 
GROUP BY 1))
)
,web_data_last_engagement AS ( #get the last engagement date for the web score
  SELECT 
   CASE WHEN web_data._domain LIKE '%ferrero%' THEN 'ferrero.com'
            WHEN web_data._domain LIKE '%www.%' THEN RIGHT(LOWER(web_data._domain),LENGTH(LOWER(web_data._domain))-STRPOS(LOWER(web_data._domain),'.')) 
            WHEN web_data._domain LIKE '%kraftheinzcompany.com/%' THEN 'kraftheinzcompany.com'
            WHEN web_data._domain LIKE '%olymel.com%' THEN 'olymel.ca' ELSE LOWER(web_data._domain)
             END AS _domain,web_data.* EXCEPT (_domain),
  _last_engagement.last_engaged_date AS _last_engagement_web,
  EXTRACT(WEEK FROM _last_engagement.last_engaged_date) AS week_web,
  EXTRACT(YEAR FROM _last_engagement.last_engaged_date) AS year_web
  FROM (
    SELECT _domain,
    MAX(last_engaged_date) AS last_engaged_date
    FROM(
    SELECT _domain,
    MAX(_timestamp) AS last_engaged_date
    FROM (
      SELECT 
      CASE WHEN _accountdomain LIKE '%ferrero%' THEN 'ferrero.com'
      WHEN _accountdomain LIKE '%www.%' THEN RIGHT(LOWER(_accountdomain),LENGTH(LOWER(_accountdomain))-STRPOS(LOWER(_accountdomain),'.')) 
      WHEN _accountdomain LIKE '%kraftheinzcompany.com/%' THEN 'kraftheinzcompany.com'
      WHEN _accountdomain LIKE '%olymel.com%' THEN 'olymel.ca' ELSE LOWER(_accountdomain) END AS _domain, 
      CASE WHEN _date = '' THEN NULL 
      ELSE CAST(_date AS DATE)END AS _timestamp
      FROM `x-marketing.thelogicfactory_mysql.db_airtable_hip` high_intent
      WHERE _accountdomain <> "" AND _engagementtype = 'Click' 
    )GROUP BY 1
      UNION ALL 
    SELECT 
        /* _website, */ REGEXP_REPLACE(RIGHT(_website,LENGTH(_website)-STRPOS(_website,'.')), '/','') AS _domain,
        MAX(_timestamp) AS last_engaged_date
        FROM (
          SELECT /* DISTINCT _source */
          DATE(main._timestamp) AS _timestamp,
          accounts._website,
          accounts._companyname ,
      -- SUM(CASE WHEN opt_in__c = true AND unsubscribed = false THEN 1 END) AS newsletter_subscription,
      SUM(_timeonpage) AS website_time_spent,
      COUNT(main._page) AS website_page_view,
      COUNT(DISTINCT main._visitid) AS website_visitor_count,
      FROM `x-marketing.thelogicfactory_mysql.leadfeeder_visits` main 
      LEFT JOIN `thelogicfactory_mysql.leadfeeder_accounts` accounts USING(_accountid)
      WHERE _timestamp >= '2022-01-01'
      GROUP BY 1,2,3
      )
    -- WHERE REGEXP_REPLACE(RIGHT(_website,LENGTH(_website)-STRPOS(_website,'.')), '/','') = 'opcw.org'
    GROUP BY 1
    ) 
    --WHERE _domain = 'starbucks.ca' 
    GROUP BY 1
    )_last_engagement
    RIGHT JOIN web_data ON  web_data._domain = _last_engagement._domain 
 ),form_filled_engagement AS (
  SELECT _domain,_formFilledtotal,
    SUM(DISTINCT(CASE WHEN _formFilledtotal >= 1 THEN 1 * 60 ELSE 0 END)) AS _formFilledscore,
    FROM
    (
      SELECT  
     _domain,
     SUM(_formFilled) AS _formFilledtotal, 
   

  FROM (
    SELECT CASE WHEN _domain = 'effem.com' THEN 'mars.com' 
    --WHEN _domain = 'pepsico.com' THEN 'pepsi.com' 
    WHEN _domain = 'coca-cola.com' THEN 'coca-colacompany.com'
    ELSE 
    _domain END AS _domain,
     
    
     SUM( CASE WHEN _engagement = 'Downloaded' THEN 1 ELSE 0 END) AS _formFilled,
     
      -- SUM(CASE WHEN _engagement = 'Web Visits' THEN 1 END) AS _webVisit,
      -- EXTRACT(WEEK FROM _timestamp) AS _week, EXTRACT(YEAR FROM _timestamp) AS _year
      
    FROM ( SELECT * EXCEPT (_domain), COALESCE(_domain, RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@'))) AS _domain FROM `thelogicfactory.db_campaign_analysis`
    WHERE _timestamp >= '2022-01-01' 
    )
    --WHERE _domain = 'fedex.com'
    GROUP BY 1
    
  ) a

  --WHERE _domain = 'foodtravelexperts.com'
  GROUP BY 1
  ORDER BY 1)
  GROUP BY 1,2
  ORDER BY _formFilledtotal DESC
), form_filled_last_engagement AS (
   SELECT form_filled_engagement.*,
 _last_engagement._last_engagement_TS AS _last_engagement_form_filled,
             EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS _form_filled_week,
           EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS _form_filled_year
  FROM 
  (
      SELECT       
      COALESCE(CASE WHEN _domain = 'effem.com' THEN 'mars.com' 
      --WHEN _domain = 'pepsico.com' THEN 'pepsi.com'
      WHEN _domain = 'coca-cola.com' THEN 'coca-colacompany.com' ELSE 
    _domain END , RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@'))) AS _domain,
        MAX(_timestamp) OVER(PARTITION BY  COALESCE(CASE WHEN _domain = 'effem.com' THEN 'mars.com' 
      --WHEN _domain = 'pepsico.com' THEN 'pepsi.com'
      WHEN _domain = 'coca-cola.com' THEN 'coca-colacompany.com' ELSE 
    _domain END , RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')))) AS _last_engagement_TS,
        ROW_NUMBER() OVER(PARTITION BY  COALESCE(CASE WHEN _domain = 'effem.com' THEN 'mars.com' 
      --WHEN _domain = 'pepsico.com' THEN 'pepsi.com'
      WHEN _domain = 'coca-cola.com' THEN 'coca-colacompany.com' ELSE 
    _domain END , RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@'))) ORDER BY _timestamp DESC) AS rownum 
    FROM `thelogicfactory.db_campaign_analysis`  email
    WHERE _engagement IN ('Downloaded')
    AND _timestamp >= '2022-01-01' 
  ) _last_engagement 
  RIGHT JOIN form_filled_engagement ON  form_filled_engagement._domain = _last_engagement._domain
  WHERE rownum = 1 
  )
 , combine_all AS ( #combine all channel data and calculate into the max data. 
   SELECT *,(COALESCE(CASE WHEN total_web_score > 50 THEN 50 ELSE total_web_score END ,0) + COALESCE(CASE WHEN _organic_social_score > 35 THEN 35 ELSE _organic_social_score END,0) + COALESCE(CASE WHEN _paid_social_score > 28 THEN 28 ELSE _paid_social_score END,0) + COALESCE(_email_score,0)+ COALESCE(_formFilledscore,0)) AS _total_score,
   CASE 
   WHEN (
     _last_engagement_email_date >= web_last_engaged_date 
     AND
     _last_engagement_email_date >= _last_engagement_paid_social_date
     AND
     _last_engagement_email_date >= _last_engagement_organic_social_date
     AND 
     _last_engagement_email_date  >=  _last_engagement_form_filled_date
     ) THEN _last_engagement_email_date
     WHEN (
       web_last_engaged_date >= _last_engagement_email_date 
       AND
    web_last_engaged_date >= _last_engagement_organic_social_date
    AND
    web_last_engaged_date >= _last_engagement_paid_social_date
    AND 
     web_last_engaged_date  >=  _last_engagement_form_filled_date
    ) THEN web_last_engaged_date
    WHEN (
    _last_engagement_organic_social_date >= web_last_engaged_date
    AND
    _last_engagement_organic_social_date >= _last_engagement_paid_social_date
    AND
    _last_engagement_organic_social_date >= _last_engagement_email_date
    AND 
    _last_engagement_organic_social_date >=  _last_engagement_form_filled_date
    ) THEN _last_engagement_organic_social_date
    WHEN (
    _last_engagement_paid_social_date >= web_last_engaged_date
    AND 
    _last_engagement_paid_social_date >= _last_engagement_organic_social_date
    AND
    _last_engagement_paid_social_date >= _last_engagement_email_date
    AND 
   _last_engagement_paid_social_date >=  _last_engagement_form_filled_date
    ) THEN _last_engagement_paid_social_date
    WHEN (
    _last_engagement_form_filled_date >= web_last_engaged_date 
     AND
     _last_engagement_form_filled_date >= _last_engagement_paid_social_date
     AND
     _last_engagement_form_filled_date >= _last_engagement_organic_social_date
     AND 
     _last_engagement_form_filled_date >= _last_engagement_email_date 
      
     ) THEN _last_engagement_form_filled_date
    END AS _last_engagement_date
    FROM (
      SELECT main.*,
      email_engagement.* EXCEPT(_domain), COALESCE(CAST(_last_engagement_email AS DATE), DATE('2000-01-01')) AS  _last_engagement_email_date,
      paid_social_ads.* EXCEPT(_domain), COALESCE(CAST(_last_engagement_paid_social AS DATE), DATE('2000-01-01')) AS  _last_engagement_paid_social_date,
      organic_social_ads.* EXCEPT(_domain), COALESCE(CAST(_last_engagement_organic_social AS DATE), DATE('2000-01-01')) AS  _last_engagement_organic_social_date,
      web_data.* EXCEPT(_domain), COALESCE(_last_engagement_web, DATE('2000-01-01')) AS  web_last_engaged_date,
      form_data.* EXCEPT(_domain), COALESCE(CAST(_last_engagement_form_filled AS DATE), DATE('2000-01-01')) AS  _last_engagement_form_filled_date,
      FROM target_bombora AS main
    LEFT JOIN email_last_engagementdate  AS email_engagement ON (main._domain = email_engagement._domain )
    LEFT JOIN paid_social_ads_last_engagementdate AS paid_social_ads ON (main._domain = paid_social_ads._domain)
    LEFT JOIN organic_social_ads_last_engagementdate AS organic_social_ads ON (main._domain = organic_social_ads._domain )
    LEFT JOIN web_data_last_engagement AS web_data ON (main._domain = web_data._domain)
    LEFT JOIN form_filled_last_engagement AS form_data ON (main._domain = form_data._domain)
    ---LEFT JOIN search_ads_ads_last_engagementdate AS search_ads ON (main._domain = search_ads._domain)

) 
)
SELECT *,  EXTRACT(YEAR FROM _last_engagement_date ) AS _last_engagemtn_year,
  EXTRACT(WEEK FROM _last_engagement_date) AS _last_engagement_weekt,          
  DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) AS days_since_last_engaged,
  CASE 
  WHEN DATE_DIFF(CURRENT_DATE(),_last_engagement_date, DAY) > 180  THEN (_total_score - 50)
  WHEN DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) > 90  THEN (_total_score - 40)
  WHEN DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) > 60 THEN (_total_score - 30)
  WHEN DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) > 30  THEN (_total_score - 20)
  ELSE _total_score
  END AS _score_new
FROM combine_all
--WHERE _domain =  'pepsico.com'
ORDER BY _last_engagement_date DESC,_total_score DESC
 
 ;

------contact score 
CREATE OR REPLACE TABLE x-marketing.thelogicfactory.contact_score AS
WITH
all_contacts AS (   
  SELECT * 
  FROM (
    SELECT DISTINCT 
    LOWER(_email) AS _email, 
    RIGHT(contacts._email,
    LENGTH(contacts._email)-STRPOS(contacts._email,'@')) AS _domain 
  FROM thelogicfactory_mysql.w_routables contacts
  WHERE 
    RIGHT(contacts._email,LENGTH(contacts._email)-STRPOS(contacts._email,'@')) IN (SELECT DISTINCT _domain FROM `x-marketing.thelogicfactory_mysql.db_bombora_accounts`)
  UNION ALL 
  SELECT DISTINCT CONCAT(_name,'@',_companydomain) AS _email,CASE WHEN _companydomain LIKE '%ferrero%' THEN 'ferrero.com'
  WHEN _companydomain LIKE '%www.%' THEN RIGHT(LOWER(_companydomain),LENGTH(LOWER(_companydomain))-STRPOS(LOWER(_companydomain),'.')) ELSE LOWER(_companydomain) END AS _companydomain
  FROM `x-marketing.thelogicfactory_mysql.db_airtable_high_intent_contacts` 
  WHERE _companydomain <> ""
      UNION ALL 
  SELECT DISTINCT CASE WHEN _contactemail = '' THEN CONCAT(_contactname,'@',_accountdomain) ELSE _contactemail END AS _email,CASE WHEN _accountdomain LIKE '%ferrero%' THEN 'ferrero.com'
  WHEN _accountdomain LIKE '%www.%' THEN RIGHT(LOWER(_accountdomain),LENGTH(LOWER(_accountdomain))-STRPOS(LOWER(_accountdomain),'.')) ELSE LOWER(_accountdomain) END AS _companydomain
  FROM `x-marketing.thelogicfactory_mysql.db_airtable_hip` 
  WHERE _accountdomain <> ""
    ORDER BY _email 
  )
  )
,email_engagements AS ( #pulling all the email engagements associated with the contacts
    SELECT _email,_domain,_emailOpentotal,_emailClickedtotal,
    SUM(DISTINCT(CASE WHEN _emailOpentotal >= 7 THEN 1 * 5 ELSE 0 END)) AS _emailopenscore_more,
    SUM(DISTINCT(CASE WHEN _emailOpentotal >= 3 THEN  1 * 3 ELSE 0 END)) AS _emailopenscore,
    SUM(DISTINCT(CASE WHEN _emailClickedtotal >= 7 THEN 1 * 10 ELSE 0 END)) AS _emailclickscore_more,
    SUM(DISTINCT(CASE WHEN _emailClickedtotal >= 3 THEN  1 * 5 ELSE 0 END)) AS _emailclickscore,
    ((CASE WHEN _emailOpentotal >= 7 THEN 1 * 5 ELSE 0 END)+(CASE WHEN _emailOpentotal >= 3 THEN  1 * 3 ELSE 0 END)+(CASE WHEN _emailClickedtotal >= 7 THEN 1 * 10 ELSE 0 END) + (CASE WHEN _emailClickedtotal >= 3 THEN  1 * 5 ELSE 0 END)) AS _email_score
    FROM
    (
      SELECT 
      _email,  
     _domain,
     SUM(_emailOpened) AS _emailOpentotal, 
    SUM(_emailClicked) AS _emailClickedtotal, 

  FROM (
    SELECT _email, 
    CASE WHEN RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) = 'effem.com' THEN 'mars.com' 
    WHEN RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) = 'pepsico.com' THEN 'pepsi.com' 
    WHEN RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) = 'coca-cola.com' THEN 'coca-colacompany.com'
    ELSE 
    RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) END AS _domain,
     
      SUM(CASE WHEN _engagement = 'Opened' THEN 1 ELSE 0 END ) AS _emailOpened,  
     SUM( CASE WHEN _engagement = 'Clicked' THEN 1 ELSE 0 END) AS _emailClicked,
     SUM( CASE WHEN _engagement = 'Downloaded' THEN 1 ELSE 0 END) AS _formFilled,
     
      -- SUM(CASE WHEN _engagement = 'Web Visits' THEN 1 END) AS _webVisit,
      -- EXTRACT(WEEK FROM _timestamp) AS _week, EXTRACT(YEAR FROM _timestamp) AS _year
      
    FROM ( SELECT * FROM `thelogicfactory.db_campaign_analysis`
    WHERE _timestamp >= '2022-01-01' AND _utm_campaign NOT LIKE '%Survey%' AND _isBot is NULL AND _utm_source LIKE '%email%'
    )
    --WHERE RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) = 'ssab.com'
    GROUP BY 1,2
    
  ) a

  --WHERE _domain = 'foodtravelexperts.com'
  GROUP BY 1,2
  ORDER BY 1, 3 DESC, 2 DESC)
  GROUP BY 1,2,3,4
  ORDER BY _emailOpentotal DESC
) 
,email_last_engagementdate AS (
  SELECT email_engagements.*,
 _last_engagement._last_engagement_TS AS _last_engagement_email,
             EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS _email_week,
           EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS _email_year
  FROM 
  (
      SELECT  _email,      
      CASE WHEN RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) = 'effem.com' THEN 'mars.com' 
      WHEN RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) = 'pepsico.com' THEN 'pepsi.com'
      WHEN RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) = 'coca-cola.com' THEN 'coca-colacompany.com' ELSE 
    RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) END  AS _domain,
        MAX(_timestamp) OVER(PARTITION BY RIGHT(email._email,LENGTH(email._email)-STRPOS(email._email,'@'))) AS _last_engagement_TS,
        ROW_NUMBER() OVER(PARTITION BY RIGHT(email._email,LENGTH(email._email)-STRPOS(email._email,'@')) ORDER BY _timestamp DESC) AS rownum 
    FROM `thelogicfactory.db_campaign_analysis`  email
    WHERE _engagement IN ( 'Opened','Clicked','Downloaded')
    --WHERE RIGHT(email._email,LENGTH(email._email)-STRPOS(email._email,'@')) ='vandenbosch.com'
    
  ) _last_engagement 
  RIGHT JOIN email_engagements ON  email_engagements._domain = _last_engagement._domain
  WHERE rownum = 1 
)
,paid_social_ads  AS ( #get all paid social score 
  SELECT *, 
    (CASE WHEN _paidClicks >= 1 THEN _paidClicks * 3 ELSE 0 END) AS _paid_ads_clicks_score,
    (CASE WHEN _paidComment >= 1 THEN _paidComment * 10 ELSE 0 END ) AS _paid_adComment_score,
    (CASE WHEN _paidShare >= 1 THEN _paidShare * 15 ELSE 0 END) AS _paid_adShare_score ,
    ((CASE WHEN _paidClicks >= 1 THEN _paidClicks * 3 ELSE 0 END) +
    (CASE WHEN _paidComment >= 1 THEN _paidComment * 10 ELSE 0 END ) +
    (CASE WHEN _paidShare >= 1 THEN _paidShare * 15 ELSE 0 END)) AS _paid_social_score 
    FROM (
  SELECT CASE WHEN _contactemail = '' THEN CONCAT(_contactname,'@',_accountdomain) ELSE _contactemail END AS _email, _accountdomain AS _domain ,
SUM( DISTINCT CASE WHEN ( _medium = 'Paid Social' AND _engagementtype = 'Click') OR   (_medium = 'Paid Social' AND _engagementtype = 'Like') THEN 1 ELSE 0 END)  AS _paidClicks,
SUM( DISTINCT CASE WHEN _medium = 'Paid Social' AND _engagementtype = 'Follow' THEN 1 ELSE 0 END)  AS _paidFollow,
SUM( DISTINCT CASE WHEN _medium = 'Paid Social' AND _engagementtype = 'Comment' THEN 1 ELSE 0 END)  AS _paidComment,
SUM( DISTINCT CASE WHEN _medium = 'Paid Social' AND _engagementtype = 'Share' THEN 1 ELSE 0 END)  AS _paidShare
FROM `x-marketing.thelogicfactory_mysql.db_airtable_hip` 
WHERE  _medium = 'Paid Social'
GROUP BY 1,2) 
)
,paid_social_ads_last_engagementdate AS ( #get the last engagement date of the paid social from the last date the account enggage to paid social
  SELECT 
  paid_social_ads.*,
  _last_engagement._last_engagement_TS AS _last_engagement_paid_social,
  EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS week,
  EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS year
  FROM(
    SELECT *,
    MAX(_timestamp) OVER(PARTITION BY _domain) AS _last_engagement_TS,
    ROW_NUMBER() OVER(PARTITION BY _domain ORDER BY _timestamp DESC) AS rownum
    FROM (
      SELECT CASE WHEN _contactemail = '' THEN CONCAT(_contactname,'@',_accountdomain) ELSE _contactemail END AS _email,_accountdomain AS _domain,
CAST(_date AS TIMESTAMP) AS _timestamp
FROM `x-marketing.thelogicfactory_mysql.db_airtable_hip` 
WHERE  _medium = 'Paid Social'
      )
      )_last_engagement
      RIGHT JOIN paid_social_ads ON  paid_social_ads._domain = _last_engagement._domain 
    WHERE rownum = 1 
)
,organic_social_ads  AS ( #get all paid social score 
  SELECT *, 
    (CASE WHEN _organicClicks >= 1 THEN _organicClicks * 3 ELSE 0 END) AS _organic_social_ads_clicks_score,
    (CASE WHEN _organicComment >= 1 THEN _organicComment * 10 ELSE 0 END ) AS _organic_social_adComment_score,
    (CASE WHEN _organicShare >= 1 THEN _organicShare * 15 ELSE 0 END) AS _organic_social_adShare_score ,
    ((CASE WHEN _organicClicks >= 1 THEN _organicClicks * 3 ELSE 0 END) +
    (CASE WHEN _organicComment >= 1 THEN _organicComment * 10 ELSE 0 END ) +
    (CASE WHEN _organicShare >= 1 THEN _organicShare * 15 ELSE 0 END)) AS _organic_social_score 
    FROM (
  SELECT CASE WHEN _contactemail = '' THEN CONCAT(_contactname,'@',_accountdomain) ELSE _contactemail END AS _email, _accountdomain AS _domain,
SUM( DISTINCT CASE WHEN ( _medium = 'Organic Social' AND _engagementtype = 'Click') OR   (_medium = 'Organic Social' AND _engagementtype = 'Like') THEN 1 ELSE 0 END)  AS _organicClicks,
SUM( DISTINCT CASE WHEN _medium = 'Organic Social' AND _engagementtype = 'Follow' THEN 1 ELSE 0 END)  AS _organicFollow,
SUM( DISTINCT CASE WHEN _medium = 'Organic Social' AND _engagementtype = 'Comment' THEN 1 ELSE 0 END)  AS _organicComment,
SUM( DISTINCT CASE WHEN _medium = 'Organic Social' AND _engagementtype = 'Share' THEN 1 ELSE 0 END)  AS _organicShare
FROM `x-marketing.thelogicfactory_mysql.db_airtable_hip` 
WHERE  _medium = 'Organic Social'
GROUP BY 1,2) 
)
,organic_social_ads_last_engagementdate AS ( #get the last engagement date of the paid social from the last date the account enggage to paid social
  SELECT 
  organic_social_ads.*,
  _last_engagement._last_engagement_TS AS _last_engagement_organic_social,
  EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS _organic_social_week,
  EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS _organic_social_year
  FROM(
    SELECT *,
    MAX(_timestamp) OVER(PARTITION BY _domain) AS _last_engagement_TS,
    ROW_NUMBER() OVER(PARTITION BY _domain ORDER BY _timestamp DESC) AS rownum
    FROM (
      SELECT CASE WHEN _contactemail = '' THEN CONCAT(_contactname,'@',_accountdomain) ELSE _contactemail END AS _email,_accountdomain AS _domain ,
CAST(_date AS TIMESTAMP) AS _timestamp
FROM `x-marketing.thelogicfactory_mysql.db_airtable_hip` 
WHERE _medium = 'Organic Social'
      )
      )_last_engagement
      RIGHT JOIN organic_social_ads ON  organic_social_ads._domain = _last_engagement._domain 
    WHERE rownum = 1 
)
, search_ads  AS ( #get all organic social ads score for account 
  SELECT *, 
  (CASE WHEN _searchads >= 1 THEN 20 ELSE 0 END) AS _searchads_score,
  ((CASE WHEN _searchads >= 1 THEN 20 ELSE 0 END)) AS _searchads_scores 
  FROM (
  
SELECT 
CASE WHEN _contactemail = '' THEN CONCAT(_contactname,'@',_accountdomain) ELSE _contactemail END AS _email,
 _accountdomain AS _domain ,
SUM( DISTINCT CASE WHEN  _medium IN( 'Organic Search','Paid Search') THEN 1 ELSE 0 END)  AS _searchads
FROM `x-marketing.thelogicfactory_mysql.db_airtable_hip` 
WHERE _medium IN( 'Organic Search','Paid Search')
GROUP BY 1,2) 

)
,search_ads_ads_last_engagementdate AS ( #getting the last engagement date
  SELECT 
  search_ads.*,
  _last_engagement._last_engagement_TS AS _last_engagement_search_ads,
  EXTRACT(WEEK FROM _last_engagement._last_engagement_TS) AS week_organic,
  EXTRACT(YEAR FROM _last_engagement._last_engagement_TS) AS year_organic
  FROM(
    SELECT *,
    MAX(_timestamp) OVER(PARTITION BY _domain) AS _last_engagement_TS,
    ROW_NUMBER()OVER(PARTITION BY _domain ORDER BY _timestamp DESC) AS rownum
    FROM (
      SELECT _accountdomain AS _domain ,
CAST(_date AS TIMESTAMP) AS _timestamp 
FROM `x-marketing.thelogicfactory_mysql.db_airtable_hip` 
WHERE _medium IN( 'Organic Search','Paid Search')
      )
      ) _last_engagement
      RIGHT JOIN search_ads ON  search_ads._domain = _last_engagement._domain 
      WHERE rownum = 1 
)
, combine_all AS ( #combine all channel data and calculate into the max data. 
   SELECT *,(COALESCE(CASE WHEN _searchads_scores > 20 THEN 20 ELSE _searchads_scores END ,0) + COALESCE(CASE WHEN _organic_social_score > 35 THEN 35 ELSE _organic_social_score END,0) + COALESCE(CASE WHEN _paid_social_score > 28 THEN 28 ELSE _paid_social_score END,0) + COALESCE(_email_score,0)) AS _total_score,
   CASE 
   WHEN (
     _last_engagement_email_date >= _last_engagement_search_ads_date 
     AND
     _last_engagement_email_date  >= _last_engagement_paid_social_date
     AND
     _last_engagement_email_date >= _last_engagement_organic_social_date
     ) THEN _last_engagement_email_date
     WHEN (
       _last_engagement_search_ads_date >= _last_engagement_email_date 
       AND
    _last_engagement_search_ads_date  >= _last_engagement_organic_social_date
    AND
    _last_engagement_search_ads_date  >= _last_engagement_paid_social_date
    ) THEN _last_engagement_search_ads_date
    WHEN (
    _last_engagement_organic_social_date >= _last_engagement_search_ads_date
    AND
    _last_engagement_organic_social_date >= _last_engagement_paid_social_date
    AND
    _last_engagement_organic_social_date >= _last_engagement_email_date
    ) THEN _last_engagement_organic_social_date
    WHEN (
    _last_engagement_paid_social_date >= _last_engagement_search_ads_date
    AND 
    _last_engagement_paid_social_date >= _last_engagement_organic_social_date
    AND
    _last_engagement_paid_social_date >= _last_engagement_email_date
    ) THEN _last_engagement_paid_social_date
    END AS _last_engagement_date
    FROM (
      SELECT main.*,
      email_engagement.* EXCEPT(_domain,_email), COALESCE(CAST(_last_engagement_email AS DATE), DATE('2000-01-01')) AS  _last_engagement_email_date,
      paid_social_ads.* EXCEPT(_domain,_email), COALESCE(CAST(_last_engagement_paid_social AS DATE), DATE('2000-01-01')) AS  _last_engagement_paid_social_date,
      organic_social_ads.* EXCEPT(_domain,_email), COALESCE(CAST(_last_engagement_organic_social AS DATE), DATE('2000-01-01')) AS  _last_engagement_organic_social_date,
      search_ads.* EXCEPT(_domain,_email), COALESCE(CAST(_last_engagement_search_ads AS DATE), DATE('2000-01-01')) AS  _last_engagement_search_ads_date,
      FROM all_contacts AS main
    LEFT JOIN email_last_engagementdate  AS email_engagement ON (main._email = email_engagement._email )
    LEFT JOIN paid_social_ads_last_engagementdate AS paid_social_ads ON (main._email = paid_social_ads._email)
    LEFT JOIN organic_social_ads_last_engagementdate AS organic_social_ads ON (main._email = organic_social_ads._email)
    --LEFT JOIN web_data_last_engagement AS web_data ON (main._domain = web_data._domain)
    LEFT JOIN search_ads_ads_last_engagementdate AS search_ads ON (main._email = search_ads._email)

) 
)
SELECT *,  EXTRACT(YEAR FROM _last_engagement_date ) AS _last_engagemtn_year,
  EXTRACT(WEEK FROM _last_engagement_date) AS _last_engagement_weekt,          
  DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) AS days_since_last_engaged,
  CASE 
  WHEN DATE_DIFF(CURRENT_DATE(),_last_engagement_date, DAY) > 180  THEN (_total_score - 50)
  WHEN DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) > 90  THEN (_total_score - 40)
  WHEN DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) > 60 THEN (_total_score - 30)
  WHEN DATE_DIFF(CURRENT_DATE(), _last_engagement_date, DAY) > 30  THEN (_total_score - 20)
  ELSE _total_score
  END AS _score_new
FROM combine_all
ORDER BY _last_engagement_date DESC,_total_score DESC