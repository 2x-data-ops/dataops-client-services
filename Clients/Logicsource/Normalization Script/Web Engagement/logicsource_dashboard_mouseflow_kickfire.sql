---------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------ Dashboard Mouseflow Kickfire ---------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------

-- This is a mouseflow based script + 6sense buying stage

TRUNCATE TABLE `x-marketing.logicsource.dashboard_mouseflow_kickfire`;

INSERT INTO `x-marketing.logicsource.dashboard_mouseflow_kickfire`  (
  _recordingID,
  _visitorID,
  _ipAddr,
  _timespent,
  _engagementTime,
  _timestamp,
  _country,
  _source,
  _recordingURL,
  _entrypage,
  _domain,
  _name,
  _region,
  _city,
  _entryURL,
  _utmsource,
  _utmcampaign,
  _utmmedium,
  _utmcontent,
  _utmterm,
  _isisp,
  _industry,
  _revenue,
  _phone,
  _employees,
  _webActivity,
  _webActivityURL
)
WITH 
  mouseflow_recording AS (
    SELECT DISTINCT
        _recordingID, 
        _visitorID,
        _ipAddr,
        _timespent,
        _engagementTime,
        _timestamp,
        _country,
        _utmsource AS _source,
        _recordingURL, 
        _page AS _entrypage, -- First touch point
        _domain, -- _website
        _name,
        _region, 
        _city, 
        -- _industry, ipcompany_6sense
        -- _location, irrelevant?
        -- _revenue, ipcompany_6sense
        -- _phone, ipcompany_6sense
        -- _employees, ipcompany_6sense
        -- _webActivity, STRING_AGG of all the _page
        _fullurl AS _entryURL,
        _utmsource,
        _utmcampaign,
        _utmmedium,
        _utmcontent,
        _utmterm,
        _isisp,
        CONCAT(_visitorid, _timestamp) AS _key,
        -- _totalPages total pages visited groupped by domain and day
        ROW_NUMBER() OVER(PARTITION BY _domain, _recordingid, _visitorid, EXTRACT(DATE FROM _timestamp) ORDER BY _timestamp) AS _order
      FROM
        `x-marketing.logicsource.db_web_engagements_log`
  ),
  webtrack_6sense AS (
    SELECT
      _6sense._ipaddr AS _ipAddr,
      CASE
        WHEN _6sense._category IS NULL AND _6sense._category2 IS NULL THEN ''
        WHEN _6sense._category = '' AND _6sense._category2 = '' THEN ''
        ELSE CONCAT(_6sense._category,', ', _6sense._category2) 
      END AS _industry,
      /* CASE
        WHEN _6sense._city IS NULL AND _6sense._region IS NULL THEN ''
        WHEN _6sense._city = '' AND _6sense._region = '' THEN ''
        ELSE CONCAT(_6sense._city,', ', _6sense._region) 
      END AS _location, */
      _6sense._revenue AS _revenue,
      _6sense._phone AS _phone,
      _6sense._employees AS _employees
    FROM
      `x-marketing.webtrack_ipcompany.webtrack_ipcompany_6sense` _6sense
  ),
  mouseflow_recording_order AS (
    SELECT DISTINCT _key FROM mouseflow_recording WHERE _order = 1
  ),
  mouseflow_recording_agg AS (
    SELECT
        _key,
        STRING_AGG( _entrypage, ", \n" ) OVER(PARTITION BY _visitorid, DATE(_timestamp) ORDER BY _timestamp) AS _webActivity,
        STRING_AGG( _entryurl, ", \n"  ) OVER(PARTITION BY _visitorid, DATE(_timestamp) ORDER BY _timestamp) AS _webActivityURL,    
        #_fullurl AS _fullurl,
        -- ROW_NUMBER() OVER(PARTITION BY _userid, DATE(_timestamp) ORDER BY _timestamp) _page_order
      FROM
        mouseflow_recording
  ),
  mouseflow_pageviews AS (
    SELECT
      *
    FROM mouseflow_recording_agg
    JOIN mouseflow_recording_order
      USING(_key)
  )
SELECT
  mouseflow_recording.*EXCEPT(_order,_key),
  webtrack_6sense.* EXCEPT(_ipAddr),
  mouseflow_pageviews.* EXCEPT (_key) 
FROM 
  mouseflow_recording 
LEFT JOIN 
  mouseflow_pageviews 
    ON mouseflow_recording._order = 1 AND mouseflow_recording._key = mouseflow_pageviews._key
LEFT JOIN 
  webtrack_6sense 
    ON mouseflow_recording._ipAddr = webtrack_6sense._ipAddr
--WHERE
  --_order = 1
  --AND _domain != '2x.marketing'
  --AND 
  --_country = 'United States'
;


------- updating certain fields for kickfire -------
UPDATE
  `x-marketing.logicsource.dashboard_mouseflow_kickfire` logicsource_mflow
SET
  logicsource_mflow._domain    = kickfire._website,
  logicsource_mflow._name       = kickfire._name,
  logicsource_mflow._location   = kickfire._location,
  logicsource_mflow._industry   = kickfire._category,
  logicsource_mflow._revenue    = kickfire._revenue,
  logicsource_mflow._employees  = kickfire._employees,
  logicsource_mflow._phone      = kickfire._phone
FROM (
  SELECT
    _ipaddr,
    _website,
    _name,
    _region,
    _city,
    _category,
    CASE
      WHEN _city IS NULL AND _region IS NULL THEN ''
      WHEN _city = '' AND _region = '' THEN ''
      ELSE CONCAT(_city,', ', _region) 
    END AS _location,
    _revenue,
    _phone,
    _employees
  FROM
    `webtrack_ipcompany.webtrack_ipcompany`
  WHERE
    _isisp = 0 ) kickfire
WHERE
  logicsource_mflow._ipAddr = kickfire._ipaddr
  AND (LENGTH(logicsource_mflow._domain) = 0
    OR logicsource_mflow._domain IS NULL);


-- Add Buying Stage Field

/* UPDATE
  `x-marketing.logicsource.dashboard_mouseflow_kickfire` main
SET
  main._buyingstage = side._buyingstage
FROM (

  SELECT * EXCEPT(rownum)
  FROM (
    SELECT 
      PARSE_DATE('%m/%d/%Y', _extractdate) AS _extractdate,
      _6sensedomain,
      _buyingstageend AS _buyingstage,
      ROW_NUMBER() OVER(
        PARTITION BY _6sensedomain
        ORDER BY PARSE_DATE('%m/%d/%Y', _extractdate) DESC
      ) AS rownum
    FROM `x-marketing.logicsource_mysql.db_account_initial_buying_stage` 
  )
  WHERE rownum = 1

) side
WHERE main._domain = side._6sensedomain;

 */