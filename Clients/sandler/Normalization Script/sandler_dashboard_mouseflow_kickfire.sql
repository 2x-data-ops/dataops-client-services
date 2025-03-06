---------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------ Dashboard Mouseflow Kickfire ---------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------

-- This is a mouseflow based script + 6sense buying stage

TRUNCATE TABLE `x-marketing.sandler.dashboard_mouseflow_kickfire`;

INSERT INTO `x-marketing.sandler.dashboard_mouseflow_kickfire`  (
  _recordingID,
  _visitorID,
  _viewid,
  _ipAddr,
  _timespent,
  _engagementTime,
  _timestamp,
  _country,
  _source,
  _recordingURL,
  _entrypage,
  _cleanpage,
  _nextpage,
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
  _userstatus,
  _totalsessionviews,
  _industry,
  _revenue,
  _phone,
  _employees,
  _webActivity,
  _webActivityURL,
  _totalPages
)
WITH 
  mouseflow_recording AS (
    SELECT
      DISTINCT *
    FROM
    (
      SELECT
        _recordingID, 
        _visitorID,
        _viewid,
        _ipAddr,
        _timespent,
        _engagementTime,
        _timestamp,
        _country,
        _utmsource AS _source,
        _recordingURL, 
        _page AS _entrypage, -- First touch point
        -- _cleanpage,
        CASE
          WHEN _cleanpage = "https://www.sandler.com/" THEN "https://www.sandler.com/"
          WHEN _cleanpage = "https://www.uk.sandler.com/" THEN "https://www.sandler.com/"
          WHEN _cleanpage = "https://reports.sandler.com/" THEN "https://www.sandler.com/"
          WHEN _cleanpage = "https://www.sandler.com/#" THEN "https://www.sandler.com/"
          WHEN _cleanpage = "https://enterprise.sandler.com/" THEN "https://www.sandler.com/"
          WHEN _cleanpage LIKE "%#:~:%" THEN REGEXP_REPLACE(_cleanpage, r'#:\~:.*', '')
          WHEN _cleanpage LIKE "%utm_source%" THEN REGEXP_REPLACE(_cleanpage, r'utm_source.*', '')
          ELSE _cleanpage
        END AS  _cleanpage,
        _nextpage,
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
        _userstatus,
        _totalsessionviews,
        -- _totalPages total pages visited groupped by domain and day
        ROW_NUMBER() OVER(PARTITION BY _domain, _recordingid, _visitorid, EXTRACT(DATE FROM _timestamp) ORDER BY _timestamp) AS _order
      FROM
        `x-marketing.sandler.db_web_engagements_log`
    )
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
  mouseflow_pageviews AS (
    SELECT
      _recordingID AS _recordingID,
      STRING_AGG(
        _entrypage, ", \n"
        ORDER BY _timestamp
      ) AS _webActivity,
      STRING_AGG(
        _entryurl, ", \n"
        ORDER BY _timestamp
      ) AS _webActivityURL,    
      #_fullurl AS _fullurl,
      COUNT(DISTINCT(_entrypage)) AS _totalPages
    FROM
      mouseflow_recording
    GROUP BY 
      _recordingID
  )
SELECT
  mouseflow_recording.*EXCEPT(_order),
  webtrack_6sense.* EXCEPT(_ipAddr),
  mouseflow_pageviews.* EXCEPT (_recordingID) 
FROM 
  mouseflow_recording 
LEFT JOIN 
  mouseflow_pageviews 
    ON mouseflow_recording._order = 1 AND mouseflow_recording._recordingID = mouseflow_pageviews._recordingID
LEFT JOIN 
  webtrack_6sense 
    ON mouseflow_recording._ipAddr = webtrack_6sense._ipAddr
WHERE
  _order = 1
;


------- updating certain fields for kickfire -------
UPDATE
  `x-marketing.sandler.dashboard_mouseflow_kickfire` sandler_mflow
SET
  sandler_mflow._domain    = kickfire._website,
  sandler_mflow._name       = kickfire._name,
  sandler_mflow._location   = kickfire._location,
  sandler_mflow._industry   = kickfire._category,
  sandler_mflow._revenue    = kickfire._revenue,
  sandler_mflow._employees  = kickfire._employees,
  sandler_mflow._phone      = kickfire._phone
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
  sandler_mflow._ipAddr = kickfire._ipaddr
  AND (LENGTH(sandler_mflow._domain) = 0
    OR sandler_mflow._domain IS NULL);


-- Add Buying Stage Field

/* UPDATE
  `x-marketing.sandler.dashboard_mouseflow_kickfire` main
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
    FROM `x-marketing.sandler_mysql.db_account_initial_buying_stage` 
  )
  WHERE rownum = 1

) side
WHERE main._domain = side._6sensedomain;

 */