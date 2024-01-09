TRUNCATE TABLE `x-marketing.blend360.dashboard_mouseflow_kickfire`;

INSERT INTO `x-marketing.blend360.dashboard_mouseflow_kickfire`  (
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
-- CREATE OR REPLACE TABLE `x-marketing.blend360.dashboard_mouseflow_kickfire` AS
WITH 
  mouseflow_recording AS (
    SELECT
      DISTINCT *
    FROM
    (
      SELECT
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
        -- CAST(NULL AS STRING) AS _location, 
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
        -- _totalPages total pages visited groupped by domain and day
        ROW_NUMBER() OVER(PARTITION BY _domain, _recordingid, _visitorid, EXTRACT(DATE FROM _timestamp) ORDER BY _timestamp) AS _order
      FROM
        `x-marketing.blend360.db_web_engagements_log`
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
      *
    FROM
    (
      SELECT
        CONCAT(_visitorid, _timestamp) AS _key,
        STRING_AGG( _entrypage, ", \n" ) OVER(PARTITION BY _visitorid, DATE(_timestamp) ORDER BY _timestamp) AS _webActivity,
        STRING_AGG( _entryurl, ", \n"  ) OVER(PARTITION BY _visitorid, DATE(_timestamp) ORDER BY _timestamp) AS _webActivityURL,    
        #_fullurl AS _fullurl,
        -- ROW_NUMBER() OVER(PARTITION BY _userid, DATE(_timestamp) ORDER BY _timestamp) _page_order
      FROM
        mouseflow_recording
    )
    JOIN
      (SELECT DISTINCT CONCAT(_visitorid, _timestamp) AS _key FROM mouseflow_recording WHERE _order = 1) USING(_key)
    
  )
SELECT
  mouseflow_recording.*EXCEPT(_order),
  webtrack_6sense.* EXCEPT(_ipAddr),
  mouseflow_pageviews.* EXCEPT (_key)
FROM 
  mouseflow_recording 
LEFT JOIN 
  mouseflow_pageviews 
    ON mouseflow_recording._order = 1 
    AND CONCAT(mouseflow_recording._visitorid, mouseflow_recording._timestamp) = mouseflow_pageviews._key
LEFT JOIN 
  webtrack_6sense 
    ON mouseflow_recording._ipAddr = webtrack_6sense._ipAddr
WHERE
  _order = 1
  AND _domain != '2x.marketing'
  AND _country = 'United States'
;



------- updating certain fields for kickfire -------
UPDATE
  `x-marketing.blend360.dashboard_mouseflow_kickfire` blend360_mflow
SET
  blend360_mflow._domain    = kickfire._website,
  blend360_mflow._name       = kickfire._name,
  blend360_mflow._location   = kickfire._location,
  blend360_mflow._industry   = kickfire._category,
  blend360_mflow._revenue    = kickfire._revenue,
  blend360_mflow._employees  = kickfire._employees,
  blend360_mflow._phone      = kickfire._phone
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
  blend360_mflow._ipAddr = kickfire._ipaddr
  AND (LENGTH(blend360_mflow._domain) = 0
    OR blend360_mflow._domain IS NULL);