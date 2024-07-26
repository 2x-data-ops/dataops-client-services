---------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------ Dashboard Mouseflow Kickfire ---------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------

-- This is a mouseflow based script + 6sense buying stage

TRUNCATE TABLE `x-marketing.3x.dashboard_mouseflow_kickfire`;

INSERT INTO `x-marketing.3x.dashboard_mouseflow_kickfire`  (
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
  _totalPages,_target_accounts, _account_type
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
          WHEN _cleanpage = 'https://www.2x.marketing/' THEN 'https://www.2x.marketing/'
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
        ROW_NUMBER() OVER(PARTITION BY _domain,  _recordingid,_visitorid, EXTRACT(DATE FROM _timestamp) ORDER BY _timestamp) AS _order
      FROM
         `x-marketing.3x.db_web_engagements_log`

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
), all_engagement AS (
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
), _icp_account AS (
      SELECT DISTINCT 
      _domain, 
      coalesce(_target_accounts,0) AS _target_accounts,
      _account_type 
      FROM `x-marketing.3x.db_icp_database_log`
) SELECT all_engagement.*,
      coalesce(_target_accounts,0) AS _target_accounts,
      _account_type
       FROM all_engagement
 LEFT JOIN _icp_account ON all_engagement._domain = _icp_account._domain
--WHERE all_engagement._domain = 'alation.com'
;

------- updating certain fields for kickfire -------
UPDATE
  `x-marketing.3x.dashboard_mouseflow_kickfire` _3x_msfw
SET
  _3x_msfw._domain    = kickfire._website,
  _3x_msfw._name       = kickfire._name,
  _3x_msfw._location   = kickfire._location,
  _3x_msfw._industry   = kickfire._category,
  _3x_msfw._revenue    = kickfire._revenue,
  _3x_msfw._employees  = kickfire._employees,
  _3x_msfw._phone      = kickfire._phone,
   _3x_msfw._target_accounts = kickfire._target_accounts ,
    _3x_msfw._account_type = kickfire._account_type
FROM (
  WITH kickfire AS (
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
    _isisp = 0
), _icp_account AS (
    SELECT * EXCEPT( _order)
    FROM (

      SELECT DISTINCT 
      _domain, 
      _target_accounts,
      _account_type ,
      ROW_NUMBER() OVER(PARTITION BY _domain ORDER BY _target_accounts) AS _order
      FROM `x-marketing.3x.db_icp_database_log`
    ) WHERE _order = 1
    ) SELECT kickfire.*,
    _target_accounts,
      _account_type FROM kickfire
    LEFT JOIN _icp_account ON kickfire._website = _icp_account._domain ) kickfire
WHERE
  _3x_msfw._ipAddr = kickfire._ipaddr
  AND (LENGTH(_3x_msfw._domain) = 0
    OR _3x_msfw._domain IS NULL);


-- Add Buying Stage Field

/* UPDATE
  `x-marketing.3x.dashboard_mouseflow_kickfire` main
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
    FROM `x-marketing.sbi_mysql.db_account_initial_buying_stage` 
  )
  WHERE rownum = 1

) side
WHERE main._domain = side._6sensedomain;

 */