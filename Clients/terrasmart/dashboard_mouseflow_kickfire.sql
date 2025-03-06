TRUNCATE TABLE `x-marketing.terrasmart.dashboard_mouseflow_kickfire` ;
INSERT INTO `x-marketing.terrasmart.dashboard_mouseflow_kickfire`
WITH 
  mouseflow_recording AS (
    SELECT
      DISTINCT *
    FROM
    (
      SELECT
        _recordingID, 
        user_pseudo_id AS _visitorID,
        --_ipAddr,
        _timespent,
        _engagementTime,
        _timestamp,
        _country,
        --_utmsource AS _source,
        _recordingURL, 
        _page AS _entrypage, -- First touch point
        _domain, -- _website
        _name,
        --_region, 
        --_city, 
        -- _industry, ipcompany_6sense
        -- _location, irrelevant?
        -- _revenue, ipcompany_6sense
        -- _phone, ipcompany_6sense
        -- _employees, ipcompany_6sense
        -- _webActivity, STRING_AGG of all the _page
        _fullurl AS _entryURL,
        ---_utmsource,
        _utmcampaign,
        --_utmmedium,
        --_utmcontent,
       -- _utmterm,
        --_isisp,
        -- _totalPages total pages visited groupped by domain and day
        ROW_NUMBER() OVER(PARTITION BY _domain, user_pseudo_id ,EXTRACT(DATE FROM _timestamp) ORDER BY _timestamp) AS _order
      FROM
        x-marketing.terrasmart.db_web_engagements_log
    )
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
  --webtrack_6sense.* EXCEPT(_ipAddr),
  mouseflow_pageviews.* EXCEPT (_key) 
FROM 
  mouseflow_recording 
LEFT JOIN 
  mouseflow_pageviews 
    ON mouseflow_recording._order = 1 AND CONCAT(mouseflow_recording._visitorid, mouseflow_recording._timestamp) = mouseflow_pageviews._key