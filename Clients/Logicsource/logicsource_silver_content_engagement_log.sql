--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------------ Updated Content script ------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------
TRUNCATE TABLE `x-marketing.logicsource.db_content_engagements_log`;

--CREATE OR REPLACE TABLE `x-marketing.logicsource.db_content_engagements_log` AS
INSERT INTO `x-marketing.logicsource.db_content_engagements_log` (
  _type,
  _wiseid,
  _publishdate,
  _url,
  _status,
  _requester,
  _lastreviewed,
  _airtableid,
  _vertical,
  _gating,
  _requestdate,
  _buyerstage,
  _writer,
  _persona,
  _completedate,
  _summary,
  _startdate,
  _created,
  _title,
  _intentkeyword,
  _contentID,
  _visitDate,
  _pageviews,
  _visitorID,
  _domain
)
WITH content AS (
  SELECT DISTINCT
    *
  FROM `x-marketing.logicsource_mysql.content_wise_mapping`
  WHERE
    _url != ''
),
web AS (
  SELECT DISTINCT
    _visitorid,
    _domain,
    _timestamp,
    _fullurl,
  FROM `x-marketing.logicsource.db_web_engagements_log`
  ORDER BY
    _timestamp DESC
)
SELECT DISTINCT
  content.* EXCEPT (
    _id,
    _sdc_batched_at,
    _sdc_received_at,
    _sdc_sequence,
    _sdc_table_version
  ),
  content._id AS _contentID,
  -- PARSE_DATE('%e-%b-%y', content._publishdate) AS _publishedDate,
  web._timestamp AS _visitDate,
  1 AS _pageviews,
  web._visitorid AS _visitorID,
  web._domain,
FROM content
LEFT JOIN web
  ON content._url = web._fullurl;

-- Extracting hot intent topics based on the content wise mapping
TRUNCATE TABLE `x-marketing.logicsource.report_content_topic_metrics`;

INSERT INTO `x-marketing.logicsource.report_content_topic_metrics` (
  _domain,
  _keywords,
  _visitDate,
  _contentID,
  _url,
  _pageviews
)
SELECT DISTINCT
  _domain,
  LOWER(keywords) AS _keywords,
  _visitDate,
  _contentID,
  _url,
  _pageviews
FROM `x-marketing.logicsource.db_content_engagements_log` content,
  UNNEST (SPLIT(_intentkeyword, ', ')) AS keywords
WHERE _intentkeyword IS NOT NULL
  AND _intentkeyword != ''
  AND _visitdate IS NOT NULL
ORDER BY 1;