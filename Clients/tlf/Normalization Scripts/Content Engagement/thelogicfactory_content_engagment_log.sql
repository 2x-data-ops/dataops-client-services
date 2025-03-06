TRUNCATE TABLE `x-marketing.thelogicfactory.db_content_engagements_log`;

INSERT INTO `x-marketing.thelogicfactory.db_content_engagements_log`

---Select data from table content_wise join with web_engagement on url---
SELECT
  content.* EXCEPT (_id,
                    _sdc_batched_at,
                    _sdc_received_at,
                    _sdc_sequence,
                    _sdc_table_version
                    -- _sdc_deleted_at
                    ),
  content._id AS _contentID,
  web._timestamp AS _visitDate,
  1 AS _pageviews,
  web._visitorid AS _visitorID,
  web._domain
FROM (
  SELECT *
  FROM
    `x-marketing.thelogicfactory_mysql.content_wise_mapping`
  WHERE _url != ''
) AS content
LEFT JOIN (
  SELECT
    DISTINCT _visitorid,
    _domain,
    _timestamp,
    _fullurl,
  FROM
    `x-marketing.thelogicfactory.db_web_engagements_log`
  ORDER BY
    _timestamp DESC
) AS web
ON content._url = web._fullurl;


---Extract hot intent topics---
--Only insert if intentkeyword and visitdate not null--
--separate each keyword by comma

TRUNCATE TABLE `x-marketing.thelogicfactory.report_content_topic_metrics`;
INSERT INTO `x-marketing.thelogicfactory.report_content_topic_metrics`
SELECT DISTINCT
  _domain,
  LOWER(keywords) AS _keywords,
  _visitDate,
  _contentID,
  _url,
  _pageviews
FROM
 `x-marketing.thelogicfactory.db_content_engagements_log` content,
  UNNEST(SPLIT(_intentkeyword, ', ')) AS keywords
WHERE _intentkeyword IS NOT NULL
  AND _intentkeyword != ''
  AND _visitdate IS NOT NULL
ORDER BY 1;

