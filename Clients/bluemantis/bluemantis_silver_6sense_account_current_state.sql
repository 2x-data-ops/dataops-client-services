-- Get all target accounts and their segments
TRUNCATE TABLE `x-marketing.bluemantis.db_6sense_account_current_state`;
INSERT INTO `x-marketing.bluemantis.db_6sense_account_current_state` (
  _6sense_company_name,
  _6sense_country,
  _6sense_domain,
  _6sense_industry,
  _6sense_employee_range,
  _6sense_revenue_range,
  _added_on,
  _data_source,
  _country_account,
  _segment,
  _first_impressions,
  _website_engagement
)
WITH target_accounts AS (
  SELECT DISTINCT 
    _6sensecompanyname AS _6sense_company_name,
    _6sensecountry AS _6sense_country,
    _6sensedomain AS _6sense_domain,
    _industrylegacy AS _6sense_industry,
    _6senseemployeerange AS _6sense_employee_range,
    _6senserevenuerange AS _6sense_revenue_range,
    CASE 
      WHEN _extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _extractdate)
      ELSE PARSE_DATE('%F', _extractdate)
    END AS _added_on,
    '6sense' AS _data_source,
    CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account,
    _segmentname AS _segment
  FROM `x-marketing.bluemantis_mysql.db_target_account`
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY _6sensecompanyname, _6sensecountry, _6sensedomain
    ORDER BY _extractdate) = 1 -- Get the LATEST date of appearance of each account
),
-- List out the unique campaign ID
list_of_campaigns AS (
  SELECT DISTINCT 
    _campaignid
  FROM `x-marketing.bluemantis_mysql.db_campaign_daily_performance`
  WHERE _datatype = 'Campaign'
),
parsed_reached_data AS (
  SELECT
    _6sensecompanyname,
    _6sensecountry,
    _6sensedomain,
    PARSE_DATE(
      CASE 
        WHEN _extractdate LIKE '%/%' THEN '%m/%e/%Y'
        ELSE '%F'
      END, 
      _latestimpression
    ) AS parsed_date,
    NULLIF(_websiteengagement, '-') AS _website_engagement,
    CONCAT(_6sensecompanyname, _6sensecountry, _6sensedomain) AS _country_account,
    _campaignid
  FROM `x-marketing.bluemantis_mysql.db_campaign_accounts_reached`
),
-- Get date when account had first impression
reached_related_info AS (
  SELECT DISTINCT 
    MIN(parsed_date) OVER (PARTITION BY _country_account) AS _first_impressions,
    _website_engagement,
    parsed._country_account
  FROM parsed_reached_data AS parsed
  JOIN list_of_campaigns AS list
    ON parsed._campaignid = list._campaignid
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY _country_account
    ORDER BY parsed_date DESC) = 1
),
-- Get the buying stage info each account
-- buying_stage_related_info AS (
--     SELECT DISTINCT
--         _previous_stage,
--         _previous_stage_order,
--         _current_stage,
--         _current_stage_order,
--         _movement,
--         _activities_on AS _movement_date,
--         _account_key
--     FROM `brp.db_6sense_buying_stages_movement`
--     WHERE _data_source = '6sense'
-- ),
-- Attach all other data parts to target accounts
combined_data AS (
    SELECT DISTINCT 
        _target.*, 
        _reached.* EXCEPT(_country_account),
        --six_qa.* EXCEPT(_account_key),
        -- stage.* EXCEPT(_account_key)   
    FROM target_accounts AS _target
    LEFT JOIN reached_related_info AS _reached 
    USING (_country_account)
    -- LEFT JOIN
    --     buying_stage_related_info AS stage
    -- USING (_account_key) 
)
SELECT 
  *
FROM combined_data;