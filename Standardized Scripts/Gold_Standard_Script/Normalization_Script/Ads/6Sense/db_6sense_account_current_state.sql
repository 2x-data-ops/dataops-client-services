-- 6sense Account Current State
-- CREATE OR REPLACE TABLE `jellyvision_v2.db_6sense_account_current_state` AS
TRUNCATE TABLE `jellyvision_v2.db_6sense_account_current_state`;

INSERT INTO `jellyvision_v2.db_6sense_account_current_state` (
  _6sense_company_name,
  _6sense_country,
  _6sense_domain,
  _domain,
  _6sense_industry,
  _6sense_employee_range,
  _6sense_revenue_range,
  _added_on,
  _country_account,
  _first_impressions,
  _website_engagement,
  _6qa_date,
  _is_6qa,
  _6sense_score,
  _prev_stage,
  _prev_order,
  _current_stage,
  _curr_order,
  _movement,
  _movement_date,
  _crm_account_id,
  _crm_domain,
  _crm_account
)
WITH segment_target_account AS (
    SELECT DISTINCT
      _6sensecompanyname AS _6sense_company_name,
      _6sensecountry AS _6sense_country,
      _6sensedomain AS _6sense_domain,
      SPLIT(_6sensedomain, '.') [SAFE_OFFSET(0)] AS _domain,
      _industrylegacy AS _6sense_industry,
      _6senseemployeerange AS _6sense_employee_range,
      _6senserevenuerange AS _6sense_revenue_range,
      CASE
        WHEN _extractdate LIKE '0%' THEN PARSE_DATE('%m/%d/%y', _extractdate)
        ELSE PARSE_DATE('%m/%d/%Y', _extractdate)
      END AS _added_on,
      CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account
    FROM `x-marketing.jellyvision_mysql.jellyvision_db_segment_target_account`
    WHERE LENGTH(_extractdate) > 0
  ),
  -- Get the earliest date of appearance of each account
  country_account_list AS (
    SELECT DISTINCT
      MIN(
        CASE
          WHEN _extractdate LIKE '0%' THEN PARSE_DATE('%m/%d/%y', _extractdate)
          ELSE PARSE_DATE('%m/%d/%Y', _extractdate)
        END
      ) AS _added_on,
      CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account
    FROM `x-marketing.jellyvision_mysql.jellyvision_db_segment_target_account`
    WHERE LENGTH(_extractdate) > 0
    GROUP BY CONCAT(_6sensecountry, _6sensecompanyname)
  ),
  target_accounts AS (
    SELECT DISTINCT
      segment_target_account.*
    FROM segment_target_account
    JOIN country_account_list
      ON segment_target_account._country_account = country_account_list._country_account
      AND segment_target_account._added_on = country_account_list._added_on
  ),
  airtable_ads_campaignid AS (
    SELECT DISTINCT
      _campaignid
    FROM `x-marketing.jellyvision_mysql.jellyvision_optimization_airtable_ads_6sense`
    WHERE _campaignid != ''
  ),
  -- Get date when account had first impression
  reached_related_info AS (
    SELECT DISTINCT
      MIN(
        CASE
          WHEN _latestimpression LIKE '0%' THEN PARSE_DATE('%m/%d/%y', _latestimpression)
          ELSE PARSE_DATE('%m/%d/%Y', _latestimpression)
        END
      ) OVER (
        PARTITION BY CONCAT(_6sensecountry, _6sensecompanyname)
      ) AS _first_impressions,
      CASE
        WHEN _websiteengagement = '-' THEN CAST(NULL AS STRING)
        ELSE _websiteengagement
      END AS _website_engagement,
      CONCAT(_6sensecountry, _6sensecompanyname) AS _country_account
    FROM `x-marketing.jellyvision_mysql.jellyvision_db_campaign_reached_accounts`
    JOIN airtable_ads_campaignid
      USING(_campaignid)
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY CONCAT(_6sensecountry, _6sensecompanyname)
      ORDER BY (
        CASE
          WHEN _latestimpression LIKE '0%' THEN PARSE_DATE('%m/%d/%y', _latestimpression)
          ELSE PARSE_DATE('%m/%d/%Y', _latestimpression)
        END
      ) DESC
    ) = 1
  ),
  salesforce_account AS (
    SELECT DISTINCT
      DATE(account6qastartdate6sense__c) AS _6qa_date,
      account6qa6sense__c AS _is_6qa,
      accountprofilescore6sense__c AS _6sense_score,
      act.name AS _account_name,
      web_domain_name__c AS _domain,
      --COALESCE(act.shippingcountry, act.billingcountry) AS _country
    FROM `jellyvision_salesforce.Account` act
    WHERE isdeleted IS FALSE
  ),
  -- Get the date when account first became a 6QA
  six_qa_related_info AS (
    SELECT DISTINCT
      MIN(salesforce_account._6qa_date) AS _6qa_date,
      salesforce_account._is_6qa,
      _6sense_score,
      main._country_account
    FROM target_accounts AS main -- This gets all possible 6QA dates for each account
    JOIN salesforce_account -- Tie with target accounts to get their 6sense account info, instead of using Salesforce's
      ON (
      salesforce_account._domain = SPLIT(main._6sense_domain, '.') [SAFE_OFFSET(0)]
      AND (
          LENGTH(main._6sense_domain) > 0
          AND salesforce_account._domain IS NOT NULL
      ) -- AND
      --     (LENGTH(main._6sensedomain) > 0 AND side._domain IS NOT NULL)
      -- AND
      --     main._6sensecompanyname = side._account_name
      -- AND
      --     main._6sensecountry = side._country
      ) --    OR (
      --             side._domain NOT LIKE CONCAT('%', SPLIT(main._6sensedomain, '.')[SAFE_OFFSET(0)], '%')
      --         AND 
      --             main._6sensecompanyname = side._account_name
      --     ) 
    GROUP BY salesforce_account._is_6qa, _6sense_score, main._country_account
  ),
  -- Get the buying stage info each account
  buying_stage_related_info AS (
    SELECT DISTINCT
      _prev_stage,
      _prev_order,
      _current_stage,
      _curr_order,
      _movement,
      _activities_on AS _movement_date,
      _country_account,
    FROM `jellyvision.db_6sense_buying_stages_movement`
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY  _country_account
      ORDER BY _activities_on DESC
    ) = 1
  ),
  -- Attach all other data parts to target accounts
  combined_data AS (
    SELECT DISTINCT
      target.*,
      reached.* EXCEPT (_country_account),
      six_qa.* EXCEPT (_country_account),
      stage.* EXCEPT (_country_account)
    FROM target_accounts AS target
    LEFT JOIN reached_related_info AS reached
      USING (_country_account)
    LEFT JOIN six_qa_related_info AS six_qa
      USING (_country_account)
    LEFT JOIN buying_stage_related_info AS stage
      USING (_country_account)
  ),
  account_lookup AS (
    SELECT
      _crmaccountid AS _crm_account_id,
      _crmdomain AS _crm_domain,
      _crmaccount AS _crm_account,
      _6sensedomain AS _6sense_domain,
      _6senseaccount
    FROM `x-marketing.jellyvision_mysql.jellyvision_db_6sense_lookup_table`
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY _crmaccountid, _6sensedomain
      ORDER BY crmaccountid
    ) = 1
  )
SELECT
  combined_data.*,
  account_lookup.* EXCEPT (_6sense_domain, _6senseaccount)
FROM combined_data
LEFT JOIN account_lookup 
  ON CONCAT(
    LOWER(combined_data._6sense_company_name),
    combined_data._6sense_domain
    ) = CONCAT(
    LOWER(_6senseaccount),
    account_lookup._6sense_domain
  );