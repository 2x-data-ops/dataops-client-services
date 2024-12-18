TRUNCATE TABLE `ridecell.db_6sense_engagement_log`;

INSERT INTO `ridecell.db_6sense_engagement_log` (
    _6sensecompanyname,
    _6sensecountry,
    _6sensedomain,
    _6senseindustry,
    _6senseemployeerange,
    _6senserevenuerange,
    _added_on,
    _data_source,
    _country_account,
    sfdc_account_18_digit_id,
    _ridecell_industry,
    _account_tier,
    _linkedin,
    original_lead_source_details__c,
    most_recent_lead_source_details__c,
    most_recent_lead_source__c,
    user_name,
    _first_impressions,
    _websiteengagement,
    _6qa_date,
    _is_6qa,
    _previous_stage,
    _previous_stage_order,
    _current_stage,
    _current_stage_order,
    _movement,
    _movement_date,
    _timestamp,
    _engagement,
    _engagement_data_source,
    _description,
    _notes,
    _total_6sense_campaign_reached,
    _total_6sense_ad_clicks,
    _total_6sense_form_fills,
    _total_webpage_visits,
    _total_bombora_topics,
    _total_searched_keywords,
    _total_email_open,
    _total_email_click,
    _no_engagement
)

WITH target_accounts AS (
    SELECT
        *
    FROM `ridecell.db_6sense_account_current_state`
),
-- Reached information as left join later on
-- Classify as 'reached' when it do have campaignid
reached_accounts_data AS (
    SELECT DISTINCT
        CAST(reached._clicks AS INTEGER) AS _clicks,
        CAST(reached._influencedformfills AS INTEGER) AS _influencedformfills,
        reached._latestimpression,
        CASE
        WHEN reached._extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', reached._extractdate)
        ELSE PARSE_DATE('%F', reached._extractdate)
        END AS _activities_on, 
        reached._campaignid,
        -- Need label to distingush 6sense and Linkedin campaigns
        side.campaign_name AS _campaignname,
        CONCAT(master_list._sfdcaccountname, master_list._sfdcbillingcountry, master_list._sfdcwebsite) AS _country_account
    FROM `x-marketing.ridecell_mysql.ridecell_db_6s_reached_account` reached
    JOIN `x-marketing.ridecell_mysql.ridecell_db_sf_6s_account_list` master_list
        ON reached._6sensecompanyname = master_list._6sensename
        AND reached._6sensecountry = master_list._6sensecountry
        AND reached._6sensedomain = master_list._6sensedomain
    JOIN (  SELECT DISTINCT 
                CAST(_campaignid AS STRING) AS _campaignid, 
                campaign_name,
            FROM `x-marketing.ridecell_campaign.Campaigns`
    ) side
    USING(_campaignid) 
    -- QUALIFY ROW_NUMBER() OVER (PARTITION BY CONCAT(reached._6sensecompanyname, reached._6sensecountry, reached._6sensedomain) ORDER BY reached._latestimpression DESC) = 1
    ),
-- All 6sense activities
-- Union all for easy reading and modularity
combined_sixsense_activities AS (
  SELECT * FROM (
    -- Campaign Reached
    SELECT DISTINCT 
        _country_account, 
        '' AS sfdc_account_18_digit_id,
        MIN(DATE(_latestimpression)) OVER(
            PARTITION BY _country_account, _campaignname
            ORDER BY _latestimpression
        ) AS _timestamp,
        '6sense Campaign Reached' AS _engagement,
        '6sense' AS _engagement_data_source, 
        _campaignname AS _description, 
        1 AS _notes,
        NULL as _old_notes  -- Added for uniformity
    FROM reached_accounts_data

    UNION ALL

    -- Ad Clicks
    SELECT DISTINCT 
        _country_account,
        '' AS sfdc_account_18_digit_id, 
        _activities_on AS _timestamp,
        '6sense Ad Clicks' AS _engagement, 
        '6sense' AS _engagement_data_source,
        _campaignname AS _description,  
        _clicks AS _notes,
        LAG(_clicks) OVER(
            PARTITION BY _country_account, _campaignname
            ORDER BY _activities_on
        ) AS _old_notes
    FROM reached_accounts_data 
    WHERE _clicks >= 1

    UNION ALL

    -- Form Fills
    SELECT DISTINCT  
        _country_account, 
        '' AS sfdc_account_18_digit_id,
        _activities_on AS _timestamp,
        '6sense Influenced Form Fill' AS _engagement, 
        '6sense' AS _engagement_data_source,
        _campaignname AS _description,  
        _influencedformfills AS _notes,
        LAG(_influencedformfills) OVER(
            PARTITION BY _country_account, _campaignname
            ORDER BY _activities_on
        ) AS _old_notes
    FROM reached_accounts_data 
    WHERE _influencedformfills >= 1
    ) all_activities
    -- Apply increase check only for clicks and form fills
    WHERE (_engagement = '6sense Campaign Reached' OR (_notes - COALESCE(_old_notes, 0)) >= 1)
),
-- Main sales intelligence data
sales_intelligence AS (
    SELECT    
        _activitytype,
        _activitytarget,
        _activitymetainfo,
        _contactname,
        _email,
        _accountname,
    CASE 
        WHEN _crmaccountid LIKE 'CMA%' THEN SUBSTR(_crmaccountid, 4)
        ELSE _crmaccountid
    END AS _crmaccountid,
    CASE 
        WHEN _activitydate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', _activitydate)
        ELSE PARSE_DATE('%F', _activitydate)
    END AS _date
    FROM `x-marketing.ridecell_mysql.ridecell_db_sales_intelligence_report`    
),
-- Salesforce data as truth of source
sfdc AS (
    SELECT
        CONCAT(masterlist._sfdcaccountname, masterlist._sfdcbillingcountry, masterlist._sfdcwebsite) AS _country_account,
        salesforce.ridecell_industry__c AS _ridecell_industry,
        salesforce.account_tier__c AS _account_tier,
        salesforce.company_linkedin_url__c AS _linkedin,
        salesforce.original_lead_source_details__c,
        salesforce.most_recent_lead_source_details__c,
        salesforce.most_recent_lead_source__c,
        salesforce.x18_digit_id__c AS sfdc_account_18_digit_id
    FROM `x-marketing.ridecell_salesforce.Account` salesforce
    JOIN `x-marketing.ridecell_mysql.ridecell_db_sf_6s_account_list` masterlist
        ON salesforce.name = masterlist._sfdcaccountname
        AND salesforce.billingcountry = masterlist._sfdcbillingcountry
        AND salesforce.website = masterlist._sfdcwebsite
),
-- Count same activities per country_account
activity_counts AS (
    SELECT 
        sales_intelligence._activitytype,
        sales_intelligence._activitytarget,
        sales_intelligence._activitymetainfo,
        sales_intelligence._contactname,
        sales_intelligence._email,
        sales_intelligence._date,
        sfdc._country_account,
        sfdc.sfdc_account_18_digit_id,
        COUNT(*) AS _count
    FROM sales_intelligence
    JOIN sfdc
        ON sfdc.sfdc_account_18_digit_id = sales_intelligence._crmaccountid
    GROUP BY  
        sales_intelligence._activitytype,
        sales_intelligence._activitytarget,
        sales_intelligence._activitymetainfo,
        sales_intelligence._contactname,
        sales_intelligence._email,
        sales_intelligence._date,
        sfdc._country_account,
        sfdc_account_18_digit_id
),
-- The aggregated activity count table table is supplemented by salesforce data
aggregate_sales_intel_data AS (
    SELECT 
        activity_counts.* EXCEPT (sfdc_account_18_digit_id),
        sfdc_details._ridecell_industry,
        sfdc_details._account_tier,
        sfdc_details._linkedin,
        sfdc_details.original_lead_source_details__c,
        sfdc_details.most_recent_lead_source_details__c,
        sfdc_details.most_recent_lead_source__c,
        sfdc_details.sfdc_account_18_digit_id
    FROM activity_counts
    LEFT JOIN sfdc sfdc_details
        ON sfdc_details.sfdc_account_18_digit_id = activity_counts.sfdc_account_18_digit_id
),
-- All sales_intelligence data
sales_intelligence_campaign_reached AS (
    SELECT DISTINCT 
        _country_account,
        sfdc_account_18_digit_id,
        _date AS _timestamp,
        CASE 
        -- Campaign reached activities
        WHEN _activitytype LIKE '%Web Visit%' THEN '6sense Web Visits'
        WHEN _activitytype LIKE '%KW Research%' THEN '6sense Searched Keywords'
        WHEN REGEXP_CONTAINS(_activitytype,'Email Open|Email Click') THEN CONCAT(_activitytype, 'ed')
        -- Other engagements
        WHEN REGEXP_CONTAINS(_activitytype,'Bombora') THEN 'Bombora Topic Surged'
        WHEN REGEXP_CONTAINS(_activitytype,'Form Fill') THEN 'Form Filled'
        WHEN REGEXP_CONTAINS(_activitytype,'Email Reply') THEN 'Email Replied'
        WHEN REGEXP_CONTAINS(_activitytype,'Page Click') THEN 'Webpage Clicked'
        WHEN REGEXP_CONTAINS(_activitytype,'Submit') THEN 'Submitted'
        WHEN REGEXP_CONTAINS(_activitytype,'Video Play') THEN 'Video Played'
        WHEN REGEXP_CONTAINS(_activitytype,'Attend') THEN _activitytype
        WHEN REGEXP_CONTAINS(_activitytype,'Register') THEN _activitytype
        ELSE _activitytype -- Keep original activity type for reached/clicks
        END AS _engagement,
        'Sales Intelligence' AS _engagement_data_source,
        CASE   
        WHEN _activitytarget = '' AND NOT REGEXP_CONTAINS(_activitytype,'Reached|Ad Click|Web Visit|KW Research|Email Open|Email Click')
        THEN CONCAT('Contact: ', IF(_contactname != '', _contactname, _email))
        ELSE _activitytarget
        END AS _description,
        _count AS _notes,
        -- NULL AS _old_notes
    FROM aggregate_sales_intel_data
),
target_w_6sense AS (
    SELECT
        target_accounts.*,
        combined_sixsense_activities.* EXCEPT (_old_notes, _country_account, sfdc_account_18_digit_id)
    FROM target_accounts
    LEFT JOIN combined_sixsense_activities
        USING (_country_account)
),
target_w_si AS (
    SELECT
        target_accounts.*,
        sales_intelligence_campaign_reached.* EXCEPT (_country_account, sfdc_account_18_digit_id)
    FROM target_accounts
    LEFT JOIN sales_intelligence_campaign_reached
        USING (_country_account)
),
-- Combined 6sense data (with target) and sales intelligense data (with target)
combined_data AS (
    SELECT * FROM target_w_6sense
    UNION ALL
    SELECT * FROM target_w_si
),
-- Get accumulated values for each engagement
accumulated_engagement_values AS (
    SELECT
        combined_data.*,
        -- The aggregated values
        SUM(CASE WHEN _engagement = '6sense Campaign Reached' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6sense_campaign_reached,
        SUM(CASE WHEN _engagement = '6sense Ad Clicks' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6sense_ad_clicks,
        SUM(CASE WHEN _engagement = '6sense Influenced Form Fill' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_6sense_form_fills,
        SUM(CASE WHEN _engagement = '6sense Website Visit' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_webpage_visits,
        SUM(CASE WHEN _engagement = 'Bombora Topic Surged' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_bombora_topics,
        SUM(CASE WHEN _engagement = '6sense Searched Keywords' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_searched_keywords,
        SUM(CASE WHEN _engagement = 'Email Opened' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_email_open,
        SUM(CASE WHEN _engagement = 'Email Clicked' THEN _notes ELSE 0 END) OVER(PARTITION BY _country_account) AS _total_email_click
    FROM combined_data     
),
-- Add custom no_engagement column
no_engagement_column AS (
    SELECT
        _country_account,
        STRING_AGG(DISTINCT _engagement, ',' ORDER BY _engagement) AS distinct_engagement,
        ARRAY_LENGTH(SPLIT(STRING_AGG(DISTINCT _engagement, ',' ORDER BY _engagement), ',')) as engagement_count
    FROM accumulated_engagement_values
    GROUP BY _country_account
),
-- No engagement when satisfies these 3 condition
no_engagement_table AS (
    SELECT
        _country_account,
        CASE
            WHEN REGEXP_CONTAINS(distinct_engagement, 'Bombora Topic Surged')
            AND REGEXP_CONTAINS(distinct_engagement, '6sense Searched Keywords')
            AND REGEXP_CONTAINS(distinct_engagement, '6sense Campaign Reached')
            AND engagement_count = 3 THEN 'True'
            ELSE NULL
        END AS _no_engagement
    FROM no_engagement_column
)
-- Adding no_engagement column to main table
SELECT
    accumulated_engagement_values.*,
    no_engagement_table._no_engagement
FROM accumulated_engagement_values
LEFT JOIN no_engagement_table
    USING (_country_account);