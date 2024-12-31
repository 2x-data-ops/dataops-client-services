TRUNCATE TABLE `ridecell.db_6sense_account_current_state`;

INSERT INTO `ridecell.db_6sense_account_current_state` (
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
    _movement_date
)

WITH target AS (
    SELECT 
        master_list._sfdcaccountname AS _6sensecompanyname,
        master_list._6sensecountry AS _6sensecountry,
        master_list._sfdcwebsite AS _6sensedomain,
        target._industry AS _6senseindustry,
        target._6senseemployeerange,
        target._6senserevenuerange,
        CASE
            WHEN target._extractdate LIKE '%/%' THEN PARSE_DATE('%m/%e/%Y', target._extractdate)
            ELSE PARSE_DATE('%F', target._extractdate)
        END AS _added_on,
        '6sense' AS _data_source,
        CONCAT(master_list._sfdcaccountname, master_list._6sensecountry, master_list._sfdcwebsite) AS _country_account
    FROM `x-marketing.ridecell_mysql.ridecell_db_6s_target_account` target
    JOIN `x-marketing.ridecell_mysql.ridecell_db_sf_6s_account_list` master_list
        ON target._6sensecompanyname = master_list._6sensename
        AND target._6sensecountry = master_list._6sensecountry
        AND target._6sensedomain = master_list._6sensedomain
),
get_salesforce_info AS (
    SELECT
        salesforce.x18_digit_id__c AS sfdc_account_18_digit_id,
        salesforce.name,
        salesforce.billingcountry,
        salesforce.website,
        salesforce.ridecell_industry__c AS _ridecell_industry, 
        salesforce.account_tier__c AS _account_tier, 
        salesforce.company_linkedin_url__c AS _linkedin, 
        salesforce.original_lead_source_details__c, 
        salesforce.most_recent_lead_source_details__c, 
        salesforce.most_recent_lead_source__c,
        user.name AS user_name
    FROM `x-marketing.ridecell_salesforce.Account` salesforce
    LEFT JOIN `x-marketing.ridecell_salesforce.User` user 
        ON user.id = salesforce.ownerid
),
combined_target_data AS (
    SELECT
        target.*,
        get_salesforce_info.*
    FROM target
    LEFT JOIN get_salesforce_info
        ON target._6sensecompanyname = get_salesforce_info.name
        AND target._6sensecountry = get_salesforce_info.billingcountry
        AND target._6sensedomain = get_salesforce_info.website
),
--TARGET AGGREGATION DATA--
combined_data_target_aggregation AS (
    SELECT
        *
    FROM combined_target_data
    QUALIFY ROW_NUMBER() OVER(PARTITION BY _country_account ORDER BY _added_on ) =1
),
--REACHED AGGREGATION DATA--
reached_related_info AS (
    SELECT
        MIN(reached._latestimpression) OVER (PARTITION BY CONCAT(master_list._sfdcaccountname, master_list._sfdcbillingcountry, master_list._sfdcwebsite)) AS _first_impressions,
        CASE
            WHEN reached._websiteengagement = '-' THEN CAST(NULL AS STRING)
            ELSE reached._websiteengagement
        END AS _websiteengagement,
        CONCAT(master_list._sfdcaccountname, master_list._sfdcbillingcountry, master_list._sfdcwebsite) AS _country_account
    FROM `x-marketing.ridecell_mysql.ridecell_db_6s_reached_account` reached
    JOIN `x-marketing.ridecell_mysql.ridecell_db_sf_6s_account_list` master_list
        ON reached._6sensecompanyname = master_list._6sensename
        AND reached._6sensecountry = master_list._6sensecountry
        AND reached._6sensedomain = master_list._6sensedomain
    WHERE _campaignid IN (  SELECT DISTINCT 
                                CAST(_campaignid AS STRING)
                            FROM `x-marketing.ridecell_campaign.Campaigns`
                            WHERE _campaignid IS NOT NULL)
    QUALIFY ROW_NUMBER() OVER (PARTITION BY CONCAT(master_list._sfdcaccountname, master_list._sfdcbillingcountry, master_list._sfdcwebsite) ORDER BY reached._latestimpression DESC) = 1  
),
--6QA DATA--
get_six_qa_account_in_sf AS (
    SELECT
        salesforceacc.account6qastartdate6sense__c AS _6qa_date,
        true AS _is_6qa,
        CONCAT(target._6sensecompanyname, target._6sensecountry, target._6sensedomain) AS _country_account
    FROM `x-marketing.ridecell_salesforce.Account` salesforceacc
    JOIN target
        ON target._6sensecompanyname = salesforceacc.name 
        AND target._6sensecountry = salesforceacc.billingcountry 
        AND target._6sensedomain = salesforceacc.website
    WHERE account6qa6sense__c = true
),
--BUYING STAGES DATA--
buying_stage_related_info AS (
    SELECT DISTINCT
        _previous_stage,
        _previous_stage_order,
        _current_stage,
        _current_stage_order,
        _movement,
        _activities_on AS _movement_date,
        _country_account,
    FROM `ridecell.db_6sense_buying_stages_movement`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY _country_account ORDER BY _activities_on DESC) = 1
),
--COMBINED ALL DATA--
combined_all_aggregation AS (
    SELECT
        combined_data_target_aggregation.* EXCEPT(name, billingcountry, website),
        reached_related_info.* EXCEPT(_country_account),
        get_six_qa_account_in_sf.* EXCEPT(_country_account),
        buying_stage_related_info.* EXCEPT(_country_account)
    FROM combined_data_target_aggregation
    LEFT JOIN reached_related_info
        USING (_country_account)
    LEFT JOIN get_six_qa_account_in_sf
        USING (_country_account)
    LEFT JOIN buying_stage_related_info
        USING (_country_account)
    QUALIFY ROW_NUMBER() OVER(PARTITION BY _country_account ORDER BY _added_on  ) =1
)
SELECT
    *
FROM combined_all_aggregation;