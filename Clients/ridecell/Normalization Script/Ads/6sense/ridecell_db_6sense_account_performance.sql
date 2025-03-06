TRUNCATE TABLE `ridecell.db_6sense_account_performance`;

INSERT INTO `ridecell.db_6sense_account_performance` (
    _6sensecompanyname
    _6sensecountry
    _6sensedomain
    segment_name
    _campaignname
    _is_reached
    _has_clicks
    _has_impressions
)
-- Get all target accounts and their campaigns
WITH target_accounts AS (
    SELECT DISTINCT 
        master_list._sfdcaccountname AS _6sensecompanyname,
        master_list._sfdcbillingcountry AS _6sensecountry,
        master_list._sfdcwebsite AS _6sensedomain,     
        side.segment_name,
        CAST(side._campaignid AS STRING) AS _campaignid,
        side.campaign_name AS _campaignname
    FROM `x-marketing.ridecell_mysql.ridecell_db_6s_target_account` target
    JOIN `x-marketing.ridecell_mysql.ridecell_db_sf_6s_account_list` master_list
        ON target._6sensecompanyname = master_list._6sensename
        AND target._6sensecountry = master_list._6sensecountry
        AND target._6sensedomain = master_list._6sensedomain 
    JOIN `x-marketing.ridecell_campaign.Campaigns` side
        ON target._segmentname = side.segment_name
),
-- Mark those target accounts that have been reached by their campaigns
reached_accounts AS (
    SELECT DISTINCT 
        main.* EXCEPT(_campaignid),
        CASE 
            WHEN side._campaignid IS NOT NULL THEN true
            END AS _is_reached,
        CASE 
            WHEN SAFE_CAST(side._clicks AS INTEGER) > 0 THEN true 
            END AS _has_clicks,
        CASE 
            WHEN SAFE_CAST(side._impressions AS INTEGER) > 0 THEN true 
            END AS _has_impressions
    FROM target_accounts AS main
    JOIN `x-marketing.ridecell_mysql.ridecell_db_sf_6s_account_list` master_list
        ON main._6sensecompanyname = master_list._sfdcaccountname
        AND main._6sensecountry = master_list._sfdcbillingcountry
        AND main._6sensedomain = master_list._sfdcwebsite 
    LEFT JOIN `x-marketing.ridecell_mysql.ridecell_db_6s_reached_account` side
        ON master_list._6sensename = side._6sensecompanyname
        AND master_list._6sensecountry = side._6sensecountry
        AND master_list._6sensedomain = side._6sensedomain
        AND main._campaignid = side._campaignid
)
SELECT
    *
FROM reached_accounts;