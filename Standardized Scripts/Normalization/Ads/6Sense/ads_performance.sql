
-- Get ads data
WITH ads AS (
    
    SELECT
        * EXCEPT(rownum)
    FROM (

        SELECT DISTINCT 
            _campaignid,
            _name AS _advariation,
            _6senseid AS _adid,
            CAST(REPLACE(REPLACE(_spend, '$', ''), ',', '') AS FLOAT64) AS _spend,
            CAST(REPLACE(_clicks, ',', '') AS INTEGER) AS _clicks, 
            CAST(REPLACE(_impressions, ',', '') AS INTEGER) AS _impressions, 
            PARSE_DATE('%m/%e/%Y', _date) AS _date,

            ROW_NUMBER() OVER(
                PARTITION BY _campaignid, _6senseid, _date
                ORDER BY PARSE_DATE('%m/%e/%Y', _extractdate) DESC
            )
            AS rownum

        FROM
            `sandler_mysql.db_daily_campaign_performance`
        WHERE
            _datatype = 'Ad'
    
    )
    WHERE rownum = 1

),

-- Get campaign level fields
campaign_fields AS (
    
    SELECT
        * EXCEPT(_date, rownum)
    FROM (

        SELECT
            _campaignid,
            PARSE_DATE('%m/%e/%Y', _date) AS _date,
            _status AS _campaignstatus,
            
            CASE 
                WHEN _accountsnewlyengagedlifetime = '-'
                THEN 0
                ELSE CAST(_accountsnewlyengagedlifetime AS INT64)
            END AS _newly_engaged_accounts,

            CASE 
                WHEN _accountswithincreasedengagementlifetime = '-'
                THEN 0
                ELSE CAST(_accountswithincreasedengagementlifetime AS INT64)
            END AS _increased_engagement_accounts,

            ROW_NUMBER() OVER(
                PARTITION BY _campaignid
                ORDER BY PARSE_DATE('%m/%e/%Y', _date) DESC
            ) 
            AS rownum

        FROM 
            `sandler_mysql.db_daily_campaign_performance`
        WHERE
            _datatype = 'Campaign'

    )
    WHERE rownum = 1

),

-- Get airtable data for 6sense
airtable_6sense AS (

    SELECT DISTINCT  
        _campaignid, 
        _campaignname, 
        '6sense Advertising' AS _campaigntype,
        _screenshot AS _adscreenshot,
        _adid
    FROM
        `sandler_mysql.optimization_airtable_ads_6sense`
    WHERE
        _campaignid IS NOT NULL
    
),

-- Get airtable data for Linkedin 
airtable_linkedin AS (

    SELECT 
        * EXCEPT(_segment)
    FROM (

        SELECT DISTINCT 
            _campaignid, 
            _name AS _campaignname,
            'LinkedIn' AS _campaigntype,  
            _segmentname AS _segment
        FROM
            `sandler_mysql.db_airtable_6sense_segment`
        WHERE 
            _sdc_deleted_at IS NULL

    ) main

    JOIN (

        SELECT DISTINCT 
            _screenshot AS _adscreenshot,
            _adname AS _advariation,
            _segment
        FROM 
            `sandler_mysql.optimization_airtable_ads_linkedin` 
        WHERE 
            _segment != ''
        AND 
            _sdc_deleted_at IS NULL

    ) side

    USING(_segment)

),

-- Combine 6sense ads data with Linkedin ads data
combined_data AS (

    SELECT
        airtable_6sense._campaignname,
        airtable_6sense._campaigntype,
        campaign_fields._campaignstatus,
        ads.*,
        airtable_6sense._adscreenshot,
        campaign_fields._newly_engaged_accounts,
        campaign_fields._increased_engagement_accounts
    FROM 
        ads
    JOIN
        airtable_6sense 
    USING (
        _adid,
        _campaignid
    )
    LEFT JOIN 
        campaign_fields
    
    USING (_campaignid)

    UNION ALL

    SELECT
        airtable_linkedin._campaignname,
        airtable_linkedin._campaigntype,
        campaign_fields._campaignstatus,
        ads.*,
        airtable_linkedin._adscreenshot,
        campaign_fields._newly_engaged_accounts,
        campaign_fields._increased_engagement_accounts
    FROM 
        ads
    JOIN
        airtable_linkedin 
    USING (
        _advariation,
        _campaignid
    )
    LEFT JOIN 
        campaign_fields
    
    USING (_campaignid)

),

-- Add campaign numbers to each ad
campaign_numbers AS (

    SELECT
        *
    FROM
        combined_data 
    -- Get accounts that are being targeted
    JOIN (
        
        SELECT DISTINCT
            _campaignid,
            COUNT(*) AS _target_accounts
        FROM (

            SELECT DISTINCT 
                main._6sensecompanyname,
                main._6sensecountry,
                main._6sensedomain,
                main._segmentname,
                side._campaignid
            FROM 
                `sandler_mysql.db_segment_target_accounts` main
            
            JOIN 
                `sandler_mysql.db_airtable_6sense_segment` side
            
            USING(_segmentname)

            WHERE 
                side._sdc_deleted_at IS NULL

        )
        GROUP BY 
            1

    ) target

    USING(_campaignid)

    -- Get accounts that have been reached
    JOIN (

        SELECT DISTINCT
            _campaignid,
            COUNT(*) AS _reached_accounts
        FROM (

            SELECT DISTINCT 
                main._6sensecompanyname,
                main._6sensecountry,
                main._6sensedomain,
                main._segmentname,
                side._campaignid
            FROM 
                `sandler_mysql.db_segment_target_accounts` main
            
            JOIN 
                `sandler_mysql.db_airtable_6sense_segment` side
            
            USING(_segmentname)

            JOIN 
                `sandler_mysql.db_campaign_reached_account_new` extra

            USING(
                _6sensecompanyname,
                _6sensecountry,
                _6sensedomain,
                _campaignid
            )

            WHERE 
                side._sdc_deleted_at IS NULL
            
        )
        GROUP BY 
            1

    ) reach

    USING(_campaignid)

    -- Get accounts that have been clicks
    JOIN (

        SELECT DISTINCT
            _campaignid,
            SUM(CASE WHEN _clicks > 0 THEN 1 ELSE 0 END) AS _clicked_accounts
        FROM (

            SELECT DISTINCT 
                main._6sensecompanyname,
                main._6sensecountry,
                main._6sensedomain,
                main._segmentname,
                side._campaignid,
                MAX(CAST(extra._clicks AS INT64)) AS _clicks
            FROM 
                `sandler_mysql.db_segment_target_accounts` main
            
            JOIN 
                `sandler_mysql.db_airtable_6sense_segment` side
            
            USING(_segmentname)

            JOIN 
                `sandler_mysql.db_campaign_reached_account_new` extra

            USING(
                _6sensecompanyname,
                _6sensecountry,
                _6sensedomain,
                _campaignid
            )

            WHERE 
                side._sdc_deleted_at IS NULL

            GROUP BY 1, 2, 3, 4, 5
            
        )
        GROUP BY 
            1

    ) click

    USING(_campaignid)

    -- Get accounts that are 6QA
    JOIN (

        SELECT DISTINCT
            _campaignid,
            COUNT(*) AS _6qa_accounts
        FROM (
            
            SELECT DISTINCT 
                main._6sensecompanyname,
                main._6sensecountry,
                main._6sensedomain,
                main._segmentname,
                side._campaignid,

            FROM 
                `sandler_mysql.db_segment_target_accounts` main
            
            JOIN 
                `sandler_mysql.db_airtable_6sense_segment` side
            
            USING(_segmentname)

            JOIN 
                `sandler.db_6sense_account_current_state` extra
            
            USING(
                _6sensecompanyname,
                _6sensecountry,
                _6sensedomain
            )

            WHERE 
                extra._6qa_date IS NOT NULL
            AND 
                side._sdc_deleted_at IS NULL

        )
        GROUP BY 
            1

    )

    USING(_campaignid)

),

-- Get frequency of ad occurrence of each campaign
total_ad_occurrence_per_campaign AS (

    SELECT
        *,
        
        COUNT(*) OVER (
            PARTITION BY _campaignid
        ) 
        AS _occurrence

    FROM 
        campaign_numbers

),

-- Reduced the campaign numbers by the occurrence
reduced_campaign_numbers AS (

    SELECT
        *,
        _newly_engaged_accounts / _occurrence AS _reduced_newly_engaged_accounts,
        _increased_engagement_accounts / _occurrence AS _reduced_increased_engagement_accounts,
        _target_accounts / _occurrence AS _reduced_target_accounts,
        _reached_accounts / _occurrence AS _reduced_reached_accounts,
        _clicked_accounts / _occurrence AS _reduced_clicked_accounts,
        _6qa_accounts / _occurrence AS _reduced_6qa_accounts
    FROM 
        total_ad_occurrence_per_campaign

)

SELECT * FROM reduced_campaign_numbers;


