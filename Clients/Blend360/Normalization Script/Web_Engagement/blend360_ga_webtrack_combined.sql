-- CREATE OR REPLACE TABLE `x-marketing.blend360.ga_webtrack_combined_data` AS
TRUNCATE TABLE `x-marketing.blend360.ga_webtrack_combined_data`;

INSERT INTO `x-marketing.blend360.ga_webtrack_combined_data` (
	_date,
	_company_name,
	_company_website,
	_industry,
	_country,
	_page_url,
	_default_channel_grouping,
	_avg_session_duration,
	_sessions,
	_users,
	_new_users,
	_bounces,
	_bounce_rate,
	_page_views,
	_unique_page_views,
	_goal_conversion_rate,
	_data_source,
	_bu,
	_target_type
)
WITH web_visits_data AS (

    SELECT  
        ga_date AS _date,
        ga_dimension10 AS _company_name,
        ga_dimension11 AS _company_website,
        ga_dimension12 AS _industry,
        ga_country AS _country,
        ga_pagepath AS _page_url,
        ga_channelgrouping AS _default_channel_grouping,
        ga_avgsessionduration AS _avg_session_duration,
        ga_sessions AS _sessions,
        ga_users AS _users,
        ga_newusers AS _new_users,
        ga_bounces AS _bounces,
        ga_bouncerate / 100 AS _bounce_rate,
        ga_pageviews AS _page_views,
        ga_uniquepageviews AS _unique_page_views,
        ga_goalconversionrateall / 100 AS _goal_conversion_rate,
        'Dealfront' AS _data_source,
        1 AS _order
    FROM 
        `x-marketing.blend360_google_analytics.Dealfront_Company_Data` 
    WHERE
        ga_pagepath IS NOT NULL

    UNION ALL

    SELECT  
        ga_date AS _date,
        ga_dimension2 AS _company_name,
        ga_dimension3 AS _company_website,
        ga_dimension4 AS _industry,
        ga_country AS _country,
        ga_pagepath AS _page_url,
        ga_channelgrouping AS _default_channel_grouping,
        ga_avgsessionduration AS _avg_session_duration,
        ga_sessions AS _sessions,
        ga_users AS _users,
        ga_newusers AS _new_users,
        ga_bounces AS _bounces,
        ga_bouncerate / 100 AS _bounce_rate,
        ga_pageviews AS _page_views,
        ga_uniquepageviews AS _unique_page_views,
        ga_goalconversionrateall / 100 AS _goal_conversion_rate,
        'Demandbase' AS _data_source,
        2 AS _order
    FROM 
        `x-marketing.blend360_google_analytics.Demandbase_Company_Data`
    WHERE
        ga_pagepath IS NOT NULL 

    UNION ALL

    SELECT DISTINCT
        TIMESTAMP(DATE(_timestamp)) AS _date,
        _name AS _company_name,
        _domain AS _company_website,
        _industry,
        _country,
        _entryPage,
        CAST(NULL AS STRING),
        CAST(NULL AS STRING),
        CAST(NULL AS INTEGER),
        CAST(NULL AS INTEGER),
        CAST(NULL AS INTEGER),
        CAST(NULL AS INTEGER),
        CAST(NULL AS FLOAT64),

        COUNT(_entryPage) OVER(
            PARTITION BY 
                _domain,
                DATE(_timestamp)
        ) 
        AS _totalPages,
        
        COUNT(DISTINCT _entryPage) OVER(
            PARTITION BY 
                _domain,
                DATE(_timestamp)
        ) 
        AS _uniquePages,
        
        CAST(NULL AS FLOAT64),
        'Webtrack' AS _data_source,
        3 AS _order
    FROM 
        `x-marketing.blend360.dashboard_mouseflow_kickfire` 

),

remove_duplicate_web_visits AS (

    SELECT 
        *
        FROM 
            web_visits_data
        WHERE 
            _company_website IS NOT NULL
    QUALIFY ROW_NUMBER() OVER(
                PARTITION BY _date, _company_website, _page_url
                ORDER BY _order
            ) = 1
    UNION ALL 

    SELECT 
        *
    FROM 
        web_visits_data
    WHERE 
        _company_website IS NULL

),

gsheet_mapping AS (

    SELECT  
        blend360_company_name AS _company_name,
        website_url AS _company_website,
        bu AS _bu
    FROM 
        `x-marketing.blend360_google_sheet.Mapping`

),

combined_data AS (

    SELECT
        main.* EXCEPT(_order),
        side._bu,
        CASE
            WHEN side._bu = 'DSX' THEN 'Targeted'
            WHEN side._bu = 'BTS' THEN 'Targeted'
            WHEN side._bu = 'Both' THEN 'Targeted'
            ELSE 'Non-Targeted'
        END AS _target_type
    FROM 
        remove_duplicate_web_visits AS main 
    
    LEFT JOIN 
        gsheet_mapping AS side 
    ON 
        main._company_website = side._company_website

)

SELECT * FROM combined_data;

