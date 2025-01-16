------------------------------------------------------------------------------------
--------------------------------- ACCOUNT HEALTH -----------------------------------
------------------------------------------------------------------------------------


CREATE OR REPLACE TABLE `x-marketing.terrasmart.db_account_health` AS
WITH tam_contacts AS (
    SELECT 
        main.* EXCEPT(_rownum),
        sfcontact.accountid AS _accountid, 
    FROM (
    SELECT DISTINCT
        COALESCE(crm_contact_fid, crm_lead_fid) AS _leadorcontactid,
        CASE 
          WHEN crm_contact_fid IS NOT NULL THEN "Contact"
          WHEN crm_contact_fid IS NULL THEN "Lead"
        END AS _contact_type,
        first_name AS _firstname, 
        last_name AS _lastname, 
        job_title AS _title, 
        CAST(NULL AS STRING) AS _seniority,
        email AS _email,
        RIGHT(email, LENGTH(email)-STRPOS(email,'@')) AS _domain, 
        company AS _accountname, 
        industry AS _industry, 
        CAST(NULL AS STRING) AS _tier, 
        "" AS _annualrevenue,
        ROW_NUMBER() OVER(
            PARTITION BY email 
            ORDER BY _sdc_received_at DESC
        ) _rownum
    FROM 
      `terrasmart_pardot.prospects` prosp
    WHERE 
      NOT REGEXP_CONTAINS(email, 'terrasmart|2x.marketing') 
  ) main
  LEFT JOIN
    (SELECT id, accountid FROM terrasmart_salesforce.Contact) sfcontact ON (sfcontact.id = main._leadorcontactid AND main._contact_type = 'Contact')
  WHERE _rownum = 1
),
email_engagement AS (
    SELECT 
        *
    FROM ( 
        SELECT DISTINCT 
            _email, 
            RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) AS _domain, 
            TIMESTAMP(FORMAT_TIMESTAMP('%F %I:%M:%S %Z', _timestamp)) AS _date,
            EXTRACT(WEEK FROM _timestamp) AS _week,  
            EXTRACT(YEAR FROM _timestamp) AS _year,
            _utmcampaign AS _contentTitle, 
            CONCAT("Email ", INITCAP(_engagement)) AS _engagement,
            _description
        FROM 
            (SELECT * FROM `terrasmart.db_email_engagements_log`)
        WHERE 
            /* (EXTRACT(DATE FROM _timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY ) AND CURRENT_DATE() )
            AND */ LOWER(_engagement) NOT IN ('delivered', 'downloaded', 'processed', 'deffered', 'spam', 'suppressed', 'dropped')
    ) a
    WHERE 
        (NOT REGEXP_CONTAINS(_domain,'2x.marketing|terrasmart') OR _domain IS NULL)
    AND 
        _domain IS NOT NULL 
    ORDER BY 
    1, 3 DESC, 2 DESC
),
/*web_views AS (
    SELECT 
        CAST(NULL AS STRING) AS _email, 
        _domain, 
        _date, 
        EXTRACT(WEEK FROM _date) AS _week,  
        EXTRACT(YEAR FROM _date) AS _year, 
        _page AS _pageName, 
        "Web Visit" AS _engagement, 
        CONCAT("Engagement Time:", CAST(_engagementtime AS STRING)) AS _description
    FROM `x-marketing.terrasmart.db_web_metrics` 
    WHERE NOT REGEXP_CONTAINS(LOWER(_utmsource), 'linkedin|google|bing|email') 
),
ad_clicks AS (
    SELECT 
        CAST(NULL AS STRING) AS _email, 
        _domain, 
        _date, 
        EXTRACT(WEEK FROM _date) AS _week,  
        EXTRACT(YEAR FROM _date) AS _year, 
        _page AS _pageName, 
        "Ad Clicks" AS _engagement, 
        _fullpage AS _description
    FROM `x-marketing.terrasmart.db_web_metrics` 
    WHERE REGEXP_CONTAINS(LOWER(_utmsource), 'linkedin|google|bing|paid_ads')
),*/
form_fills AS (
    SELECT * 
    FROM ( 
        SELECT 
            LOWER(_email) AS _email, 
            RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) AS _domain, 
            EXTRACT(DATETIME FROM _timestamp) AS _date , 
            EXTRACT(WEEK FROM _timestamp) AS _week,  
            EXTRACT(YEAR FROM _timestamp) AS _year,
            _contentTitle AS _contentTitle, 
            CONCAT("Email ", INITCAP(_engagement)) AS _engagement,
            CAST(NULL AS STRING) AS _description
        FROM 
            (SELECT * FROM `terrasmart.db_campaign_analysis_pardot`)
        WHERE 
        /* (EXTRACT(DATE FROM _timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY ) AND CURRENT_DATE() )
        AND */ 
            _engagement IN ('Downloaded')
    ) a
    WHERE 
        NOT REGEXP_CONTAINS(_domain,'2x.marketing|terrasmart|gmail|yahoo|outlook|hotmail') 
        AND _domain IS NOT NULL 
    ORDER BY 1, 3 DESC, 2 DESC
),
dummy_dates AS ( 
    SELECT
        _date,
        EXTRACT(WEEK FROM _date) AS _week,
        EXTRACT(YEAR FROM _date) AS _year
    FROM UNNEST(
        GENERATE_DATE_ARRAY(
            '2022-01-01', 
            CURRENT_DATE(), 
            INTERVAL 1 DAY
        )
    ) AS _date 
),
first_party_score AS (
    SELECT DISTINCT 
        _domain,
        _week,
        _year,
        -- _weekly_first_party_score,
        -- _ytd_first_party_score
    FROM `x-marketing.terrasmart.account_scoring`
), 
engagement_grade AS (
    SELECT DISTINCT 
        _week, 
        _year, 
        _email, 
        _weekly_contact_score,
        _ytd_contact_score,
        CASE 
            WHEN _weekly_contact_score < 39 THEN 'Low'
            WHEN _weekly_contact_score BETWEEN 40 AND 69 THEN 'Medium'
            WHEN _weekly_contact_score BETWEEN 70 AND 130 THEN 'High'
            WHEN _weekly_contact_score > 80 THEN 'Very High'
        END AS _ytd_grade 
    FROM `x-marketing.terrasmart.contact_scoring`
    ORDER BY _week DESC
), 
engagements AS (
    -- Contact based engagement query
    SELECT DISTINCT 
        tam_contacts._domain, 
        tam_contacts._email,
        dummy_dates.* EXCEPT(_date), 
        engagements.* EXCEPT(_date, _week, _year, _domain, _email),
        CAST(NULL AS INTEGER) AS _avg_bombora_score,
        tam_contacts.* EXCEPT(_domain, _email),
        engagements._date
    FROM dummy_dates
    JOIN (
        SELECT * FROM email_engagement 
        -- UNION ALL
        -- SELECT * FROM form_fills
    ) engagements 
    USING(_week, _year)
    RIGHT JOIN tam_contacts 
    USING(_email) 
    
    /*UNION DISTINCT
    
    -- Account based engagement query
    SELECT DISTINCT 
        tam_accounts._domain, 
        CAST(NULL AS STRING) AS _email,
        dummy_dates.* EXCEPT(_date), 
        engagements.* EXCEPT(_date, _week, _year, _domain, _email),
        CAST(NULL AS INTEGER) AS _avg_bombora_score,
        CAST(NULL AS STRING) AS _firstname, 
        CAST(NULL AS STRING) AS _lastname,
        CAST(NULL AS STRING) AS _title,
        CAST(NULL AS STRING) AS _2xseniority,
        tam_accounts.* EXCEPT(_domain),
        engagements._date
    FROM dummy_dates
    JOIN (
        SELECT * FROM web_views
        UNION ALL
        SELECT * FROM ad_clicks 
    ) engagements 
    USING(_week, _year)
    JOIN (
        SELECT DISTINCT 
            _domain, 
            _accountid, 
            _accountname, 
            _industry,
            _tier,  
            _annualrevenue 
        FROM tam_contacts
    ) tam_accounts
    USING(_domain)*/
)
SELECT DISTINCT 
    engagements.*,
    -- COALESCE(first_party_score._weekly_first_party_score, 0) AS _weekly_first_party_score, 
    -- COALESCE(first_party_score._ytd_first_party_score, 0) AS _ytd_first_party_score, 
    engagement_grade._weekly_contact_score, 
    engagement_grade._ytd_contact_score,
    engagement_grade._ytd_grade
FROM engagements
LEFT JOIN first_party_score USING(_domain, _week, _year)
LEFT JOIN engagement_grade USING(_email, _week, _year)
ORDER BY _week DESC;

------------------------------------------------------------------------------------
--------------------------------- CONTACT SCORING ----------------------------------
------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE `x-marketing.terrasmart.contact_scoring` AS
WITH contacts AS (
    SELECT * EXCEPT(_rownum) 
    FROM (
        SELECT DISTINCT
            CONCAT(prospect.first_name, ' ', prospect.last_name) _name,
            -- CASE
            --   WHEN prospect._title LIKE "%0%"
            --   THEN CAST(" " AS STRING)
            --   ELSE prospect._title
            -- END AS _title,
            -- prospect._seniority AS _seniority,
            LOWER(prospect.email) AS _email,
            CAST(prospect_account_id AS STRING) AS _accountid,
            RIGHT(email,LENGTH(email)-STRPOS(email,'@')) AS _domain,
            prospect.company AS _accountname,
            industry AS _industry,
            -- _tier AS _tier,
            -- _revenue AS _annualrevenue,
            ROW_NUMBER() OVER( 
                PARTITION BY prospect.email
                ORDER BY prospect_account_id DESC
            ) _rownum
        FROM `x-marketing.terrasmart_pardot.prospects` prospect
        -- WHERE NOT REGEXP_CONTAINS(prospect._email, '2x.marketing|terrasmart|test|gibraltar') 
        WHERE NOT REGEXP_CONTAINS(prospect.email, 'terrasmart|test|gibraltar') 
    )
    WHERE _rownum = 1
),
dummy_dates AS (
    SELECT
        _date,
        DATE_TRUNC(_date, WEEK(MONDAY)) AS _extract_date,
        EXTRACT(WEEK FROM _date) AS _week,
        EXTRACT(YEAR FROM _date) AS _year
    FROM UNNEST(
        GENERATE_DATE_ARRAY(
            '2022-01-01', 
            CURRENT_DATE(), 
            INTERVAL 1 WEEK
        )
    ) AS _date 
    ORDER BY 1 DESC
),
cross_join_dates AS (
    SELECT DISTINCT 
        *
    FROM contacts
    CROSS JOIN dummy_dates
),
engagements AS (
    SELECT DISTINCT
        _name,
        _accountname,
        _domain,
        _accountid,
        _email,
        _week,
        _year,
        _contentTitle,
        COUNT(DISTINCT 
            CASE 
                WHEN _engagement = 'Email Sent' 
                THEN CONCAT(_email, _contentTitle) 
            END
        ) AS _emailSent,
        COUNT(DISTINCT 
            CASE 
                WHEN _engagement = 'Email Bounced' 
                THEN CONCAT(_email, _contentTitle) 
            END
        ) AS _emailBounced,
        COUNT(DISTINCT 
            CASE 
                WHEN _engagement = 'Email Opened' 
                THEN CONCAT(_email, _contentTitle) 
            END
        ) AS _emailOpened,
        COUNT(DISTINCT 
            CASE 
                WHEN _engagement = 'Email Clicked' 
                THEN CONCAT(_email, _contentTitle) 
            END
        ) AS _emailClicked,
        COUNT(DISTINCT 
            CASE 
                WHEN _engagement = 'Email Unsubscribed' 
                THEN CONCAT(_email, _contentTitle) 
            END
        ) AS _emailUnsubscribed,
        COUNT(DISTINCT 
            CASE 
                WHEN _engagement = 'Email Downloaded' 
                -- AND REGEXP_CONTAINS(
                --     LOWER(_contentTitle), 
                --     'contact|demo|webinar'
                -- ) 
                THEN CONCAT(_email, _contentTitle) 
            END 
        ) AS _formFilled
    FROM (
        SELECT DISTINCT 
            CASE
                WHEN CONCAT(_firstname, ' ', _lastname) LIKE "%0%"
                THEN CAST(" " AS STRING)
                ELSE CONCAT(_firstname, ' ', _lastname)
            END AS _name,
            _accountname,
            _domain,
            _accountid,
            _email,
            _date,
            _week,
            _year,
            _engagement,
            _contentTitle
        -- FROM engagements_data
         FROM `x-marketing.terrasmart.db_account_health`
        WHERE _engagement IN (
            'Email Sent',
            'Email Bounced',
            'Email Opened', 
            'Email Clicked',
            'Email Downloaded',
            'Email Unsubscribed'
        )
    )
    GROUP BY 1, 2, 3, 4 ,5, 6, 7, 8
    ORDER BY _year, _week
),
engagement_pivot AS(
    SELECT DISTINCT
        cross_join_dates.* EXCEPT(_extract_date),
        _contentTitle,
        COALESCE(
          IF(
            engagements._emailBounced = 0,
            engagements._emailSent,
            (engagements._emailSent - engagements._emailBounced)
          ),
          0
        ) AS _emailDelivered,
        COALESCE(engagements._emailOpened, 0) AS _emailOpened,
        COALESCE(engagements._emailClicked, 0) AS _emailClicked,
        COALESCE(engagements._emailUnsubscribed, 0) AS _emailUnsubscribed,
        COALESCE(engagements._formFilled, 0) AS _formFilled,
        cross_join_dates._extract_date
    FROM cross_join_dates
    LEFT JOIN engagements 
    USING(_email, _week, _year)
),
combined_data AS (
    SELECT DISTINCT 
        *,
        -- Calculating total email score
        (
            CASE
                -- WHEN _emailOpened >= 1 THEN 5 * _emailOpened
                -- WHEN _emailOpened BETWEEN 3 AND 6 THEN _emailOpened * 3
                WHEN _emailOpened = 1 THEN 5 
                WHEN _emailOpened = 2 THEN 10
                WHEN _emailOpened >= 3 THEN 15 
                ELSE 0
            END
            + 
            CASE
                WHEN _emailClicked = 1 THEN 10 
                WHEN _emailClicked = 2 THEN 15
                WHEN _emailClicked >= 3 THEN 20 
                -- WHEN _emailClicked BETWEEN 3 AND 6 THEN _emailClicked * 5
                ELSE 0
            END 
            +
            CASE
                WHEN _emailUnsubscribed >= 1 THEN -50 
                -- WHEN _emailUnsubscribed BETWEEN 3 AND 6 THEN _emailUnsubscribed * 5
                ELSE 0
            END 
        ) AS _email_score,
        -- Calculating form fill score
        (
            CASE
                WHEN _formFilled >= 1 THEN 25 
                ELSE 0
            END 
        ) AS _form_fill_score
    FROM engagement_pivot 
)

SELECT
    *,
    SUM(_weekly_contact_score) OVER(
        PARTITION BY _email 
        ORDER BY _year, _week 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS _ytd_contact_score,
FROM (
    SELECT 
        *,
        (_email_score + _form_fill_score) AS _weekly_contact_score
    FROM combined_data
    ORDER BY _weekly_contact_score DESC 
) main
ORDER BY _week DESC;

------------------------------------------------------------------------------------
--------------------------------- ACCOUNT SCORING ----------------------------------
------------------------------------------------------------------------------------


CREATE OR REPLACE TABLE `x-marketing.terrasmart.account_scoring` AS
-- Accumulate scores of contact based on account
WITH all_accounts AS (
    SELECT DISTINCT 
        _domain,
        _week,
        _year,
        -- _accountname,
        -- _name as _contactname
    FROM `x-marketing.terrasmart.contact_scoring`
),
contact_score_accumulative AS (
    SELECT DISTINCT 
        _domain,
        _week,
        _year,
        _extract_date,
        COUNT( 
            CASE 
                WHEN _emailOpened >= 1 
                THEN _email 
            END
        ) AS _distinctOpen,
        COUNT( 
            CASE 
                WHEN _emailClicked >= 1 
                THEN _email 
            END
        ) AS _distinctClick,
        COUNT( 
            CASE 
                WHEN _emailUnsubscribed >= 1 
                THEN _email 
            END
        ) AS _distinctUnsubscribed,
    FROM `x-marketing.terrasmart.contact_scoring`
    GROUP BY 1, 2, 3, 4
    ORDER BY _week DESC
),
distinct_scoring AS (
    SELECT DISTINCT 
        *,
        -- Calculating total email score
        (
            CASE
                WHEN _distinctOpen >= 1 THEN 5
                -- WHEN _distinctOpen BETWEEN 3 AND 6 THEN 3
                ELSE 0
            END 
            +
            CASE
                WHEN _distinctClick >= 1 THEN 10
                -- WHEN _distinctClick BETWEEN 3 AND 6 THEN 5
                ELSE 0
            END
            +
            CASE
                WHEN _distinctUnsubscribed >= 1 THEN -100
                -- WHEN _distinctUnsubscribed BETWEEN 3 AND 6 THEN 5
                ELSE 0
            END
        ) AS _email_score,
        -- Unavailable scores
    FROM contact_score_accumulative 
),
ytd_contact_based AS (
    SELECT 
        *,
        SUM(_weekly_account_score) OVER(
            PARTITION BY _domain 
            ORDER BY _year, _week 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS _ytd_account_score,
    FROM (
        SELECT 
            *,
            (_email_score) 
            AS _weekly_account_score
        FROM distinct_scoring
    )
),
ads_data AS (
    SELECT DISTINCT 
        CASE 
            WHEN _accountdomain LIKE "%www.%"
            THEN (
                REGEXP_REPLACE(
                    SUBSTR(_accountdomain, INSTR(_accountdomain, '://') + 3),
                    '(www\\.|/+$)',
                    ''
                )
            )
            ELSE (
                REGEXP_REPLACE(
                    _accountdomain,
                    '(https?://|www\\.|/+$)',
                    ''
                )
            )
        END AS _accountdomain,
        RIGHT(_contactemail,LENGTH(_contactemail)-STRPOS(_contactemail,'@')) AS _domain,
        _contactname,
        EXTRACT(WEEK FROM DATE(_date)) AS _week,  
        EXTRACT(YEAR FROM DATE(_date)) AS _year,
        _engagementtype,
        CASE
            WHEN 'Like' IN UNNEST(SPLIT(_engagementtype, ','))
            THEN COALESCE(
                COUNT(DISTINCT CONCAT(_accountdomain, _campaignname)),
                0
            )
            ELSE 0
        END AS _distinctAdLikes,
        CASE
            WHEN 'Follow' IN UNNEST(SPLIT(_engagementtype, ','))
            THEN COALESCE(
                COUNT(DISTINCT CONCAT(_accountdomain, _campaignname)),
                0
            )
            ELSE 0
        END AS _distinctAdFollow,
        CASE
            WHEN 'Comment' IN UNNEST(SPLIT(_engagementtype, ','))
            THEN COALESCE(
                COUNT(DISTINCT CONCAT(_accountdomain, _campaignname)),
                0
            )
            ELSE 0
        END AS _distinctAdComment,
        CASE
            WHEN 'Share' IN UNNEST(SPLIT(_engagementtype, ','))
            THEN COALESCE(
                COUNT(DISTINCT CONCAT(_accountdomain, _campaignname)),
                0
            )
            ELSE 0
        END AS _distinctAdShare,
        _accountName,
        _campaignname,_medium
    FROM `x-marketing.gibraltar_mysql.db_airtable_abm_engagement`,
    UNNEST(SPLIT(_engagementtype, ',')) AS sepEngagementType
    WHERE TRIM(sepEngagementType) IN (
        'Like','Share','Comment','Follow'
        )
    AND RIGHT(_contactemail,LENGTH(_contactemail)-STRPOS(_contactemail,'@')) != '2x.marketing'
    GROUP BY 1, 2, 3, 4, 5, _engagementtype,_accountName,sepEngagementType,_campaignname,_medium
),
apply_ads_score AS (
    SELECT
        *,
        (
            CASE 
                WHEN _distinctAdLikes >= 1 THEN 5 
                ELSE 0 
            END
            +
            CASE 
                WHEN _distinctAdFollow >= 1 THEN 5 
                ELSE 0 
            END
            +
            CASE 
                WHEN _distinctAdComment >= 1 THEN 10
                ELSE 0 
            END
            +
            CASE 
                WHEN _distinctAdShare >= 1 THEN 15
                ELSE 0 
            END
        ) AS _weekly_ads_score
    FROM ads_data
    LEFT JOIN all_accounts 
    USING (_domain, _week, _year)
),
ytd_ads_score AS (
    SELECT
        *,
        -- Getting the YTD ads score, sets limit for YTD ads score
        /*CASE 
            WHEN (
                SUM(_weekly_ads_score) OVER(
                    PARTITION BY _domain  
                    ORDER BY _week 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) >= 3
            ) 
            THEN 3
            ELSE (
                SUM(_weekly_ads_score) OVER(
                    PARTITION BY _domain  
                    ORDER BY _week
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW 
                )
            )
        END AS _ytd_ads_score*/
        SUM(_weekly_ads_score) OVER(
            PARTITION BY _domain  
            ORDER BY _week
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW 
        )AS _ytd_ads_score
    FROM apply_ads_score
)
/*web_data AS (
   SELECT
        _domain,
        EXTRACT(WEEK FROM _timestamp) AS _week,
        EXTRACT(YEAR FROM _timestamp) AS _year,
        COALESCE((SUM(_website_time_spent)), 0) AS _website_time_spent,
        COALESCE(SUM(_website_page_view), 0) AS _website_page_view,
        COALESCE(SUM(_website_visitor_count), 0) AS _website_visitor_count,
        TRUE AS _visited_website
    FROM (
        SELECT
            visit.first_visitor_page_view_at AS _timestamp,
            RIGHT(email,LENGTH(email)-STRPOS(email,'@'))  AS _domain,
            COALESCE((SUM(CAST(visit.duration_in_seconds AS DECIMAL))), 0) AS _website_time_spent,
            COUNT(DISTINCT(visitor_page_views.visitor_page_view[SAFE_OFFSET(0)].value.title)) AS _website_page_view,
            COUNT(CAST(visit.visitor_page_view_count AS INT)) AS _website_visitor_count,
        FROM `x-marketing.terrasmart_pardot.visitors` main
        LEFT JOIN `x-marketing.terrasmart_pardot.visits` visit 
ON main.id = visit.visitor_id
LEFT JOIN `x-marketing.terrasmart_pardot.prospects` prospect
    ON visit.prospect_id = prospect.id
        LEFT JOIN 
        (
            SELECT DISTINCT 
                _website AS _domain, 
                _ipaddr AS ip_address 
            FROM `webtrack_ipcompany.webtrack_ipcompany`
            WHERE _website IS NOT NULL
        ) supp USING(ip_address)
        GROUP BY 1, 2 
    )
    WHERE _domain IS NOT NULL 
    GROUP BY 1, 2, 3
),
apply_web_score AS (
    SELECT
        *,
        -- Set the weekly limit of web visit score
        COALESCE(
            CASE
                WHEN (
                    _website_time_spent_score 
                    + 
                    _website_page_view_score 
                    + 
                    _website_visitor_count_score 
                    + 
                    _visited_website_score
                ) > 50 
                THEN 50
                ELSE (
                    _website_time_spent_score 
                    + 
                    _website_page_view_score 
                    + 
                    _website_visitor_count_score 
                    + 
                    _visited_website_score
                )
            END, 0
        ) AS _weekly_web_score
    FROM (
        SELECT
            *,
            CASE 
                WHEN _website_time_spent >= 120 THEN 20
                WHEN _website_time_spent < 120 THEN 10
                ELSE 0
            END AS _website_time_spent_score,
            CASE 
                WHEN _website_page_view >= 5 THEN 15
                WHEN _website_page_view < 5 THEN 10
                ELSE 0
            END AS _website_page_view_score,
            CASE 
                WHEN _website_visitor_count >= 3 THEN 10
                WHEN _website_visitor_count < 3 THEN 5
                ELSE 0
            END AS _website_visitor_count_score,
            5 AS _visited_website_score
        FROM web_data 
    ) 
    RIGHT JOIN all_accounts 
    USING (_domain, _week, _year)
),
ytd_web_score AS (
    SELECT
        *,
        -- Getting the YTD web score, sets limit for YTD web score
        CASE 
            WHEN (
                SUM(_weekly_web_score) OVER(
                    PARTITION BY _domain  
                    ORDER BY _week
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW 
                ) >= 50
            ) 
            THEN 50
            ELSE (
                SUM(_weekly_web_score) OVER(
                    PARTITION BY _domain  
                    ORDER BY _week 
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                )
            )
        END AS _ytd_web_score
    FROM apply_web_score
)
*/
-- SELECT DISTINCT 
--     * EXCEPT(
--         _weekly_account_score, 
--         _ytd_account_score
--         -- _weekly_web_score,
--         -- _ytd_web_score
--     ),
--     -- COALESCE(_weekly_web_score, 0) AS _weekly_web_score,
--     CASE
--         WHEN (
--             _weekly_account_score 
--             -- + 
--             -- COALESCE(_weekly_web_score, 0)
--         ) >= 70 
--         THEN 70
--         ELSE (
--             _weekly_account_score 
--             -- + 
--             -- COALESCE(_weekly_web_score, 0)
--         )
--     END AS _weekly_first_party_score,
--     _ytd_account_score,
--     -- COALESCE(_ytd_web_score, 0) AS _ytd_web_score,
--     CASE
--         WHEN (
--             _ytd_account_score 
--             -- + 
--             -- COALESCE(_ytd_web_score, 0)
--         ) >= 70 
--         THEN 70
--         ELSE (
--             _ytd_account_score 
--             -- + 
--             -- COALESCE(_ytd_web_score, 0)
--         )
--     END AS _ytd_first_party_score
-- FROM ytd_contact_based
-- LEFT JOIN ytd_ads_score USING(_domain, _week, _year)
-- -- LEFT JOIN ytd_web_score USING(_domain, _week, _year)
-- ORDER BY _week DESC;
SELECT DISTINCT * FROM ytd_ads_score ORDER BY _week DESC;


------------------------------------------------------------------------------------
---------------------------- ACCOUNT&CONTACT SCORING -------------------------------
------------------------------------------------------------------------------------