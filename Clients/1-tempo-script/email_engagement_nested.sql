
CREATE OR REPLACE TABLE `dummy_table.thelogicfactory_nested_demo` AS
WITH prospect_info AS (
    SELECT * EXCEPT(_rownum)
    FROM (
        SELECT 
            LOWER(_email) AS _email, 
            _phone, 
            _name, 
            _companyname, 
            _industry, 
            _country, 
            _function, 
            _seniority, 
            _companywebsite, 
            _state, 
            _city, 
            CAST(_employees AS STRING), 
            _persona, 
            _revenuerange, 
            _tier, 
            _title,
            ROW_NUMBER() OVER(
                PARTITION BY _email
                ORDER BY _updated DESC
            ) _rownum
        FROM 
            `x-marketing.thelogicfactory_mysql.w_routables`
    )
    WHERE _rownum = 1
), 
airtable_info AS (
    SELECT * EXCEPT(_rownum)
    FROM (
        SELECT 
            _pardotid,
            _campaignname, 
            _screenshot, 
            _assettitle, 
            _subject, 
            _code, 
            TIMESTAMP(_livedate) AS _livedate,  
            _utm_medium, 
            _landingPage,
            _naming,
            ROW_NUMBER() OVER (
                PARTITION BY _pardotid
                ORDER BY _sdc_sequence DESC
            ) AS _rownum
        FROM 
            `x-marketing.thelogicfactory_mysql.db_airtable_email`  
        WHERE 
            _pardotid IS NOT NULL 
        AND 
            _pardotid != '' 
        AND 
            _code NOT LIKE 'SAMPLE' 
        AND 
            _livedate != ''
        AND 
            _code NOT IN(
                '2022-03-14_WP-01_V1', 
                '2022-03-14_WP-01_V3', 
                '2022-03-08_RWP-01_W2-F-2', 
                '2022-03-08_RWP-01_W2-G-2', 
                '2022-03-08_RWP-01_W2-O-2'
            )
    ) 
    WHERE _rownum = 1
), 
email_sent AS (
    SELECT sents.* EXCEPT(rownum) 
    FROM (
        SELECT
            _sdc_sequence, 
            _email AS _email, 
            'Sent' AS _engagement, 
            _dateevent AS _timestamp, 
            CAST(_campaignID AS STRING) AS _campaignID, 
            'email' AS _utm_source,
            CAST(NULL AS STRING) AS _description,
            ROW_NUMBER() OVER(
                PARTITION BY _campaignID, _email  
                ORDER BY _dateevent DESC
            ) AS rownum
        FROM
            `x-marketing.thelogicfactory_mysql.db_sib_deliver`
        WHERE
            _dateevent IS NOT NULL
        AND _email NOT LIKE '%2x.marketing%'
        AND _email NOT LIKE '%test%'
        AND _email NOT LIKE '%thelogicfactory%'
        UNION ALL 
         SELECT
            _sdc_sequence, 
            _email AS _email, 
            'Sent' AS _engagement, 
            _dateevent AS _timestamp, 
            CAST(_campaignID AS STRING) AS _campaignID, 
            'email' AS _utm_source,
            _reason AS _description,
            ROW_NUMBER() OVER(
                PARTITION BY _campaignID, _email  
                ORDER BY _dateevent DESC
            ) AS rownum
        FROM
            `x-marketing.thelogicfactory_mysql.db_sib_bounce2`
        WHERE
            _dateevent IS NOT NULL 
            AND _email NOT LIKE '%2x.marketing%'
            AND _email NOT LIKE '%test%'
            AND _email NOT LIKE '%thelogicfactory%'
    ) sents
    JOIN 
        airtable_info 
    ON 
        sents._campaignID = airtable_info._pardotid
    WHERE rownum = 1
),
email_delivered AS (
    SELECT delivereds.* EXCEPT(rownum) 
    FROM (
        SELECT
            _sdc_sequence, 
            _email AS _email, 
            'Delivered' AS _engagement, 
            _dateevent AS _timestamp, 
            CAST(_campaignID AS STRING) AS _campaignID, 
            'email' AS _utm_source,
            CAST(NULL AS STRING) AS _description,
            ROW_NUMBER() OVER(
                PARTITION BY _campaignID, _email  
                ORDER BY _dateevent DESC
            ) AS rownum
        FROM
            `x-marketing.thelogicfactory_mysql.db_sib_deliver`
        WHERE
            _dateevent IS NOT NULL
        AND _email NOT LIKE '%2x.marketing%'
        AND _email NOT LIKE '%test%'
        AND _email NOT LIKE '%thelogicfactory%'
    ) delivereds
    JOIN 
        airtable_info 
    ON 
        delivereds._campaignID = airtable_info._pardotid
    WHERE rownum = 1
), 
email_open AS (
    SELECT opens.* EXCEPT(rownum) 
    FROM (
        SELECT
            _sdc_sequence, 
            _email AS _email, 
            'Opened' AS _engagement, 
            _dateevent AS _timestamp, 
            CAST(_campaignID AS STRING) AS _campaignID, 
            'email' AS _utm_source,
            CAST(NULL AS STRING) AS _description,
            ROW_NUMBER() OVER(
                PARTITION BY _campaignID, _email  
                ORDER BY _dateevent DESC
            ) AS rownum
        FROM
            `x-marketing.thelogicfactory_mysql.db_sib_open`
        WHERE
            _dateevent IS NOT NULL
        AND _email NOT LIKE '%2x.marketing%'
        AND _email NOT LIKE '%test%'
        AND _email NOT LIKE '%thelogicfactory%'
    ) opens
    JOIN 
        airtable_info 
    ON 
        opens._campaignID = airtable_info._pardotid
    WHERE rownum = 1
), 
email_click AS (
    SELECT clicks.* EXCEPT(rownum) 
    FROM (
        SELECT 
            _sdc_sequence, 
            _email AS _email, 
            'Clicked' AS _engagement, 
            _dateevent AS _timestamp, 
            CAST(_campaignID AS STRING) AS _campaignID, 
            'email' AS _utm_source,
            CASE 
                WHEN _botURL_exist IS NOT NULL
                THEN _botURL_exist ELSE _url
            END AS _description,
            ROW_NUMBER() OVER(
                PARTITION BY _campaignID, _email  
                ORDER BY _dateevent DESC
            ) AS rownum
        FROM (
            SELECT
                *,
                MAX(
                    CASE 
                        WHEN _url LIKE '%thelogicfactory.wise-portal.com/iclick/iclick.php?pid%' 
                        THEN _url  
                    END
                ) 
                OVER(
                    PARTITION BY _campaignID, _email
                ) AS _botURL_exist
            FROM
                `x-marketing.thelogicfactory_mysql.db_sib_click`
            WHERE
                _email NOT LIKE '%2x.marketing%'
            AND _email NOT LIKE '%test%'
            AND _email NOT LIKE '%thelogicfactory%'
        )
    ) clicks
    JOIN 
        airtable_info 
    ON 
        clicks._campaignID = airtable_info._pardotid
    WHERE rownum = 1
), 
email_unsubscribe AS (
    SELECT unsubs.* EXCEPT(rownum) 
    FROM (
        SELECT
            _sdc_sequence, 
            _email AS _email, 
            'Unsubscribed' AS _engagement, 
            _dateevent AS _timestamp, 
            CAST(_campaignID AS STRING) AS _campaignID, 
            'email' AS _utm_source,
            CAST(NULL AS STRING) AS _description,
            ROW_NUMBER() OVER(
                PARTITION BY _campaignID, _email  
                ORDER BY _dateevent DESC
            ) AS rownum
        FROM
            `x-marketing.thelogicfactory_mysql.db_sib_unsubscribe`
        WHERE
            _dateevent IS NOT NULL
        AND _email NOT LIKE '%2x.marketing%'
        AND _email NOT LIKE '%test%'
        AND _email NOT LIKE '%thelogicfactory%'
    ) unsubs
    JOIN 
        airtable_info 
    ON 
        unsubs._campaignID = airtable_info._pardotid
    WHERE rownum = 1
), 
email_bounce AS (
    SELECT bounces.* EXCEPT(rownum) 
    FROM (
        SELECT
            _sdc_sequence, 
            _email AS _email, 
            'Hard Bounced' AS _engagement, 
            _dateevent AS _timestamp, 
            CAST(_campaignID AS STRING) AS _campaignID, 
            'email' AS _utm_source,
            _reason AS _description,
            ROW_NUMBER() OVER(
                PARTITION BY _campaignID, _email  
                ORDER BY _dateevent DESC
            ) AS rownum
        FROM
           `x-marketing.thelogicfactory_mysql.db_sib_bounce2`
        WHERE
            _dateevent IS NOT NULL AND _event = 'hard_bounce'
            AND _email NOT LIKE '%2x.marketing%'
            AND _email NOT LIKE '%test%'
            AND _email NOT LIKE '%thelogicfactory%'
    ) bounces
    JOIN 
        airtable_info 
    ON 
        bounces._campaignID = airtable_info._pardotid
    WHERE rownum = 1
), 
email_soft_bounce AS (
    SELECT bounces.* EXCEPT(rownum) 
    FROM (
        SELECT
            _sdc_sequence, 
            _email AS _email, 
            'Soft Bounced' AS _engagement, 
            _dateevent AS _timestamp, 
            CAST(_campaignID AS STRING) AS _campaignID, 
            'email' AS _utm_source,
            _reason AS _description,
            ROW_NUMBER() OVER(
                PARTITION BY _campaignID, _email  
                ORDER BY _dateevent DESC
            ) AS rownum
        FROM
            `x-marketing.thelogicfactory_mysql.db_sib_bounce2`
        WHERE
            _dateevent IS NOT NULL AND _event = 'soft_bounce'
            AND _email NOT LIKE '%2x.marketing%'
            AND _email NOT LIKE '%test%'
            AND _email NOT LIKE '%thelogicfactory%'
    ) bounces
    JOIN 
        airtable_info 
    ON 
        bounces._campaignID = airtable_info._pardotid
    WHERE rownum = 1
), 
email_download AS (
    SELECT 
        downloads._sdc_sequence, 
        downloads._email, 
        downloads._engagement, 
        downloads._timestamp, 
        CAST(airtable_info._pardotid AS STRING), 
        downloads._utm_source,
        downloads._description 
    FROM (
        SELECT 
            _sdc_sequence, 
            _email, 
            'Downloaded' AS _engagement, 
            _timestamp, 
            _utm_campaign, 
            _utm_source,
            _download_title AS _description,
            ROW_NUMBER() OVER(
                PARTITION BY _email, _utm_campaign 
                ORDER BY _timestamp DESC
            ) AS rownum
        FROM 
            `x-marketing.thelogicfactory_mysql.w_lead_downloads`
        WHERE 
            _email NOT LIKE '%2x.marketing%'
        AND _email NOT LIKE '%test%'
        AND _email NOT LIKE '%thelogicfactory%'
    ) downloads 
    LEFT JOIN 
        airtable_info 
    ON 
        downloads._utm_campaign = airtable_info._code
    WHERE rownum = 1 
)
SELECT
    engagements._email,
    prospect_info._phone, 
    prospect_info._name, 
    prospect_info._companyname, 
    prospect_info._industry, 
    prospect_info._country, 
    prospect_info._function, 
    prospect_info._seniority, 
    prospect_info._companywebsite, 
    prospect_info._state, 
    prospect_info._city, 
    -- prospect_info._employees,
    prospect_info._persona, 
    prospect_info._revenuerange, 
    prospect_info._tier, 
    prospect_info._title,
    engagements._campaignID,
    airtable_info._campaignname,
    airtable_info._screenshot, 
    airtable_info._assettitle, 
    airtable_info._subject, 
    airtable_info._code, 
    airtable_info._livedate,  
    airtable_info._utm_medium, 
    airtable_info._landingPage,
    airtable_info._naming,
    ARRAY_AGG(STRUCT(
        engagements._engagement,
        engagements._sdc_sequence,
        engagements._timestamp,
        engagements._utm_source,
        engagements._description,
        CAST(NULL AS STRING) AS _showExport,
        CAST(NULL AS STRING) AS _isBot
    )) AS engagement_details,
FROM (
    SELECT * FROM email_sent
    UNION ALL
    SELECT * FROM email_delivered
    UNION ALL
    SELECT * FROM email_open
    UNION ALL
    SELECT * FROM email_click
    UNION ALL
    SELECT * FROM email_unsubscribe
    UNION ALL
    SELECT * FROM email_bounce
    UNION ALL
    SELECT * FROM email_soft_bounce
    UNION ALL
    SELECT * FROM email_download
) AS engagements
LEFT JOIN airtable_info ON engagements._campaignID = airtable_info._pardotid
LEFT JOIN prospect_info ON engagements._email = prospect_info._email
GROUP BY    engagements._email,
            prospect_info._phone, 
            prospect_info._name, 
            prospect_info._companyname, 
            prospect_info._industry, 
            prospect_info._country, 
            prospect_info._function, 
            prospect_info._seniority, 
            prospect_info._companywebsite, 
            prospect_info._state, 
            prospect_info._city, 
            -- prospect_info._employees,
            prospect_info._persona, 
            prospect_info._revenuerange, 
            prospect_info._tier, 
            prospect_info._title,
            engagements._campaignID,
            airtable_info._campaignname,
            airtable_info._screenshot, 
            airtable_info._assettitle, 
            airtable_info._subject, 
            airtable_info._code, 
            airtable_info._livedate,  
            airtable_info._utm_medium, 
            airtable_info._landingPage,
            airtable_info._naming
---UPDATES
--- Label Bots
-- UPDATE `x-marketing.thelogicfactory.db_email_engagements_log` origin  
-- SET origin._isBot = 'Bot'
-- FROM (
--     SELECT 
--         _email, _utm_campaign
--     FROM 
--         `x-marketing.thelogicfactory.db_email_engagements_log`
--     WHERE
--         _engagement = 'Clicked'
--     AND
--         _description LIKE '%thelogicfactory.wise-portal.com/iclick/iclick.php?pid%'
-- ) scenario
-- WHERE origin._email = scenario._email
-- AND origin._utm_campaign = scenario._utm_campaign;


-- --- Case 2: Time Opened - Time Delivered < 3
-- UPDATE `x-marketing.thelogicfactory.db_email_engagements_log` origin  
-- SET origin._isBot = 'Bot'
-- FROM (
--     WITH delivered_emails AS (
--         SELECT
--             _email, _utm_campaign, _timestamp
--         FROM
--             `x-marketing.thelogicfactory.db_email_engagements_log`
--         WHERE
--             _engagement = 'Delivered'
--     ),
--     opened_emails AS (
--         SELECT
--             _email, _utm_campaign, _timestamp
--         FROM
--             `x-marketing.thelogicfactory.db_email_engagements_log`
--         WHERE
--             _engagement = 'Opened'       
--     )
--     SELECT 
--         open._email, open._utm_campaign, 
--         deliver._timestamp AS deliver_timestamp, 
--         open._timestamp AS open_timestamp
--     FROM 
--         delivered_emails AS deliver
--     JOIN 
--         opened_emails AS open
--     ON 
--         deliver._email = open._email
--     AND
--         deliver._utm_campaign = open._utm_campaign
-- ) scenario
-- WHERE origin._email = scenario._email
-- AND origin._utm_campaign = scenario._utm_campaign
-- AND TIMESTAMP_DIFF(open_timestamp, deliver_timestamp, SECOND) < 3;


-- --- Case 3: Time Clicked - Time Opened < 3
-- UPDATE `x-marketing.thelogicfactory.db_email_engagements_log` origin  
-- SET origin._isBot = 'Bot'
-- FROM (
--     WITH opened_emails AS (
--         SELECT
--             _email, _campaignID, _timestamp
--         FROM
--             `x-marketing.thelogicfactory.db_email_engagements_log`
--         WHERE
--             _engagement = 'Opened'       
--     ),
--     clicked_emails AS (
--         SELECT
--             _email, CAST(_campaignID AS STRING) AS _campaignID, _dateevent AS _timestamp
--         FROM
--             thelogicfactory_mysql.db_sib_click
--     ),
--     combined_data AS (
--         SELECT 
--             click._email, click._campaignID, 
--             open._timestamp AS open_timestamp, 
--             click._timestamp AS click_timestamp
--         FROM 
--             opened_emails AS open
--         JOIN 
--             clicked_emails AS click
--         ON 
--             open._email = click._email
--         AND
--             open._campaignID = click._campaignID
--     )
--     SELECT DISTINCT
--         _email, _campaignID
--     FROM 
--         combined_data
--     WHERE 
--         TIMESTAMP_DIFF(click_timestamp, open_timestamp, SECOND) < 3
-- ) scenario
-- WHERE origin._email = scenario._email
-- AND origin._campaignID = scenario._campaignID;


-- -- Case 4: All Links Clicked Are The Same
-- UPDATE `x-marketing.thelogicfactory.db_email_engagements_log` origin  
-- SET origin._isBot = 'Bot'
-- FROM (
--     WITH unique_links_clicked_and_timestamp AS (
--         SELECT * EXCEPT(_rownum)
--         FROM (
--             SELECT
--                 _email,
--                 _campaignname,
--                 _dateevent,
--                 _url,
--                 ROW_NUMBER() OVER(
--                     PARTITION BY _campaignID, _email, _url
--                     ORDER BY _dateevent DESC
--                 ) AS _rownum
--             FROM thelogicfactory_mysql.db_sib_click
--         )
--         WHERE _rownum = 1
--     ),
--     bots_scenario AS (
--         SELECT * FROM (
--             SELECT
--                 _email,
--                 _campaignname,
--                 COUNT(_dateevent) OVER(PARTITION BY _campaignname, _email) total_clicks,
--                 MIN(_dateevent) OVER(PARTITION BY _campaignname, _email) min_timestamp,
--                 MAX(_dateevent) OVER(PARTITION BY _campaignname, _email) max_timestamp
--             FROM unique_links_clicked_and_timestamp
--         )
--         WHERE TIMESTAMP_DIFF(max_timestamp, min_timestamp, SECOND) <= 10
--         AND total_clicks >= 3
--     )
--     SELECT DISTINCT _email, _campaignname FROM bots_scenario
-- ) scenario
-- WHERE origin._email = scenario._email
-- AND origin._utm_campaign = scenario._campaignname;


-- -- Case 5: Clicked >=3 & Time Clicked < 5s
-- UPDATE `x-marketing.thelogicfactory.db_email_engagements_log` origin  
-- SET origin._isBot = 'Bot'
-- FROM (
--     WITH clicked_emails AS (
--       SELECT
--         _email, _campaignid, _dateevent
--       FROM
--         `x-marketing.thelogicfactory_mysql.db_sib_click`
--     ),
--     timestamp_clicked AS (
--       SELECT
--         _email,
--         _campaignid,
--         _dateevent,
--         CASE
--         WHEN 
--           (
--             (
--               TIMESTAMP_DIFF(
--               _dateevent,
--               LAG(_dateevent, 1) OVER (PARTITION BY _url ORDER BY _dateevent),
--               SECOND
--               ) < 5
--             ) 
--             OR s
--             _email IN (
--               SELECT 
--                 _email
--               FROM
--                 `x-marketing.thelogicfactory_mysql.db_sib_click`
--               GROUP BY _email
--               HAVING COUNT(_email) >= 2
--             )
--           ) 
--         THEN 1
--         END AS BOTIFONE
--       FROM
--         `x-marketing.thelogicfactory_mysql.db_sib_click`
--     )
--     SELECT 
--       click._email,
--       click._campaignid,
--       click._dateevent,
--       ROW_NUMBER() OVER (
--         PARTITION BY click._email, click._campaignid
--         ORDER BY click._dateevent DESC
--       ) AS rownum,
--       BOTIFONE
--     FROM 
--       clicked_emails AS click
--     LEFT JOIN
--       timestamp_clicked AS ts_clicked
--     ON
--       ts_clicked._dateevent = click._dateevent
--       AND ts_clicked._campaignid = click._campaignid
--     WHERE
--       BOTIFONE = 1
-- ) scenario
-- WHERE origin._email = scenario._email
-- AND scenario.rownum = 1
-- AND origin._campaignid = CAST(scenario._campaignid AS STRING)
-- AND origin._engagement = 'Clicked';


-- --- Label Opens With a Subsequent Unsubscribe
-- UPDATE `x-marketing.thelogicfactory.db_email_engagements_log` origin  
-- SET origin._falseOpen = true
-- FROM (
--     WITH opened_emails AS (
--         SELECT
--             _email, _utm_campaign
--         FROM
--             `x-marketing.thelogicfactory.db_email_engagements_log`
--         WHERE
--             _engagement = 'Opened'       
--     ),
--     unsub_emails AS (
--         SELECT
--             _email, _utm_campaign
--         FROM
--             `x-marketing.thelogicfactory.db_email_engagements_log`
--         WHERE
--             _engagement = 'Unsubscribed'       
--     )
--     SELECT 
--         open._email, 
--         open._utm_campaign
--     FROM 
--         opened_emails AS open
--     JOIN 
--         unsub_emails AS unsub
--     ON 
--         open._email = unsub._email
--     AND
--         open._utm_campaign = unsub._utm_campaign
-- ) scenario
-- WHERE origin._email = scenario._email
-- AND origin._utm_campaign = scenario._utm_campaign
-- AND origin._engagement = 'Opened';


--- Set Show Export
-- UPDATE `x-marketing.thelogicfactory.db_email_engagements_log` origin
-- SET origin._showExport = 'Yes'
-- FROM (
--     WITH focused_engagement AS (
--         SELECT 
--             _email, 
--             _engagement, 
--             _utm_campaign,
--             CASE WHEN _engagement = 'Opened' THEN 1
--                 WHEN _engagement = 'Clicked' THEN 2
--                 WHEN _engagement = 'Downloaded' THEN 3
--             END AS _priority
--         FROM 
--             `x-marketing.thelogicfactory.db_email_engagements_log`
--         WHERE 
--             _engagement IN('Opened', 'Clicked', 'Downloaded')
--         ORDER BY 1, 3, 4 DESC 
--     ),
--     final_engagement AS (
--         SELECT * EXCEPT(_priority, _rownum)
--         FROM (
--             SELECT 
--                 *, 
--                 ROW_NUMBER() OVER(
--                     PARTITION BY _email, _utm_campaign 
--                     ORDER BY _priority DESC
--                 ) AS _rownum
--             FROM 
--                 focused_engagement
--         )
--         WHERE _rownum = 1
--     )    
--     SELECT * FROM final_engagement 
-- ) AS final
-- WHERE origin._email = final._email
-- AND origin._engagement = final._engagement
-- AND origin._utm_campaign = final._utm_campaign;