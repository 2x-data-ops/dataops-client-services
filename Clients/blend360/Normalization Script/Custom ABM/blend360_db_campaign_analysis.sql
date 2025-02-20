
-- BLEND 360 EMAIL PERFORMANCE (HUBSPOT + AIRTABLE)

-- First part of code comes from the old hubspot of integration
-- Data from the old integration was not transferred to the new integration

CREATE OR REPLACE TABLE `x-marketing.blend360.db_campaign_analysis` AS
WITH campaign_info AS (
    SELECT 
        * EXCEPT(_rownum)
    FROM (

        SELECT 
            _pardotid,

            CASE 
                WHEN LENGTH(TRIM(_livedate)) = 0 THEN NULL
                ELSE CAST(_livedate AS TIMESTAMP)
            END 
            AS _liveDate,

            _code AS _contentTitle,
            _subject,
            _screenshot,
            _landingPage,
            _emailfilters,
            _costofevent,
            _preposteventemail,
            _eventlevel,
            ROW_NUMBER() OVER(
                PARTITION BY _pardotid 
                ORDER BY _id DESC
            ) _rownum
        FROM 
            `x-marketing.blend360_mysql.db_airtable_email` 

    ) 
    WHERE _rownum = 1

), 
prospect_info AS (

    /*SELECT * EXCEPT(_rownum) 
    FROM (

        SELECT
            property_email.value AS _email,
            CONCAT(property_firstname.value, ' ', property_lastname.value) AS _name,
            property_phone.value AS _phone,
            property_jobtitle.value AS _jobTitle,
            COALESCE(associated_company.properties.name.value, property_company.value) AS _company,
            INITCAP(REPLACE(associated_company.properties.industry.value, '_', ' ')) AS _industry,
            associated_company.properties.annualrevenue.value AS _revenue,
            COALESCE(
                property_city.value,
                associated_company.properties.city.value
            ) AS _city, 
            COALESCE(
                property_state.value,
                associated_company.properties.state.value
            ) AS _state,
            COALESCE(
                property_country.value, 
                associated_company.properties.country.value
            ) AS _country,
            CASE
                WHEN property_lifecyclestage.value = 'marketingqualifiedlead' THEN 'Marketing Qualified Lead' 
                WHEN property_lifecyclestage.value = 'salesqualifiedlead' THEN 'Sales Qualified Lead' 
                ELSE INITCAP(property_lifecyclestage.value)
            END AS _lifecycleStage,
            CAST(NULL AS FLOAT64) AS _leadScore,
            ROW_NUMBER() OVER(
                PARTITION BY property_email.value 
                ORDER BY vid DESC
            ) AS _rownum
        FROM 
            `x-marketing.blend360_hubspot.contacts`
        WHERE 
            property_email.value IS NOT NULL 
        AND 
            property_email.value NOT LIKE '%2x.marketing%'

    ) 
    WHERE _rownum = 1*/
    SELECT
        DISTINCT CAST(_id AS STRING) AS _id,
        _email,
        _name,
        _domain,
        _jobtitle,
        _seniority,
        _function,
        _phone,
        _company,
        _industry,
        _revenue,
        _employee,
        _city,
        _state,
        _persona,
        _lifecycleStage,
        _createddate,
        _country,
        _num_tied_contacts,
        _num_form_contacts,
        _leadScore,
        -- _formSubmissions,
        -- _formSubmissionsTitle,
        -- _formSubmissionsURL,
        _pageViews,
        -- _unsubscribed
        _hubspotlink,
        _accountScoring,
      FROM
        `blend360.db_icp_database_log`

),
total_sent AS (

    SELECT 
        * EXCEPT(_rownum) 
    FROM (

        SELECT
            recipient AS _email,
            CAST(emailcampaignid AS STRING) AS _campaignID,
            url AS _description,
            created AS _timestamp,
            'Sent' AS _engagement,
            ROW_NUMBER() OVER(
                PARTITION BY recipient, emailcampaignid
                ORDER BY created DESC
            ) AS _rownum
        FROM 
            `x-marketing.blend360_hubspot.email_events` 
        WHERE 
            type = 'SENT' 
        AND 
            recipient NOT LIKE '%2x.marketing%'

    )
    WHERE _rownum = 1

),
unique_dropped AS (

    SELECT 
        * EXCEPT(_rownum) 
    FROM (

        SELECT
            recipient AS _email,
            CAST(emailcampaignid AS STRING) AS _campaignID,
            url AS _description,
            created AS _timestamp,
            'Dropped' AS _engagement,
            ROW_NUMBER() OVER(
                PARTITION BY recipient, emailcampaignid
                ORDER BY created DESC
            ) AS _rownum
        FROM 
            `x-marketing.blend360_hubspot.email_events` 
        WHERE 
            type = 'DROPPED' 
        AND 
            recipient NOT LIKE '%2x.marketing%'

    )
    WHERE _rownum = 1

),
unique_sent AS (

    SELECT 
        *
    FROM 
        total_sent
    WHERE 
        CONCAT(_email, _campaignID) NOT IN (
            SELECT 
                CONCAT(_email, _campaignID)
            FROM unique_dropped
        ) 

),
total_delivered AS (

    SELECT 
        * EXCEPT(_rownum) 
    FROM (

        SELECT
            recipient AS _email,
            CAST(emailcampaignid AS STRING) AS _campaignID,
            url AS _description,
            created AS _timestamp,
            'Delivered' AS _engagement,
            ROW_NUMBER() OVER(
                PARTITION BY recipient, emailcampaignid
                ORDER BY created DESC
            ) AS _rownum
        FROM 
            `x-marketing.blend360_hubspot.email_events` 
        WHERE 
            type = 'DELIVERED' 
        AND 
            recipient NOT LIKE '%2x.marketing%'

    )
    WHERE _rownum = 1

),
unique_bounced AS (

    SELECT 
        * EXCEPT(_rownum) 
    FROM (

        SELECT
            recipient AS _email,
            CAST(emailcampaignid AS STRING) AS _campaignID,
            response AS _description,
            created AS _timestamp,
            'Bounced' AS _engagement,
            ROW_NUMBER() OVER(
                PARTITION BY recipient, emailcampaignid
                ORDER BY created DESC
            ) AS _rownum
        FROM 
            `x-marketing.blend360_hubspot.email_events` 
        WHERE 
            type = 'BOUNCE' 
        AND 
            recipient NOT LIKE '%2x.marketing%'

    )
    WHERE _rownum = 1 

),
unique_delivered AS (

    SELECT 
        *
    FROM 
        total_delivered
    WHERE 
        CONCAT(_email, _campaignID) NOT IN (
            SELECT 
                CONCAT(_email, _campaignID)
            FROM unique_bounced
        ) 

),
unique_opened AS (

    SELECT 
        * EXCEPT(_rownum) 
    FROM (

        SELECT
            recipient AS _email,
            CAST(emailcampaignid AS STRING) AS _campaignID,
            url AS _description,
            created AS _timestamp,
            'Opened' AS _engagement,
            ROW_NUMBER() OVER(
                PARTITION BY recipient, emailcampaignid
                ORDER BY created DESC
            ) AS _rownum
        FROM 
            `x-marketing.blend360_hubspot.email_events` 
        WHERE 
            type = 'OPEN' 
        AND 
            recipient NOT LIKE '%2x.marketing%'
        AND 
            filteredevent = false

    )
    WHERE _rownum = 1 

),
total_clicked AS (

    SELECT 
        * EXCEPT(_rownum) 
    FROM (

        SELECT
            recipient AS _email,
            CAST(emailcampaignid AS STRING) AS _campaignID,
            url AS _description,
            created AS _timestamp,
            'Clicked' AS _engagement,
            ROW_NUMBER() OVER(
                PARTITION BY recipient, emailcampaignid
                ORDER BY created DESC
            ) AS _rownum
        FROM 
            `x-marketing.blend360_hubspot.email_events` 
        WHERE 
            type = 'CLICK' 
        AND 
            recipient NOT LIKE '%2x.marketing%'
        AND 
            filteredevent = false

    )
    WHERE _rownum = 1

),
unique_clicked AS (

    SELECT 
        *
    FROM 
        total_clicked
    WHERE 
        CONCAT(_email, _campaignID) IN (
            SELECT 
                CONCAT(_email, _campaignID)
            FROM unique_opened
        ) 

),
unique_unsubcribed AS (

    SELECT 
        * EXCEPT(_rownum) 
    FROM (

        SELECT
            recipient AS _email,
            CAST(emailcampaignid AS STRING) AS _campaignID,
            url AS _description,
            created AS _timestamp,
            'Unsubscribed' AS _engagement,
            ROW_NUMBER() OVER(
                PARTITION BY recipient, emailcampaignid
                ORDER BY created DESC
            ) AS _rownum
        FROM 
            `x-marketing.blend360_hubspot.email_events` 
        WHERE 
            type = 'STATUSCHANGE' 
        AND 
            recipient NOT LIKE '%2x.marketing%'

    )
    WHERE _rownum = 1 

),
total_downloaded AS (

    SELECT
        contact.properties.email.value AS _email,
        CASE
          WHEN form.value.page_url IS NOT NULL
          THEN SPLIT(SUBSTR(form.value.page_url, STRPOS(form.value.page_url, '_hsmi=') + 6), '&')[ORDINAL(1)] 
          ELSE NULL
        END AS _campaignID,
        CASE 
            WHEN form.value.page_url IS NOT NULL
            THEN SPLIT(SUBSTR(form.value.page_url, STRPOS(form.value.page_url, '_campaign=') + 10), '&')[ORDINAL(1)]
            ELSE NULL
        END AS _utm_campaign,
        form.value.page_url AS _description,
        form.value.timestamp AS _timestamp, 
        'Downloaded' AS _engagement
    FROM 
        `x-marketing.blend360_hubspot.contacts` contact, 
        UNNEST(form_submissions) AS form
    WHERE 
        contact.properties.email.value NOT LIKE '%2x.marketing%'
    AND 
        form.value.page_url LIKE '%utm_campaign%'

),
unique_downloaded AS (

    SELECT 
        * EXCEPT(_rownum) 
    FROM (

        SELECT
            main._email,
            side._pardotid AS _campaignID,
            main._description,
            main._timestamp, 
            main._engagement,
            ROW_NUMBER() OVER(
                PARTITION BY main._email, main._utm_campaign
                ORDER BY main._timestamp DESC
            ) AS _rownum
        FROM 
            total_downloaded AS main
        JOIN 
            `x-marketing.blend360_mysql.db_airtable_email` AS side
        ON 
            -- main._utm_campaign = side._utm_campaign
            main._campaignID = side._pardotid
    
    )
    WHERE _rownum = 1

),
combined_data AS (

    SELECT 
        engagements.*,
        campaign_info.* EXCEPT(_pardotid),
        prospect_info.* EXCEPT(_email),
        CASE
            WHEN LOWER(_engagement) = 'unsubscribed' THEN 'Y'
            ELSE 'N'
        END AS _unsubscribed
    FROM (
        SELECT * FROM unique_sent
        UNION ALL
        SELECT * FROM unique_delivered
        UNION ALL
        SELECT * FROM unique_opened
        UNION ALL
        SELECT * FROM unique_clicked
        UNION ALL
        SELECT * FROM unique_bounced
        UNION ALL
        SELECT * FROM unique_unsubcribed
        UNION ALL 
        SELECT * FROM unique_downloaded
    ) engagements
    JOIN 
        campaign_info
    ON 
        engagements._campaignID = campaign_info._pardotid
    LEFT JOIN
        prospect_info
    ON
        engagements._email = prospect_info._email

)
SELECT * FROM combined_data;


------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------

-- Second part of the code comes from the new hubspot integration

INSERT INTO `x-marketing.blend360.db_campaign_analysis` 
WITH campaign_info AS (

    SELECT 
        * EXCEPT(_rownum)
    FROM (

        SELECT 

            CASE 
                WHEN LENGTH(TRIM(_livedate)) = 0 THEN NULL
                ELSE CAST(_livedate AS TIMESTAMP)
            END 
            AS _liveDate,

            _code AS _contentTitle,
            _subject,
            _screenshot,
            _landingPage,
            _emailfilters,
            _costofevent,
            _preposteventemail,
            _eventlevel,
            ROW_NUMBER() OVER(
                PARTITION BY _pardotid 
                ORDER BY _id DESC
            ) _rownum
        FROM 
            `x-marketing.blend360_mysql.db_airtable_email` 

    ) 
    WHERE _rownum = 1

), 
prospect_info AS (

    /*SELECT * EXCEPT(_rownum) 
    FROM (

        SELECT
            property_email.value AS _email,
            CONCAT(property_firstname.value, ' ', property_lastname.value) AS _name,
            property_phone.value AS _phone,
            property_jobtitle.value AS _jobTitle,
            COALESCE(associated_company.properties.name.value, property_company.value) AS _company,
            INITCAP(REPLACE(associated_company.properties.industry.value, '_', ' ')) AS _industry,
            associated_company.properties.annualrevenue.value AS _revenue,
            COALESCE(
                property_city.value,
                associated_company.properties.city.value
            ) AS _city, 
            COALESCE(
                property_state.value,
                associated_company.properties.state.value
            ) AS _state,
            COALESCE(
                property_country.value, 
                associated_company.properties.country.value
            ) AS _country,
            CASE
                WHEN property_lifecyclestage.value = 'marketingqualifiedlead' THEN 'Marketing Qualified Lead' 
                WHEN property_lifecyclestage.value = 'salesqualifiedlead' THEN 'Sales Qualified Lead' 
                ELSE INITCAP(property_lifecyclestage.value)
            END AS _lifecycleStage,
            property_blend360___lead_score.value AS _leadScore,
            ROW_NUMBER() OVER(
                PARTITION BY property_email.value 
                ORDER BY vid DESC
            ) AS _rownum
        FROM 
            `x-marketing.blend360_hubspot_v2.contacts`
        WHERE 
            property_email.value IS NOT NULL 
        AND 
            property_email.value NOT LIKE '%2x.marketing%'

    ) 
    WHERE _rownum = 1*/
    SELECT
        DISTINCT CAST(_id AS STRING) AS _id,
        _email,
        _name,
        _domain,
        _jobtitle,
        _seniority,
        _function,
        _phone,
        _company,
        _industry,
        _revenue,
        _employee,
        _city,
        _state,
        _persona,
        _lifecycleStage,
        _createddate,
        _country,
        _num_tied_contacts,
        _num_form_contacts,
        _leadScore,
        -- _formSubmissions,
        -- _formSubmissionsTitle,
        -- _formSubmissionsURL,
        _pageViews,
        -- _unsubscribed
        _hubspotlink,
        _accountScoring,
      FROM
        `blend360.db_icp_database_log`

),
total_sent AS (

    SELECT 
        * EXCEPT(_rownum) 
    FROM (

        SELECT
            main.recipient AS _email,
            CAST(main.emailcampaignid AS STRING) AS _campaignID,
            side.name AS _contentTitle,
            main.url AS _description,
            main.created AS _timestamp,
            'Sent' AS _engagement,
            ROW_NUMBER() OVER(
                PARTITION BY main.recipient, main.emailcampaignid
                ORDER BY main.created DESC
            ) AS _rownum
        FROM 
            `x-marketing.blend360_hubspot_v2.email_events` main
        JOIN
            `x-marketing.blend360_hubspot_v2.campaigns` side
        ON
            main.emailcampaignid = side.id
        WHERE 
            main.type = 'SENT' 
        AND 
            main.recipient NOT LIKE '%2x.marketing%'

    )
    WHERE _rownum = 1

),
unique_dropped AS (

    SELECT 
        * EXCEPT(_rownum) 
    FROM (

        SELECT
            main.recipient AS _email,
            CAST(main.emailcampaignid AS STRING) AS _campaignID,
            side.name AS _contentTitle,
            main.url AS _description,
            main.created AS _timestamp,
            'Dropped' AS _engagement,
            ROW_NUMBER() OVER(
                PARTITION BY main.recipient, main.emailcampaignid
                ORDER BY main.created DESC
            ) AS _rownum
        FROM 
            `x-marketing.blend360_hubspot_v2.email_events` main
        JOIN
            `x-marketing.blend360_hubspot_v2.campaigns` side
        ON
            main.emailcampaignid = side.id
        WHERE 
            main.type = 'DROPPED' 
        AND 
            main.recipient NOT LIKE '%2x.marketing%'

    )
    WHERE _rownum = 1

),
unique_sent AS (

    SELECT 
        *
    FROM 
        total_sent
    WHERE 
        CONCAT(_email, _campaignID) NOT IN (
            SELECT 
                CONCAT(_email, _campaignID)
            FROM unique_dropped
        ) 

),
total_delivered AS (

    SELECT 
        * EXCEPT(_rownum) 
    FROM (

        SELECT
            main.recipient AS _email,
            CAST(main.emailcampaignid AS STRING) AS _campaignID,
            side.name AS _contentTitle,
            main.url AS _description,
            main.created AS _timestamp,
            'Delivered' AS _engagement,
            ROW_NUMBER() OVER(
                PARTITION BY main.recipient, main.emailcampaignid
                ORDER BY main.created DESC
            ) AS _rownum
        FROM 
            `x-marketing.blend360_hubspot_v2.email_events` main
        JOIN
            `x-marketing.blend360_hubspot_v2.campaigns` side
        ON
            main.emailcampaignid = side.id
        WHERE 
            main.type = 'DELIVERED' 
        AND 
            main.recipient NOT LIKE '%2x.marketing%'

    )
    WHERE _rownum = 1

),
unique_bounced AS (

    SELECT 
        * EXCEPT(_rownum) 
    FROM (

        SELECT
            main.recipient AS _email,
            CAST(main.emailcampaignid AS STRING) AS _campaignID,
            side.name AS _contentTitle,
            main.response AS _description,
            main.created AS _timestamp,
            'Bounced' AS _engagement,
            ROW_NUMBER() OVER(
                PARTITION BY main.recipient, main.emailcampaignid
                ORDER BY main.created DESC
            ) AS _rownum
        FROM 
            `x-marketing.blend360_hubspot_v2.email_events` main
        JOIN
            `x-marketing.blend360_hubspot_v2.campaigns` side
        ON
            main.emailcampaignid = side.id
        WHERE 
            main.type = 'BOUNCE' 
        AND 
            main.recipient NOT LIKE '%2x.marketing%'

    )
    WHERE _rownum = 1 

),
unique_delivered AS (

    SELECT 
        *
    FROM 
        total_delivered
    WHERE 
        CONCAT(_email, _campaignID) NOT IN (
            SELECT 
                CONCAT(_email, _campaignID)
            FROM unique_bounced
        ) 

),
unique_opened AS (

    SELECT 
        * EXCEPT(_rownum) 
    FROM (

        SELECT
            main.recipient AS _email,
            CAST(main.emailcampaignid AS STRING) AS _campaignID,
            side.name AS _contentTitle,
            main.url AS _description,
            main.created AS _timestamp,
            'Opened' AS _engagement,
            ROW_NUMBER() OVER(
                PARTITION BY main.recipient, main.emailcampaignid
                ORDER BY main.created DESC
            ) AS _rownum
        FROM 
            `x-marketing.blend360_hubspot_v2.email_events` main
        JOIN
            `x-marketing.blend360_hubspot_v2.campaigns` side
        ON
            main.emailcampaignid = side.id
        WHERE 
            main.type = 'OPEN' 
        AND 
            main.recipient NOT LIKE '%2x.marketing%'
        AND 
            main.filteredevent = false

    )
    WHERE _rownum = 1 

),
total_clicked AS (

    SELECT 
        * EXCEPT(_rownum) 
    FROM (

        SELECT
            main.recipient AS _email,
            CAST(main.emailcampaignid AS STRING) AS _campaignID,
            side.name AS _contentTitle,
            main.url AS _description,
            main.created AS _timestamp,
            'Clicked' AS _engagement,
            ROW_NUMBER() OVER(
                PARTITION BY main.recipient, main.emailcampaignid
                ORDER BY main.created DESC
            ) AS _rownum
        FROM 
            `x-marketing.blend360_hubspot_v2.email_events` main
        JOIN
            `x-marketing.blend360_hubspot_v2.campaigns` side
        ON
            main.emailcampaignid = side.id
        WHERE 
            main.type = 'CLICK' 
        AND 
            main.recipient NOT LIKE '%2x.marketing%'
        AND 
            main.filteredevent = false

    )
    WHERE _rownum = 1

),
unique_clicked AS (

    SELECT 
        *
    FROM 
        total_clicked
    WHERE 
        CONCAT(_email, _campaignID) IN (
            SELECT 
                CONCAT(_email, _campaignID)
            FROM unique_opened
        ) 

),
unique_unsubcribed AS (

    SELECT 
        * EXCEPT(_rownum) 
    FROM (

        SELECT
            main.recipient AS _email,
            CAST(main.emailcampaignid AS STRING) AS _campaignID,
            side.name AS _contentTitle,
            main.url AS _description,
            main.created AS _timestamp,
            'Unsubscribed' AS _engagement,
            ROW_NUMBER() OVER(
                PARTITION BY main.recipient, main.emailcampaignid
                ORDER BY main.created DESC
            ) AS _rownum
        FROM 
            `x-marketing.blend360_hubspot_v2.email_events` main
        JOIN
            `x-marketing.blend360_hubspot_v2.campaigns` side
        ON
            main.emailcampaignid = side.id
        WHERE 
            main.type = 'STATUSCHANGE' 
        AND 
            main.recipient NOT LIKE '%2x.marketing%'

    )
    WHERE _rownum = 1 

),
total_downloaded AS (

    SELECT
        contact.properties.email.value AS _email,
        CASE
          WHEN form.value.page_url IS NOT NULL
          THEN SPLIT(SUBSTR(form.value.page_url, STRPOS(form.value.page_url, '_hsmi=') + 6), '&')[ORDINAL(1)] 
          ELSE NULL
        END AS _campaignID,
        CASE 
            WHEN form.value.page_url IS NOT NULL
            THEN SPLIT(SUBSTR(form.value.page_url, STRPOS(form.value.page_url, '_campaign=') + 10), '&')[ORDINAL(1)]
            ELSE NULL
        END AS _utm_campaign,
        form.value.page_url AS _description,
        form.value.timestamp AS _timestamp, 
        'Downloaded' AS _engagement
    FROM 
        `x-marketing.blend360_hubspot_v2.contacts` contact, 
        UNNEST(form_submissions) AS form
    WHERE 
        contact.properties.email.value NOT LIKE '%2x.marketing%'
    AND 
        form.value.page_url LIKE '%utm_campaign%'

),
unique_downloaded AS (

    SELECT 
        * EXCEPT(_rownum) 
    FROM (

        SELECT
            main._email,
            side._pardotid AS _campaignID,
            side._code AS _contentTitle,
            main._description,
            main._timestamp, 
            main._engagement,
            ROW_NUMBER() OVER(
                PARTITION BY main._email, main._utm_campaign
                ORDER BY main._timestamp DESC
            ) AS _rownum
        FROM 
            total_downloaded AS main
        JOIN 
            `x-marketing.blend360_mysql.db_airtable_email` AS side
        ON 
            -- main._utm_campaign = side._utm_campaign
            main._campaignID = side._pardotid
    
    )
    WHERE _rownum = 1

),
combined_data AS (

    SELECT 
        engagements.* EXCEPT(_contentTitle),
        campaign_info.*,
        prospect_info.* EXCEPT(_email),
        CASE
            WHEN LOWER(_engagement) = 'unsubscribed' THEN 'Y'
            ELSE 'N'
        END AS _unsubscribed
    FROM (
        SELECT * FROM unique_sent
        UNION ALL
        SELECT * FROM unique_delivered
        UNION ALL
        SELECT * FROM unique_opened
        UNION ALL
        SELECT * FROM unique_clicked
        UNION ALL
        SELECT * FROM unique_bounced
        UNION ALL
        SELECT * FROM unique_unsubcribed
        UNION ALL 
        SELECT * FROM unique_downloaded
    ) engagements
    JOIN 
        campaign_info
    ON 
        engagements._contentTitle = campaign_info._contentTitle
    LEFT JOIN
        prospect_info
    ON
        engagements._email = prospect_info._email

)
SELECT * FROM combined_data;


------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------

-- Side part to get the new contacts from event list 

CREATE OR REPLACE TABLE `blend360.db_event_members` AS
SELECT DISTINCT
    CONCAT(
        contact.property_firstname.value, ' ',
        contact.property_lastname.value
    )
    AS name,
    contact.property_email.value AS email,
    contact.property_phone.value AS phone,
    CONCAT(
        owner.firstname, ' ',
        owner.lastname
    )
    AS contact_owner,
    contact.associated_company.properties.name.value AS primary_company,
    INITCAP(contact.property_hs_lead_status.value) AS lead_status,
    IF(
        contact.property_hs_marketable_status.value = 'true', 
        'Marketing contact',
        'Non-marketing contact'
    )  
    AS marketing_contact_status,
    form.value.title AS form_submitted,
    contact.property_createdate.value AS created_date,
    contact.property_notes_last_updated.value AS last_activity_date
FROM 
    `blend360_hubspot_v2.contacts` contact, 
    UNNEST(form_submissions) AS form
LEFT JOIN
    `blend360_hubspot_v2.owners` owner
ON 
    contact.property_hubspot_owner_id.value = CAST(owner.ownerid AS STRING)
WHERE 
    form.value.title LIKE '%HubSpot Webinar 2023 LP form January 31, 2023 5:00:44 PM CET%'
AND
    property_email.value NOT LIKE '%test%'
ORDER BY
    email
;


------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
------------------------------------------------------------------------------------------------
-- Multiple tables joining
CREATE OR REPLACE TABLE `x-marketing.blend360.multiple_engagement_data` AS
WITH email_clicks AS ( --90 days
    SELECT     	
        std_name AS _standardizedcompanyname,
        industry,
        NULL AS _ads_engagements,
        CAST(NULL AS STRING) AS _visitoridleadfeeder,
        NULL AS _impressions,
        'Clicked' AS _engagement,
        NULL AS fd_click_delivered,
        NULL AS db_clicks
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching`
    LEFT JOIN `x-marketing.blend360.db_campaign_analysis` ON hs_name = _company
    WHERE _engagement = 'Clicked' AND _emailfilters = 'Campaign'
    AND CAST(_timestamp AS DATE) >= DATE_ADD(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL -90 DAY)
    AND CAST(_timestamp AS DATE) < DATE_ADD(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL 1 DAY)

),

ads_engagement AS ( --use in google sheet
    SELECT 
       std_name AS _standardizedcompanyname,
       industry,
       CAST(ad_engagements AS INT64) AS _ads_engagements,
       CAST(NULL AS STRING) as _visitoridleadfeeder,
       impressions AS _impressions,
       '' AS _engagement,
       NULL AS fd_click_delivered,
       NULL AS db_clicks
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching`
    LEFT JOIN `x-marketing.blend360_campaign_data.LI_Account_Engagement` ON li_name = li_company_name

),

unique_web AS ( --90 days
    SELECT 
        std_name AS _standardizedcompanyname,
        industry,
       NULL AS _ads_engagements,
       _visitoridleadfeeder,
       NULL AS _impressions,
       '' AS _engagement,
       NULL AS fd_click_delivered,
       NULL AS db_clicks
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching`
    LEFT JOIN `x-marketing.blend360_mysql.db_dealfront_web_visits` ON df_name = _companyname
    WHERE PARSE_DATE('%m/%d/%Y',_visitstartdate) >= DATE_ADD(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL -90 DAY)
    AND PARSE_DATE('%m/%d/%Y',_visitstartdate) < DATE_ADD(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL 1 DAY)
    --AND std_name = 'American Express'
),

foundry_performance AS ( --google sheet
    SELECT 
        std_name AS _standardizedcompanyname,
        a.industry,
        NULL AS _ads_engagements,
        CAST(NULL AS STRING) AS _visitoridleadfeeder,
        impressions_delivered AS _impressions,
        '' AS _engagement,
        clicks_delivered AS fd_click_delivered,
        NULL AS db_clicks
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` a
    LEFT JOIN `x-marketing.blend360_campaign_data.FD_Account_Data` ON fd_name = company
),

demandbase_performance AS ( --google sheet
    SELECT
        std_name AS _standardizedcompanyname,
        a.industry,
        NULL AS _ads_engagements,
        CAST(NULL AS STRING) AS _visitoridleadfeeder,
        impressions,
        '' AS _engagement,
        NULL AS fd_click_delivered,
        clicks AS db_clicks
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching` a
    LEFT JOIN `x-marketing.blend360_campaign_data.DB_Domain_Summary` ON db_name = domain_name

), all_data AS (

SELECT *
FROM (
    SELECT * FROM email_clicks
    UNION ALL
    SELECT * FROM ads_engagement
    UNION ALL
    SELECT * FROM unique_web
    UNION ALL
    SELECT * FROM foundry_performance
    UNION ALL
    SELECT * FROM demandbase_performance
)
) , reached AS ( 
    SELECT _standardizedcompanyname,industry, CASE WHEN SUM(_impressions) >= 1 THEN 'Y' ELSE 'N' END AS _reacheds
FROM all_data
GROUP BY 1,2
) SELECT all_data.*,_reacheds
 FROM all_data
LEFT JOIN reached ON all_data._standardizedcompanyname = reached._standardizedcompanyname AND all_data.industry = reached.industry;


--aggregated dealfront web visit
CREATE OR REPLACE TABLE `x-marketing.blend360.aggregated_dealfront_web_visit` AS 
WITH _90days AS (
SELECT 
    std_name AS _standardizedcompanyname,
    industry,
    '90 days' AS _data,
    COUNT(DISTINCT _visitoridleadfeeder) AS unique_visit,
    MAX(PARSE_DATE('%m/%d/%Y',_visitstartdate)) AS last_visit_date
FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching`
LEFT JOIN `x-marketing.blend360_mysql.db_dealfront_web_visits` ON df_name = _companyname
WHERE PARSE_DATE('%m/%d/%Y',_visitstartdate) >= DATE_ADD(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL - 90 DAY)
AND PARSE_DATE('%m/%d/%Y',_visitstartdate) < DATE_ADD(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL 1 DAY)
--AND std_name = 'Amazon'
GROUP BY 1,2
),
_all AS (
    SELECT 
    std_name AS _standardizedcompanyname,
    industry,
    'Overall' AS _data,
    COUNT(DISTINCT _visitoridleadfeeder) AS unique_visit,
    MAX(PARSE_DATE('%m/%d/%Y',_visitstartdate)) AS last_visit_date
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching`
    LEFT JOIN `x-marketing.blend360_mysql.db_dealfront_web_visits` ON df_name = _companyname
    --AND std_name = 'Amazon'
    GROUP BY 1,2
)
SELECT * 
FROM (
    SELECT * FROM _90days
    UNION ALL
    SELECT * FROM _all
);


--list of web visit activities
CREATE OR REPLACE TABLE `x-marketing.blend360.dealfront_web_visit` AS
WITH _90days AS (
SELECT
    std_name AS _standardizedcompanyname,
    industry,
    CASE WHEN _url LIKE '%utm_%' THEN 'www.blend360.com/'
    WHEN _url LIKE '%hsa_%' THEN SPLIT(_url,'?hsa_acc')[SAFE_OFFSET(0)]
    ELSE _url END AS page_visit,
    PARSE_DATE('%m/%d/%Y',_visitstartdate) AS visit_date,
    _timeonpageseconds,
    '90 days' AS _data
FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching`
LEFT JOIN `x-marketing.blend360_mysql.db_dealfront_web_visits` ON df_name = _companyname
WHERE PARSE_DATE('%m/%d/%Y',_visitstartdate) >= DATE_ADD(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL - 90 DAY)
AND PARSE_DATE('%m/%d/%Y',_visitstartdate) < DATE_ADD(DATE_TRUNC(CURRENT_DATE(), WEEK(MONDAY)), INTERVAL 1 DAY)
),
_all AS (
    SELECT
    std_name AS _standardizedcompanyname,
    industry,
    CASE WHEN _url LIKE '%utm_%' THEN 'www.blend360.com/'
    WHEN _url LIKE '%hsa_%' THEN SPLIT(_url,'?hsa_acc')[SAFE_OFFSET(0)]
    ELSE _url END AS page_visit,
    PARSE_DATE('%m/%d/%Y',_visitstartdate) AS visit_date,
    _timeonpageseconds,
    'Overall' AS _data
    FROM `x-marketing.blend360_master_list.F1000_Acc_Name_Matching`
    LEFT JOIN `x-marketing.blend360_mysql.db_dealfront_web_visits` ON df_name = _companyname
    WHERE _url IS NOT NULL
)
SELECT *
FROM (
    SELECT * FROM _90days
    UNION ALL
    SELECT * FROM _all
);




/*
-- temporary not in use
CREATE OR REPLACE TABLE `x-marketing.blend360.aggregated_engagement_data` AS
SELECT  _standardizedcompanyname, 
        industry, 
        COUNT(DISTINCT _visitoridleadfeeder) AS unique_visitor,
        SUM(CASE WHEN _engagement = 'Clicked' THEN 1 ELSE 0 END) AS email_clicks,
        CAST(AVG(_ads_engagements) AS INT64) AS li_ad_engagements,
        SUM(fd_click_delivered) AS fd_click,
        SUM(db_clicks) AS db_click,
        CASE WHEN SUM(_ads_engagements) > 0 AND SUM(db_clicks) >= 0 AND SUM(fd_click_delivered) >= 0 THEN (SUM(CASE WHEN _engagement = 'Clicked' THEN 1 ELSE 0 END) + AVG(_ads_engagements) + 
        COUNT(DISTINCT _visitoridleadfeeder) + SUM(fd_click_delivered) + SUM(db_clicks))
        WHEN sum(_ads_engagements) > 0 AND SUM(db_clicks) >= 0 THEN (SUM(CASE WHEN _engagement = 'Clicked' THEN 1 ELSE 0 END) + AVG(_ads_engagements) + 
        COUNT(DISTINCT _visitoridleadfeeder) + SUM(db_clicks))
        WHEN sum(_ads_engagements) > 0 AND SUM(fd_click_delivered) >= 0 THEN (SUM(CASE WHEN _engagement = 'Clicked' THEN 1 ELSE 0 END) + AVG(_ads_engagements) + 
        COUNT(DISTINCT _visitoridleadfeeder) + SUM(fd_click_delivered))
        WHEN SUM(fd_click_delivered) >= 0 AND SUM(db_clicks) >= 0 THEN (SUM(CASE WHEN _engagement = 'Clicked' THEN 1 ELSE 0 END) + COUNT(DISTINCT _visitoridleadfeeder) + SUM(fd_click_delivered) + SUM(db_clicks))
        WHEN SUM(fd_click_delivered) >= 0 THEN (SUM(CASE WHEN _engagement = 'Clicked' THEN 1 ELSE 0 END) + COUNT(DISTINCT _visitoridleadfeeder) + SUM(fd_click_delivered))
        WHEN SUM(db_clicks) >= 0 THEN (SUM(CASE WHEN _engagement = 'Clicked' THEN 1 ELSE 0 END) + COUNT(DISTINCT _visitoridleadfeeder) + SUM(db_clicks))
        WHEN SUM(_ads_engagements) IS NULL AND SUM(fd_click_delivered) IS NULL AND SUM(db_clicks) IS NULL THEN (SUM(CASE WHEN _engagement = 'Clicked' THEN 1 ELSE 0 END) + COUNT(DISTINCT _visitoridleadfeeder))
        WHEN SUM(_ads_engagements) IS NULL THEN (SUM(CASE WHEN _engagement = 'Clicked' THEN 1 ELSE 0 END) + COUNT(DISTINCT _visitoridleadfeeder) + 
        SUM(fd_click_delivered) + SUM(db_clicks))
        WHEN SUM(fd_click_delivered) IS NULL THEN (SUM(CASE WHEN _engagement = 'Clicked' THEN 1 ELSE 0 END) + COUNT(DISTINCT _visitoridleadfeeder) + AVG(_ads_engagements))
        WHEN SUM(db_clicks) IS NULL THEN (SUM(CASE WHEN _engagement = 'Clicked' THEN 1 ELSE 0 END) + COUNT(DISTINCT _visitoridleadfeeder) + AVG(_ads_engagements))
        END AS total_engagement
FROM `x-marketing.blend360.multiple_engagement_data`
GROUP BY 1,2;

-- currently not in use
CREATE OR REPLACE TABLE `x-marketing.blend360.aggregated_industry_engagement_data` AS
SELECT DISTINCT industry,
    CAST(unique_visitor AS INT64) AS unique_visitor,
    CAST(email_clicks AS INT64) AS email_clicks,
    CAST((CASE WHEN li_ad_engagements IS NULL THEN 0 ELSE li_ad_engagements END) AS INT64) AS li_ad_engagements,
    CAST((CASE WHEN fd_click IS NULL THEN 0 ELSE fd_click END) AS INT64) AS fd_click,
    CAST((CASE WHEN db_click IS NULL THEN 0 ELSE db_click END) AS INT64) AS db_click,
    CAST(unique_visitor AS INT64) + CAST(email_clicks AS INT64) + CAST((CASE WHEN li_ad_engagements IS NULL THEN 0 ELSE li_ad_engagements END) AS INT64) +
    CAST((CASE WHEN fd_click IS NULL THEN 0 ELSE fd_click END) AS INT64) + CAST((CASE WHEN db_click IS NULL THEN 0 ELSE db_click END) AS INT64) AS total_engagements
FROM `x-marketing.blend360.aggregated_engagement_data`;*/