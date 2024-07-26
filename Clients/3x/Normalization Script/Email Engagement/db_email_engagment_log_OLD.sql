TRUNCATE TABLE `x-marketing.3x.db_email_engagements_log`;

INSERT INTO `x-marketing.3x.db_email_engagements_log`

-- Get campaign related info from Airtable
WITH campaign_info AS (

    SELECT 
        * EXCEPT(_rownum)
    FROM (

        SELECT 

            CAST(campaign.id AS STRING) AS _pardotid,
            CAST(airtable._livedate AS TIMESTAMP) AS _liveDate,
            airtable._code AS _contentTitle,
            airtable._subject,
            airtable._screenshot,
            airtable._landingPage,

            ROW_NUMBER() OVER(
                PARTITION BY airtable._pardotid 
                ORDER BY airtable._id DESC
            ) 
            AS _rownum
        
        FROM 
            `x-marketing.webtrack_ipcompany.db_airtable_3x_email` airtable
        JOIN
            `x-marketing.x3x_hubspot.campaigns` campaign
        ON 
            airtable._code = campaign.name

    ) 
    WHERE _rownum = 1

), 

-- Get campaign member related info from Hubspot
prospect_info AS (

    SELECT 
        * EXCEPT(_rownum) 
    FROM (

        SELECT

            property_email.value AS _email,
            CONCAT(property_firstname.value, ' ', property_lastname.value) AS _name,
            property_phone.value AS _phone,
            property_jobtitle.value AS _jobTitle,
            property_job_function.value AS _function,

            CASE
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Assistant to%") THEN "Non-Manager"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Senior Counsel%") THEN "VP"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%General Counsel%") THEN "C-Level"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Founder%") THEN "C-Level"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%C-Level%") THEN "C-Level"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CDO%") THEN "C-Level"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CIO%") THEN "C-Level"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CMO%") THEN "C-Level"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CFO%") THEN "C-Level"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CEO%") THEN "C-Level"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Chief%") THEN "C-Level"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%coordinator%") THEN "Non-Manager"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%COO%") THEN "C-Level"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Sr. V.P.%") THEN "Senior VP"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Sr.VP%") THEN "Senior VP"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Senior-Vice Pres%") THEN "Senior VP"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%srvp%") THEN "Senior VP"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Senior VP%") THEN "Senior VP"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%SR VP%") THEN "Senior VP"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Sr Vice Pres%") THEN "Senior VP"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Sr. VP%") THEN "Senior VP"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Sr. Vice Pres%") THEN "Senior VP"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%S.V.P%") THEN "Senior VP"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Senior Vice Pres%") THEN "Senior VP"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Exec Vice Pres%") THEN "Senior VP"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Exec Vp%") THEN "Senior VP"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Executive VP%") THEN "Senior VP"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Exec VP%") THEN "Senior VP"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Executive Vice President%") THEN "Senior VP"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%EVP%") THEN "Senior VP"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%E.V.P%") THEN "Senior VP"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%SVP%") THEN "Senior VP"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%V.P%") THEN "VP"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%VP%") THEN "VP"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Vice Pres%") THEN "VP"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%V P%") THEN "VP"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%President%") THEN "C-Level"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Director%") THEN "Director"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CTO%") THEN "C-Level"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Dir%") THEN "Director"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Dir.%") THEN "Director"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%MDR%") THEN "Non-Manager"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%MD%") THEN "Director"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%GM%") THEN "Director"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Head%") THEN "VP"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Manager%") THEN "Manager"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%escrow%") THEN "Non-Manager"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%cross%") THEN "Non-Manager"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%crosse%") THEN "Non-Manager"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Partner%") THEN "C-Level"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%CRO%") THEN "C-Level"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Chairman%") THEN "C-Level"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Owner%") THEN "C-Level"
                WHEN LOWER(properties.jobtitle.value) LIKE LOWER("%Team Lead%") THEN "Manager"
            END
            AS _seniority,

            COALESCE(
                associated_company.properties.name.value,
                property_company.value
            ) 
            AS _company, 

            INITCAP(REPLACE(associated_company.properties.industry.value, '_', ' ')) AS _industry,
            associated_company.properties.annualrevenue.value AS _revenue,

            COALESCE(
                property_city.value,
                associated_company.properties.city.value
            ) 
            AS _city, 

            COALESCE(
                property_state.value,
                associated_company.properties.state.value
            ) 
            AS _state,

            COALESCE(
                property_country.value, 
                associated_company.properties.country.value
            ) 
            AS _country,

            CASE
                WHEN property_lifecyclestage.value = 'marketingqualifiedlead' THEN 'Marketing Qualified Lead' 
                WHEN property_lifecyclestage.value = 'salesqualifiedlead' THEN 'Sales Qualified Lead' 
                ELSE INITCAP(property_lifecyclestage.value)
            END 
            AS _lifecycleStage,

            ROW_NUMBER() OVER(
                PARTITION BY property_email.value 
                ORDER BY vid DESC
            ) 
            AS _rownum

        FROM 
            `x-marketing.x3x_hubspot.contacts`
        WHERE 
            property_email.value IS NOT NULL 
        AND 
            property_email.value NOT LIKE '%2x.marketing%'

    ) 
    WHERE _rownum = 1

),

-- Get all email sent engagements
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
            ) 
            AS _rownum
        
        FROM 
            `x-marketing.x3x_hubspot.email_events` 
        WHERE 
            type = 'SENT' 
        AND 
            recipient NOT LIKE '%2x.marketing%'

    )
    WHERE _rownum = 1

),

-- Get all email dropped engagements
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
            ) 
            AS _rownum
        
        FROM 
            `x-marketing.x3x_hubspot.email_events` 
        WHERE 
            type = 'DROPPED' 
        AND 
            recipient NOT LIKE '%2x.marketing%'

    )
    WHERE _rownum = 1

),

-- Get all email sent engagements that were not dropped
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

-- Get all email delivered engagements
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
            ) 
            AS _rownum

        FROM 
            `x-marketing.x3x_hubspot.email_events` 
        WHERE 
            type = 'DELIVERED' 
        AND 
            recipient NOT LIKE '%2x.marketing%'

    )
    WHERE _rownum = 1

),

-- Get all email bounced engagements
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
            ) 
            AS _rownum

        FROM 
            `x-marketing.x3x_hubspot.email_events` 
        WHERE 
            type = 'BOUNCE' 
        AND 
            recipient NOT LIKE '%2x.marketing%'

    )
    WHERE _rownum = 1 

),

-- Get all email delivered that did not bounce 
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

-- Get all email opened engagements
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
            ) 
            AS _rownum

        FROM 
            `x-marketing.x3x_hubspot.email_events` 
        WHERE 
            type = 'OPEN' 
        AND 
            recipient NOT LIKE '%2x.marketing%'
        AND 
            filteredevent = false

    )
    WHERE _rownum = 1 

),

-- Get all email clicked engagements
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
            ) 
            AS _rownum

        FROM 
            `x-marketing.x3x_hubspot.email_events` 
        WHERE 
            type = 'CLICK' 
        AND 
            recipient NOT LIKE '%2x.marketing%'
        AND 
            filteredevent = false

    )
    WHERE _rownum = 1

),

-- Get all email clicked engagements that has an email opened
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

-- Get all email unsubscribed engagements
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
            ) 
            AS _rownum

        FROM 
            `x-marketing.x3x_hubspot.email_events` 
        WHERE 
            type = 'STATUSCHANGE' 
        AND 
            recipient NOT LIKE '%2x.marketing%'

    )
    WHERE _rownum = 1 

),

-- Combine all email engagements and tie with campaign info and prospect info
combined_data AS (

    SELECT 
        engagements.*,
        campaign_info.* EXCEPT(_pardotid),
        prospect_info.* EXCEPT(_email)

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

    ) engagements

    JOIN 
        campaign_info
    ON 
        engagements._campaignID = campaign_info._pardotid

    LEFT JOIN
        prospect_info
    ON
        engagements._email = prospect_info._email

),

-- Get all open click pairs to allow comparison between open time and click time
-- Get those where difference in open time and click time is less than 3 seconds
bot_open_click_pairs AS (

    SELECT DISTINCT

        click._email, 
        click._contentTitle
    
    FROM (

        SELECT
            _email, 
            _contentTitle, 
            _timestamp
        FROM
            combined_data
        WHERE
            _engagement = 'Opened'  

    ) open
    
    JOIN (

        SELECT
            _email, 
            _contentTitle, 
            _timestamp
        FROM
            combined_data
        WHERE
            _engagement = 'Clicked' 

    ) click
    
    ON 
        open._email = click._email
    AND
        open._contentTitle = click._contentTitle

    WHERE 
        TIMESTAMP_DIFF(click._timestamp, open._timestamp, SECOND) < 3

),

-- Get those who clicked the WISE portal link
bot_link_clicked AS (

    SELECT DISTINCT

        _email,
        _contentTitle

    FROM 
        combined_data
    WHERE 
        _description LIKE '%https://3x.wise-portal.com/iclick/iclick.php%'

),

-- Label prospects as bot for campaigns where bot activities are detected 
label_bots AS (

    SELECT
        main.*,

        CASE 
            -- Bot condition 1 : (Click time - Open time) < 3
           -- WHEN bot_1._email IS NOT NULL AND bot_1._contentTitle IS NOT NULL THEN true
            -- Bot condition 2 : Link clicked is WISE portal link
            WHEN bot_2._email IS NOT NULL AND bot_2._contentTitle IS NOT NULL THEN true
            ELSE false 
        END 
        AS _isBot

    FROM 
        combined_data AS main
    
    LEFT JOIN
        bot_open_click_pairs AS bot_1
    ON 
        main._email = bot_1._email
    AND 
        main._contentTitle = bot_1._contentTitle
    
    LEFT JOIN
        bot_link_clicked AS bot_2
    ON 
        main._email = bot_2._email
    AND 
        main._contentTitle = bot_2._contentTitle

)

SELECT * FROM label_bots;



---------------------------------------------- new script----------------