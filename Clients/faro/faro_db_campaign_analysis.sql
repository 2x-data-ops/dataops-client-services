-- TRUNCATE TABLE `x-marketing.faro.db_campaign_analysis`;

/* INSERT INTO `x-marketing.faro.db_campaign_analysis` (
  _sdc_sequence,
  _prospectID,
  _email,
  _campaignID,
  _contentTitle,
  _timestamp,
  _description,
  _engagement,
  list_email_id,
  ---_campaignSentDate,
  _utm_campaign,
  ---_subject,
  ---_screenshot,
  ---,
  _name,
  _title,
  ---_function,
  ---_persona,
  _seniority,
  _company,
  _industry,
  _revenue,
  _employees,
  _city,
  _state,
  _country
) */
CREATE OR REPLACE TABLE `x-marketing.faro.db_campaign_analysis` AS
WITH /* campaign_info AS (
  SELECT * EXCEPT(rownum)
  FROM(
    SELECT
      CAST(id AS STRING) AS _campaignid,
      name AS _utm_campaign,
      ROW_NUMBER() OVER(
        PARTITION BY id
        ORDER BY id DESC
      ) AS rownum
    FROM
      `faro_pardot.campaigns`
  )
  WHERE rownum = 1
), */
email_template AS ( #Manually updated - to be changed to db_airtable_email from mysql later.
  SELECT 
    -- CAST(_emailtemplateid AS INT) AS _email_template_id,
    SAFE_CAST(_emailtemplateid AS INT) AS _email_template_id,
    CAST(_campaignID AS STRING) AS _campaignID,
    _utm_campaign AS _utm_campaign,
    _subject AS _subject,
    _screenshot AS _screenshot,
    _landingpage AS _landingPage,
    _name AS _email_template_name,
    _campaignname AS _campaign_name
    -- CAST(_livedate AS TIMESTAMP) AS _campaignSentDate
  -- FROM `faro.airtable_email`
  FROM `x-marketing.faro_mysql.db_airtable_email_template`
),
market_segment AS (
  SELECT * EXCEPT(rownum) 
  FROM (
    SELECT leadorcontactid, pull_market_segment__c,
    ROW_NUMBER() OVER(PARTITION BY leadorcontactid, pull_market_segment__c ORDER BY lastmodifieddate DESC) AS rownum
    FROM `faro_salesforce.CampaignMember` main
  ) WHERE rownum = 1
),
prospect_info AS (
  SELECT * EXCEPT(rownum)
  FROM (
    SELECT
      prospect.email AS _email,
      CONCAT(prospect.first_name, ' ', prospect.last_name) AS _name,
      prospect.job_title AS _title,
      CAST(NULL AS STRING) AS _seniority,
      prospect.company AS _company,
      prospect.industry AS _industry,
      prospect.annual_revenue AS _revenuerange,
      prospect.employees AS _employees,
      prospect.city AS _city,
      prospect.state AS _state,
      prospect.country AS _country,
      prospect.crm_lead_fid AS _sfdcLeadid,
      prospect.crm_contact_fid AS _sfdcContactid,
      prospect.crm_owner_fid AS _sfdcOwnerid,
      prospect.source AS _source,
      COALESCE(cnt.leadsource, ld.leadsource) AS _lead_source,
      COALESCE(cnt.waterfall_stage__c, ld.waterfall_stage__c) AS _waterfall_stage,
      COALESCE(cnt.division_region__c, ld.division_region__c) AS _division_region,
      pull_market_segment__c AS _market_segment,
      ld.initial_opt_in_lead__c AS lead_initial_opt_in,
      cnt.initial_opt_in_contact__c AS contact_initial_opt,
      /* 
      campaign name DONE
      2. lead/contact ID from salesforce* DONE
      3. name* DONE
      4. division/region* DONE
      5. market segment* DONE
      6. email* DONE
      7. company DONE
      8.title DONE
      9. country* DONE
      10.lead source DONE
      11.waterfall stage DONE
      */
      ROW_NUMBER() OVER(
        PARTITION BY prospect.email
        ORDER BY prospect.email DESC
      ) AS rownum
    FROM
      `x-marketing.faro_pardot.prospects` prospect
    LEFT JOIN 
      `faro_salesforce.Contact` cnt ON prospect.crm_contact_fid = cnt.id
    LEFT JOIN 
      `faro_salesforce.Lead` ld ON prospect.crm_lead_fid = ld.id
    LEFT JOIN 
      market_segment ON COALESCE(crm_contact_fid, crm_lead_fid) = market_segment.leadorcontactid
    -- LEFT JOIN
    --   seniority
    -- ON
    --   prospect.job_title LIKE CONCAT ('%',seniority.title,'%')
  )
  WHERE rownum = 1
),
sent_email AS (
  SELECT * EXCEPT(rownum)
  FROM (
    SELECT
      activity._sdc_sequence,
      CAST(activity.prospect_id AS STRING) AS _prospectID,
      prospect.email AS _email,
      CAST(activity.campaign_id AS STRING) AS _campaignID,
      campaign.name AS _contentTitle,
      activity.created_at AS _timestamp,
      activity.details AS _description,
      'Sent' AS _engagement,
      CAST(activity.list_email_id AS STRING) AS _email_id,
      activity.email_template_id AS email_template_id,
      ROW_NUMBER() OVER(
        PARTITION BY activity.prospect_id, activity.email_template_id
        ORDER BY activity.created_at DESC
      ) AS rownum
    FROM
      `x-marketing.faro_pardot.visitor_activities` activity
    LEFT JOIN
      `x-marketing.faro_pardot.prospects` prospect
    ON
      activity.prospect_id = prospect.id
    LEFT JOIN
      `x-marketing.faro_pardot.campaigns` campaign
    ON
      activity.campaign_id = campaign.id
    WHERE
      activity.type_name = 'Email'
    AND
      activity.type = 6   /* Sent */
    /*AND (
      (email NOT LIKE '%2x.marketing%'
      AND email NOT LIKE '%faro.com%'
      AND email NOT LIKE '%faroeurope.com%')
      OR
      email IS NULL
    )*/
  )
  WHERE rownum = 1 
),
opened_email AS (
  SELECT * EXCEPT(rownum)
  FROM (
    SELECT
      activity._sdc_sequence,
      CAST(activity.prospect_id AS STRING) AS _prospectID,
      prospect.email AS _email,
      CAST(activity.campaign_id AS STRING) AS _campaignID,
      campaign.name AS _contentTitle,
      activity.created_at AS _timestamp,
      activity.details AS _description,
      'Opened' AS _engagement,
      CAST(activity.list_email_id AS STRING) AS _email_id,
      activity.email_template_id AS email_template_id,
      ROW_NUMBER() OVER(
        PARTITION BY activity.prospect_id, activity.email_template_id
        ORDER BY activity.created_at DESC
      ) AS rownum
    FROM
      `x-marketing.faro_pardot.visitor_activities` activity
    LEFT JOIN
      `x-marketing.faro_pardot.prospects` prospect
    ON
      activity.prospect_id = prospect.id
    LEFT JOIN
      `x-marketing.faro_pardot.campaigns` campaign
    ON
      activity.campaign_id = campaign.id
    WHERE
      activity.type_name = 'Email'
    AND
      activity.type = 11   /* Open */
    /*AND (
      (email NOT LIKE '%2x.marketing%'
      AND email NOT LIKE '%faro.com%'
      AND email NOT LIKE '%faroeurope.com%')
      OR
      email IS NULL
    )*/
  )
  WHERE rownum = 1
),
clicked_email_cta AS (
  SELECT * EXCEPT(rownum)
  FROM (
    SELECT
      activity._sdc_sequence,
      CAST(activity.prospect_id AS STRING) AS _prospectID,
      prospect.email AS _email,
      CAST(activity.campaign_id AS STRING) AS _campaignID,
      campaign.name AS _contentTitle,
      activity.created_at AS _timestamp,
      activity.details AS _description,
      'Clicked' AS _engagement,
      CAST(activity.list_email_id AS STRING) AS _email_id,
      activity.email_template_id AS email_template_id,
      ROW_NUMBER() OVER(
        PARTITION BY activity.prospect_id, activity.email_template_id
        ORDER BY activity.created_at DESC
      ) AS rownum
    FROM
      `x-marketing.faro_pardot.visitor_activities` activity
    LEFT JOIN
      `x-marketing.faro_pardot.prospects` prospect
    ON
      activity.prospect_id = prospect.id
    LEFT JOIN
      `x-marketing.faro_pardot.campaigns` campaign
    ON
      activity.campaign_id = campaign.id
    WHERE
      activity.type_name IN ('Email', 'Email Tracker')
    AND activity.type = 1   /* Click */
    -- AND details LIKE '%utm_%' 
    /*AND (
      (email NOT LIKE '%2x.marketing%'
      AND email NOT LIKE '%faro.com%'
      AND email NOT LIKE '%faroeurope.com%')
      OR
      email IS NULL
    )*/
  )
  WHERE rownum = 1
),
bounced_email AS (
  SELECT * EXCEPT(rownum)
  FROM (
    SELECT
      activity._sdc_sequence,
      CAST(activity.prospect_id AS STRING) AS _prospectID,
      prospect.email AS _email,
      CAST(activity.campaign_id AS STRING) AS _campaignID,
      campaign.name AS _contentTitle,
      activity.created_at AS _timestamp,
      activity.details AS _description,
      'Bounced' AS _engagement,
      CAST(activity.list_email_id AS STRING) AS _email_id,
      activity.email_template_id AS email_template_id,
      ROW_NUMBER() OVER(
        PARTITION BY activity.prospect_id, activity.email_template_id
        ORDER BY activity.created_at DESC
      ) AS rownum
    FROM
      `x-marketing.faro_pardot.visitor_activities` activity
    LEFT JOIN
      `x-marketing.faro_pardot.prospects` prospect
    ON
      activity.prospect_id = prospect.id
    LEFT JOIN
      `x-marketing.faro_pardot.campaigns` campaign
    ON
      activity.campaign_id = campaign.id
    WHERE
      activity.type_name = 'Email'
    AND
      activity.type IN (13, 36)  /* Bounced / Indirect Bounce */
    /*AND (
      (email NOT LIKE '%2x.marketing%'
      AND email NOT LIKE '%faro.com%'
      AND email NOT LIKE '%faroeurope.com%')
      OR
      email IS NULL
    )*/
  )
  WHERE rownum = 1
),
/* delivered_email AS (
  SELECT * FROM sent_email
  WHERE CONCAT(sent_email._campaignID, sent_email._prospectID) NOT IN (SELECT CONCAT(_campaignid, _prospectID) FROM bounced_email)
  
), */
clicked_email_opt_out AS (
  SELECT
    activity._sdc_sequence,
    CAST(activity.prospect_id AS STRING) AS _prospectID,
    prospect.email AS _email,
    CAST(activity.campaign_id AS STRING) AS _campaignID,
    campaign.name AS _contentTitle,
    activity.created_at AS _timestamp,
    activity.details AS _description,
    'Unsubscribed' AS _engagement,
    CAST(activity.list_email_id AS STRING) AS _email_id,
    activity.email_template_id AS email_template_id,
    'Clicked' AS type
  FROM
    `x-marketing.faro_pardot.visitor_activities` activity
  LEFT JOIN
    `x-marketing.faro_pardot.prospects` prospect
  ON
    activity.prospect_id = prospect.id
  LEFT JOIN
    `x-marketing.faro_pardot.campaigns` campaign
  ON
    activity.campaign_id = campaign.id
  WHERE
    activity.type_name IN ('Email', 'Email Tracker')
  AND 
    activity.type = 1   /* Click */
  AND 
    LOWER(details) LIKE '%opt%out%' 
),
opt_out_form_fill AS (
  SELECT
    activity._sdc_sequence,
    CAST(activity.prospect_id AS STRING) AS _prospectID,
    prospect.email AS _email,
    CAST(activity.campaign_id AS STRING) AS _campaignID,
    campaign.name AS _contentTitle,
    activity.created_at AS _timestamp,
    activity.details AS _description,
    'Unsubscribed' AS _engagement,
    CAST(NULL AS STRING) AS _email_id,
    CAST(NULL AS INT64) AS email_template_id,
    'Form Filled' AS type
  FROM
    `x-marketing.faro_pardot.visitor_activities` activity
  LEFT JOIN
    `x-marketing.faro_pardot.prospects` prospect
  ON
    activity.prospect_id = prospect.id
  LEFT JOIN
    `x-marketing.faro_pardot.campaigns` campaign
  ON
    activity.campaign_id = campaign.id
  WHERE
    activity.type_name IN ('Form', 'Form Handler')
  AND 
    activity.type = 4   /* Download */
  AND 
    form_handler_id = 8822
),
unsubscribed_email_form_fill AS (
  SELECT 
    * EXCEPT(type, next_type, next_type_timestamp)
  FROM (
    SELECT
      *,
      LEAD(type) OVER(
        PARTITION BY _prospectID
        ORDER BY _sdc_sequence
      ) AS next_type,
      LEAD(_timestamp) OVER(
        PARTITION BY _prospectID
        ORDER BY _sdc_sequence
      ) AS next_type_timestamp
    FROM ( 
      SELECT * FROM clicked_email_opt_out 
      UNION ALL
      SELECT * FROM opt_out_form_fill 
    )
    ORDER BY _sdc_sequence
  )
  WHERE 
    type = 'Clicked'
  AND 
    next_type = 'Form Filled'
),
unsubscribed_email_non_form_fill AS (
  SELECT * EXCEPT(rownum)
  FROM (
    SELECT
      activity._sdc_sequence,
      CAST(activity.prospect_id AS STRING) AS _prospectID,
      prospect.email AS _email,
      CAST(activity.campaign_id AS STRING) AS _campaignID,
      campaign.name AS _contentTitle,
      activity.created_at AS _timestamp,
      activity.details AS _description,
      'Unsubscribed' AS _engagement,
      CAST(activity.list_email_id AS STRING) AS _email_id,
      activity.email_template_id AS email_template_id,
      ROW_NUMBER() OVER(
        PARTITION BY activity.prospect_id, activity.email_template_id
        ORDER BY activity.created_at DESC
      ) AS rownum
    FROM
      `x-marketing.faro_pardot.visitor_activities` activity
    LEFT JOIN
      `x-marketing.faro_pardot.prospects` prospect
    ON
      activity.prospect_id = prospect.id
    LEFT JOIN
      `x-marketing.faro_pardot.campaigns` campaign
    ON
      activity.campaign_id = campaign.id
    WHERE
      activity.type_name = 'Email'
    AND
      activity.type IN (12, 35)   /* Unsubscribe Page / Indirect Unsubscribe Open */
  )
  WHERE rownum = 1
),

-- all_involved_people AS ( # Get all people that was sent an email - to fill up the email template id for unsubscribed data with email template id 
--   SELECT DISTINCT 
--     _prospectID, 
--     _campaignID, 
--     _email_id,
--     email_template_id
--   FROM sent_email 
-- ),
-- unsubscribed_email AS ( # New subquery based on the requirement - filled up the centralized unsubscribed form
--   SELECT * EXCEPT(rownum)
--   FROM (
--     SELECT
--       activity._sdc_sequence,
--       CAST(activity.prospect_id AS STRING) AS _prospectID,
--       prospect.email AS _email,
--       CAST(activity.campaign_id AS STRING) AS _campaignID,
--       campaign.name AS _contentTitle,
--       activity.created_at AS _timestamp,
--       activity.details AS _description,
--       'Unsubscribed' AS _engagement,
--       sent._email_id AS _email_id,
--       sent.email_template_id AS email_template_id,
--       ROW_NUMBER() OVER(
--         PARTITION BY activity.prospect_id, activity.email_template_id
--         ORDER BY activity.created_at DESC
--       ) AS rownum
--     FROM
--       `x-marketing.faro_pardot.visitor_activities` activity
--     LEFT JOIN
--       `x-marketing.faro_pardot.prospects` prospect
--     ON
--       activity.prospect_id = prospect.id
--     LEFT JOIN
--       `x-marketing.faro_pardot.campaigns` campaign
--     ON
--       activity.campaign_id = campaign.id
--     LEFT JOIN 
--       all_involved_people AS sent
--     ON 
--       CAST(activity.prospect_id AS STRING) = sent._prospectID
--       AND
--       CAST(activity.campaign_id AS STRING) = sent._campaignID
--     WHERE
--       activity.type_name IN ('Form', 'Form Handler')
--     AND 
--       activity.type = 4   /* Download */
--     AND form_handler_id = 8822
--   )
--   WHERE rownum = 1 
-- ),
downloaded_email AS ( #Check with client
  SELECT * EXCEPT(rownum)
  FROM (
    SELECT
      activity._sdc_sequence,
      CAST(activity.prospect_id AS STRING) AS _prospectID,
      prospect.email AS _email,
      CAST(activity.campaign_id AS STRING) AS _campaignID,
      campaign.name AS _contentTitle,
      activity.created_at AS _timestamp,
      activity.details AS _description,
      'Downloaded' AS _engagement,
      CAST(activity.list_email_id AS STRING) AS _email_id,
      activity.email_template_id AS email_template_id,
      ROW_NUMBER() OVER(
        PARTITION BY activity.prospect_id, activity.email_template_id
        ORDER BY activity.created_at DESC
      ) AS rownum
    FROM
      `x-marketing.faro_pardot.visitor_activities` activity
    LEFT JOIN
      `x-marketing.faro_pardot.prospects` prospect
    ON
      activity.prospect_id = prospect.id
    LEFT JOIN
      `x-marketing.faro_pardot.campaigns` campaign
    ON
      activity.campaign_id = campaign.id
    WHERE
      activity.type_name IN ('Form', 'Form Handler')
    AND 
      activity.type = 4   /* Download */
    /*AND (
      (email NOT LIKE '%2x.marketing%'
      AND email NOT LIKE '%faro.com%'
      AND email NOT LIKE '%faroeurope.com%')
      OR
      email IS NULL
    )*/
  )
  WHERE rownum = 1
),
spam_email AS (
  SELECT * EXCEPT(rownum)
  FROM (
    SELECT
      activity._sdc_sequence,
      CAST(activity.prospect_id AS STRING) AS _prospectID,
      prospect.email AS _email,
      CAST(activity.campaign_id AS STRING) AS _campaignID,
      campaign.name AS _contentTitle,
      activity.created_at AS _timestamp,
      activity.details AS _description,
      'Spam' AS _engagement,
      CAST(activity.list_email_id AS STRING) AS _email_id,
      activity.email_template_id AS email_template_id,
      ROW_NUMBER() OVER(
        PARTITION BY activity.prospect_id, activity.email_template_id
        ORDER BY activity.created_at DESC
      ) AS rownum
    FROM
      `x-marketing.faro_pardot.visitor_activities` activity
    LEFT JOIN
      `x-marketing.faro_pardot.prospects` prospect
    ON
      activity.prospect_id = prospect.id
    LEFT JOIN
      `x-marketing.faro_pardot.campaigns` campaign
    ON
      activity.campaign_id = campaign.id
    WHERE
      activity.type_name = 'Email'
    AND
      activity.type = 14   /* SPAM COMPLAINT */
    /*AND (
      (email NOT LIKE '%2x.marketing%'
      AND email NOT LIKE '%faro.com%'
      AND email NOT LIKE '%faroeurope.com%')
      OR
      email IS NULL
    )*/
  )
  WHERE rownum = 1 
),
campaign_sent_date AS ( #added since the airtable isnt updated
  SELECT 
    DISTINCT email_template_id, EXTRACT(DATE FROM MIN(_timestamp)) AS _email_sent_date 
  FROM sent_email 
  GROUP BY 1
)
SELECT 
  *, 
  CAST(NULL AS STRING) AS _showExport, CAST(NULL AS STRING) AS  _isBot 
  /* DISTINCT _campaignID, email_template_id, template_name,
  COUNT(DISTINCT CASE WHEN _engagement = 'Sent' THEN _email END) AS _sent,
  COUNT(DISTINCT CASE WHEN _engagement = 'Opened' THEN _email END) AS _opened,
  COUNT(DISTINCT CASE WHEN _engagement = 'Clicked' THEN _email END) AS _clicked,
  COUNT(DISTINCT CASE WHEN _engagement = 'Downloaded' THEN _email END) AS _mql    */
FROM 
(
  SELECT
    engagements.*,
    campaign_sent_date._email_sent_date,
    email_template.* EXCEPT(_campaignID, _email_template_id),
    prospect_info.* EXCEPT(_email),
  FROM (
    SELECT * FROM sent_email
    /* UNION ALL
    SELECT * FROM delivered_email */
    UNION ALL
    SELECT * FROM opened_email
    UNION ALL
    SELECT * FROM clicked_email_cta
    UNION ALL
    SELECT * FROM downloaded_email
    UNION ALL
    SELECT * FROM unsubscribed_email_form_fill
    UNION ALL
    SELECT * FROM unsubscribed_email_non_form_fill
    UNION ALL
    SELECT * FROM bounced_email
    UNION ALL
    SELECT * FROM spam_email
  ) AS engagements
  JOIN
    email_template
  ON
    engagements.email_template_id = email_template._email_template_id
  LEFT JOIN
    prospect_info 
  ON
    engagements._email = prospect_info._email
  LEFT JOIN
    campaign_sent_date
  ON
    engagements.email_template_id = campaign_sent_date.email_template_id
)
-- WHERE REGEXP_CONTAINS(_email, '2x|@faro')
-- GROUP BY 1,2,3
-- WHERE CONCAT(_engagement,_sdc_sequence) NOT IN (SELECT DISTINCT CONCAT(_engagement,_sdc_sequence) FROM `faro.db_campaign_analysis`)
;

/*
--- Label Bots #Check with client
UPDATE 
    `x-marketing.faro.db_campaign_analysis` origin
SET 
    origin._isBot = 'Yes'
FROM (
    SELECT
        _email,
      _email_template_name
    FROM 
        `x-marketing.faro.db_campaign_analysis`
    WHERE
        _engagement = 'Clicked'
    AND _description = 'https://.com' 
) bot
WHERE 
    origin._email = bot._email
AND origin._email_template_name = bot._email_template_name
AND origin._engagement IN ('Sent', 'Opened','Clicked');
*/
/*
--- Set Show Export
UPDATE 
    `x-marketing.faro.db_campaign_analysis` origin
SET 
    origin._showExport = 'Yes'
FROM (
    WITH focused_engagement AS (
        SELECT 
            _email, 
            _email_template_name, 
            _engagement,
            CASE 
                WHEN _engagement = 'Opened' THEN 1
                WHEN _engagement = 'Clicked' THEN 2
                WHEN _engagement = 'Downloaded' THEN 3
            END AS _priority
        FROM 
            `x-marketing.faro.db_campaign_analysis`
        WHERE 
            _engagement IN ('Opened', 'Clicked', 'Downloaded')
    ),
    final_engagement AS (
        SELECT 
            * EXCEPT(_priority, _rownum)
        FROM (
            SELECT 
                _email, 
                _email_template_name, 
                _engagement, 
                _priority,
                ROW_NUMBER() OVER(
                    PARTITION BY _email, _email_template_name 
                    ORDER BY _priority DESC
                ) AS _rownum
            FROM focused_engagement
        )
        WHERE _rownum = 1
    )    
    SELECT * FROM final_engagement 
) AS export
WHERE 
    origin._email = export._email
AND 
    origin._email_template_name = export._email_template_name
AND 
    origin._engagement = export._engagement;
*/

--- Update the seniority

UPDATE `x-marketing.faro.db_campaign_analysis`
SET _seniority = CASE
      WHEN LOWER(_title) LIKE LOWER("%Assistant to%") THEN "Non-Manager" 
      WHEN LOWER(_title) LIKE LOWER("%Senior Counsel%") THEN "VP"  
      WHEN LOWER(_title) LIKE LOWER("%General Counsel%") THEN "C-Level" 
      WHEN LOWER(_title) LIKE LOWER("%Founder%") THEN "C-Level" 
      WHEN LOWER(_title) LIKE LOWER("%C-Level%") THEN "C-Level" 
      WHEN LOWER(_title) LIKE LOWER("%CDO%") THEN "C-Level" 
      WHEN LOWER(_title) LIKE LOWER("%CIO%") THEN "C-Level" 
      WHEN LOWER(_title) LIKE LOWER("%CMO%") THEN "C-Level" 
      WHEN LOWER(_title) LIKE LOWER("%CFO%") THEN "C-Level" 
      WHEN LOWER(_title) LIKE LOWER("%CEO%") THEN "C-Level" 
      WHEN LOWER(_title) LIKE LOWER("%Chief%") THEN "C-Level" 
      WHEN LOWER(_title) LIKE LOWER("%coordinator%") THEN "Non-Manager" 
      WHEN LOWER(_title) LIKE LOWER("%COO%") THEN "C-Level" 
      WHEN LOWER(_title) LIKE LOWER("%Sr. V.P.%") THEN "Senior VP" 
      WHEN LOWER(_title) LIKE LOWER("%Sr.VP%") THEN "Senior VP" 
      WHEN LOWER(_title) LIKE LOWER("%Senior-Vice Pres%") THEN "Senior VP" 
      WHEN LOWER(_title) LIKE LOWER("%srvp%") THEN "Senior VP" 
      WHEN LOWER(_title) LIKE LOWER("%Senior VP%") THEN "Senior VP" 
      WHEN LOWER(_title) LIKE LOWER("%SR VP%") THEN "Senior VP" 
      WHEN LOWER(_title) LIKE LOWER("%Sr Vice Pres%") THEN "Senior VP" 
      WHEN LOWER(_title) LIKE LOWER("%Sr. VP%") THEN "Senior VP" 
      WHEN LOWER(_title) LIKE LOWER("%Sr. Vice Pres%") THEN "Senior VP" 
      WHEN LOWER(_title) LIKE LOWER("%S.V.P%") THEN "Senior VP" 
      WHEN LOWER(_title) LIKE LOWER("%Senior Vice Pres%") THEN "Senior VP" 
      WHEN LOWER(_title) LIKE LOWER("%Exec Vice Pres%") THEN "Senior VP" 
      WHEN LOWER(_title) LIKE LOWER("%Exec Vp%") THEN "Senior VP" 
      WHEN LOWER(_title) LIKE LOWER("%Executive VP%") THEN "Senior VP" 
      WHEN LOWER(_title) LIKE LOWER("%Exec VP%") THEN "Senior VP" 
      WHEN LOWER(_title) LIKE LOWER("%Executive Vice President%") THEN "Senior VP" 
      WHEN LOWER(_title) LIKE LOWER("%EVP%") THEN "Senior VP" 
      WHEN LOWER(_title) LIKE LOWER("%E.V.P%") THEN "Senior VP" 
      WHEN LOWER(_title) LIKE LOWER("%SVP%") THEN "Senior VP" 
      WHEN LOWER(_title) LIKE LOWER("%V.P%") THEN "VP" 
      WHEN LOWER(_title) LIKE LOWER("%VP%") THEN "VP" 
      WHEN LOWER(_title) LIKE LOWER("%Vice Pres%") THEN "VP" 
      WHEN LOWER(_title) LIKE LOWER("%V P%") THEN "VP" 
      WHEN LOWER(_title) LIKE LOWER("%President%") THEN "C-Level" 
      WHEN LOWER(_title) LIKE LOWER("%Director%") THEN "Director" 
      WHEN LOWER(_title) LIKE LOWER("%CTO%") THEN "C-Level" 
      WHEN LOWER(_title) LIKE LOWER("%Dir%") THEN "Director" 
      WHEN LOWER(_title) LIKE LOWER("%Dir.%") THEN "Director" 
      WHEN LOWER(_title) LIKE LOWER("%MDR%") THEN "Non-Manager" 
      WHEN LOWER(_title) LIKE LOWER("%MD%") THEN "Director" 
      WHEN LOWER(_title) LIKE LOWER("%GM%") THEN "Director" 
      WHEN LOWER(_title) LIKE LOWER("%Head%") THEN "VP" 
      WHEN LOWER(_title) LIKE LOWER("%Manager%") THEN "Manager" 
      WHEN LOWER(_title) LIKE LOWER("%escrow%") THEN "Non-Manager" 
      WHEN LOWER(_title) LIKE LOWER("%cross%") THEN "Non-Manager" 
      WHEN LOWER(_title) LIKE LOWER("%crosse%") THEN "Non-Manager" 
      WHEN LOWER(_title) LIKE LOWER("%Partner%") THEN "C-Level" 
      WHEN LOWER(_title) LIKE LOWER("%CRO%") THEN "C-Level" 
      WHEN LOWER(_title) LIKE LOWER("%Chairman%") THEN "C-Level" 
      WHEN LOWER(_title) LIKE LOWER("%Owner%") THEN "C-Level"
      WHEN LOWER(_title) LIKE LOWER("%Team Lead%") THEN "Manager"
END
WHERE _seniority IS NULL AND _title IS NOT NULL;


--- Set UTM Campaign
-- UPDATE 
--     `x-marketing.faro.db_campaign_analysis` origin
-- SET 
--     origin._utm_campaign = (
--         REPLACE(
--         REPLACE(
--         REPLACE(
--         REPLACE(
--         REPLACE(
--         REPLACE(
--         REPLACE(
--         REPLACE(
--         REPLACE(
--         REPLACE(
--         REPLACE(
--           SPLIT(SUBSTR(_description, STRPOS(_description, '_campaign=') + 10), '&')[ORDINAL(1)]
--         , '%20', ' ')
--         , '%3A', ':')
--         , '%2F', '/')
--         , '%28', '(')
--         , '%29', ')')
--         , '%7C', '|')
--         , '%26', '&')
--         , '%2B', '+')
--         , '%2C', ',')
--         , '%5B', '[')
--         , '%5D', ']')
--     )
-- WHERE 
--     origin._utm_campaign IS NOT NULL;


--- Label Segment
/* UPDATE 
    `x-marketing.faro.db_campaign_analysis` origin
SET 
    origin._segment = 'Warm Lead'
WHERE 
    origin._utm_campaign LIKE '%WM_%';


UPDATE 
    `x-marketing.faro.db_campaign_analysis` origin
SET 
    origin._segment = 'Cold Lead'
WHERE 
    origin._utm_campaign LIKE '%CD_%';


UPDATE 
    `x-marketing.faro.db_campaign_analysis` origin
SET 
    origin._segment = 'Current Client'
WHERE 
    origin._utm_campaign LIKE '%CL_%'; */



---------------------------------------------------------------------------
--------------------------- ACCOUNT ENGAGEMENTS ---------------------------
---------------------------------------------------------------------------

CREATE OR REPLACE TABLE `x-marketing.faro.db_account_engagements` AS 
WITH 
#Query to pull all the contacts in the leads table from Marketo
tam_contacts AS (
  SELECT * EXCEPT(_rownum) 
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
        CAST(NULL AS STRING) AS _2xseniority,
        email AS _email,
        CAST(NULL AS STRING) AS _accountid,
        RIGHT(email, LENGTH(email)-STRPOS(email,'@')) AS _domain, 
        company AS _accountname, 
        industry AS _industry, 
        CAST(NULL AS STRING) AS _tier,
        COALESCE(annual_revenue, CAST(NULL AS STRING)) AS _annualrevenue,
        ROW_NUMBER() OVER(
            PARTITION BY email 
            ORDER BY prosp._sdc_received_at DESC
        ) _rownum
    FROM 
      `faro_pardot.prospects` prosp
    -- LEFT JOIN
    --    `faro_mysql.w_routables` main ON main._email = prosp.email
    WHERE 
      NOT REGEXP_CONTAINS(email, 'faro|2x.marketing') 
  )
  WHERE _rownum = 1
),
#Query to pull the email engagement 
email_engagement AS (
    SELECT * 
    FROM ( 
      SELECT _email, 
      RIGHT(_email,LENGTH(_email)-STRPOS(_email,'@')) AS _domain, 
      EXTRACT(DATETIME FROM _timestamp) AS _date , 
      EXTRACT(WEEK FROM _timestamp) AS _week,  
      EXTRACT(YEAR FROM _timestamp) AS _year,
      _contentTitle, 
      CONCAT("Email ", INITCAP(_engagement)) AS _engagement,
      _description
      FROM 
        (SELECT * FROM `faro.db_campaign_analysis`)
      WHERE 
        /* (EXTRACT(DATE FROM _timestamp) BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY ) AND CURRENT_DATE() )
        AND */ LOWER(_engagement) NOT IN ('sent','delivered', 'bounced', 'unsubscribed')
    ) a
    WHERE 
      NOT REGEXP_CONTAINS(_domain,'2x.marketing|faro|gmail|yahoo|outlook|hotmail') 
      AND NOT REGEXP_CONTAINS(_contentTitle, 'test')
      AND _domain IS NOT NULL
      -- AND _year = 2022
    ORDER BY 1, 3 DESC, 2 DESC
),
web_views AS (
  SELECT 
    CAST(NULL AS STRING) AS _email, 
    _domain, 
    _date, 
    EXTRACT(WEEK FROM _date) AS _week,  
    EXTRACT(YEAR FROM _date) AS _year, 
    _page AS _pageName, 
    "Web Visit" AS _engagement, 
    CAST(_engagementtime AS STRING) AS _description,
  FROM 
    `faro.web_metrics` web 
  WHERE 
    NOT REGEXP_CONTAINS(_page, 'Unsubscribe')
    AND NOT REGEXP_CONTAINS(LOWER(_utmsource), 'linkedin|google|email') 
    AND (_domain IS NOT NULL AND _domain != '')
  UNION ALL 
  SELECT 
    CAST('' AS STRING) AS _email,
    CASE 
      WHEN domain IS NULL  
      THEN company 
      ELSE domain 
    END  AS _domain,
    CAST(extractDate AS DATETIME) AS _date,   
    EXTRACT(WEEK FROM extractDate) AS _week,  
    EXTRACT(YEAR FROM extractDate) AS _year,    
    '' AS _pageName, 
    "Web Visit" AS _engagement, 
    CAST(websiteEngagement AS STRING) AS _description 
    FROM `x-marketing.faro_6sense.db_reached_accounts` main
  WHERE websiteEngagement IN ('New','Increased') AND  
    (domain IS NOT NULL AND domain != '')
  ORDER BY 
    _date DESC
  
),
ad_clicks AS (
  SELECT 
    CAST('' AS STRING) AS _email,
    CASE 
      WHEN domain IS NULL  
      THEN company 
      ELSE domain 
    END  AS _domain,
    CAST(extractDate AS DATETIME) AS _date,   
    EXTRACT(WEEK FROM extractDate) AS _week,  
    EXTRACT(YEAR FROM extractDate) AS _year,    
    '' AS _pageName, 
    "Ad Clicks" AS _engagement, 
    CAST(clicks AS STRING) AS _description 
    FROM `x-marketing.faro_6sense.db_reached_accounts` main
  WHERE clicks > 0 AND  
    (domain IS NOT NULL AND domain != '')
  ORDER BY 
    CAST(extractDate AS TIMESTAMP) DESC
),
content_engagement AS (
  SELECT 
    CAST(NULL AS STRING) AS _email, 
    _domain, 
    _date, 
    EXTRACT(WEEK FROM _date) AS _week,  
    EXTRACT(YEAR FROM _date) AS _year, 
    _page AS _pageName, 
    "Content Engagement" AS _engagement, 
    _page AS _description
  FROM 
    faro.web_metrics web 
  WHERE 
    NOT REGEXP_CONTAINS(_page, 'Unsubscribe')
    AND REGEXP_CONTAINS(LOWER(_page), 'blog|commid=')
    AND (_domain IS NOT NULL AND _domain != '')
  ORDER BY 
    _date DESC
),
form_fills AS (
    SELECT 
      DISTINCT email AS _email, 
      RIGHT(email, LENGTH(email)-STRPOS(email, '@')) AS _domain,
      EXTRACT(DATETIME FROM _timestamp) AS _date , 
      EXTRACT(WEEK FROM _timestamp) AS _week,  
      EXTRACT(YEAR FROM _timestamp) AS _year,
      _formTitle, 
      INITCAP(_engagement) AS _engagement,
      referrer_url AS _description
    FROM ( 
        SELECT
          DISTINCT email,
          COALESCE(form_id, campaign_id) AS _campaignid,
          activities.created_at AS _timestamp,
          details AS _formTitle,
          'form filled' AS _engagement,
          pages.url AS referrer_url,
          ROW_NUMBER() OVER(
              PARTITION BY activities.prospect_id, COALESCE(form_id, campaign_id) 
              ORDER BY activities.created_at DESC
          ) AS rownum
        FROM
          `faro_pardot.visitor_activities` activities
        LEFT JOIN
          (SELECT visitor_id, page.value.url FROM `faro_pardot.visits`, UNNEST(visitor_page_views.visitor_page_view) AS page ) pages USING(visitor_id)
        LEFT JOIN
          ( SELECT DISTINCT id, email FROM `faro_pardot.prospects` ) contacts ON contacts.id = activities.prospect_id
        WHERE 
          type = 4
          AND type_name LIKE 'Form%'
          AND NOT REGEXP_CONTAINS(LOWER(details),'unsubscribe|become a partner|test')
          
    ) A 
    WHERE 
      rownum = 1
),
dummy_dates AS ( # Each domain needs to be shown regardless if they are part of the bombora report or has 0 engagements
  SELECT
    _date,
    EXTRACT(WEEK FROM _date) AS _week,
    EXTRACT(YEAR FROM _date) AS _year
  FROM 
    UNNEST(GENERATE_DATE_ARRAY('2022-01-01', CURRENT_DATE(), INTERVAL 1 DAY)) AS _date 
),
#Combining the engagements - Contact based and account based engagements
contact_engagement AS (
#Contact based engagement query
  SELECT 
    DISTINCT 
    tam_contacts._domain, 
    tam_contacts._email,
    dummy_dates.*EXCEPT(_date), 
    engagements.*EXCEPT(_date, _week, _year, _domain, _email),
    -- CAST(NULL AS INTEGER) AS _avg_bombora_score,
    tam_contacts.*EXCEPT(_domain, _email),
    engagements._date
  FROM 
    dummy_dates
  JOIN (
    SELECT * FROM email_engagement UNION ALL
    SELECT * FROM form_fills
  ) engagements USING(_week, _year)
  JOIN
    tam_contacts USING(_email) 
),
account_engagement AS (
#Account based engagement query
   SELECT 
    DISTINCT 
    tam_accounts._domain, 
    CAST(NULL AS STRING) AS _email,
    dummy_dates.*EXCEPT(_date), 
    engagements.*EXCEPT(_date, _week, _year, _domain, _email),
    CAST(NULL AS STRING) AS _id, 
    CAST(NULL AS STRING) AS _contact_type,
    CAST(NULL AS STRING) AS _firstname, 
    CAST(NULL AS STRING) AS _lastname,
    CAST(NULL AS STRING) AS _title,
    CAST(NULL AS STRING) AS _2xseniority,
    tam_accounts.*EXCEPT(_domain),
    engagements._date
  FROM 
    dummy_dates
  JOIN (
    /* SELECT * FROM intent_score UNION ALL */
    SELECT * FROM web_views UNION ALL
    SELECT * FROM ad_clicks UNION ALL
    SELECT * FROM content_engagement
  ) engagements USING(_week, _year)
  JOIN
    (
      SELECT 
        DISTINCT _domain, 
        _accountid, 
        _accountname, 
        _industry, 
        _tier, 
        _annualrevenue 
      FROM 
        tam_contacts
    ) tam_accounts
    USING(_domain)
),
combined_engagements AS (
  SELECT * FROM contact_engagement
  UNION DISTINCT
  SELECT * FROM account_engagement
)
SELECT 
  DISTINCT
  _domain,
  _accountid,
  _date,
  SUM(IF(_engagement = 'Email Opened', 1, 0)) AS _emailOpens,
  SUM(IF(_engagement = 'Email Clicked', 1, 0)) AS _emailClicks,
  SUM(IF(_engagement = 'Email Downloaded', 1, 0)) AS _emailDownloads,
  SUM(IF(_engagement = 'Form Filled', 1, 0)) AS _gatedForms,
  SUM(IF(_engagement = 'Web Visit', 1, 0)) AS _webVisits,
  SUM(IF(_engagement = 'Ad Clicks', 1, 0)) AS _adClicks,
FROM 
  combined_engagements
GROUP BY 
  1, 2, 3
ORDER BY _date DESC
;