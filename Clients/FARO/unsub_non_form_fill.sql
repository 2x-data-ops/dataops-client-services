

-- everyone who clicked on those Subscribe-Opt-Out URLs
-- but did not fill up the form
-- Something like just the Non Form Fill Unsubs people

CREATE OR REPLACE TABLE `x-marketing.faro.unsub_non_form_fill` AS 

WITH unsub_non_form_fill AS (
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
      activity.email_template_id AS _email_template_id,
      activity.list_email_id AS _list_email_id,
      ROW_NUMBER() OVER(
        PARTITION BY activity.prospect_id, activity.list_email_id
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
    -- AND prospect.email ='esfum@mailto.plus'
  )
  WHERE rownum = 1
),
clicked_email_opt_out AS (
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
      activity.list_email_id AS _list_email_id,
      'Clicked' AS type,
      ROW_NUMBER() OVER(
        PARTITION BY activity.prospect_id, activity.list_email_id
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
    AND 
      activity.type = 1   /* Click */
    AND 
      LOWER(details) LIKE '%opt%out%'
    -- AND
    --   prospect.email ='esfum@mailto.plus'
  )
  WHERE rownum = 1
)
SELECT clicked_email_opt_out.*
FROM clicked_email_opt_out
JOIN unsub_non_form_fill
  ON unsub_non_form_fill._prospectID = clicked_email_opt_out._prospectID
  AND unsub_non_form_fill._list_email_id = clicked_email_opt_out._list_email_id
-- WHERE unsub_non_form_fill._prospectID IS NULL
--   AND unsub_non_form_fill._list_email_id IS NULL




WITH opt_out_form_fill AS (
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
)
SELECT _email
FROM opt_out_form_fill
WHERE _email = 'esfum@mailto.plus'