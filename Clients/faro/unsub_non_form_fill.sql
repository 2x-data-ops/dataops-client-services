CREATE OR REPLACE TABLE `x-marketing.faro.unsub_non_form_fill` AS 
  
  WITH opt_out_click AS (
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
        CAST(activity.list_email_id AS STRING) AS _email_id,
        activity.email_template_id AS _email_template_id,
        activity.list_email_id AS _list_email_id,
        form_handler_id,
        activity.type,
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
      WHERE LOWER(activity.details) LIKE '%subscribe-opt-out%'
      
      -- WHERE
      --   activity.type_name IN ('Email', 'Email Tracker')
    --   AND
    --     activity.type IN (12, 35)   /* Unsubscribe Page / Indirect Unsubscribe Open */
    )
    WHERE rownum = 1
  ),
opt_out_request_action AS (
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
        CAST(activity.list_email_id AS STRING) AS _email_id,
        activity.email_template_id AS _email_template_id,
        activity.list_email_id AS _list_email_id,
        form_handler_id,
        activity.type,
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
        LOWER(activity.details) = 'gbl_optoutrequest'
      -- WHERE
      --   activity.type_name IN ('Email', 'Email Tracker')
    --   AND
    --     activity.type IN (12, 35)   /* Unsubscribe Page / Indirect Unsubscribe Open */
    )
    WHERE rownum = 1
)
SELECT
  opt_out_click._prospectID,
  opt_out_click._email,
  opt_out_click._timestamp,
  opt_out_click._description,
  opt_out_click._contentTitle AS _campaign_name,
  opt_out_request_action.form_handler_id AS _form_handler_id
 FROM opt_out_click
LEFT JOIN opt_out_request_actions
  ON opt_out_request_action._prospectID = opt_out_click._prospectID
  WHERE opt_out_request_action._prospectID IS NULL
  -- AND opt_out_request_action._list_email_id = opt_out_click._list_email_id 

  -- SELECT DISTINCT test._prospectID FROM test
  -- --url with subscribe-opt-out
  -- WHERE test._description = 'GBL_OptOutRequest'
  -- OR LOWER(test._description) LIKE '%subscribe%'


-- pudja@vermessung-hydro.com

