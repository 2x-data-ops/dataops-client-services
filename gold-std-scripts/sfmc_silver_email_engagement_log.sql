TRUNCATE TABLE `x-marketing.pcs.plan_sponsor_email_performance_log`;

INSERT INTO `x-marketing.pcs.plan_sponsor_email_performance_log` (
  _sdc_sequence,
  _prospect_id,
  _campaign_id,
  _event_type,
  _timestamp,
  _url,
  _utm_source,
  _utm_content,
  _utm_medium,
  _content_downloaded,
  _linked_clicked,
  _utm_campaign,
  _subject,
  _created_date,
  _code,
  _screenshot,
  _live_date,
  _story_brand_stage,
  _sub_campaign,
  _landing_page,
  _segment,
  _link,
  _root_campaign,
  _sent_date,
  _send_date,
  _status,
  _email_id,
  _from_name,
  _email_name,
  _from_address,
  _is_always_on,
  _trim_code,
  _asset_title,
  _subject_email,
  _what_we_do,
  _campaign_name,
  _id,
  _journey_name,
  _notes,
  _email_segment,
  _preview,
  _title,
  _phone,
  _financial_account_name,
  _total_market_value_amt,
  _contributing__c,
  _salesforce_link,
  _number_of_employees,
  _industry,
  _email,
  _name,
  _region,
  _remove_bot
)
WITH open_event AS (
  SELECT
    activity._sdc_sequence,
    subscriberkey AS _prospect_id,
    CAST(sendid AS STRING) AS _campaign_id,
    'Opened' AS _event_type,
    PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ", eventdate) AS _timestamp,
    url AS _url,
    '' AS _utm_source,
    '' AS _utm_content,
    '' AS _utm_medium,
    '' AS _content_downloaded,
    '' AS _linked_clicked,
  FROM `x-marketing.pcs_sfmc.event` activity
  WHERE eventtype = 'Open'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY sendid, subscriberkey ORDER BY eventdate DESC) = 1
),
click_activity AS (
  SELECT
    _sdc_sequence,
    subscriberkey AS _subscriber_key,
    CAST(sendid AS STRING) AS _send_id,
    PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ", eventdate) AS _event_date,
    url AS _url,
    eventtype AS _event_type
  FROM `x-marketing.pcs_sfmc.event`
  UNION ALL
  SELECT
    _sdc_sequence,
    subscriberkey AS _subscriber_key,
    jobid AS _job_id,
    PARSE_TIMESTAMP('%m/%d/%Y %T %p', clickdate) AS _event_date,
    url AS _url,
    'Click' AS _event_type
  FROM `x-marketing.pcs_sfmc.data_extension_Bot_Clicks`
  UNION ALL
  SELECT
    _sdc_sequence,
    lead_id AS _subscriber_key,
    CAST(job_id AS STRING) AS _job_id,
    PARSE_TIMESTAMP('%m/%d/%Y %T %p', click_date) AS _event_date,
    url AS _url,
    'Click' AS _event_type
  FROM `x-marketing.pcs_sfmc.data_extension_All_Email_Link_Clicks`
  UNION ALL
  SELECT
    _sdc_sequence,
    lead_id AS _subscriber_key,
    REGEXP_EXTRACT(url, r'Job_ID=([0-9]+)') AS _job_id,
    PARSE_TIMESTAMP('%m/%d/%Y %T %p', click_date) AS _event_date,
    url AS _url,
    'Click' AS _event_type
  FROM `x-marketing.pcs_sfmc.data_extension_Plan_Sponsor_Email_Link_Clicks`
),
click_event AS (
  SELECT
    activity._sdc_sequence,
    _subscriber_key AS _prospect_id,
    CAST(_send_id AS STRING) AS _campaign_id,
    'Clicked' AS _event_type,
    _event_date AS _timestamp,
    _url,
    SPLIT(SUBSTR(activity._url, STRPOS(activity._url, 'utm_source=') + 11), '&') [ORDINAL(1)] AS _utm_source,
    SPLIT(SUBSTR(activity._url, STRPOS(activity._url, 'utm_content=') + 12), '&') [ORDINAL(1)] AS _utm_content,
    SPLIT(SUBSTR(activity._url, STRPOS(activity._url, 'utm_medium=') + 11), '&') [ORDINAL(1)] AS _utm_medium,
    SPLIT(
      SUBSTR(activity._url, STRPOS(activity._url, 'content_downloaded=') + 19),
      '&'
    ) [ORDINAL(1)] AS _content_downloaded,
    activity._url _linked_clicked,
  FROM click_activity AS activity
  WHERE _event_type = 'Click'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _send_id, _subscriber_key ORDER BY _timestamp DESC) = 1
),
unique_click AS (
  SELECT
    activity._sdc_sequence,
    subscriberkey AS _prospect_id,
    CAST(sendid AS STRING) AS _campaign_id,
    'Unique' AS _event_type,
    PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ", eventdate) AS _timestamp,
    url AS _url,
    SPLIT(SUBSTR(activity.url, STRPOS(activity.url, 'utm_source=') + 11), '&') [ORDINAL(1)] AS _utm_source,
    SPLIT(SUBSTR(activity.url, STRPOS(activity.url, 'utm_content=') + 12), '&') [ORDINAL(1)] AS _utm_content,
    SPLIT(SUBSTR(activity.url, STRPOS(activity.url, 'utm_medium=') + 11), '&') [ORDINAL(1)] AS _utm_medium,
    SPLIT(
      SUBSTR(activity.url, STRPOS(activity.url, 'content_downloaded=') + 19),
      '&'
    ) [ORDINAL(1)] AS _content_downloaded,
    activity.url AS _linked_clicked,
  FROM `x-marketing.pcs_sfmc.event` activity
  WHERE eventtype = 'Click'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY sendid, subscriberkey ORDER BY eventdate DESC) = 1
),
sent_event AS (
  SELECT
    activity._sdc_sequence,
    subscriberkey AS _prospect_id,
    CAST(sendid AS STRING) AS _campaign_id,
    'Sent' AS _event_type,
    PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ", eventdate) AS _timestamp,
    url AS _url,
    '' AS _utm_source,
    '' AS _utm_content,
    '' AS _utm_medium,
    '' AS _content_downloaded,
    '' AS _linked_clicked,
  FROM `x-marketing.pcs_sfmc.event` activity
  WHERE eventtype = 'Sent'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY sendid, subscriberkey ORDER BY eventdate DESC) = 1
),
bounce_category AS (
  SELECT
    bounce_category AS _bounce_category,
    lead_id AS _lead_id,
    bounce_subcategory AS _bounce_subcategory,
    bounce_date AS _bounce_date,
    bounce_category_id AS _bounce_category_id,
    bounce_subcategory_id AS _bounce_subcategory_id,
    categoryid AS _category_id,
    email_name AS _email_name,
    _customobjectkey AS _custom_object_key
  FROM `x-marketing.pcs_sfmc.data_extension_Email_Bounce`
),
hard_bounce AS (
  SELECT
    activity._sdc_sequence,
    subscriberkey AS _prospect_id,
    CAST(sendid AS STRING) AS _campaign_id,
    CASE
      WHEN _bounce_category = 'Technical/Other bounce' THEN 'Soft bounce'
      WHEN _bounce_category = 'Unknown bounce' THEN 'Soft bounce'
      ELSE _bounce_category
    END AS _event_type,
    PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ", eventdate) AS _timestamp,
    url AS _url,
    '' AS _utm_source,
    '' AS _utm_content,
    '' AS _utm_medium,
    '' AS _content_downloaded,
    '' AS _linked_clicked,
  FROM `x-marketing.pcs_sfmc.event` activity
  JOIN bounce_category AS bounce
    ON activity.subscriberkey = bounce._lead_id
    AND CAST(PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ", eventdate) AS DATE) = CAST(PARSE_TIMESTAMP('%m/%d/%Y %T %p', bounce._bounce_date) AS DATE)
  WHERE eventtype IN ('HardBounce', 'OtherBounce', 'SoftBounce') --- get category )
  QUALIFY ROW_NUMBER() OVER (PARTITION BY sendid, subscriberkey ORDER BY eventdate DESC) = 1
),
unsubscribe AS (
  SELECT
    activity._sdc_sequence,
    subscriberkey AS _prospect_id,
    CAST(sendid AS STRING) AS _campaign_id,
    'Unsubscribe' AS _event_type,
    PARSE_TIMESTAMP("%Y-%m-%dT%H:%M:%SZ", eventdate) AS _timestamp,
    url AS _url,
    '' AS _utm_source,
    '' AS _utm_content,
    '' AS _utm_medium,
    '' AS _content_downloaded,
    '' AS _linked_clicked,
  FROM `x-marketing.pcs_sfmc.event` activity
  WHERE eventtype = 'Unsubscribe'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY sendid, subscriberkey ORDER BY eventdate DESC) = 1
),
email_campaign AS (
  SELECT DISTINCT
    _preview AS _notes,
    _status AS _status,
    _campaign_code AS _trim_code,
    _ad_visual AS _screenshot,
    _form_submission AS _asset_title,
    _subject_line AS _subject,
    _email_name AS _what_we_do,
    _campaign_id AS _campaign_id,
    _campaign_name AS _utm_campaign,
    _preview,
    _campaign_code AS _code,
    _campaign AS _journey_name,
    _campaign_name AS _campaign_name,
    _form_submission AS _form_submission,
    _email_id AS _id,
    CASE
      WHEN _live_date = '' THEN NULL
      ELSE PARSE_TIMESTAMP('%m/%d/%Y', _live_date)
    END AS _live_date,
    "" AS _utm_source,
    _email_name AS _email_name,
    '' AS _assignee,
    '' AS _utm_medium,
    _landing_page_url AS _landing_page,
    _email_segment AS _segment,
    _asset_url AS _link,
    _campaign AS _root_campaign,
    _email_segment AS _email_segment
  FROM `x-marketing.pcs_retirement_google_sheets.db_email_campaign`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _campaign_id, _code ORDER BY _live_date DESC) = 1
),
airtable AS (
  SELECT
    name.value.name AS _utm_campaign,
    TRIM(subject) AS _subject,
    PARSE_DATETIME("%Y-%m-%dT%H:%M:%SZ", airtable.createddate) AS _created_date,
    airtable.id AS _airtable_id,
    _code,
    _screenshot,
    _live_date,
    _utm_source AS _story_brand_stage,
    _utm_medium AS _sub_campaign,
    _landing_page,
    _segment,
    _link,
    _root_campaign,
    sentdate AS _sent_date,
    senddate AS _send_date,
    status AS _status,
    CAST(emailid AS STRING) AS _email_id,
    fromname AS _from_name,
    TRIM(airtable.emailname) AS _email_name,
    fromaddress AS _from_address,
    CAST(isalwayson AS STRING) AS _is_always_on,
    _trim_code,
    _asset_title,
    _subject AS _subject_email,
    _what_we_do,
    _campaign_name,
    CAST(_id AS STRING) AS _id,
    _journey_name,
    _notes,
    _email_segment,
    _preview,
  FROM `x-marketing.pcs_sfmc.send` airtable,
    UNNEST (partnerproperties) name
  JOIN email_campaign
    ON CAST(airtable.id AS STRING) = SAFE_CAST(_campaign_id AS STRING)
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY emailname, airtable.id, emailid, _segment
    ORDER BY senddate DESC
  ) = 1
),
contact AS (
  SELECT
    acc.id AS _contact_id,
    title AS _title,
    phone AS _phone,
    fin.name AS _financial_account_name,
    total_market_value_amt__c AS _total_market_value_amt,
    contributing__c AS _contributing__c,
    CONCAT(
      'https://pcsretirement.lightning.force.com/lightning/r/Lead/',
      masterrecordid,
      '/view'
    ) AS _salesforce_link,
    CAST(numberofemployees AS STRING) AS _number_of_employees,
    industry AS _industry,
    email AS _email,
    CONCAT(firstname, ' ', lastname) AS _name,
    territory__c AS _region,
  FROM `x-marketing.pcs_salesforce.Lead` acc
  LEFT JOIN `x-marketing.pcs_salesforce.NEW_Financial_Account__c` fin
    ON acc.id = fin.account_holder__c
  WHERE acc.isdeleted IS FALSE
  UNION ALL
  SELECT
    acc.id AS _contact_id,
    title AS _title,
    phone AS _phone,
    fin.name AS _financial_account_name,
    total_market_value_amt__c AS _total_market_value_amt,
    contributing__c AS _contributing__c,
    CONCAT(
      'https://pcsretirement.lightning.force.com/lightning/r/Lead/',
      masterrecordid,
      '/view'
    ) AS _salesforce_link,
    '' AS _number_of_employees,
    '' AS _industry,
    email AS _email,
    CONCAT(firstname, ' ', lastname) AS _name,
    territory__c AS _region,
  FROM `x-marketing.pcs_salesforce.Contact` acc
  LEFT JOIN `x-marketing.pcs_salesforce.NEW_Financial_Account__c` fin
    ON acc.id = fin.account_holder__c
  WHERE acc.isdeleted IS FALSE
),
engagements AS (
  SELECT
    *
  FROM open_event
  UNION ALL
  SELECT
    *
  FROM click_event
  UNION ALL
  SELECT
    *
  FROM sent_event
  UNION ALL
  SELECT
    *
  FROM hard_bounce
  UNION ALL
  SELECT
    *
  FROM unsubscribe
  UNION ALL
  SELECT
    *
  FROM unique_click
),
combine_all AS (
  SELECT
    engagements.*,
    airtable.* EXCEPT (_airtable_id),
    contact.* EXCEPT (_contact_id),
  FROM  engagements
  JOIN airtable
    ON engagements._campaign_id = CAST(airtable._airtable_id AS STRING)
  LEFT JOIN contact
    ON engagements._prospect_id = contact._contact_id
),
engagement_filtered AS (
    SELECT
    _campaign_id,
    _email,
    _prospect_id,
    SUM(
      CASE
        WHEN _linked_clicked LIKE "%DG-EM%" THEN 1
        WHEN _linked_clicked = "Content_downloaded" THEN 1
      END
    ) AS _content_donwloaded,
    SUM(
      CASE
        WHEN _linked_clicked = "Bot" THEN 1
      END
    ) AS _bot
  FROM combine_all
  WHERE _event_type = 'Clicked'
  GROUP BY 1, 2, 3
),
_remove_bot AS (
  SELECT
    _campaign_id,
    _email,
    _prospect_id,
    'True' AS _remove_bot
  FROM engagement_filtered
  WHERE _bot IS NULL
    AND _content_donwloaded IS NOT NULL
)
SELECT
  origin.*,
  _remove_bot
FROM combine_all AS origin
LEFT JOIN _remove_bot scenario
  ON (
    origin._email = scenario._email
    AND origin._campaign_id  = scenario._campaign_id 
    AND origin._prospect_id = origin._prospect_id
    AND origin._event_type = "Clicked"
    AND _linked_clicked LIKE "%DG-EM%"
  )
  OR (
    origin._email = scenario._email
    AND origin._campaign_id  = scenario._campaign_id 
    AND origin._prospect_id = origin._prospect_id
    AND origin._event_type = "Clicked"
    AND _linked_clicked LIKE "Content_downloaded"
  );