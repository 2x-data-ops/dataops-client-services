TRUNCATE TABLE `x-marketing.averydennison.db_email_engagements_log`;

INSERT INTO`x-marketing.averydennison.db_email_engagements_log` ( 
    _sdc_sequence,
    _campaignID,
    _campaignname,
    _assetname,
    _timestamp,
    _emailsendtype,
    _prospectID,
    _email,
    _engagement,
    _firstname,
    _lastname,
    _name,
    _domain,
    _industry,
    _address,
    _country,
    _territory,
    _title,
    _mql_date,
    _lead_source,
    _company,
    _accountname,
    _city,
    _segment,
    _campaignSentDate,
    _actualcost,
    _region,
    _type,
    _campaigncategory,
    _campaigntype,
    _budgetedcost,
    _endat,
    _product )
WITH send AS (
  SELECT
    _sdc_sequence,
    campaignid,
    campaignname,
    assetname,
    activitydate,
    emailsendtype,
    contactid,
    emailaddress,
    'Sent' AS engagement,
  FROM`x-marketing.averydennison_eloqua.activity_email_send` 
),
bounce_back AS (
  SELECT
    _sdc_sequence,
    campaignid,
    campaignname,
    assetname,
    activitydate,
    '' AS emailsendtype,
    contactid,
    emailaddress,
    'Bounced' AS engagement,
  FROM`x-marketing.averydennison_eloqua.activity_bounceback` 
),
open AS (
  SELECT
    _sdc_sequence,
    campaignid,
    campaignname,
    assetname,
    activitydate,
    emailsendtype,
    contactid,
    emailaddress,
    'Opened' AS engagement,
  FROM`x-marketing.averydennison_eloqua.activity_email_open` 
),
click AS (
  SELECT
    _sdc_sequence,
    campaignid,
    campaignname,
    assetname,
    activitydate,
    emailsendtype,
    contactid,
    emailaddress,
    'Clicked' AS engagement,
  FROM`x-marketing.averydennison_eloqua.activity_email_clickthrough` 
),
form_submit AS (
  SELECT
    _sdc_sequence,
    campaignid,
    campaignname,
    assetname,
    activitydate,
    '' AS emailsendtype,
    contactid,
    REPLACE(REGEXP_EXTRACT(rawdata, r'emailAddress=([^&]+)'), '%40', '@') AS emailaddress,
    'Downloaded' AS engagement,
  FROM`x-marketing.averydennison_eloqua.activity_form_submit` 
),
unsubsribe AS (
  SELECT
    _sdc_sequence,
    campaignid,
    campaignname,
    assetname,
    activitydate,
    '' AS emailsendtype,
    contactid,
    emailaddress,
    'Unsubscribed' AS engagement,
  FROM`x-marketing.averydennison_eloqua.activity_unsubscribe` 
),
campaign AS (
  SELECT
    id AS campaignid,
    name AS campaignname,
    startat,
    SAFE_CAST(actualcost AS NUMERIC),
    region,
    type,
    campaigncategory,
    campaigntype,
    SAFE_CAST(budgetedcost AS NUMERIC),
    endat,
    product
  FROM`x-marketing.averydennison_eloqua.campaigns` 
),
delivered AS (
  SELECT
    send.* EXCEPT(engagement),
    'Delivered' AS engagement
  FROM
    send
  LEFT JOIN bounce_back
  ON CONCAT(send.contactid,send.campaignid) = CONCAT(bounce_back.contactid,bounce_back.campaignid)

  WHERE
    CONCAT(bounce_back.contactid,bounce_back.campaignid) IS NULL 
),
contacts AS (
  SELECT
    id AS contactid,
    c_firstname,
    c_lastname,
    c_firstandlastname,
    c_emailaddress,
    c_emailaddressdomain,
    c_industry1,
    c_address1,
    c_country,
    c_territory,
    c_job_title1,
    c_mql_date1,
    c_lead_source___most_recent1,
    c_company,
    accountname,
    c_city,
    c_segment1
  FROM `x-marketing.averydennison_eloqua.contacts` 
),
combine_engagement AS (
  SELECT
    *
  FROM send
  UNION ALL
  SELECT
    *
  FROM bounce_back
  UNION ALL
  SELECT
    *
  FROM open
  UNION ALL
  SELECT
    *
  FROM click
  UNION ALL
  SELECT
    *
  FROM form_submit
  UNION ALL
  SELECT
    *
  FROM unsubsribe
  UNION ALL
  SELECT
    *
  FROM delivered 
)
SELECT
  engagement.*,
  contacts.* EXCEPT(contactid,
    c_emailaddress),
  campaign.* EXCEPT(campaignid,
    campaignname)
FROM combine_engagement engagement
LEFT JOIN contacts contacts
ON
  engagement.contactid = contacts.contactid
LEFT JOIN campaign
ON
  engagement.campaignid = campaign.campaignid