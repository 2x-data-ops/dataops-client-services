
CREATE OR REPLACE TABLE `x-marketing.wklien.db_email_engagement_pilot` AS

WITH prospect_info AS (
  SELECT
    id AS prospectid,
    INITCAP(c_firstandlastname) AS name,
    c_emailaddress AS email,
    c_title,
    INITCAP(c_company) AS company,
    c_busphone AS phone,
    c_city AS city,
    c_country AS country,
    c_zip_postal AS zip,
    isbounceback
  FROM `x-marketing.wklien_eloqua.contacts` contacts
),
send_email AS (
  SELECT * EXCEPT(rownum)
  FROM (
    SELECT 
      subjectline,
      campaignid,
      campaignname,
      emailaddress,
      contactid,
      'Send' AS engagement,
      emaildeploymentid,
      activitydate,
      ROW_NUMBER() OVER (PARTITION BY activity.contactid, activity.campaignid
      ORDER BY activitydate DESC) AS rownum
    FROM `x-marketing.wklien_eloqua.activity_email_send` activity
    -- WHERE
    -- emaildeploymentid = '30124'
    -- campaignid = '751'
  )
  WHERE rownum = 1
),
open_email AS(
  SELECT * EXCEPT(rownum)
  FROM (
    SELECT 
      subjectline,
      campaignid,
      campaignname,
      emailaddress,
      contactid,
      'Opened' AS engagement,      
      emaildeploymentid,
      activitydate,
      ROW_NUMBER() OVER (PARTITION BY activity.contactid, activity.campaignid
      ORDER BY activitydate DESC) AS rownum
    FROM `x-marketing.wklien_eloqua.activity_email_open` activity
    -- WHERE
    -- emaildeploymentid = '30124'
    -- campaignid = '751'
  )
  WHERE rownum = 1
),
bounce_email AS (
  SELECT * EXCEPT(rownum)
  FROM (
    SELECT 
      '' AS subjectline,
      campaignid,
      campaignname,
      emailaddress,
      contactid,
      'Bounced' AS engagement,
      emaildeploymentid,
      activitydate,
      ROW_NUMBER() OVER (PARTITION BY activity.contactid, activity.campaignid
      ORDER BY activitydate DESC) AS rownum
    FROM `x-marketing.wklien_eloqua.activity_bounceback` activity
    -- WHERE
    -- emaildeploymentid = '30124'
    -- campaignid = '751'
  )
  WHERE rownum = 1
),
-- delivered_email AS
click_email AS (
    SELECT * EXCEPT(rownum)
  FROM (
    SELECT 
      subjectline,
      campaignid,
      campaignname,
      emailaddress,
      contactid,
      'Clicked' AS engagement,
      emaildeploymentid,
      activitydate,
      ROW_NUMBER() OVER (PARTITION BY activity.contactid, activity.campaignid
      ORDER BY activitydate DESC) AS rownum
    FROM `x-marketing.wklien_eloqua.activity_email_clickthrough` activity
    -- WHERE
    -- emaildeploymentid = '30124'
    -- campaignid = '751'
  )
  WHERE rownum = 1
),
unsubscribe_email AS(
  SELECT * EXCEPT(rownum)
  FROM (
    SELECT 
      '' AS subjectline,
      campaignid,
      campaignname,
      emailaddress,
      contactid,
      'Unsubscribed' AS engagement,
      CAST(NULL AS STRING) AS emaildeploymentid,
      activitydate,
      ROW_NUMBER() OVER (PARTITION BY activity.contactid, activity.campaignid
      ORDER BY activitydate DESC) AS rownum
    FROM `x-marketing.wklien_eloqua.activity_unsubscribe` activity
    -- WHERE
    -- emaildeploymentid = '30124'
    -- campaignid = '751'
  )
  WHERE rownum = 1
),
download_email AS(
  SELECT * EXCEPT(rownum)
  FROM (
    SELECT 
      '' AS subjectline,
      campaignid,
      campaignname,
      REPLACE(REGEXP_EXTRACT(rawdata, r'emailAddress=([^&]+)'), '%40', '@') AS emailaddress,
      contactid,
      'Downloaded' AS engagement,
      CAST(NULL AS STRING) AS emaildeploymentid,
      activitydate,
      ROW_NUMBER() OVER (PARTITION BY activity.contactid, activity.campaignid
      ORDER BY activitydate DESC) AS rownum
    FROM `x-marketing.wklien_eloqua.activity_form_submit` activity
    -- WHERE
    -- -- emaildeploymentid = '30124'
    -- campaignid = '751'
  )
  WHERE rownum = 1

),
combined_engagement AS (
SELECT * FROM send_email
UNION ALL
SELECT * FROM bounce_email
UNION ALL
SELECT * FROM open_email
UNION ALL
SELECT * FROM click_email
UNION ALL
SELECT * FROM unsubscribe_email
)
SELECT *
FROM combined_engagement
LEFT JOIN prospect_info
ON prospect_info.prospectid = combined_engagement.contactid



































