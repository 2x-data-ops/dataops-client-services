TRUNCATE TABLE `x-marketing.logicsource.db_form_fill_log`;
INSERT INTO  `x-marketing.logicsource.db_form_fill_log` (
  _email,
  _domain,
  _timestamp,
  _week,
  _year,
  _form_title,
  _engagement,
  _description,
  _utmsource,
  _utmcampaign,
  _utmmedium,
  _utmcontent,
  _fullurl
)
WITH activity AS (
    SELECT 
            CAST(NULL AS STRING) AS devicetype,
            IF( form.value.page_url LIKE '%utm_content%',
                REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_content=([^&]+)'),
                CAST(NULL AS STRING)
            ) AS _campaignID, #utm_content
            form.value.title AS form_title,
            properties.email.value AS email, 
            form.value.timestamp AS timestamp, 
            form.value.page_url AS description,
            campaignguid,
          FROM  
              `x-marketing.logicsource_hubspot.contacts` contacts, UNNEST(form_submissions) AS form
          JOIN 
            `x-marketing.logicsource_hubspot.forms` forms ON form.value.form_id = forms.guid
  ),
  forms AS (
    SELECT 
        activity.email AS _email,
        RIGHT(email, LENGTH(email)-STRPOS(email, '@')) AS _domain,
        activity.timestamp AS _timestamp,
        EXTRACT(WEEK FROM activity.timestamp) AS _week,  
        EXTRACT(YEAR FROM activity.timestamp) AS _year,
        form_title AS _form_title,
        'Form Filled' AS _engagement, 
        activity.description AS _description,
        REGEXP_EXTRACT(activity.description, r'[?&]utm_source=([^&]+)') AS _utmsource,
        COALESCE(REGEXP_EXTRACT(activity.description, r'[?&]utm_campaign=([^&]+)'), campaign.name) AS _utmcampaign,
        REGEXP_EXTRACT(activity.description, r'[?&]utm_medium=([^&]+)') AS _utmmedium,
        REGEXP_EXTRACT(activity.description, r'[?&]utm_content=([^&]+)') AS _utmcontent,
        activity.description AS _fullurl
      FROM activity
      LEFT JOIN 
        `x-marketing.logicsource_hubspot.campaigns` campaign ON activity._campaignID = CAST(campaign.id AS STRING)
      QUALIFY ROW_NUMBER() OVER(PARTITION BY email, campaign.name ORDER BY timestamp DESC) = 1
  )
SELECT
  *
FROM 
  forms
;
