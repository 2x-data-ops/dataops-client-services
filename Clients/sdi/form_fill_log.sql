CREATE OR REPLACE TABLE `sdi.db_form_fill_log` AS
WITH forms AS (

  SELECT 
          CAST(NULL AS STRING) AS devicetype,
          IF( form.value.page_url LIKE '%utm_content%',
              SPLIT(SUBSTR(form.value.page_url, STRPOS(form.value.page_url, '_content=') + 9), '&')[ORDINAL(1)],
              CAST(NULL AS STRING)
          ) AS _campaignID, #utm_content
          IF( form.value.page_url LIKE '%utm_campaign%',
              REGEXP_REPLACE(REGEXP_REPLACE(SPLIT(SUBSTR(form.value.page_url, STRPOS(form.value.page_url, 'utm_campaign=') + 13), '&')[ORDINAL(1)], '%20', ' '), '%3A',':'),
              CAST(NULL AS STRING) 
          ) AS _campaign,
          IF( form.value.page_url LIKE '%utm_source%',
              SPLIT(SUBSTR(form.value.page_url, STRPOS(form.value.page_url, '_source=') + 8), '&')[ORDINAL(1)],
              CAST(NULL AS STRING) 
          ) AS _utm_source,
          form.value.title AS form_title,
          properties.email.value AS email,
          associated_company.properties.domain.value AS domain, 
          form.value.timestamp AS timestamp, 
          form.value.page_url AS description,
          forms.campaignguid,
          -- Use form field labels to check for legitimate form fills
          forms.labels AS form_field_labels,
          CASE 
            WHEN (      
              forms.labels LIKE '%first name%'
              AND
              forms.labels LIKE '%last name%'
              AND
              forms.labels LIKE '%company name%'
              AND
              forms.labels LIKE '%email%'
              AND
              forms.labels LIKE '%phone%'
              AND
              forms.labels LIKE '%how did you hear about us?%'
              AND
              forms.labels LIKE '%message%'
            )
            THEN 'Matching Labels'
            ELSE CAST(NULL AS STRING)
          END  
          AS label_match
        FROM  
            `x-marketing.sdi_hubspot.contacts` contacts, UNNEST(form_submissions) AS form
        JOIN (
          SELECT
            guid, campaignguid, name, STRING_AGG(LOWER(field.value.label), ', \n') AS labels
          FROM 
            `x-marketing.sdi_hubspot.forms`, UNNEST(formfieldgroups) AS fieldgrp, UNNEST(fieldgrp.value.fields) AS field
          GROUP BY
            1, 2, 3
        ) AS forms 
        ON 
          form.value.form_id = forms.guid

)
      SELECT 
        activity.email AS _email,
        COALESCE(domain, RIGHT(email, LENGTH(email)-STRPOS(email, '@'))) AS _domain,
        activity.timestamp AS _timestamp,
        EXTRACT(WEEK FROM activity.timestamp) AS _week,  
        EXTRACT(YEAR FROM activity.timestamp) AS _year,
        -- Add label to form title instead of creating new field
        CASE
          WHEN label_match IS NOT NULL
          THEN CONCAT(form_title, ' (', label_match, ')')
          ELSE form_title
        END AS _form_title,
        'Form Filled' AS _engagement,
        activity.description AS _description,
        REGEXP_EXTRACT(activity.description, r'[?&]utm_source=([^&]+)') AS _utmsource,
        REGEXP_EXTRACT(activity.description, r'[?&]utm_campaign=([^&]+)') AS _utmcampaign,
        REGEXP_EXTRACT(activity.description, r'[?&]utm_medium=([^&]+)') AS _utmmedium,
        REGEXP_EXTRACT(activity.description, r'[?&]utm_content=([^&]+)') AS _utmcontent,
        activity.description AS _fullurl
      FROM 
        forms activity
      LEFT JOIN 
        `x-marketing.sdi_hubspot.campaigns` campaign ON activity._campaignID = CAST(campaign.id AS STRING)