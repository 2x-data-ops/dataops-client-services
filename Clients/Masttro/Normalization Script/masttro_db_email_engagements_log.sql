--CREATE OR REPLACE TABLE `x-marketing.masttro.db_email_engagements_log` AS
TRUNCATE TABLE `x-marketing.masttro.db_email_engagements_log`;
INSERT INTO `x-marketing.masttro.db_email_engagements_log` (
    _sdc_sequence,
    _email,
    _campaignID,
    _campaignName,
    _timestamp,
    _description,
    _device_type,
    _linkid,
    _duration,
    _response,
    _engagement,
    _prospectID,
    _name,
    _phone,
    _title,
    _seniority,
    _company,
    _domain,
    _industry,
    _country,
    _city,
    _revenue,
    _employees,
    _lifecycleStage,
    _contentTitle,
    _campaignSubject
  ) WITH prospect_info AS (
    SELECT properties.email.value AS _email,
      CAST(vid AS STRING) AS _prospectID,
      CONCAT(
        properties.firstname.value,
        ' ',
        properties.lastname.value
      ) AS _name,
      properties.phone.value AS _phone,
      properties.jobtitle.value AS _title,
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
      END AS _seniority,
      properties.company.value AS _company,
      associated_company.properties.domain.value AS _domain,
      REPLACE(
        INITCAP(associated_company.properties.industry.value),
        '_',
        ' '
      ) AS _industry,
      properties.country.value AS _country,
      properties.city.value AS _city,
      CAST(
        associated_company.properties.annualrevenue.value AS STRING
      ) AS _revenue,
      CAST(
        associated_company.properties.numberofemployees.value AS STRING
      ) AS _employees,
      CASE
        WHEN property_lifecyclestage.value = 'marketingqualifiedlead' THEN 'Marketing Qualified Lead'
        WHEN property_lifecyclestage.value = 'salesqualifiedlead' THEN 'Sales Qualified Lead'
        WHEN property_lifecyclestage.value = '55758351' THEN 'Sales Accepted Lead'
        WHEN property_lifecyclestage.value = '161283257' THEN 'Onboarding'
        WHEN property_lifecyclestage.value = '161201966' THEN 'Client At Risk'
        WHEN property_lifecyclestage.value = '172403121' THEN 'Churn'
        WHEN property_lifecyclestage.value = '' THEN NULL
        ELSE INITCAP(CAST(properties.lifecyclestage.value AS STRING))
      END AS _lifecycleStage,
      FROM `x-marketing.masttro_hubspot.contacts` QUALIFY ROW_NUMBER() OVER(
        PARTITION BY property_email.value,
        CONCAT(
          properties.firstname.value,
          ' ',
          properties.lastname.value
        )
        ORDER BY vid DESC
      ) = 1
  ),
  airtable_info AS (
    SELECT CAST(id AS STRING) as _campaignID,
      name AS _contentTitle,
      subject AS _campaignSubject
    FROM `x-marketing.masttro_hubspot.campaigns` QUALIFY ROW_NUMBER() OVER(
        PARTITION BY name,
        id
        ORDER BY id
      ) = 1
  ),
  engagements AS (
    WITH email_fields AS (
      SELECT activity.id,
        activity._sdc_sequence AS _sdc_sequence,
        activity.recipient AS _email,
        CAST(activity.emailcampaignid AS STRING) AS _campaignID,
        campaign.name AS _campaignName,
        CAST(activity.created AS TIMESTAMP) AS _timestamp,
        activity.url AS _description,
        INITCAP(activity.devicetype) AS _device_type,
        CAST(activity.linkid AS STRING) AS _linkid,
        CAST(activity.duration AS STRING) AS _duration,
        activity.response AS _response,
        activity.type AS _type,
        FROM `x-marketing.masttro_hubspot.email_events` activity
        JOIN `x-marketing.masttro_hubspot.campaigns` campaign ON activity.emailcampaignid = campaign.id
        AND campaign.name IS NOT NULL
    ),
    Dropped AS (
      SELECT *
      EXCEPT(_type, id),
        'Dropped' AS _engagement,
        FROM email_fields
      WHERE _type = 'DROPPED' QUALIFY ROW_NUMBER() OVER (
          PARTITION BY _email,
          _campaignName
          ORDER BY _timestamp DESC
        ) = 1
    ),
    Deferred AS (
      SELECT *
      EXCEPT(_type, id),
        'Deferred' AS _engagement,
        FROM email_fields
      WHERE _type = 'DEFERRED' QUALIFY ROW_NUMBER() OVER (
          PARTITION BY _email,
          _campaignName
          ORDER BY _timestamp DESC
        ) = 1
    ),
    Suppressed AS (
      SELECT *
      EXCEPT(_type, id),
        'Suppressed' AS _engagement,
        FROM email_fields
      WHERE _type = 'SUPPRESSED' QUALIFY ROW_NUMBER() OVER (
          PARTITION BY _email,
          _campaignName
          ORDER BY _timestamp DESC
        ) = 1
    ),
    Opened AS (
      SELECT *
      EXCEPT(_type, id),
        'Opened' AS _engagement,
        FROM email_fields
      WHERE _type = 'OPEN' QUALIFY ROW_NUMBER() OVER (
          PARTITION BY _email,
          _campaignName
          ORDER BY _timestamp DESC
        ) = 1
    ),
    Clicked AS (
      SELECT *
      EXCEPT(_type, id),
        'Clicked' AS _engagement,
        FROM email_fields
      WHERE _type = 'CLICK' QUALIFY ROW_NUMBER() OVER (
          PARTITION BY _email,
          _campaignName
          ORDER BY _timestamp DESC
        ) = 1
    ),
    Unsubscribed AS (
      SELECT email_fields.*
      EXCEPT(_type, id),
        'Unsubscribed' AS _engagement,
        FROM `x-marketing.masttro_hubspot.subscription_changes`,
        UNNEST(changes) AS status
        JOIN email_fields ON status.value.causedbyevent.id = email_fields.id
      WHERE _type = 'STATUSCHANGE'
        AND status.value.change = 'UNSUBSCRIBED' QUALIFY ROW_NUMBER() OVER (
          PARTITION BY _email,
          _campaignName
          ORDER BY _timestamp DESC
        ) = 1
    ),
    Downloaded AS (
      SELECT activity._sdc_sequence,
        activity.email AS _email,
        -- CAST(campaign.id AS STRING) AS _campaignID,
        activity._utmcontent AS _campaignID,
        --COALESCE(form_title, campaign.name) AS _campaignName,
        form_title AS _campaignName,
        activity.timestamp AS _timestamp,
        activity.description AS _description,
        activity.devicetype,
        '' AS linkid,
        '' AS duration,
        '' AS response,
        'Downloaded' AS _engagement,
        FROM (
          SELECT contacts._sdc_sequence,
            contacts.properties.email.value AS email,
            form.value.title AS form_title,
            form.value.timestamp AS timestamp,
            form.value.page_url AS description,
            CAST(NULL AS STRING) AS devicetype,
            REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_source=([^&]+)') AS _utmsource,
            REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_campaign=([^&]+)') AS _utmcampaign,
            REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_medium=([^&]+)') AS _utmmedium,
            REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_content=([^&]+)') AS _utmcontent,
            FROM `x-marketing.masttro_hubspot.contacts` contacts,
            UNNEST(form_submissions) AS form
            JOIN `x-marketing.masttro_hubspot.forms` forms ON form.value.form_id = forms.guid
        ) activity
        LEFT JOIN `x-marketing.masttro_hubspot.campaigns` campaign ON activity._utmcontent = CAST(campaign.id AS STRING) QUALIFY ROW_NUMBER() OVER (
          PARTITION BY _email,
          _campaignName
          ORDER BY _timestamp DESC
        ) = 1
    ),
    SoftBounced AS (
      SELECT *
      EXCEPT(_type, id),
        'Soft Bounced' AS _engagement,
        FROM email_fields
      WHERE _type = 'BOUNCE' QUALIFY ROW_NUMBER() OVER (
          PARTITION BY _email,
          _campaignName
          ORDER BY _timestamp DESC
        ) = 1
    ),
    HardBounced AS (
      SELECT hb.*
      FROM(
          SELECT email_fields.*
          EXCEPT(_type, id),
            'Hard Bounced' AS _engagement
          FROM `x-marketing.masttro_hubspot.subscription_changes`,
            UNNEST(changes) AS status
            JOIN email_fields ON status.value.causedbyevent.id = email_fields.id
          WHERE status.value.change = 'BOUNCED' QUALIFY ROW_NUMBER() OVER (
              PARTITION BY _email,
              _campaignName
              ORDER BY _timestamp DESC
            ) = 1
        ) hb
        JOIN SoftBounced ON hb._email = SoftBounced._email
        AND hb._campaignID = SoftBounced._campaignID
    ),
    Sent AS (
      SELECT sent.*
      FROM(
          SELECT *
          EXCEPT(_type, id),
            'Sent' AS _engagement,
            FROM email_fields
          WHERE _type = 'SENT' QUALIFY ROW_NUMBER() OVER (
              PARTITION BY _email,
              _campaignName
              ORDER BY _timestamp DESC
            ) = 1
        ) sent
        LEFT JOIN HardBounced ON sent._email = HardBounced._email
        AND sent._campaignID = HardBounced._campaignID
        LEFT JOIN Dropped ON sent._email = Dropped._email
        AND sent._campaignID = Dropped._campaignID
      WHERE HardBounced._email IS NULL
        AND Dropped._email IS NULL
    ),
    Delivered AS (
      SELECT delivered.*
      FROM (
          SELECT *
          EXCEPT(_type, id),
            'Delivered' AS _engagement,
            FROM email_fields
          WHERE _type = 'DELIVERED' QUALIFY ROW_NUMBER() OVER (
              PARTITION BY _email,
              _campaignName
              ORDER BY _timestamp DESC
            ) = 1
        ) delivered
        LEFT JOIN HardBounced ON delivered._email = HardBounced._email
        AND delivered._campaignID = HardBounced._campaignID
        LEFT JOIN Dropped ON delivered._email = Dropped._email
        AND delivered._campaignID = Dropped._campaignID
      WHERE HardBounced._email IS NULL
        AND Dropped._email IS NULL
    )
    SELECT *
    FROM Sent
    UNION ALL
    SELECT *
    FROM Dropped
    UNION ALL
    SELECT *
    FROM Deferred
    UNION ALL
    SELECT *
    FROM Suppressed
    UNION ALL
    SELECT *
    FROM Delivered
    UNION ALL
    SELECT *
    FROM Opened
    UNION ALL
    SELECT *
    FROM Clicked
    UNION ALL
    SELECT *
    FROM Unsubscribed
    UNION ALL
    SELECT *
    FROM HardBounced
    UNION ALL
    SELECT *
    FROM SoftBounced
    UNION ALL
    SELECT *
    FROM Downloaded
  )
SELECT engagements.*,
  prospect_info.*
EXCEPT (_email),
  airtable_info.*
EXCEPT (_campaignID),
  FROM engagements
  LEFT JOIN prospect_info ON engagements._email = prospect_info._email
  JOIN airtable_info ON engagements._campaignid = CAST(airtable_info._campaignID AS STRING);