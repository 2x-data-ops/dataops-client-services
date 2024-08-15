--CREATE OR REPLACE TABLE `x-marketing.equiteq.db_email_engagements_log` AS
TRUNCATE TABLE `x-marketing.equiteq.db_email_engagements_log`;
INSERT INTO `x-marketing.equiteq.db_email_engagements_log` (
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
    _company,
    _domain,
    _industry,
    _country,
    _city,
    _revenue,
    _employees,
    _lifecycleStage,
    _contentTitle,
    _campaignSubject,
    _campaignType
)

WITH prospect_info AS (
    SELECT 
        CAST(vid AS STRING) AS _prospectID,
        properties.email.value AS _email,
        CONCAT(
            properties.firstname.value,
            ' ',
            properties.lastname.value
        ) AS _name,
        properties.phone.value AS _phone,
        properties.jobtitle.value AS _title,
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
            WHEN REGEXP_CONTAINS(properties.lifecyclestage.value, r'[0-9]+') THEN NULL
            WHEN properties.lifecyclestage.value = '' THEN NULL
            WHEN property_lifecyclestage.value = 'marketingqualifiedlead' THEN 'Marketing Qualified Lead'
            WHEN property_lifecyclestage.value = 'salesqualifiedlead' THEN 'Sales Qualified Lead'
            ELSE INITCAP(CAST(properties.lifecyclestage.value AS STRING))
        END AS _lifecycleStage
    FROM `x-marketing.equiteq_hubspot.contacts` 
    QUALIFY ROW_NUMBER() OVER(
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
    SELECT 
        CAST(id AS STRING) as _campaignID,
        name AS _contentTitle,
        subject AS _campaignSubject,
        REPLACE(INITCAP(type), '_', ' ') AS _campaignType
    FROM `x-marketing.equiteq_hubspot.campaigns` 
    QUALIFY ROW_NUMBER() OVER(
        PARTITION BY name,
        id
        ORDER BY id
    ) = 1
),
shared_fields AS (
    SELECT 
        activity.id,
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
    FROM `x-marketing.equiteq_hubspot.email_events` activity
    JOIN `x-marketing.equiteq_hubspot.campaigns` campaign 
        ON activity.emailcampaignid = campaign.id
    AND campaign.name IS NOT NULL
),
Dropped AS (
    SELECT *
    EXCEPT(_type, id),
        'Dropped' AS _engagement,
    FROM shared_fields
    WHERE _type = 'DROPPED' 
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY _email,
        _campaignName
        ORDER BY _timestamp DESC
    ) = 1
),
Deferred AS (
    SELECT *
    EXCEPT(_type, id),
        'Deferred' AS _engagement,
    FROM shared_fields
    WHERE _type = 'DEFERRED' 
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY _email,
        _campaignName
        ORDER BY _timestamp DESC
    ) = 1
),
Suppressed AS (
    SELECT *
    EXCEPT(_type, id),
        'Suppressed' AS _engagement,
    FROM shared_fields
    WHERE _type = 'SUPPRESSED' 
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY _email,
        _campaignName
        ORDER BY _timestamp DESC
    ) = 1
),
Opened AS (
    SELECT *
    EXCEPT(_type, id),
        'Opened' AS _engagement,
    FROM shared_fields
    WHERE _type = 'OPEN' 
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY _email,
        _campaignName
        ORDER BY _timestamp DESC
    ) = 1
),
Clicked AS (
    SELECT *
    EXCEPT(_type, id),
        'Clicked' AS _engagement,
    FROM shared_fields
    WHERE _type = 'CLICK' 
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY _email,
        _campaignName
        ORDER BY _timestamp DESC
    ) = 1
),
Unsubscribed AS (
    SELECT 
        shared_fields.*
    EXCEPT(_type, id),
        'Unsubscribed' AS _engagement,
    FROM `x-marketing.equiteq_hubspot.subscription_changes`,
        UNNEST(changes) AS status
    JOIN shared_fields 
        ON status.value.causedbyevent.id = shared_fields.id
    WHERE _type = 'STATUSCHANGE'
        AND status.value.change = 'UNSUBSCRIBED' 
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY _email,
        _campaignName
        ORDER BY _timestamp DESC
    ) = 1
),
form_filled AS (
    SELECT 
        contacts._sdc_sequence,
        contacts.properties.email.value AS email,
        form.value.title AS form_title,
        form.value.timestamp AS timestamp,
        form.value.page_url AS description,
        CAST(NULL AS STRING) AS devicetype,
        REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_source=([^&]+)') AS _utmsource,
        REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_campaign=([^&]+)') AS _utmcampaign,
        REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_medium=([^&]+)') AS _utmmedium,
        REGEXP_EXTRACT(form.value.page_url, r'[?&]utm_content=([^&]+)') AS _utmcontent,
    FROM `x-marketing.equiteq_hubspot.contacts` contacts,
        UNNEST(form_submissions) AS form
    JOIN `x-marketing.equiteq_hubspot.forms` forms 
        ON form.value.form_id = forms.guid
),
Downloaded AS (
    SELECT 
        activity._sdc_sequence,
        activity.email AS _email,
        CAST(campaign.id AS STRING) AS _campaignID,
        COALESCE(form_title, campaign.name) AS _campaignName,
        activity.timestamp AS _timestamp,
        activity.description AS _description,
        activity.devicetype,
        '' AS linkid,
        '' AS duration,
        '' AS response,
        'Downloaded' AS _engagement,
    FROM form_filled activity
    LEFT JOIN `x-marketing.equiteq_hubspot.campaigns` campaign 
        ON activity._utmcontent = CAST(campaign.id AS STRING) 
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY _email,
        _campaignName
        ORDER BY _timestamp DESC
    ) = 1
),
SoftBounced AS (
    SELECT *
    EXCEPT(_type, id),
        'Soft Bounced' AS _engagement,
    FROM shared_fields
    WHERE _type = 'BOUNCE' 
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY _email,
        _campaignName
        ORDER BY _timestamp DESC
    ) = 1
),
subs_change_bounce AS (
    SELECT 
        shared_fields.*
    EXCEPT(_type, id),
        'Hard Bounced' AS _engagement
    FROM `x-marketing.equiteq_hubspot.subscription_changes`,
        UNNEST(changes) AS status
    JOIN shared_fields 
        ON status.value.causedbyevent.id = shared_fields.id
    WHERE status.value.change = 'BOUNCED' 
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY _email,
        _campaignName
        ORDER BY _timestamp DESC
    ) = 1
),
HardBounced AS (
    SELECT 
        subs_change_bounce.*
    FROM subs_change_bounce
    JOIN SoftBounced 
        ON subs_change_bounce._email = SoftBounced._email
        AND subs_change_bounce._campaignID = SoftBounced._campaignID
),
Sent AS (
    SELECT *
    EXCEPT(_type, id),
        'Sent' AS _engagement,
    FROM shared_fields
    WHERE _type = 'SENT' 
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY _email,
        _campaignName
        ORDER BY _timestamp DESC
    ) = 1
),
Delivered AS (
    SELECT *
    EXCEPT(_type, id),
        'Delivered' AS _engagement,
    FROM shared_fields
    WHERE _type = 'DELIVERED' 
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY _email,
        _campaignName
        ORDER BY _timestamp DESC
    ) = 1
),
engagements AS (
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
SELECT 
    engagements.*,
    prospect_info.*
EXCEPT (_email),
    airtable_info.*
EXCEPT (_campaignID),
    FROM engagements
    LEFT JOIN prospect_info 
        ON engagements._email = prospect_info._email
    JOIN airtable_info 
        ON engagements._campaignid = CAST(airtable_info._campaignID AS STRING);