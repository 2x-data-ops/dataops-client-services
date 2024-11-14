TRUNCATE TABLE `x-marketing.epam.db_campaign_analysis_marketo`;

INSERT INTO `x-marketing.epam.db_campaign_analysis_marketo` (
    _sdc_sequence,
    _primary_attribute_name,
    _campaignID,
    _timestamp,
    _dayofweek,
    _prospectID,
    _url_link,
    _engagement,
    _isBot,
    _utm_source,
    _utm_medium,
    _subject,
    _description,
    _campaignSentDate,
    _screenshot,
    _landingpage,
    _email,
    _name,
    _title,
    _seniority,
    _phone,
    _company,
    _industry,
    _city,
    _state,
    _country,
    _region,
    _leadcreatedDate,
    _leadqualification,
    _leadstatus,
    _industrypreference,
    _personsource,
    _segment,
    _requestername,
    _branding,
    _partnermarketing,
    _marketingmanager,
    _industrybusinesstopic,
    _contenttype
)
  
WITH merged_id AS (
  SELECT 
    a.leadid, 
    m.value, 
    l.email
  FROM `x-marketing.epam_marketo.activities_merge_leads` a
  JOIN UNNEST(a.merge_ids) AS m
  JOIN `x-marketing.epam_marketo.leads` l 
    ON m.value = l.id
),
prospect AS (
  SELECT 
    CAST(marketo.id AS STRING) AS _leadid,
    COALESCE(marketo.email, merged.email) AS _email,
    CONCAT(marketo.firstname,' ', marketo.lastname) AS _name,
    master._jobtitle AS _title,
    CASE 
      WHEN master._seniority = '' THEN NULL
      ELSE master._seniority 
    END AS _seniority,
    marketo.phone,
    marketo.company,
    CASE 
      WHEN LOWER(marketo.industry) = 'Retail' THEN 'Consumer'
      WHEN LOWER(marketo.industry) = 'Information Technology' THEN 'Software & Hi-Tech'
      WHEN LOWER(marketo.industry) = 'MACH' THEN 'Software & Hi-Tech'
      WHEN LOWER(marketo.industry) = 'Automotive' THEN 'Industrial'
      ELSE marketo.industry 
    END AS industry,
    marketo.city,
    marketo.state,
    CASE 
      WHEN master._country = 'Unknown value' THEN 'Other'
      WHEN master._country LIKE '%US%' OR master._country LIKE '%USA%' THEN 'United States' 
      WHEN master._country LIKE '%UK%' THEN 'United Kingdom'
      WHEN master._country LIKE '%South Korea%' THEN 'Korea, Republic of' 
      ELSE master._country 
    END AS country,
    CASE 
      WHEN master._region = '' THEN NULL
      ELSE master._region
    END AS region,
    createdat,
    CASE 
      WHEN LOWER(title) LIKE '%journalist%' OR LOWER(title) LIKE '%reporter%' OR LOWER(title) LIKE 'student' OR LOWER(title) LIKE 'students' OR LOWER(title) LIKE 'studentin' OR LOWER(title) LIKE 'grad student' OR LOWER(title) LIKE 'master student' OR LOWER(title) LIKE 'intern' OR LOWER(title) LIKE 'mba intern' OR LOWER(title) LIKE 'machine learning intern' OR LOWER (title) LIKE '%publication%' OR LOWER(title) LIKE 'freelance' THEN 'Disqualified Leads' 
      ELSE 'Qualified Leads'  
    END AS _leadqualification,
    leadsource,
    industrypreference,
    master._personsource
  FROM `x-marketing.epam_marketo.leads` marketo
  LEFT JOIN `x-marketing.epam_mysql.epam_db_masterleads` master
    ON marketo.id = CAST(master._leadid AS INT64)
  LEFT JOIN merged_id AS merged  --this table is to get the email for the merge id 
    ON marketo.id = merged.leadid  
  WHERE (emailinvalid IS FALSE OR unsubscribed IS FALSE)
  QUALIFY ROW_NUMBER() OVER( PARTITION BY CAST(marketo.id AS STRING) ORDER BY createdat DESC) = 1
),
prospect_info AS (
  SELECT 
    * 
  FROM prospect
  WHERE _email NOT IN ('skylarulry@yahoo.com', 'sonam.gupta@capgemini.com')
  AND NOT REGEXP_CONTAINS(_email, r'(@2x\.marketing|2X|test)')
),
sent_email AS (
  SELECT
    activity._sdc_sequence,
    activity.primary_attribute_name,
    CAST(activity.primary_attribute_value_id AS STRING) AS campaignID,
    activity.activitydate AS _timestamp,
    EXTRACT(DAYOFWEEK FROM activity.activitydate) AS _dayofweek,
    CAST( activity.leadid AS STRING) AS _leadid, 
    email AS _email,
    campaign._campaignname AS _description,
    '' as _url_link,
    'Sent' AS _engagement,
    CAST(null AS String) AS _isBot
  FROM `x-marketing.epam_marketo.activities_send_email` activity 
  JOIN `x-marketing.epam_mysql.epam_db_airtable_email` campaign 
    ON activity.primary_attribute_value_id =campaign._pardotid
  JOIN `x-marketing.epam_marketo.leads` l 
    ON l.id = activity.leadid
  LEFT JOIN `x-marketing.epam_marketo.activities_delete_lead` d 
    ON d.leadid = activity.leadid
  LEFT JOIN prospect_info 
    ON CAST(prospect_info._leadid AS INT64) = activity.leadid
  WHERE prospect_info._email NOT LIKE '%@2x.marketing%'
    AND d.leadid IS NULL
  QUALIFY ROW_NUMBER() OVER( 
    PARTITION BY activity.leadid, activity.primary_attribute_value 
    ORDER BY activity.activitydate DESC
  ) = 1
),
delivered_email AS ( 
  SELECT
    activity._sdc_sequence,
    activity.primary_attribute_name,
    CAST(activity.primary_attribute_value_id AS STRING) AS campaignID,
    activity.activitydate AS _timestamp, 
    EXTRACT(DAYOFWEEK FROM activity.activitydate) AS _dayofweek,
    CAST( activity.leadid AS STRING) AS _leadid, 
    email AS _email,
    campaign._campaignname AS _description,
    '' as _url_link,
    'Delivered' AS _engagement,
    CAST(null AS String)  AS _isBot
  FROM `x-marketing.epam_marketo.activities_email_delivered` activity 
  JOIN `x-marketing.epam_mysql.epam_db_airtable_email` campaign 
    ON activity.primary_attribute_value_id =campaign._pardotid
  JOIN `x-marketing.epam_marketo.leads` l 
    ON l.id = activity.leadid
  LEFT JOIN prospect_info 
    ON CAST(prospect_info._leadid AS INT64) = activity.leadid
  WHERE prospect_info._email NOT LIKE '%@2x.marketing%'
  QUALIFY ROW_NUMBER() OVER( 
    PARTITION BY activity.leadid, activity.primary_attribute_value 
    ORDER BY activity.activitydate DESC
  ) = 1
),
open_email AS (
  SELECT
    activity._sdc_sequence,
    activity.primary_attribute_name,
    CAST(activity.primary_attribute_value_id AS STRING) AS campaignID,
    activity.activitydate AS _timestamp, 
    EXTRACT(DAYOFWEEK FROM activity.activitydate) AS _dayofweek,
    CAST( activity.leadid AS STRING) AS _leadid, 
    email AS _email,
    CASE 
      WHEN activity.primary_attribute_value = 'Ecosystem Education Webinar Email 2.Email 2' THEN 'Ecosystem Email 2.Email 2' 
      WHEN activity.primary_attribute_value = 'Ecosystem Education Webinar Email 1.Email 1' THEN 'Ecosystem Email 1.Email 1' 
      ELSE campaign._campaignname 
    END AS _description, 
    '' as _url_link,
    'Opened' AS _engagement,
    CAST(is_bot_activity AS STRING) AS _isBot  
  FROM `x-marketing.epam_marketo.activities_open_email` activity
  JOIN `x-marketing.epam_mysql.epam_db_airtable_email` campaign 
    ON activity.primary_attribute_value_id =campaign._pardotid
  JOIN `x-marketing.epam_marketo.leads` l 
    ON l.id = activity.leadid  
  LEFT JOIN `x-marketing.epam_marketo.activities_delete_lead` d 
    ON d.leadid = activity.leadid  
  LEFT JOIN prospect_info 
    ON CAST(prospect_info._leadid AS INT64) = activity.leadid
  WHERE prospect_info._email NOT LIKE '%@2x.marketing%'
    AND d.leadid IS NULL
  QUALIFY ROW_NUMBER() OVER( 
    PARTITION BY activity.leadid, activity.primary_attribute_value 
    ORDER BY activity.activitydate DESC
  ) = 1
),
clicked_email AS (
  SELECT
    activity._sdc_sequence,
    activity.primary_attribute_name,
    CAST(activity.primary_attribute_value_id AS STRING) AS campaignID,
    activity.activitydate AS _timestamp, 
    EXTRACT(DAYOFWEEK FROM activity.activitydate) AS _dayofweek,
    CAST( activity.leadid AS STRING) AS _leadid, 
    email AS _email,
    campaign._campaignname AS _description, 
    activity.link as _url_link,
    'Clicked' AS _engagement,
    CAST(is_bot_activity AS STRING) AS _isBot,
    ROW_NUMBER() OVER( 
      PARTITION BY activity.leadid,activity.primary_attribute_value 
      ORDER BY activity.activitydate DESC
    ) AS rownum
  FROM `x-marketing.epam_marketo.activities_click_email` activity 
  JOIN `x-marketing.epam_mysql.epam_db_airtable_email` campaign 
    ON activity.primary_attribute_value_id =campaign._pardotid
  JOIN `x-marketing.epam_marketo.leads` l 
    ON l.id = activity.leadid
  LEFT JOIN `x-marketing.epam_marketo.activities_delete_lead` d 
    ON d.leadid = activity.leadid
  WHERE CAST(is_bot_activity AS STRING) = 'false' 
    --AND activity.link NOT LIKE '%iclick%'
    AND l.email NOT LIKE '%@2x.marketing%'
    AND d.leadid IS NULL
    AND activity._sdc_sequence <> 1697681411029821012
),
unique_click AS (
  SELECT 
    * EXCEPT(rownum) 
  FROM (SELECT * FROM clicked_email) 
  WHERE rownum = 1
),
iclick_history AS (
  SELECT 
    * EXCEPT(rownum)
  FROM clicked_email
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11
  HAVING COUNTIF (_url_link NOT LIKE '%iclick%') = 0
),
total_click AS (
  SELECT * FROM unique_click
  WHERE _leadid NOT IN (
    SELECT _leadid FROM iclick_history
  )
),
--merge open and click data
open_click AS (
  SELECT * FROM open_email
  UNION ALL
  SELECT * FROM total_click

),
--to populate the data in 'Clicked' but not appear in 'Opened'
new_open AS (
  SELECT 
    * EXCEPT (_engagement,_isBot), 
    'Opened' AS _engagement, 
    CAST(_isBot AS STRING) AS _isBot
  FROM open_click
  WHERE _engagement <> 'Opened' 
    AND _engagement = 'Clicked'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _leadid,_description ORDER BY _timestamp DESC) = 1
), 
--remove duplicate between the Clicked and Opened list data
total_open AS (
  SELECT 
    open.*
  FROM (
    SELECT * FROM open_email
    UNION ALL
    SELECT * FROM new_open
  ) open
  QUALIFY ROW_NUMBER() OVER (PARTITION BY _leadid,_description ORDER BY _timestamp DESC) = 1
), 
unsubscribed_email AS (
  SELECT
    activity._sdc_sequence,
    activity.primary_attribute_name,
    CAST(activity.primary_attribute_value_id AS STRING) AS campaignID,
    activity.activitydate AS _timestamp, 
    EXTRACT(DAYOFWEEK FROM activity.activitydate) AS _dayofweek,
    CAST( activity.leadid AS STRING) AS _leadid, 
    email AS _email,
    campaign._campaignname AS _description, 
    activity.referrer_url as _url_link,
    'Unsubscribed' AS _engagement,
    CAST(null AS String) AS _isBot
  FROM `x-marketing.epam_marketo.activities_unsubscribe_email` activity
  JOIN `x-marketing.epam_mysql.epam_db_airtable_email` campaign 
    ON activity.primary_attribute_value_id =campaign._pardotid 
  JOIN `x-marketing.epam_marketo.leads` l 
    ON l.id = activity.leadid
  LEFT JOIN `x-marketing.epam_marketo.activities_delete_lead` d 
    ON d.leadid = activity.leadid
  WHERE l.email NOT LIKE '%@2x.marketing%'
    AND d.leadid IS NULL
  QUALIFY ROW_NUMBER() OVER( 
    PARTITION BY activity.leadid, activity.primary_attribute_value 
    ORDER BY activity.activitydate DESC
  ) = 1
),
bounced_email AS (
    SELECT * EXCEPT(rownum) 
    FROM ( 
        SELECT
            activity._sdc_sequence,
            activity.primary_attribute_name,
            CAST(activity.primary_attribute_value_id AS STRING) AS campaignID,
            activity.activitydate AS _timestamp,
            EXTRACT(DAYOFWEEK FROM activity.activitydate) AS _dayofweek, 
            CAST( activity.leadid AS STRING) AS _leadid,
            l.email AS _email,
            campaign._campaignname AS _description,
            activity.details as _url_link,
            'Bounced' AS _engagement,
            CAST(null AS String) AS _isBot,
            ROW_NUMBER() OVER( PARTITION BY activity.leadid,activity.primary_attribute_value ORDER BY activity.activitydate DESC) AS rownum
        FROM `x-marketing.epam_marketo.activities_email_bounced` activity 
        JOIN `x-marketing.epam_mysql.epam_db_airtable_email` campaign ON activity.primary_attribute_value_id =campaign._pardotid
        JOIN `x-marketing.epam_marketo.leads` l ON l.id = activity.leadid
    ) 
    WHERE rownum = 1

),soft_bounced_email AS (
    SELECT * EXCEPT(rownum) 
    FROM ( 
        SELECT
            activity._sdc_sequence,
            activity.primary_attribute_name,
            CAST(activity.primary_attribute_value_id AS STRING) AS campaignID,
            activity.activitydate AS _timestamp, 
            EXTRACT(DAYOFWEEK FROM activity.activitydate) AS _dayofweek,
            CAST( activity.leadid AS STRING) AS _leadid, 
            l.email AS _email,
            campaign._campaignname AS _description, 
            '' AS _url_link,
            'Soft Bounced' AS _engagement,
            CAST(null AS String) AS _isBot,
            ROW_NUMBER() OVER( PARTITION BY activity.leadid,activity.primary_attribute_value ORDER BY activity.activitydate DESC) AS rownum
        FROM `x-marketing.epam_marketo.activities_email_bounced_soft` activity 
        JOIN `x-marketing.epam_mysql.epam_db_airtable_email` campaign ON activity.primary_attribute_value_id =campaign._pardotid
        JOIN `x-marketing.epam_marketo.leads` l ON l.id = activity.leadid
    ) 
    WHERE rownum = 1
),

soft_hard_email AS ( --merge soft and hard bounced
    SELECT *
    FROM (
        SELECT * FROM bounced_email
        UNION ALL
        SELECT * FROM soft_bounced_email
    )
),

new_delivered_email AS( --remove soft and hard bounced in delivered list
    SELECT d.*
    FROM delivered_email d
    LEFT JOIN soft_hard_email b ON d.campaignID = b.campaignID AND d._leadid = b._leadid
    WHERE b.campaignID IS NULL AND b._leadid IS NULL
),

overall_delivered AS ( --delivered based on current smart list
    SELECT n.* FROM new_delivered_email n
LEFT JOIN `x-marketing.epam_marketo.activities_delete_lead` d ON CAST(d.leadid AS STRING) = _leadid
WHERE d.leadid IS NULL

),
missing_delivered AS (
    SELECT
        open._sdc_sequence,
        open.primary_attribute_name,
        open.campaignID,
        open._timestamp,
        open._dayofweek,
        open._leadid,
        open._email,
        open._description,
        open._url_link,
        'Delivered' AS _engagement,
        open._isBot
    FROM total_open open
    --open_email open
    LEFT JOIN overall_delivered delivered
    ON open._leadid = delivered._leadid
    AND open.campaignID = delivered.campaignID
    WHERE delivered._leadid IS NULL
    AND delivered.campaignID IS NULL
),
all_delivered AS (
    SELECT * FROM overall_delivered
    UNION ALL 
    SELECT * FROM missing_delivered
),
asset_info AS (
    SELECT *
    FROM `x-marketing.epam_mysql.epam_db_airtable_email`

)
SELECT engagements.* EXCEPT (_description,_email),
    asset_info._utm_source,
    asset_info._utm_medium,
    asset_info._subject, 
    asset_info._campaignname,
    CASE WHEN LENGTH(asset_info._livedate) > 0 
         THEN CAST(asset_info._livedate AS TIMESTAMP)
         ELSE NULL END AS _campaignSentDate,
    asset_info._screenshot, 
    asset_info._landingpage,
    prospect_info.* EXCEPT (_leadid),
    asset_info._segment,
    asset_info._requestername,
    asset_info._branding,
    CASE WHEN region = 'EMEA' AND asset_info._campaignname LIKE '%Adobe%' THEN 'April Leatherman' 
    WHEN region = 'NA' AND asset_info._campaignname LIKE '%Adobe%' THEN 'Kiley Groves'
    WHEN region = 'Nordics' AND asset_info._campaignname LIKE '%Adobe%' THEN 'April Leatherman'
    WHEN region = 'UKI' AND asset_info._campaignname LIKE '%Adobe%' THEN 'April Leatherman'
    WHEN region = 'MENA' AND asset_info._campaignname LIKE '%Adobe%' THEN 'TBD'
    WHEN region = 'NA' AND asset_info._campaignname LIKE '%Sitecore%' THEN 'Ellen Waugh'
    WHEN region = 'EMEA' AND asset_info._campaignname LIKE '%Sitecore%' THEN 'Victoria Smith'
    WHEN region = 'MENA' AND asset_info._campaignname LIKE '%Sitecore%' THEN 'TBD' ELSE NULL END AS _partnermarketing,
    asset_info._marketingManager,
    asset_info._industrybusinesstopic,
    asset_info._contenttype
FROM (
    SELECT * FROM all_delivered
    UNION ALL
    SELECT * FROM total_open
    UNION ALL
    SELECT * FROM total_click
    UNION ALL
    SELECT * FROM sent_email 
    UNION ALL 
    SELECT * FROM unsubscribed_email
    UNION ALL
    SELECT * FROM bounced_email
    UNION ALL
    SELECT * FROM soft_bounced_email
) AS engagements
LEFT JOIN asset_info ON engagements.campaignID = asset_info._pardotid
LEFT JOIN prospect_info ON LOWER(engagements._leadid) = LOWER(prospect_info._leadid);




--- Label Bots
UPDATE `x-marketing.epam.db_campaign_analysis_marketo` origin  
SET origin._isBot = 'Yes'
FROM (
    SELECT
        CASE WHEN TIMESTAMP_DIFF(click._timestamp, open._timestamp, SECOND) <= 2 THEN click._email 
        ELSE NULL 
        END AS _email, 
        click._utm_campaign 
    FROM `x-marketing.epam.db_campaign_analysis_marketo` AS click
    JOIN `x-marketing.epam.db_campaign_analysis_marketo` AS open ON LOWER(click._email) = LOWER(open._email)
    AND click._utm_campaign = open._utm_campaign
    WHERE click._engagement = 'Clicked'AND open._engagement = 'Opened'
    EXCEPT DISTINCT
    SELECT 
        conversion._email, 
        conversion._utm_campaign
    FROM `x-marketing.epam.db_campaign_analysis_marketo` AS conversion
    WHERE conversion._engagement IN ('Downloaded')
) bot
WHERE 
    origin._email = bot._email
AND origin._utm_campaign = bot._utm_campaign
AND origin._engagement IN ('Clicked','Opened');

--- Set Show Export
UPDATE `x-marketing.epam.db_campaign_analysis_marketo` origin
SET origin._showExport = 'Yes'
FROM (
    WITH focused_engagement AS (
        SELECT 
            _email, 
            _engagement, 
            _description,
            CASE WHEN _engagement = 'Opened' THEN 1
                WHEN _engagement = 'Clicked' THEN 2
                WHEN _engagement IN ( 'Downloaded') THEN 3
            END AS _priority
        FROM `x-marketing.epam.db_campaign_analysis_marketo`
        WHERE _engagement IN('Opened', 'Clicked', 'Downloaded')
        ORDER BY 1, 3, 4 DESC 
    ),
    final_engagement AS (
        SELECT * EXCEPT(_priority, _rownum)
        FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY _email, _description ORDER BY _priority DESC) AS _rownum
            FROM focused_engagement
        )
        WHERE _rownum = 1
    )    
    SELECT * FROM final_engagement
) AS final
WHERE origin._email = final._email
AND origin._engagement = final._engagement
AND origin._description = final._description;

--false delivered
UPDATE `x-marketing.epam.db_campaign_analysis_marketo` origin
SET origin._falseDelivered = 'True'
FROM (

      SELECT 
        _email,
        _engagement,
        _description,
        _hasDelivered,_hasBounced
    FROM (
        SELECT 
            _email,
            _engagement,
            _description,
            SUM(CASE WHEN _engagement = 'Delivered' THEN 1 END) AS _hasDelivered,
            SUM(CASE WHEN _engagement = 'Bounced' THEN 1 END) AS _hasBounced,
            SUM(CASE WHEN _engagement = 'Soft Bounced' THEN 1 END) AS _hasSoftBounced
        FROM 
            `x-marketing.epam.db_campaign_analysis_marketo`
        WHERE
            _engagement IN ('Delivered', 'Bounced', 'Soft Bounced')
        GROUP BY
            1, 2, 3
    )
    WHERE 
        _hasDelivered IS NOT NULL
    AND (_hasBounced IS NOT NULL OR _hasSoftBounced IS NOT NULL)
) scenario
WHERE
    origin._email = scenario._email
AND origin._description = scenario._description
AND origin._engagement IN ( 'Delivered');




--leads dashboard
CREATE OR REPLACE TABLE `x-marketing.epam.db_leads` AS 
WITH leads AS (
    SELECT leads.* EXCEPT (leadsource),
    CASE WHEN LOWER(leads.leadsource) = 'contentsyndication' THEN 'Content Syndication'
        WHEN LOWER(leads.leadsource) = 'paidlinkedin' THEN 'Paid LinkedIn'
        WHEN LOWER(leads.leadsource) = 'inpersonevent' THEN 'InPerson Event'
    ELSE leads.leadsource END AS leadsource , 
    CASE WHEN LOWER(title) LIKE '%journalist%' OR LOWER(title) LIKE '%reporter%' OR LOWER(title) LIKE 'student' OR LOWER(title) LIKE 'students' OR LOWER(title) LIKE 'studentin' OR LOWER(title) LIKE 'grad student' OR LOWER(title) LIKE 'master student' OR LOWER(title) LIKE 'intern' OR LOWER(title) LIKE 'mba intern' OR LOWER(title) LIKE 'machine learning intern' OR LOWER (title) LIKE '%publication%' OR LOWER(title) LIKE 'freelance' THEN 'Disqualified Leads' ELSE 'Qualified Leads' END AS _leadqualification, 
    program.name, 
    program.channel, 
    program.type, 
    program.workspace, 
    master._clasification,
    master._region,
    CASE WHEN LOWER(master._personsource) = 'contentsyndication' THEN 'Content Syndication'
        WHEN LOWER(master._personsource) = 'paidlinkedin' THEN 'Paid LinkedIn'
        WHEN LOWER(master._personsource) = 'inpersonevent' THEN 'InPerson Event'
    ELSE master._personsource END AS _personsource
FROM `x-marketing.epam_marketo.leads` leads
LEFT JOIN `x-marketing.epam_marketo.programs` program 
ON leads.acquisitionprogramid = CAST(program.id AS STRING)
LEFT JOIN `x-marketing.epam_mysql.epam_db_masterleads` master
ON leads.id = CAST(master._leadid AS INT64)
),
email_lead AS (
SELECT * FROM `x-marketing.epam.db_campaign_analysis_marketo` email WHERE _engagement = 'Delivered'
)
SELECT 
    leads.*,
    email_lead._description,
    email_lead._segment,
    email_lead._contenttype,
    DATE_DIFF(CURRENT_DATE('Hongkong'),
    CAST(createdat AS DATE),DAY) as day_diff,
    email_lead._segment AS _email_segment
FROM leads
JOIN email_lead ON email_lead._prospectID = CAST(leads.id AS STRING);


--cold and warm leads (database page)
CREATE OR REPLACE TABLE `x-marketing.epam.db_leads_status` AS
WITH overall_lead AS (
    SELECT 
        DISTINCT CAST(id AS STRING) AS _prospectID, 
        persontype, 
        leads.country, 
        company, 
        email, 
        industry,
        createdat, 
        mktoname, 
        phone,
        CASE 
            WHEN master._region = '' THEN NULL
            ELSE master._region 
        END AS _region,
        --master._region,
        leadsource,
        industrypreference,
        CASE 
            WHEN _personsource = '' THEN NULL
            WHEN LOWER(_personsource)  = 'contentsyndication' THEN 'Content Syndication'
            WHEN LOWER(_personsource) = 'paidlinkedin' THEN 'Paid LinkedIn'
            WHEN LOWER(_personsource) = 'inpersonevent' THEN 'InPerson Event'
            ELSE _personsource 
        END AS _personsource,
        CASE 
            WHEN _seniority = '' THEN NULL
            ELSE _seniority 
        END AS _seniority,
        _industrystandard,
        '' AS _segment, 
        title AS _title
    FROM `x-marketing.epam_marketo.leads` leads
    LEFT JOIN `x-marketing.epam_mysql.epam_db_masterleads` master
    ON leads.id = CAST(master._leadid AS INT64)
    WHERE  email  NOT LIKE '%@2x.marketing%' AND email NOT LIKE '%2X%' AND email NOT LIKE 'skylarulry@yahoo.com' AND email NOT LIKE '%test%' AND marketingsuspended IS false
    AND master._status = 'Marketable'
),
warm_lead AS (
    SELECT 
        DISTINCT m._prospectID, 
        'Contact' AS persontype, 
        m._country, 
        m._company, 
        m._email, 
        m._industry,
        m._leadcreatedDate, 
        m._name, 
        m._phone,
        m._region,
        m._leadstatus, 
        m._industrypreference,
        CASE 
            WHEN m._personsource = '' THEN NULL
            WHEN LOWER(m._personsource)  = 'contentsyndication' THEN 'Content Syndication'
            WHEN LOWER(m._personsource) = 'paidlinkedin' THEN 'Paid LinkedIn'
            WHEN LOWER(m._personsource) = 'inpersonevent' THEN 'InPerson Event'
            ELSE m._personsource 
        END AS _personsource,
        CASE 
            WHEN master._seniority = '' THEN NULL
            ELSE master._seniority 
        END AS _seniority,
        master._industrystandard,
        m._segment,  
        master._jobtitle AS _title,
        m._campaignID, 
        m._engagement, 
        m._description,
        m._subject,
        'Warm' AS _type
    FROM `x-marketing.epam.db_campaign_analysis_marketo` m
    LEFT JOIN `x-marketing.epam_mysql.epam_db_masterleads` master
    ON m._prospectID = master._leadid
    WHERE _engagement NOT IN ('Sent','Soft Bounced','Bounced','Unsubscribed')
    AND master._status = 'Marketable'
),
cold_lead AS (
    SELECT 
        o.*,
        '' AS _campaignID,
        '' AS _engagement,
        '' AS _description,
        '' AS _subject,
        'Cold' AS contacttype
    FROM overall_lead o
    LEFT JOIN warm_lead w ON w._prospectID = o._prospectID
    WHERE w._prospectID IS NULL
)
SELECT a.* 
FROM (
    SELECT * FROM warm_lead
    UNION ALL
    SELECT * FROM cold_lead
) a
LEFT JOIN `x-marketing.epam_mysql.epam_db_ipqs_list` ipqs 
ON ipqs._prospectid = a._prospectID
LEFT JOIN `x-marketing.epam_marketo.activities_delete_lead` d 
ON d.leadid = CAST(a._prospectID AS INT64)
LEFT JOIN `x-marketing.epam_mysql.epam_db_deleted_leads` dlead 
ON dlead._prospectid = a._prospectID
WHERE ipqs._prospectid IS NULL
AND d.leadid IS NULL
AND dlead._prospectid IS NULL
AND (a._personsource NOT IN ('TA_LATAM','TA') OR a._personsource IS NULL)
;


--account engagement
CREATE OR REPLACE TABLE epam.db_account_engagements AS 
WITH 
tam_contacts AS (
  SELECT * EXCEPT(_rownum) 
  FROM (
    SELECT DISTINCT
        id AS _leadorcontactid,
        persontype AS _contact_type,
        firstname AS _firstname, 
        lastname AS _lastname, 
        title AS _title, 
        NULL AS _2xseniority,
        email AS _email,
        CAST(NULL AS STRING) AS _accountid,
        RIGHT(email, LENGTH(email)-STRPOS(email,'@')) AS _domain, 
        company AS _accountname, 
        industry AS _industry, 
        /*COALESCE(_tier, CAST(NULL AS STRING))*/ NULL AS _tier, 
        --CAST(_revenue AS INTEGER) AS _annualrevenue,
        ROW_NUMBER() OVER(
            PARTITION BY email 
            ORDER BY prosp._sdc_received_at DESC
        ) _rownum
    FROM 
      `epam_marketo.leads` prosp
    
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
      _description, 
      CONCAT("Email ", INITCAP(_engagement)) AS _engagement
      FROM 
        (SELECT * FROM `epam.db_campaign_analysis_marketo`)
      WHERE 
     LOWER(_engagement) NOT IN ('sent','delivered', 'bounced', 'unsubscribed')
    ) a
    WHERE 
      NOT REGEXP_CONTAINS(_domain,'2x.marketing|gmail|yahoo|outlook|hotmail') 
      AND _domain IS NOT NULL
      --AND _year = 2022 AND _year = 2023
    ORDER BY 1, 3 DESC, 2 DESC
),
dummy_dates AS ( 
    SELECT _date,
    EXTRACT(WEEK FROM _date) AS _week,
    EXTRACT(YEAR FROM _date) AS _year
  FROM 
    UNNEST(GENERATE_DATE_ARRAY('2022-01-01', CURRENT_DATE(), INTERVAL 1 DAY)) AS _date 
),

contact_engagement AS (

  SELECT 
    DISTINCT 
    tam_contacts._domain, 
    tam_contacts._email,
    dummy_dates.*EXCEPT(_date), 
    engagements.*EXCEPT(_date, _week, _year, _domain, _email),
    tam_contacts.*EXCEPT(_domain, _email),
    engagements._date
  FROM 
    dummy_dates
  JOIN (
    SELECT * FROM email_engagement /*UNION ALL
    SELECT * FROM form_fills*/
  ) engagements USING(_week, _year)
  JOIN
    tam_contacts USING(_email) 
),
combined_engagements AS (
  SELECT * FROM contact_engagement
  --UNION DISTINCT
  --SELECT * FROM account_engagement
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