-- For the new lead score dashboard
-- Where only those who have a category or an increase in score are shown in the dashboard
-- Each row is the event with the exact date of occurence
CREATE OR REPLACE TABLE `x-marketing.corcentric.marketo_daily_targets` AS 
-- Get all leads and contacts that are QP, Discovery, and MQL
-- Due to there only being first and last fields, no way to get historical dates
-- Have to take combo of both fields to get the best possible historical dates
WITH contacts AS (
  SELECT DISTINCT
    email,
    id AS salesforce_id,
    DATE(ds_03_last_qp__c) AS QP,
    DATE(ds_04_last_discovery__c) AS Discovery,
    DATE(ds_05_last_mql__c) AS MQL
  FROM `x-marketing.corcentric_salesforce.Contact`
  WHERE isdeleted = FALSE
    -- To satisfy marketing relationship -> QP must happen
    AND ds_03_last_qp__c IS NOT NULL
  UNION DISTINCT
  SELECT DISTINCT
    email,
    id AS salesforce_id,
    DATE(ds_03_first_qp__c) AS QP,
    DATE(ds_04_first_discovery__c) AS Discovery,
    DATE(ds_05_first_mql__c) AS MQL
  FROM `x-marketing.corcentric_salesforce.Contact`
  WHERE isdeleted = FALSE -- To satisfy marketing relationship -> QP must happen
    AND ds_03_first_qp__c IS NOT NULL
),
leads AS (
  SELECT
    email,
    id AS salesforce_id,
    DATE(ds_03_last_qp__c) AS QP,
    DATE(ds_04_last_discovery__c) AS Discovery,
    DATE(ds_05_last_mql__c) AS MQL
  FROM `x-marketing.corcentric_salesforce.Lead`
  WHERE isdeleted = FALSE -- To satisfy marketing relationship -> QP must happen
    AND ds_03_last_qp__c IS NOT NULL -- To exclude leads that have become contacts
    AND email NOT IN (
      SELECT DISTINCT
        email
      FROM contacts
    )
  UNION DISTINCT
  SELECT
    email,
    id AS salesforce_id,
    DATE(ds_03_first_qp__c) AS QP,
    DATE(ds_04_first_discovery__c) AS Discovery,
    DATE(ds_05_first_mql__c) AS MQL
  FROM `x-marketing.corcentric_salesforce.Lead`
  WHERE isdeleted = FALSE -- To satisfy marketing relationship -> QP must happen
    AND ds_03_first_qp__c IS NOT NULL -- To exclude leads that have become contacts
    AND email NOT IN (
      SELECT DISTINCT
        email
      FROM contacts
    )
),
target_category AS (
  SELECT
    *
  FROM contacts
  UNION ALL
  SELECT
    *
  FROM leads
),
-- Unpivot table to put category in rows 
unpivot_target_category AS (
  SELECT DISTINCT
    email,
    salesforce_id,
    category,
    CASE
      WHEN category = 'QP' THEN 1
      WHEN category = 'Discovery' THEN 2
      WHEN category = 'MQL' THEN 3
    END AS category_rank,
    category_date
  FROM target_category UNPIVOT(
    category_date
    FOR category IN (QP, Discovery, MQL)
  )
),
-- Get leads that are influenced by 2X campaigns and still existing in database
campaign_members AS (
  SELECT DISTINCT
    -- For ease of identification
    marketoid AS marketo_id,
    sfdcleadorcontactid AS salesforce_id,
    -- Lead info 
    email,
    name,
    title,
    company,
    industry,
    annual_revenue,
    country,
    -- Region factor
    region,
    region_rank,
    -- Campaign factor
    campaign,
    campaign_type,
    MIN(campaign_start_date) OVER (PARTITION BY campaign) AS campaign_start_date,
    MAX(campaign_end_date) OVER (PARTITION BY campaign) AS campaign_end_date,
    activity_date AS campaign_engagement_date
  FROM `x-marketing.corcentric.marketo_lead_movement` -- Exclude those in first week
  WHERE week_rank != 1 -- Get unique person and campaign combo  
    AND year = EXTRACT(YEAR FROM activity_date)
    AND WEEK = EXTRACT(WEEK FROM activity_date) -- Get people that exist in salesforce, assumed to be deleted in marketo
    AND found_in_salesforce = TRUE -- Only interested with people involved in campaigns
    AND campaign IS NOT NULL -- Get only 2X campaigns 
    AND campaign_category = '2X Campaigns'
),
-- Get the lead score of campaign members
marketo_lead_score AS (
  SELECT
    extract_date,
    CAST(id AS STRING) AS marketo_id,
    ROUND(COALESCE(leadscore, 0), 1) AS lead_score,
    ROUND(COALESCE(behavior_score__c, 0), 1) AS behaviour_score,
    -- Get the previous day score here so that the 1st row is not null
    LAG(ROUND(COALESCE(behavior_score__c, 0), 1)) OVER (PARTITION BY CAST(id AS STRING) ORDER BY extract_date) AS previous_behaviour_score,
    -- Get the earliest extract date of a person
    -- For those that dont exist during campaign start
    MIN(extract_date) OVER (PARTITION BY CAST(id AS STRING)) AS earliest_extract_date,
    -- Get the earliest lead score of a person
    -- For those that dont exist during campaign start
    MIN(ROUND(COALESCE(leadscore, 0), 1)) OVER (PARTITION BY CAST(id AS STRING)) AS earliest_score,
    -- Get the latest extract date of a person
    -- For those that dont exist during current campaign
    MAX(extract_date) OVER (PARTITION BY CAST(id AS STRING)) AS latest_extract_date,
    -- Get the latest lead score of a person
    -- For those that dont exist during current campaign
    MAX(ROUND(COALESCE(leadscore, 0), 1)) OVER (PARTITION BY CAST(id AS STRING)) AS latest_score
  FROM `x-marketing.corcentric.marketo_lead_score_snapshot`
  WHERE CAST(id AS STRING) IN (
    SELECT DISTINCT
      marketo_id
    FROM campaign_members
  )
),
-- Tie each campaign members with lead score and category
-- Will be getting a range worth of score from the day of engagement onwards
-- Category is only considered if it falls within the same range too
-- Influenced leads have increased in behaviour score or achieving a target category
combined_data AS (
  SELECT
    campaign.*,
    -- Set the campaign rank
    DENSE_RANK() OVER (ORDER BY campaign.campaign_start_date, campaign.campaign) AS campaign_rank,
    score.earliest_extract_date,
    score.earliest_score,
    score.latest_extract_date,
    score.latest_score,
    score.extract_date AS score_date,
    score.lead_score AS score,
    score.behaviour_score - score.previous_behaviour_score AS score_change,
    category.category,
    category.category_rank,
    category.category_date
  FROM campaign_members AS campaign
  LEFT JOIN marketo_lead_score AS score -- The score data only have marketo id
    -- Set the range to be a month long [CHANGE CONSTANT HERE]
    ON campaign.marketo_id = score.marketo_id
    AND score.extract_date BETWEEN campaign.campaign_engagement_date
    AND DATE_ADD(campaign.campaign_end_date, INTERVAL 30 DAY)
  LEFT JOIN unpivot_target_category AS category -- The category data only have salesforce id
    -- The range follows the score range
    ON campaign.salesforce_id = category.salesforce_id
    AND score.extract_date = category.category_date
  WHERE category.category IS NOT NULL
    OR (score.behaviour_score - score.previous_behaviour_score) > 0
),
-- Get the score at the start and end of campaigns
get_start_and_end_score AS (
  SELECT
    main.* EXCEPT (
      earliest_extract_date,
      earliest_score,
      latest_extract_date,
      latest_score
    ),
    COALESCE(score_1.lead_score, main.earliest_score) AS start_score,
    COALESCE(score_1.extract_date, main.earliest_extract_date) AS start_score_date,
    COALESCE(score_2.lead_score, main.latest_score) AS end_score,
    COALESCE(score_2.extract_date, main.latest_extract_date) AS end_score_date
  FROM combined_data AS main
  LEFT JOIN marketo_lead_score AS score_1 -- The score data only have marketo id
    -- Do an exact match of the date for score on those days
    ON main.marketo_id = score_1.marketo_id
    AND main.campaign_start_date = score_1.extract_date
  LEFT JOIN marketo_lead_score AS score_2 -- The score data only have marketo id
    -- Do an exact match of the date for score on those days
    ON main.marketo_id = score_2.marketo_id
    AND main.campaign_end_date = score_2.extract_date
),
lead_info_snapshot AS (
  SELECT
    DATE(extract_date) AS extract_date,
    marketoid AS marketo_id,
    lead_status AS status
  FROM `x-marketing.corcentric.marketo_lead_info_snapshot`
),
-- Tie filtered leads with the snapshot data to get historical lead status
get_lead_status AS (
  SELECT
    main.*,
    side.status
  FROM get_start_and_end_score AS main
  LEFT JOIN lead_info_snapshot AS side
    ON main.marketo_id = side.marketo_id -- Use score date as it is present for both score and category
    AND main.score_date = side.extract_date -- Remove those that are unqualified
  WHERE side.status != 'Unqualified'
),
-- Generate string form of week range
get_week_range AS (
  SELECT DISTINCT
    *,
    CONCAT(
      CAST(EXTRACT(YEAR FROM score_date) AS STRING), ' ',
      'Week ', CAST(EXTRACT(WEEK FROM score_date) AS STRING), ' (', 
      FORMAT_DATE("%b %d", DATE_TRUNC(score_date, WEEK(MONDAY))), ' - ',
      FORMAT_DATE("%b %d", DATE_ADD(DATE_TRUNC(score_date, WEEK(MONDAY)), INTERVAL 6 DAY)), ')'
    ) AS week_str,
    -- Set the week rank
    DENSE_RANK() OVER (ORDER BY DATE_TRUNC(score_date, WEEK(MONDAY))) AS week_rank,
  FROM get_lead_status
),
get_week_range_row_num AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY marketo_id ORDER BY score_date DESC) AS rownum
  FROM get_week_range
),
-- Label the latest info / engagement of a lead
label_latest_row_of_leads AS (
  SELECT
    * EXCEPT (rownum),
    CASE
      WHEN rownum = 1 THEN TRUE
    END AS latest_lead_row
  FROM get_week_range_row_num
)
SELECT
  *
FROM label_latest_row_of_leads;