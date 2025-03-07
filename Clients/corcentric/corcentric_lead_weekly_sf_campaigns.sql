-- Historical lead score for salesforce campaigns
TRUNCATE TABLE `x-marketing.corcentric.marketo_lead_weekly_salesforce_campaigns`;

INSERT INTO `x-marketing.corcentric.marketo_lead_weekly_salesforce_campaigns`
-- Get all marketo leads and their scores
WITH all_scores AS (
  SELECT
    -- End Dates
    DATE(DATE_TRUNC(extract_date, WEEK(MONDAY))) AS first_date,
    LAST_DAY(extract_date, WEEK(MONDAY)) AS last_date,
    -- Date Breakdown
    EXTRACT(YEAR FROM DATE(DATE_TRUNC(extract_date, WEEK(MONDAY)))) AS year,
    EXTRACT(ISOWEEK FROM DATE(DATE_TRUNC(extract_date, WEEK(MONDAY)))) AS week,
    EXTRACT(DAYOFWEEK FROM extract_date) - 1 AS day_of_week,
    -- ID and Scores
    CAST(id AS STRING) AS marketoid,
    ROUND(leadscore, 1) AS lead_score,
    ROUND(behavior_score__c, 1) AS behaviour_score,
    ROUND(demographic_score__c, 1) AS demographic_score -- True source of all types of lead score
  FROM `x-marketing.corcentric.marketo_lead_score_snapshot`
),
-- Obtain the rank of week plus week description based on historical snapshot timeline
week_rank_and_week_str AS (
  SELECT
    main.*,
    side.week_rank,
    side.week_str
  FROM all_scores AS main -- Rank and range already pre-calculated in here
  LEFT JOIN `x-marketing.corcentric.marketo_weekly_dates` side
    ON main.first_date = side.rep_date
),
-- Get the max week scores, these are the representative scores
max_week_scores AS (
  SELECT
    *,
    MAX(lead_score) OVER (PARTITION BY week_rank, marketoid) AS max_week_score,
    MAX(behaviour_score) OVER (PARTITION BY week_rank, marketoid) AS max_week_behaviour_score,
    MAX(demographic_score) OVER (PARTITION BY week_rank, marketoid) AS max_week_demographic_score
  FROM week_rank_and_week_str
),
-- Each score as a column on its own
score_pivot_table AS (
  SELECT
    marketoid,
    -- All lead scores
    MAX(IF(day_of_week = 1, lead_score, NULL)) AS day1_score,
    MAX(IF(day_of_week = 2, lead_score, NULL)) AS day2_score,
    MAX(IF(day_of_week = 3, lead_score, NULL)) AS day3_score,
    MAX(IF(day_of_week = 4, lead_score, NULL)) AS day4_score,
    MAX(IF(day_of_week = 5, lead_score, NULL)) AS day5_score,
    MAX(IF(day_of_week = 6, lead_score, NULL)) AS day6_score,
    MAX(IF(day_of_week = 0, lead_score, NULL)) AS day7_score,
    -- All behaviour scores
    MAX(IF(day_of_week = 1, behaviour_score, NULL)) AS day1_behaviour_score,
    MAX(IF(day_of_week = 2, behaviour_score, NULL)) AS day2_behaviour_score,
    MAX(IF(day_of_week = 3, behaviour_score, NULL)) AS day3_behaviour_score,
    MAX(IF(day_of_week = 4, behaviour_score, NULL)) AS day4_behaviour_score,
    MAX(IF(day_of_week = 5, behaviour_score, NULL)) AS day5_behaviour_score,
    MAX(IF(day_of_week = 6, behaviour_score, NULL)) AS day6_behaviour_score,
    MAX(IF(day_of_week = 0, behaviour_score, NULL)) AS day7_behaviour_score,
    -- All demographic scores
    MAX(IF(day_of_week = 1, demographic_score, NULL)) AS day1_demographic_score,
    MAX(IF(day_of_week = 2, demographic_score, NULL)) AS day2_demographic_score,
    MAX(IF(day_of_week = 3, demographic_score, NULL)) AS day3_demographic_score,
    MAX(IF(day_of_week = 4, demographic_score, NULL)) AS day4_demographic_score,
    MAX(IF(day_of_week = 5, demographic_score, NULL)) AS day5_demographic_score,
    MAX(IF(day_of_week = 6, demographic_score, NULL)) AS day6_demographic_score,
    MAX(IF(day_of_week = 0, demographic_score, NULL)) AS day7_demographic_score,
    -- All date info
    week,
    week_str,
    week_rank,
    year,
    -- All max week scores
    COALESCE(max_week_score, 0) AS max_week_score,
    COALESCE(max_week_behaviour_score, 0) AS max_week_behaviour_score,
    COALESCE(max_week_demographic_score, 0) AS max_week_demographic_score
  FROM max_week_scores
  GROUP BY 1, 23, 24, 25, 26, 27, 28, 29
),
-- Get marketo info of leads
marketo_info AS (
  SELECT
    EXTRACT(YEAR FROM DATE(DATE_TRUNC(extract_date, WEEK(MONDAY)))) AS year,
    EXTRACT(ISOWEEK FROM DATE(DATE_TRUNC(extract_date, WEEK(MONDAY)))) AS week,
    marketoid,
    email,
    name,
    title,
    job_function,
    state,
    country,
    COALESCE(region, 'Non-Marketable') AS region,
    -- Set the rank of region for display order in dashboard's filter
    CASE
      WHEN region = 'North America' THEN 1
      WHEN region = 'North Europe' THEN 2
      WHEN region = 'South Europe' THEN 3
      WHEN region = 'Non-Marketable' THEN 4
    END AS region_rank,
    company,
    industry,
    annual_revenue_amount AS annual_revenue,
    ds03_mql_date,
  FROM `x-marketing.corcentric.marketo_lead_info_snapshot` -- There are 7 days in a week, so take the latest day of the week data
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      marketoid,
      EXTRACT(YEAR FROM DATE(DATE_TRUNC(extract_date, WEEK(MONDAY)))),
      EXTRACT(ISOWEEK FROM DATE(DATE_TRUNC(extract_date, WEEK(MONDAY))))
    ORDER BY extract_date DESC
  ) = 1
),
-- Get salesforce info of leads
sf_info AS (
  SELECT
    marketoid,
    sfdcleadid AS sf_lead_id,
    sfdccontactid AS sf_contact_id,
    sfdctype AS sf_type,
    sfdcleadstatus AS sf_lead_status,
    sfdccontactstatus sf_contact_status,
    sf_account_website,
    sf_account_type,
    sf_account_source,
  FROM `x-marketing.corcentric.marketo_lead_info_snapshot`
  QUALIFY ROW_NUMBER() OVER (PARTITION BY marketoid ORDER BY extract_date DESC) = 1
),
-- Combine marketo and salesforce info with the main data
add_marketo_and_sf_info AS (
  SELECT
    base.*,
    extra_mkt.* EXCEPT (marketoid, week, year),
    extra_sf.* EXCEPT (marketoid)
  FROM score_pivot_table AS base -- Join with marketo based on ID and date info
  JOIN marketo_info AS extra_mkt
    ON (
      base.marketoid = extra_mkt.marketoid
      AND base.week = extra_mkt.week
      AND base.year = extra_mkt.year
    ) -- Join with salesforce based on ID alone
  LEFT JOIN sf_info AS extra_sf
    ON base.marketoid = extra_sf.marketoid
),
-- Get all campaign engagements in salesforce
salesforce_campaign_members AS (
  SELECT
    campaign.name AS campaign,
    member.createddate AS activity_date,
    EXTRACT(YEAR FROM DATE(DATE_TRUNC(member.createddate, WEEK(MONDAY)))) AS activity_year,
    EXTRACT(ISOWEEK FROM DATE(DATE_TRUNC(member.createddate, WEEK(MONDAY)))) AS activity_week,
    member.email,
    member.status AS activity_status,
    member.leadorcontactid AS sf_lead_or_contact_id
  FROM `x-marketing.corcentric_salesforce.CampaignMember` member
  JOIN `x-marketing.corcentric_salesforce.Campaign` campaign
    ON member.campaignid = campaign.id
  WHERE campaign.name IN (
      '2022-01-NUR-Lifecycle.2023-05-EM-corcentric-managed-services',
      '2022-01-NUR-Lifecycle.2023-05-EM-Corcentric-named-Leader-in-ISG-Provider-Report',
      '2022-01-NUR-Lifecycle.2023-05-EM-How CFOs-Are-Driving-Business-Through-Digital-I',
      '2022-01-NUR-Lifecycle.2023-05-EM-why-cfos-are-going-all-in-on-digital',
      '2022-01-NUR-Lifecycle.2023-06-EM-A-View-From-Above:-the-Role-of-the-Modern-CFO',
      '2022-01-NUR-Lifecycle.2023-06-EM-Digital-Payments:-A-Changing-Economy-Sparks-New',
      '2022-01-NUR-Lifecycle.2023-06-EM-Digital-Savvy-CFOs-Innovate-to-Drive-Improved-W',
      '2022-01-NUR-Lifecycle.2023-06-EM-Shortening-the-cash-conversion-cycle-through-mg',
      '2022-01-NUR-Lifecycle.2023-07-EM-Accounts-Payable-2023:-BIG-Trends',
      '2022-01-NUR-Lifecycle.2023-07-EM-Corcentric-Conversations:-Getting-Opportunistic',
      '2022-01-NUR-Lifecycle.2023-07-EM-Corcentric-Managed-AP-Datasheet',
      '2022-01-NUR-Lifecycle.2023-07-EM-Digital-Payments:-A-Changing-Economy-Sparks-New',
      '2022-01-NUR-Lifecycle.2023-07-EM-IOFM: How-AP-Can-Help-Business-Navigate',
      '2022-01-NUR-Lifecycle.2023-07-EM-Smart-moves-where-AP-and-epayables-stand',
      '2022-01-NUR-Lifecycle.2023-08-EM-9-Ways-Managed-AR-Services-Help-Businesses',
      "2022-01-NUR-Lifecycle.2023-08-EM-CFO's-Guide-to-Automating-AR",
      '2022-01-NUR-Lifecycle.2023-08-EM-Daimler-Trucks-NA-Drives-Growth-with-Mgd-AR',
      '2022-01-NUR-Lifecycle.2023-08-EM-How-Automations-Reduce-Receivables-Delays',
      '2022-01-NUR-Lifecycle.2023-08-EM-How-to-Achieve-a-Permanent-DSO-of-15-days',
      '2022-01-NUR-Lifecycle.2023-08-EM-The-Business-Case-for-Accounts-Receivable-as-a-',
      '2022-01-NUR-Lifecycle.2023-08-EM-The-Evolution-of-AR-and-Credit-Mgt'
    )
    AND member.isdeleted = FALSE
),
add_campaign AS (
  SELECT
    main.*,
    side.campaign,
    side.activity_date,
    side.activity_status,
    side.sf_lead_or_contact_id
  FROM add_marketo_and_sf_info AS main
  JOIN salesforce_campaign_members AS side
    ON main.email = side.email
    AND main.year >= side.activity_year
    AND main.week >= side.activity_week
),
-- Obtain the latest week scores out of the 7 days of the week
latest_week_scores AS (
  SELECT
    *,
    -- Latest lead score of the week
    CASE
      WHEN day7_score IS NOT NULL THEN day7_score
      WHEN day6_score IS NOT NULL THEN day6_score
      WHEN day5_score IS NOT NULL THEN day5_score
      WHEN day4_score IS NOT NULL THEN day4_score
      WHEN day3_score IS NOT NULL THEN day3_score
      WHEN day2_score IS NOT NULL THEN day2_score
      WHEN day1_score IS NOT NULL THEN day1_score
      ELSE 0
    END AS latest_week_score,
    -- Latest behaviour score of the week
    CASE
      WHEN day7_behaviour_score IS NOT NULL THEN day7_behaviour_score
      WHEN day6_behaviour_score IS NOT NULL THEN day6_behaviour_score
      WHEN day5_behaviour_score IS NOT NULL THEN day5_behaviour_score
      WHEN day4_behaviour_score IS NOT NULL THEN day4_behaviour_score
      WHEN day3_behaviour_score IS NOT NULL THEN day3_behaviour_score
      WHEN day2_behaviour_score IS NOT NULL THEN day2_behaviour_score
      WHEN day1_behaviour_score IS NOT NULL THEN day1_behaviour_score
      ELSE 0
    END AS latest_week_behaviour_score,
    -- Latest demographic score of the week
    CASE
      WHEN day7_demographic_score IS NOT NULL THEN day7_demographic_score
      WHEN day6_demographic_score IS NOT NULL THEN day6_demographic_score
      WHEN day5_demographic_score IS NOT NULL THEN day5_demographic_score
      WHEN day4_demographic_score IS NOT NULL THEN day4_demographic_score
      WHEN day3_demographic_score IS NOT NULL THEN day3_demographic_score
      WHEN day2_demographic_score IS NOT NULL THEN day2_demographic_score
      WHEN day1_demographic_score IS NOT NULL THEN day1_demographic_score
      ELSE 0
    END AS latest_week_demographic_score
  FROM add_campaign
),
-- Obtain the previous week scores
previous_week_scores AS (
  SELECT
    *,
    COALESCE(
      LAG(latest_week_score) OVER (PARTITION BY marketoid ORDER BY week_rank),
      0
    ) AS previous_week_score,
    COALESCE(
      LAG(latest_week_behaviour_score) OVER (PARTITION BY marketoid ORDER BY week_rank),
      0
    ) AS previous_week_behaviour_score,
    COALESCE(
      LAG(latest_week_demographic_score) OVER (PARTITION BY marketoid ORDER BY week_rank),
      0
    ) AS previous_week_demographic_score
  FROM latest_week_scores
),
-- Find change in score between current day and the day before
daily_score_changes AS (
  SELECT
    *,
    -- All differences in lead score
    CASE
      WHEN day1_score IS NULL THEN 0 - previous_week_score
      ELSE day1_score - previous_week_score
    END AS D1_D7_change,
    CASE
      WHEN day1_score IS NULL THEN day2_score - 0
      WHEN day2_score IS NULL THEN 0 - day1_score
      ELSE day2_score - day1_score
    END AS D2_D1_change,
    CASE
      WHEN day2_score IS NULL THEN day3_score - 0
      WHEN day3_score IS NULL THEN 0 - day2_score
      ELSE day3_score - day2_score
    END AS D3_D2_change,
    CASE
      WHEN day3_score IS NULL THEN day4_score - 0
      WHEN day4_score IS NULL THEN 0 - day3_score
      ELSE day4_score - day3_score
    END AS D4_D3_change,
    CASE
      WHEN day4_score IS NULL THEN day5_score - 0
      WHEN day5_score IS NULL THEN 0 - day4_score
      ELSE day5_score - day4_score
    END AS D5_D4_change,
    CASE
      WHEN day5_score IS NULL THEN day6_score - 0
      WHEN day6_score IS NULL THEN 0 - day5_score
      ELSE day6_score - day5_score
    END AS D6_D5_change,
    CASE
      WHEN day6_score IS NULL THEN day7_score - 0
      WHEN day7_score IS NULL THEN 0 - day6_score
      ELSE day7_score - day6_score
    END AS D7_D6_change,
    -- All differences in behaviour scores
    CASE
      WHEN day1_behaviour_score IS NULL THEN 0 - previous_week_behaviour_score
      ELSE day1_behaviour_score - previous_week_behaviour_score
    END AS D1_D7_behaviour_change,
    CASE
      WHEN day1_behaviour_score IS NULL THEN day2_behaviour_score - 0
      WHEN day2_behaviour_score IS NULL THEN 0 - day1_behaviour_score
      ELSE day2_behaviour_score - day1_behaviour_score
    END AS D2_D1_behaviour_change,
    CASE
      WHEN day2_behaviour_score IS NULL THEN day3_behaviour_score - 0
      WHEN day3_behaviour_score IS NULL THEN 0 - day2_behaviour_score
      ELSE day3_behaviour_score - day2_behaviour_score
    END AS D3_D2_behaviour_change,
    CASE
      WHEN day3_behaviour_score IS NULL THEN day4_behaviour_score - 0
      WHEN day4_behaviour_score IS NULL THEN 0 - day3_behaviour_score
      ELSE day4_behaviour_score - day3_behaviour_score
    END AS D4_D3_behaviour_change,
    CASE
      WHEN day4_behaviour_score IS NULL THEN day5_behaviour_score - 0
      WHEN day5_behaviour_score IS NULL THEN 0 - day4_behaviour_score
      ELSE day5_behaviour_score - day4_behaviour_score
    END AS D5_D4_behaviour_change,
    CASE
      WHEN day5_behaviour_score IS NULL THEN day6_behaviour_score - 0
      WHEN day6_behaviour_score IS NULL THEN 0 - day5_behaviour_score
      ELSE day6_behaviour_score - day5_behaviour_score
    END AS D6_D5_behaviour_change,
    CASE
      WHEN day6_behaviour_score IS NULL THEN day7_behaviour_score - 0
      WHEN day7_behaviour_score IS NULL THEN 0 - day6_behaviour_score
      ELSE day7_behaviour_score - day6_behaviour_score
    END AS D7_D6_behaviour_change,
    -- All differences in demographic scores
    CASE
      WHEN day1_demographic_score IS NULL THEN 0 - previous_week_demographic_score
      ELSE day1_demographic_score - previous_week_demographic_score
    END AS D1_D7_demographic_change,
    CASE
      WHEN day1_demographic_score IS NULL THEN day2_demographic_score - 0
      WHEN day2_demographic_score IS NULL THEN 0 - day1_demographic_score
      ELSE day2_demographic_score - day1_demographic_score
    END AS D2_D1_demographic_change,
    CASE
      WHEN day2_demographic_score IS NULL THEN day3_demographic_score - 0
      WHEN day3_demographic_score IS NULL THEN 0 - day2_demographic_score
      ELSE day3_demographic_score - day2_demographic_score
    END AS D3_D2_demographic_change,
    CASE
      WHEN day3_demographic_score IS NULL THEN day4_demographic_score - 0
      WHEN day4_demographic_score IS NULL THEN 0 - day3_demographic_score
      ELSE day4_demographic_score - day3_demographic_score
    END AS D4_D3_demographic_change,
    CASE
      WHEN day4_demographic_score IS NULL THEN day5_demographic_score - 0
      WHEN day5_demographic_score IS NULL THEN 0 - day4_demographic_score
      ELSE day5_demographic_score - day4_demographic_score
    END AS D5_D4_demographic_change,
    CASE
      WHEN day5_demographic_score IS NULL THEN day6_demographic_score - 0
      WHEN day6_demographic_score IS NULL THEN 0 - day5_demographic_score
      ELSE day6_demographic_score - day5_demographic_score
    END AS D6_D5_demographic_change,
    CASE
      WHEN day6_demographic_score IS NULL THEN day7_demographic_score - 0
      WHEN day7_demographic_score IS NULL THEN 0 - day6_demographic_score
      ELSE day7_demographic_score - day6_demographic_score
    END AS D7_D6_demographic_change
  FROM previous_week_scores
),
-- Set the category of the leads
add_category AS (
  SELECT
    *,
    CASE
      WHEN (
        week_rank != 1
        AND sf_lead_status != 'Unqualified'
        AND max_week_score >= 80
        AND previous_week_score < 80
        AND ds03_mql_date IS NOT NULL
      ) THEN 'MQL (Score >= 80)'
      WHEN (
        week_rank != 1
        AND sf_lead_status != 'Unqualified'
        AND (
          max_week_score >= 55
          AND max_week_score < 80
        )
        AND previous_week_score < 55
      ) THEN 'High Intent (55 <= Score < 80)'
      WHEN (
        week_rank != 1
        AND sf_lead_status != 'Unqualified'
        AND (
          max_week_score >= 20
          AND max_week_score < 55
        )
        AND previous_week_score < 20
      ) THEN 'Light Intent (20 <= Score < 55)'
    END AS category
  FROM daily_score_changes
),
-- Set the rank of category for display order in dashboard's filter
add_category_rank AS (
  SELECT
    *,
    CASE
      WHEN category = 'MQL (Score >= 80)' THEN 1
      WHEN category = 'High Intent (55 <= Score < 80)' THEN 2
      WHEN category = 'Light Intent (20 <= Score < 55)' THEN 3
    END AS category_rank
  FROM add_category
)
SELECT
  *
FROM add_category_rank;