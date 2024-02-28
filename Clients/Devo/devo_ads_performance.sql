CREATE OR REPLACE TABLE `devo.linkedin_ads_performance` AS

WITH LI_ads AS (
  SELECT
    creative_id,
    start_at AS _startDate,
    one_click_leads AS _leads,
    card_impressions AS _reach,
    cost_in_usd AS _spent,
    impressions AS _impressions,
    clicks AS _clicks,
    external_website_conversions AS _conversions
  FROM
    `devo_linkedin_ads.ad_analytics_by_creative`
),
ads_title AS (
  SELECT
    SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS cID,
    campaign_id
  FROM
    `devo_linkedin_ads.creatives`
),
campaigns AS (
  SELECT
    id AS campaignID,
    name AS _campaignName,
    status,
    cost_type,
    daily_budget.amount AS dailyBudget,
    campaign_group_id
  FROM
    `devo_linkedin_ads.campaigns`
),
-- campaign_group AS (
--   SELECT
--     id AS groupID,
--     name AS _groupName,
--     status
--   FROM
--     `devo_linkedin_ads.campaign_groups`
-- ),
_all AS (
SELECT
  LI_ads.*,
  campaigns.campaignID,
  campaigns._campaignName,
  -- campaign_group.groupID,
  -- campaign_group._groupName,
  campaigns.dailyBudget,
  campaigns.cost_type,
  -- campaign_group.status
FROM
  LI_ads
RIGHT JOIN
  ads_title
ON
  CAST(LI_ads.creative_id AS STRING) = ads_title.cID
JOIN
  campaigns
ON
  ads_title.campaign_id = campaigns.campaignID
-- JOIN
--   campaign_group
-- ON
--   campaigns.campaign_group_id = campaign_group.groupID
), total_ads AS (
  SELECT *, count(creative_id) OVER (PARTITION BY _startDate, _campaignName ) AS ads_per_campaign
  FROM _all
)
, daily_budget_per_ad_per_campaign AS (
  SELECT *,
          CASE WHEN ads_per_campaign > 0 THEN dailyBudget / ads_per_campaign
        ELSE 0 
        END
         AS dailyBudget_per_ad
  FROM total_ads
) SELECT * FROM daily_budget_per_ad_per_campaign;