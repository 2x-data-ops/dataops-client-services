CREATE OR REPLACE TABLE televerde_broadcom.linkedin_ads_performance AS

WITH LI_ads AS (
  SELECT
    creative_id,
    start_at AS _startDate,
    one_click_leads AS _leads,
    approximate_unique_impressions AS _reach,
    cost_in_usd AS _spent,
    impressions AS _impressions,
    clicks AS _clicks,
    external_website_conversions AS _conversions
  FROM
    `televerde_broadcom_linkedin_ads.ad_analytics_by_creative`
),
ads_title AS (
  SELECT
    SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)]  AS cID,
    campaign_id,
    name AS _ads_name,
    intended_status AS ads_status,
    (CASE
    WHEN name LIKE '%Appneta%' THEN "Appneta"
    WHEN name LIKE '%Automation%' THEN "Automation"
    WHEN name LIKE '%Clarity%' THEN "Clarity"
    WHEN name LIKE '%NetOps%' THEN "NetOps"
    WHEN name LIKE '%Rally%' THEN "Rally"
    ELSE "NULL"
    END) AS solution_area
  FROM
    `televerde_broadcom_linkedin_ads.creatives` c
    LEFT JOIN `televerde_broadcom_linkedin_ads.video_ads` v ON content.reference  = v.content_reference 
    --WHERE SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)]  = '234566963'
),
campaigns AS (
  SELECT
    id AS campaignID,
    name AS _campaignName,
    status AS campaign_status,
    cost_type,
    daily_budget.amount AS dailyBudget,
    campaign_group_id
  FROM
    `televerde_broadcom_linkedin_ads.campaigns`
),
campaign_group AS (
  SELECT
    id AS groupID,
    name AS _groupName,
    status AS campaign_group_status
  FROM
    `televerde_broadcom_linkedin_ads.campaign_groups`
), _all AS (
SELECT
  LI_ads.* EXCEPT (creative_id),
  campaigns.campaignID,
  campaigns._campaignName,
  campaign_group.groupID,
  campaign_group._groupName,
  campaigns.dailyBudget,
  campaigns.cost_type,
  campaigns.campaign_status,
  campaign_group.campaign_group_status,
  cID AS creative_id,
  _ads_name,
  ads_status,
  solution_area
FROM
  LI_ads
RIGHT JOIN
 ads_title
ON
CAST( LI_ads.creative_id AS STRING) = ads_title.cID
JOIN
  campaigns
ON
  ads_title.campaign_id = campaigns.campaignID
JOIN
  campaign_group
ON
  campaigns.campaign_group_id = campaign_group.groupID
), total_ads AS (
  SELECT *, 
  count(creative_id) OVER (PARTITION BY _startDate, _campaignName ) AS ads_per_campaign
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