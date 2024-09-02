--CREATE OR REPLACE TABLE `x-marketing.masttro.google_search_keyword_performance` AS
TRUNCATE TABLE `x-marketing.masttro.google_search_keyword_performance`;

INSERT INTO `x-marketing.masttro.google_search_keyword_performance` (
    _campaignID,
    _campaignName,
    _date,
    _keywords,
    _ad_group_criterion_status,
    _match_type,
    _total_cost_keywords
)
WITH unique_rows AS (
    SELECT
      campaign_id AS _campaignID,
      campaign.name AS _campaignName,
      CAST(date AS DATE) AS _date,
      ad_group_criterion_keyword.text AS _keywords,
      INITCAP(ad_group_criterion_status) AS _ad_group_criterion_status,
      INITCAP(ad_group_criterion_keyword.match_type) AS _match_type,
      cost_micros / 1000000 AS cost,
    FROM `x-marketing.masttro_google_ads.keywords_performance_report` report
    LEFT JOIN `x-marketing.masttro_google_ads.campaigns` campaign
        ON report.campaign_id = campaign.id
    QUALIFY RANK() OVER (
        PARTITION BY
          campaign_id,
          campaign.name,
          date,
          ad_group_criterion_keyword.text
        ORDER BY
          report._sdc_received_at DESC
    ) = 1
)
SELECT
  _campaignID,
  _campaignName,
  _date,
  _keywords,
  _ad_group_criterion_status,
  _match_type,
  SUM(cost) AS _total_cost_keywords
FROM unique_rows
GROUP BY
  1,
  2,
  3,
  4,
  5,
  6
ORDER BY
  _date,
  _campaignID,
  _keywords;