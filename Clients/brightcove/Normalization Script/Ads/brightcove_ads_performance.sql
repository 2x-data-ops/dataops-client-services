-- LinkedIn Ads Perfomance
CREATE OR REPLACE TABLE brightcove.linkedin_ads_performance AS

WITH LI_ads AS (
 SELECT
    n.date_range.start.year AS start_year, 
    n.date_range.start.month AS start_month, 
    n.date_range.start.day AS start_day,
    n.date_range.end.month AS end_month,
     n.date_range.end.year AS end_year, 
     n.date_range.end.day AS end_day,
     LAST_DAY( CAST(n.start_at AS DATE) ,WEEK(MONDAY)) AS last_start_day,
     TIMESTAMP_TRUNC(n.start_at, WEEK(MONDAY), 'UTC') AS start_week,
     TIMESTAMP_TRUNC(n.start_at, QUARTER, 'UTC') AS start_quater,
     TIMESTAMP_TRUNC(n.start_at, MONTH, 'UTC') AS start_month_num,
     FORMAT_DATETIME('%A', n.start_at) AS weekday,
     FORMAT_DATE('%B', n.start_at) AS start_month_name,
     EXTRACT(WEEK FROM n.start_at) AS start_week_num,
      EXTRACT(DATE FROM n.start_at) AS _date,
    DATETIME (n.start_at, "America/New_York") AS _estdate,
    CONCAT('Q',EXTRACT(QUARTER FROM n.start_at),'-',EXTRACT(YEAR FROM n.start_at) ) AS _quater_startdate,
    n.creative_id,
    n.start_at AS _startDate,
    n.end_at AS _endDate,
    n.one_click_leads AS _leads,
    n.approximate_unique_impressions AS _reach,
    n.cost_in_usd AS _spent,
    CASE WHEN n.one_click_leads = 0 THEN 0 ELSE n.cost_in_usd END AS _newspent,
    n.impressions AS _impressions,
    n.clicks AS _clicks,
    n.external_website_conversions AS _conversions,
    n.video_views AS _video_views,
    n.one_click_lead_form_opens AS _lead_form_opens,
    n.video_starts AS _video_play,
    n.video_first_quartile_completions AS _video_views_25percent,
    n.video_midpoint_completions AS _video_views_50percent,
    n.video_third_quartile_completions AS _video_views_75percent,
    n.video_completions AS _video_completions
  FROM
    `brightcove_linkedin_ads_v2.ad_analytics_by_creative` n
    --WHERE creative_id IN (302039724, 302066004, 302498364, 302551374)
    ORDER BY n.start_at DESC
),
ads_title AS (
  SELECT
    SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] AS cID,
    campaign_id
  FROM
    `brightcove_linkedin_ads_v2.creatives`
    --WHERE SPLIT(SUBSTR(id, STRPOS(id, 'sponsoredCreative:')+18))[ORDINAL(1)] IN ('302542594')
),
campaigns AS (
  SELECT
    id AS campaignID,
    name AS _campaignNames,
    status,
    cost_type,
    daily_budget.amount AS dailyBudget,
    campaign_group_id,
    account AS _account_name,
    account_id
  FROM
    `brightcove_linkedin_ads_v2.campaigns`
),
campaign_group AS (
  SELECT
    id AS groupID,
    name AS _groupName,
    status
  FROM
    `brightcove_linkedin_ads_v2.campaign_groups`
),
 airtable_ads AS (
    SELECT 
    * 
    EXCEPT(_sdc_table_version,_sdc_received_at,_sdc_sequence,_sdc_batched_at) 
    FROM `x-marketing.brightcove_mysql.db_airtable_linkedin_creative` 
), _all AS (
SELECT
airtable_ads.*EXCEPT(_adid), 
  LI_ads.*,
  campaigns.campaignID,
  campaigns._campaignNames,
  campaign_group.groupID,
  campaign_group._groupName,
  campaigns.dailyBudget,
  campaigns.cost_type,
  campaign_group.status
FROM
  LI_ads
RIGHT JOIN
  ads_title
ON
  CAST(LI_ads.creative_id AS STRING) = ads_title.cID
LEFT JOIN
  campaigns
ON
  ads_title.campaign_id = campaigns.campaignID
LEFT JOIN
  campaign_group
ON
  campaigns.campaign_group_id = campaign_group.groupID

LEFT JOIN airtable_ads 
ON 
CAST(LI_ads.creative_id AS STRING) = airtable_ads._adid
), total_ads AS (
  SELECT *, count(creative_id) OVER (PARTITION BY _startDate, _campaignNames ) AS ads_per_campaign
  FROM _all
)
, daily_budget_per_ad_per_campaign AS (
  SELECT *,
          CASE WHEN ads_per_campaign > 0 THEN dailyBudget / ads_per_campaign
        ELSE 0 
        END
         AS dailyBudget_per_ad,
         CASE WHEN _impressions > 0 then SUM(_video_views) / SUM(_impressions) ELSE 0 END AS _videoviewrate,
         
  FROM total_ads
  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64
) SELECT *,CASE WHEN _leads > 0 THEN SUM(_spent)/SUM(_leads) ELSE 0 END AS cpl
 FROM daily_budget_per_ad_per_campaign
 --WHERE _campaignname = 'SI - WEB - AMER_NA - CM - ALL - PO#3597 - ALL'
 --WHERE _quarter = 'Q4 - 2023'
 GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66;


--- Google Search Campaign Performance ---
CREATE OR REPLACE TABLE brightcove.google_search_campaign_performance AS
WITH unique_rows AS (
  SELECT * EXCEPT(_rank)
    FROM (
        SELECT
            campaign_id, 
            campaign_name, 
            date AS day, 
            customer_descriptive_name AS company_name,
            customer_currency_code AS currency,
            campaign_budget_amount_micros/1000000 AS budget, 
            cost_micros/1000000 AS cost, 
            search_rank_lost_absolute_top_impression_share,
            search_impression_share,
            search_rank_lost_top_impression_share,
            impressions, 
            CASE
                WHEN ad_network_type = 'SEARCH'
                THEN impressions
                ELSE NULL
            END search_impressions,
            clicks, 
            absolute_top_impression_percentage * impressions AS abs_top_impr,
            conversions, 
            view_through_conversions AS view_through_conv,
            all_conversions AS all_conversions,
            campaign_advertising_channel_type AS campaign_advertising_channel_type,
            campaign_status AS status,
            RANK() OVER(
                PARTITION BY date, campaign_id
                ORDER BY report._sdc_received_at DESC
            ) AS _rank
        FROM `x-marketing.brightcove_google_ads.campaign_performance_report` report
        WHERE campaign_advertising_channel_type = 'SEARCH'
        AND customer_id IN (6547715998, 5459393415, 3144549837, 5115763345, 2865583180)
        -- AND campaign_name LIKE 'US | %'
    )
    WHERE _rank = 1
),
aggregate_rows AS (
    SELECT
        campaign_id, 
        campaign_name, 
        day,
        company_name,
        currency,
        budget,
        campaign_advertising_channel_type,
        status,
        SUM(search_rank_lost_absolute_top_impression_share) AS search_rank_lost_absolute_top_impression_share,
        SUM(search_impression_share) AS search_impression_share,
        SUM(search_rank_lost_top_impression_share) AS search_rank_lost_top_impression_share ,
        SUM(cost) AS cost,
        SUM(impressions) AS impressions,
        SUM(search_impressions) AS search_impressions,
        SUM(abs_top_impr) AS abs_top_impr_value,
        SUM(clicks) AS clicks,
        SUM(conversions) AS conversions,
        SUM(view_through_conv) AS view_through_conv,
        SUM(all_conversions) AS all_conversions
    FROM unique_rows
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),
add_calculated_columns AS (
    SELECT
        * EXCEPT(search_impressions, abs_top_impr_value),
        CASE 
            WHEN impressions = 0 
            THEN NULL
            ELSE clicks / impressions 
        END AS ctr,
        CASE
            WHEN clicks = 0
            THEN NULL 
            ELSE cost / clicks 
        END AS avg_cpc,
        CASE
            WHEN impressions = 0
            THEN NULL
            ELSE abs_top_impr_value / search_impressions
        END AS abs_top_impr,
        CASE 
            WHEN clicks = 0 
            THEN NULL
            ELSE conversions / clicks 
        END AS conv_rate
    FROM aggregate_rows
)
SELECT add_calculated_columns.*
--airtable_ads.* EXCEPT(campaign_id) 
FROM add_calculated_columns
--JOIN airtable_ads ON add_calculated_columns.campaign_id = airtable_ads.campaign_id
ORDER BY day, campaign_id;


--- Google Search Ads Variation Performance ---
CREATE OR REPLACE TABLE brightcove.google_search_adsvariation_performance AS
WITH unique_rows AS (
    SELECT * EXCEPT(_rank)
    FROM (
        SELECT 
            campaign_id,
            campaign_name, 
            ad_group_id, 
            ad_group_name, 
            customer_descriptive_name AS company_name,
            date AS day, 
            id AS ad_id, 
            CASE 
                WHEN type = 'RESPONSIVE_SEARCH_AD'
                THEN REPLACE(ARRAY_TO_STRING(REGEXP_EXTRACT_ALL(responsive_search_ad.headlines, "'text': '[^']*"), "\n"), "'text': '", "")
            END AS headlines,
            --responsive_search_ad.headlines AS headlines,
            final_urls,
            customer_currency_code AS currency, 
            cost_micros/1000000 AS cost, 
            impressions, 
            CASE
                WHEN ad_network_type = 'SEARCH'
                THEN impressions
                ELSE NULL
            END search_impressions,
            clicks, 
            absolute_top_impression_percentage * impressions AS abs_top_impr, 
            conversions, 
            view_through_conversions AS view_through_conv,
            RANK() OVER(
                PARTITION BY date, campaign_id, ad_group_id, id
                ORDER BY ads._sdc_received_at DESC
            ) _rank
        FROM `x-marketing.brightcove_google_ads.ad_performance_report` ads
        WHERE customer_id IN (6547715998, 5459393415, 3144549837, 5115763345, 2865583180)
        -- AND campaign_name LIKE 'US | %'
    )
    WHERE _rank = 1
),
aggregate_rows AS (
    SELECT
        campaign_id,
        campaign_name, 
        ad_group_id, 
        ad_group_name, 
        company_name,
        day,
        ad_id,
        headlines,
        final_urls,
        currency,
        SUM(cost) AS cost,
        SUM(impressions) AS impressions,
        SUM(search_impressions) AS search_impressions,
        SUM(abs_top_impr) AS abs_top_impr_value,
        SUM(clicks) AS clicks,
        SUM(conversions) AS conversions,
        SUM(view_through_conv) AS view_through_conv
    FROM unique_rows
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),
add_calculated_columns AS (
    SELECT
        * EXCEPT(search_impressions, abs_top_impr_value),
        CASE 
            WHEN impressions = 0 
            THEN NULL
            ELSE clicks / impressions 
        END AS ctr,
        CASE
            WHEN clicks = 0
            THEN NULL 
            ELSE cost / clicks 
        END AS avg_cpc,
        CASE
            WHEN impressions = 0
            THEN NULL
            ELSE abs_top_impr_value / search_impressions
        END AS abs_top_impr,
        CASE 
            WHEN clicks = 0 
            THEN NULL
            ELSE conversions / clicks 
        END AS conv_rate
    FROM aggregate_rows
)
SELECT add_calculated_columns.*
--airtable_ads.* EXCEPT(campaign_id,ad_group_id)
FROM add_calculated_columns
--JOIN airtable_ads ON add_calculated_columns.campaign_id = airtable_ads.campaign_id
ORDER BY day, campaign_id, ad_group_id, ad_id;


-- Google Search Keyword Performance
CREATE OR REPLACE TABLE brightcove.google_search_keyword_performance AS
WITH unique_rows AS (
    SELECT * EXCEPT(_rank) 
    FROM (
        SELECT
            campaign_id, 
            campaign_name, 
            ad_group_id, 
            ad_group_name, 
            ad_group_criterion_keyword.match_type AS match_type, 
            ad_group_criterion_keyword.text AS keyword, 
            ad_group_criterion_quality_info.quality_score AS quality_score,
            customer_descriptive_name AS company_name,
            date AS day, 
            customer_currency_code AS currency, 
            cost_micros/1000000 AS cost, 
            impressions, 
            CASE
                WHEN ad_network_type = 'SEARCH'
                THEN impressions
                ELSE NULL
            END search_impressions,
            clicks, 
            absolute_top_impression_percentage * impressions AS abs_top_impr, 
            conversions, 
            view_through_conversions AS view_through_conv,
            search_rank_lost_absolute_top_impression_share,
            search_impression_share,
            search_rank_lost_top_impression_share,
            RANK() OVER (
                PARTITION BY date, campaign_id, ad_group_id, ad_group_criterion_keyword.text
                ORDER BY keywords._sdc_received_at DESC
            ) AS _rank           
        FROM `x-marketing.brightcove_google_ads.keywords_performance_report` keywords
        WHERE customer_id IN (6547715998, 5459393415, 3144549837, 5115763345, 2865583180)
        -- AND campaign_name LIKE 'US | %'
    )
    WHERE _rank = 1
),
aggregate_rows AS (
    SELECT
        campaign_id, 
        campaign_name, 
        ad_group_id, 
        ad_group_name, 
        match_type, 
        keyword, 
        quality_score,
        company_name,
        day,
        currency,
        SUM(search_rank_lost_absolute_top_impression_share) AS search_rank_lost_absolute_top_impression_share,
        SUM(search_impression_share) AS search_impression_share,
        SUM(search_rank_lost_top_impression_share) AS search_rank_lost_top_impression_share ,
        SUM(cost) AS cost,
        SUM(impressions) AS impressions,
        SUM(search_impressions) AS search_impressions,
        SUM(abs_top_impr) AS abs_top_impr_value,
        SUM(clicks) AS clicks,
        SUM(conversions) AS conversions,
        SUM(view_through_conv) AS view_through_conv
    FROM unique_rows
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
),
add_calculated_columns AS (
    SELECT
        * EXCEPT(search_impressions, abs_top_impr_value),
        CASE 
            WHEN impressions = 0 
            THEN NULL
            ELSE clicks / impressions 
        END AS ctr,
        CASE
            WHEN clicks = 0
            THEN NULL 
            ELSE cost / clicks 
        END AS avg_cpc,
        CASE
            WHEN impressions = 0
            THEN NULL
            ELSE abs_top_impr_value / search_impressions
        END AS abs_top_impr,
        CASE 
            WHEN clicks = 0 
            THEN NULL
            ELSE conversions / clicks 
        END AS conv_rate
    FROM aggregate_rows
)
SELECT add_calculated_columns.*
--airtable_ads.* EXCEPT(campaign_id,ad_group_id)
FROM add_calculated_columns
--JOIN airtable_ads ON add_calculated_columns.campaign_id = airtable_ads.campaign_id
ORDER BY day, campaign_id, ad_group_id, keyword;


-- Google Search Query Performance
CREATE OR REPLACE TABLE brightcove.google_search_query_performance AS
WITH unique_rows AS (
    SELECT * EXCEPT(_rank) 
    FROM (
        SELECT
        campaign_id, 
        campaign_name, 
        ad_group_id,
        ad_group_name, 
        keyword.info.text AS keyword, 
        --search_term_match_type AS match_type, 
        search_term_view_search_term AS search_term,
        customer_descriptive_name AS company_name,
        date AS day, 
        customer_currency_code AS currency, 
        cost_micros/1000000 AS cost, 
        impressions, 
        CASE
            WHEN ad_network_type = 'SEARCH'
            THEN impressions
            ELSE NULL
        END search_impressions,
        clicks, 
        absolute_top_impression_percentage * impressions AS abs_top_impr, 
        conversions, 
        view_through_conversions AS view_through_conv, 
        RANK() OVER( 
            PARTITION BY date, campaign_id, ad_group_id, keyword.info.text, search_term_view_search_term
            ORDER BY _sdc_received_at DESC
        ) AS _rank
        FROM `x-marketing.brightcove_google_ads.search_query_performance_report` query
        WHERE customer_id IN (6547715998, 5459393415, 3144549837, 5115763345, 2865583180)
        -- AND campaign_name LIKE 'US | %'
    )
    WHERE _rank = 1
),
aggregate_rows AS (
    SELECT
        campaign_id, 
        campaign_name, 
        ad_group_id, 
        ad_group_name,  
        keyword,
        --match_type, 
        search_term,
        company_name,
        day,
        currency,
        SUM(cost) AS cost,
        SUM(impressions) AS impressions,
        SUM(search_impressions) AS search_impressions,
        SUM(abs_top_impr) AS abs_top_impr_value,
        SUM(clicks) AS clicks,
        SUM(conversions) AS conversions,
        SUM(view_through_conv) AS view_through_conv
    FROM unique_rows
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),
add_calculated_columns AS (
    SELECT
        * EXCEPT(search_impressions, abs_top_impr_value),
        CASE 
            WHEN impressions = 0 
            THEN NULL
            ELSE clicks / impressions 
        END AS ctr,
        CASE
            WHEN clicks = 0
            THEN NULL 
            ELSE cost / clicks 
        END AS avg_cpc,
        CASE
            WHEN impressions = 0
            THEN NULL
            ELSE abs_top_impr_value / search_impressions
        END AS abs_top_impr,
        CASE 
            WHEN clicks = 0 
            THEN NULL
            ELSE conversions / clicks 
        END AS conv_rate
    FROM aggregate_rows
)
SELECT add_calculated_columns.*
--airtable_ads.* EXCEPT(campaign_id,ad_group_id)
FROM add_calculated_columns
--JOIN airtable_ads ON add_calculated_columns.campaign_id = airtable_ads.campaign_id
ORDER BY day, campaign_id, ad_group_id, keyword, search_term;


--Google Display
CREATE OR REPLACE TABLE brightcove.google_display_campaign_performance AS
WITH unique_rows AS (
  SELECT * EXCEPT(_rank)
  FROM (
    SELECT
      campaign_id,
      campaign_name,
      date AS day,
      customer_currency_code AS currency,
      campaign_budget_amount_micros/1000000 AS budget,
      cost_micros/1000000 AS cost,
      impressions,
      CASE
        WHEN ad_network_type = 'DISPLAY' THEN impressions
        ELSE NULL
      END AS search_impressions,
      clicks,
      absolute_top_impression_percentage * impressions AS abs_top_impr,
      conversions,
      view_through_conversions AS view_through_conv,
      campaign_status,
      RANK() OVER(
        PARTITION BY date, campaign_id
        ORDER BY report._sdc_received_at DESC
      ) AS _rank
    FROM 
      `brightcove_google_ads.campaign_performance_report` report
    WHERE
      campaign_advertising_channel_type = 'DISPLAY'
      AND customer_id IN (6547715998, 5459393415, 3144549837, 5115763345, 2865583180)
  )
  WHERE _rank = 1
),
aggregate_rows AS (
  SELECT
    campaign_id,
    campaign_name,
    day,
    currency,
    budget,
    campaign_status,
    SUM(cost) AS cost,
    SUM(impressions) AS impressions,
    SUM(search_impressions) AS search_impressions,
    SUM(abs_top_impr) AS abs_top_impr_value,
    SUM(clicks) AS clicks,
    SUM(conversions) AS conversions,
    SUM(view_through_conv) AS view_through_conv
  FROM
    unique_rows
  GROUP BY
    1, 2, 3, 4, 5,6
),
add_calculated_columns AS (
  SELECT
    * EXCEPT(search_impressions, abs_top_impr_value),
    CASE
      WHEN impressions = 0 THEN NULL
      ELSE clicks / impressions
    END AS ctr,
    CASE
      WHEN clicks = 0 THEN NULL
      ELSE cost / clicks
    END AS avg_cpc,
    CASE
      WHEN impressions = 0 THEN NULL
      ELSE abs_top_impr_value / search_impressions
    END AS abs_top_impr,
    CASE
      WHEN clicks = 0 THEN NULL
      ELSE conversions / clicks
    END AS conv_rate
  FROM
    aggregate_rows
) /* ,
airtable_ads AS (
    SELECT 
    * 
    EXCEPT(_sdc_table_version,_sdc_received_at,_sdc_sequence,_sdc_batched_at) 
    FROM `x-marketing.brightcove_mysql.db_airtable_google_display_ads` 
) */
SELECT add_calculated_columns.* ,
-- airtable_ads.* EXCEPT(_campaignid,_campaignname)
FROM add_calculated_columns
-- JOIN airtable_ads ON CAST(add_calculated_columns.campaign_id AS STRING) = airtable_ads._campaignid
ORDER BY day, campaign_id;


CREATE OR REPLACE TABLE brightcove.linkedin_account_reached AS
SELECT * EXCEPT (rownum) 
FROM (
SELECT 
CASE WHEN _exportdate ="" THEN NULL WHEN _exportdate LIKE '%-%' THEN PARSE_DATE('%e-%b-%y',_exportdate) ELSE PARSE_DATE('%m/%e/%Y', _exportdate) END AS _startDate,
EXTRACT(DAY FROM CASE WHEN _exportdate ="" THEN NULL WHEN _exportdate LIKE '%-%' THEN PARSE_DATE('%e-%b-%y',_exportdate) ELSE PARSE_DATE('%m/%e/%Y', _exportdate) END) AS day_daily,
EXTRACT(WEEK FROM CASE WHEN _exportdate ="" THEN NULL WHEN _exportdate LIKE '%-%' THEN PARSE_DATE('%e-%b-%y',_exportdate) ELSE PARSE_DATE('%m/%e/%Y', _exportdate)END) AS weeks_daily,
EXTRACT(MONTH FROM CASE WHEN _exportdate ="" THEN NULL WHEN _exportdate LIKE '%-%' THEN PARSE_DATE('%e-%b-%y',_exportdate) ELSE PARSE_DATE('%m/%e/%Y', _exportdate) END) AS month_daily,
CONCAT('Q',EXTRACT(QUARTER FROM CASE WHEN _exportdate ="" THEN NULL WHEN _exportdate LIKE '%-%' THEN PARSE_DATE('%e-%b-%y',_exportdate) ELSE PARSE_DATE('%m/%e/%Y', _exportdate)END),' - ',EXTRACT(YEAR FROM CASE WHEN _exportdate ="" THEN NULL WHEN _exportdate LIKE '%-%' THEN PARSE_DATE('%e-%b-%y',_exportdate) ELSE PARSE_DATE('%m/%e/%Y', _exportdate)END)) AS _quater_startdate,
j.*,
_companynamesegment AS accountName,
k.campaignID,
CASE WHEN _impressions = "" THEN 0 ELSE CAST(REPLACE(_impressions, ',', '') AS NUMERIC) END AS impressions,
CASE WHEN _percentoftotalimpressions = "" THEN 0 ELSE CAST(REPLACE(_percentoftotalimpressions, '%', '') AS DECIMAL) / 100 END AS _percentoftotalimpressions,
CASE WHEN _clicks = '-' THEN 0 WHEN _clicks = "" THEN 0 ELSE CAST(l._clicks AS NUMERIC) END AS _clicks, 
CASE WHEN _percentoftotalclicks = '-' THEN 0 WHEN _percentoftotalclicks = "" THEN 0 ELSE SAFE_CAST(REPLACE(_percentoftotalclicks, '%', '') AS NUMERIC)/100 END AS _percentoftotalclicks,
CASE WHEN _clickthroughrate = '-' THEN 0 WHEN _clickthroughrate = "" THEN 0 WHEN _clickthroughrate = "###" THEN 0 ELSE CAST(REPLACE(_clickthroughrate, '%', '') AS DECIMAL) / 100 END AS _clickthroughrate,
ROW_NUMBER() OVER( PARTITION BY _companynamesegment,l._campaignid,CONCAT('Q',EXTRACT(QUARTER FROM CASE WHEN _exportdate ="" THEN NULL WHEN _exportdate LIKE '%-%' THEN PARSE_DATE('%e-%b-%y',_exportdate) ELSE PARSE_DATE('%m/%e/%Y', _exportdate)END),' - ',EXTRACT(YEAR FROM CASE WHEN _exportdate ="" THEN NULL WHEN _exportdate LIKE '%-%' THEN PARSE_DATE('%e-%b-%y',_exportdate) ELSE PARSE_DATE('%m/%e/%Y', _exportdate)END))  ORDER BY CASE WHEN _exportdate ="" THEN NULL WHEN _exportdate LIKE '%-%' THEN PARSE_DATE('%e-%b-%y',_exportdate) ELSE PARSE_DATE('%m/%e/%Y', _exportdate) END DESC) AS rownum
FROM `x-marketing.brightcove_mysql.db_account_reached_linkedin` l
LEFT JOIN (SELECT DISTINCT id AS campaignID,name AS _campaignname FROM `x-marketing.brightcove_linkedin_ads.campaigns` ) k ON l._campaignid = CAST(k.campaignID AS STRING)
LEFT JOIN (  SELECT DISTINCT _type, _campaignid, _campaignname, _quarter /*_segment,*/
 FROM `x-marketing.brightcove_mysql.db_airtable_linkedin_creative` ) j ON l._campaignid = j._campaignid
 WHERE _sdc_deleted_at IS NULL AND CASE WHEN _exportdate ="" THEN NULL WHEN _exportdate LIKE '%-%' THEN PARSE_DATE('%e-%b-%y',_exportdate) ELSE PARSE_DATE('%m/%e/%Y', _exportdate) END IN (
  SELECT MAX(CASE WHEN _exportdate ="" THEN NULL WHEN _exportdate LIKE '%-%' THEN PARSE_DATE('%e-%b-%y',_exportdate) ELSE PARSE_DATE('%m/%e/%Y', _exportdate) END) FROM `x-marketing.brightcove_mysql.db_account_reached_linkedin`
GROUP BY (CASE WHEN _exportdate ="" THEN NULL WHEN _exportdate LIKE '%-%' THEN PARSE_DATE('%e-%b-%y',_exportdate) ELSE PARSE_DATE('%m/%e/%Y', _exportdate)END)
 )
)
WHERE rownum = 1;




CREATE OR REPLACE TABLE brightcove.db_account_engagements AS 
WITH tam_contacts AS (
      SELECT * EXCEPT(_rownum) 
  FROM (
    SELECT DISTINCT
        '' AS _firstname, 
        '' AS _lastname, 
        '' AS _title, 
        COALESCE(null, CAST(NULL AS STRING)) AS _2xseniority,
        '' AS _email, 
        '' AS _accountid,
        domain  AS _domain, 
        company AS _accountname, 
        '' AS _industry, 
        COALESCE(null, CAST(NULL AS STRING)) AS _tier,
        '' AS status, 
        CAST(NUll AS INTEGER) AS _annualrevenue,
        ROW_NUMBER() OVER(
            PARTITION BY company 
            ORDER BY extractDate DESC
        ) _rownum
    FROM 
     `x-marketing.brightcove_6sense.db_campaign_accounts` main
     UNION ALL 
        SELECT DISTINCT
        '' AS _firstname, 
        '' AS _lastname, 
        '' AS _title, 
        COALESCE(null, CAST(NULL AS STRING)) AS _2xseniority,
        '' AS _email, 
        '' AS _accountid,
        case when domain is null then accountName ELSE domain END  AS _domain, 
        accountName AS _accountname, 
        '' AS _industry, 
        COALESCE(null, CAST(NULL AS STRING)) AS _tier,
        '' AS status, 
        CAST(NUll AS INTEGER) AS _annualrevenue,
        ROW_NUMBER() OVER(
            PARTITION BY accountName 
            ORDER BY _startDate DESC
        ) _rownum
    FROM 
     `x-marketing.brightcove.linkedin_account_reached` main
     LEFT JOIN `x-marketing.brightcove_6sense.db_campaign_accounts` l ON main.accountName = l.company

  )
  WHERE _rownum = 1
)
,web_views AS (
SELECT company AS _email, domain AS _domain, CAST(extractDate AS TIMESTAMP) AS _date, 
    EXTRACT(WEEK FROM extractDate) AS _week,  
    EXTRACT(YEAR FROM extractDate) AS _year,    
    '' AS _pageName, 
    "Web Visit"AS _engagement, 
 CAST(websiteEngagement AS STRING) AS _description 
FROM `x-marketing.brightcove_6sense.db_campaign_accounts`
WHERE websiteEngagement IN ('New','Increased')
)
,ad_clicks AS (
    SELECT 
    accountName AS _email,
    case when domain is null then accountName ELSE domain END  AS _domain, 
    CAST(_startDate AS TIMESTAMP) AS _date,   
      
    EXTRACT(WEEK FROM _startDate) AS _week,  
    EXTRACT(YEAR FROM _startDate) AS _year,    
    '' AS _pageName, 
    "Ad Clicks" AS _engagement, 
    CAST(_clicks AS STRING) AS _description 
    FROM `x-marketing.brightcove.linkedin_account_reached` main
    LEFT JOIN `x-marketing.brightcove_6sense.db_campaign_accounts` l ON main.accountName = l.company
    UNION ALL 
    SELECT company, domain, CAST(extractDate AS TIMESTAMP) AS _date, 
    EXTRACT(WEEK FROM extractDate) AS _week,  
    EXTRACT(YEAR FROM extractDate) AS _year,    
    '' AS _pageName, 
    "Ad Clicks" AS _engagement, 
 CAST(clicks AS STRING) AS _description 
FROM `x-marketing.brightcove_6sense.db_campaign_accounts`
WHERE clicks >0
)
,dummy_dates AS ( # Each domain needs to be shown regardless if they are part of the bombora report or has 0 engagements
  SELECT
    _date,
    EXTRACT(WEEK FROM _date) AS _week,
    EXTRACT(YEAR FROM _date) AS _year
  FROM 
    UNNEST(GENERATE_DATE_ARRAY('2022-01-01', CURRENT_DATE(), INTERVAL 1 DAY)) AS _date 
) 
,
account_engagement AS (
#Account based engagement query
   SELECT 
    DISTINCT 
    tam_accounts._domain, 
    CAST(NULL AS STRING) AS _email,
    dummy_dates.*EXCEPT(_date), 
    engagements.*EXCEPT(_date, _week, _year, _domain, _email),
    CAST(NULL AS STRING) AS _id, 
    CAST(NULL AS STRING) AS _contact_type,
    CAST(NULL AS STRING) AS _firstname, 
    CAST(NULL AS STRING) AS _lastname,
    CAST(NULL AS STRING) AS _title,
    CAST(NULL AS STRING) AS _2xseniority,
    tam_accounts.*EXCEPT(_domain),
    CAST(engagements._date AS DATETIME) AS _date
  FROM 
    dummy_dates
  JOIN (
    /* SELECT * FROM intent_score UNION ALL */
    SELECT * FROM web_views 
    UNION ALL
    SELECT * FROM ad_clicks 
  ) engagements USING(_week, _year)
  JOIN
    (
      SELECT 
        DISTINCT _domain, 
        _accountid, 
        _accountname, 
        _industry, 
        _tier, 
        _annualrevenue 
      FROM 
        tam_contacts
    ) tam_accounts
    USING(_domain)
)
,
combined_engagements AS (

  SELECT * FROM account_engagement
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
ORDER BY _date DESC;
--Limit 1




--reached
CREATE OR REPLACE TABLE `x-marketing.brightcove.ads_performance_sheet` AS
SELECT exported_date AS _date,quarter AS _quarter,user_reached AS _reach,segment AS _segment,campaign_name AS _campaignname, campaign_id AS _campaignid, region AS _region, type_2 AS _type,impressions, total_spent,leads,CASE WHEN leads > 0 THEN total_spent ELSE 0 END AS _newspent FROM `x-marketing.brightcove_google_sheets.Overall`;


--CPL
CREATE OR REPLACE TABLE `x-marketing.brightcove.linkedin_campaign_aggregate` AS
WITH agg_spent AS (
SELECT 
FORMAT_DATE('%Y-%m-%d', PARSE_DATE('%d-%b-%y', _exportdate)) AS _date,
_campaignid,_campaignname,_quarter, _segment,_region,_type,SUM(_leads) AS total_leads,SUM(_spent) AS total_spent
FROM `x-marketing.brightcove.linkedin_ads_performance`
WHERE _campaignid IS NOT NULL
GROUP BY 1,2,3,4,5,6,7
)
SELECT *, CASE WHEN total_leads > 0 THEN total_spent ELSE NULL END AS _newspent
FROM agg_spent;