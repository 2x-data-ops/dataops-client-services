

--CREATE OR REPLACE TABLE `x-marketing.thelogicfactory.account_90days_score` AS 
-- Declaring the loop index and variables for the min & max date
DECLARE index INT64 DEFAULT 0;
DECLARE date_ranges ARRAY<STRUCT<max_date DATE, min_date DATE>>;

DELETE FROM `x-marketing.thelogicfactory.account_90days_score` WHERE _domain IS NOT NULL;
-- Creating the date range array 
SET date_ranges = ARRAY(
  SELECT AS STRUCT
    DATE_TRUNC(_date, DAY) AS max_date, DATE_SUB(DATE_TRUNC(_date, DAY), INTERVAL 180 DAY) AS min_date
  FROM 
    UNNEST(GENERATE_DATE_ARRAY(CURRENT_DATE(), '2022-01-01', INTERVAL -1 DAY)) AS _date 
  ORDER BY 
    1 DESC
);
-- Start of account scoring backfill (loop)
LOOP
  IF index = array_length(date_ranges) 
    -- Breaks when stored index reached
    THEN BREAK;
  END IF;
  BEGIN
    DECLARE date_end DATE DEFAULT date_ranges[OFFSET(index)].max_date;
    DECLARE date_start DATE DEFAULT date_ranges[OFFSET(index)].min_date;
    
    INSERT INTO `x-marketing.thelogicfactory.account_90days_score`
WITH all_accounts AS (
        SELECT DISTINCT _domain FROM `x-marketing.thelogicfactory.db_consolidated_engagements_log` WHERE _domain IS NOT NULL
), quarterly_contact_engagement AS (
         SELECT 
        *
      FROM (  
        SELECT
          DISTINCT
          _domain,
          #email
          COUNT(DISTINCT CASE WHEN _engagement = 'Email Opened' THEN CONCAT(_email, _contentTitle) END) AS _distinct_email_open,
          COUNT(DISTINCT  CASE WHEN _engagement = 'Email Clicked' THEN CONCAT(_email, _contentTitle) END) AS _distinct_email_click,
          #formfilled
          COUNT(DISTINCT CASE WHEN _engagement = 'Form Filled'  THEN CONCAT(_email, _contentTitle) END ) AS _distinct_contactus_form,
         
          #PaidAds
          SUM(CASE WHEN _engagement =  'Paid Social Click' OR  _engagement =  'Paid Social Like' THEN _frequency  ELSE 0 END ) AS _distinctpaidads_clicks,
          SUM(CASE WHEN _engagement =  'Paid Social Follow' THEN 1 ELSE 0 END ) AS _distinctpaidadsfollow,
          SUM(CASE WHEN _engagement =  'Paid Social Share'  THEN 1  ELSE 0 END ) AS _distinctpaidadsshare,
          SUM(CASE WHEN _engagement =  'Paid Social Comment' THEN 1  ELSE 0 END ) AS _distinctpaidadscomment,
          SUM(CASE WHEN _engagement =  'Paid Social Visit' THEN 1  ELSE 0 END ) AS _distinctpaidadsvisit,

          #OrganicSocial
          SUM(CASE WHEN _engagement =  'Organic Social Click' OR  _engagement =  'Organic Social Like' THEN _frequency  ELSE 0 END ) AS _distinctorganicadsclicks,
          SUM(CASE WHEN _engagement =  'Organic Social Follow' THEN 1 ELSE 0 END ) AS _distinctorganicadsfollow,
          SUM(CASE WHEN _engagement =  'Organic Social Share'  THEN 1  ELSE 0 END ) AS _distinctorganicadsshare,
          SUM(CASE WHEN _engagement =  'Organic Social Comment' THEN 1  ELSE 0 END ) AS _distinctorganicadscomment,
          SUM(CASE WHEN _engagement =  'Organic Social Visit' THEN 1  ELSE 0 END ) AS _distinctorganicadsvisit,

          #Paid Search & Organic Search

          SUM(CASE WHEN _engagement IN( 'Organic Search','Paid Search')  THEN 1  ELSE 0 END ) AS _distinctsearchads,
          
          FROM
          (SELECT DISTINCT * FROM `x-marketing.thelogicfactory.db_consolidated_engagements_log`)
      WHERE
      DATE(_timestamp) BETWEEN date_start AND date_end
      --   ---AND 
        -- _domain  = "keysight.com"
        GROUP BY
          1
      )
      ORDER BY 
        _distinct_contactus_form DESC
)
,
quarterly_contact_scoring AS (
      SELECT
        DISTINCT *,
    #Calculating total email score
          (
          (CASE ---4 or more open
              WHEN _distinct_email_open >= 7 THEN 5
              --WHEN _distinct_email_open BETWEEN 1 AND 3 THEN 3
              ELSE 0
          END) + 
           (CASE --- 1-3 open
              --WHEN _distinct_email_open >= 4 THEN 5
              WHEN   _distinct_email_open >= 3 THEN 3
              ELSE 0
          END) + #Email click
          (CASE --- 4 or more open 
              WHEN _distinct_email_click >= 7 THEN 10
              ---WHEN _distinct_email_click BETWEEN 1 AND 3 THEN 5
              ELSE 0
          END ) + 
          (CASE --- 1-3 click
              --WHEN _distinct_email_click >= 4 THEN 10
              WHEN _distinct_email_click >= 3 THEN 5
              ELSE 0
          END )  #Email Click
        ) AS _email_score,
    #Calculating total unsubscribed score
        -- (CASE WHEN _unsubscribed > 0 THEN -10 ELSE 0 END ) AS _unsubscribed_score,
        #paid_socail 

        ((CASE ---click
              WHEN _distinctpaidads_clicks >= 1 THEN 3
              ELSE 0
          END)
          + 
          (CASE ---social visit
              WHEN _distinctpaidadsvisit >= 1 THEN 3
              ELSE 0
          END)
          + 
          (CASE ---follow 
              WHEN _distinctpaidadsfollow >= 1 THEN 4
              ELSE 0
          END)
          +
          (CASE ---comment
              WHEN _distinctpaidadscomment >= 1 THEN 10
              ELSE 0
          END)
          +
          (CASE ---share 
              WHEN _distinctpaidadsshare >= 1 THEN 15
              ELSE 0
          END)) AS _paid_social_score,

          #organic social 
           ((CASE ---click
              WHEN _distinctorganicadsclicks >= 1 THEN 3
              ELSE 0
          END)
          + 
          (CASE ---social visit
              WHEN _distinctorganicadsvisit >= 1 THEN 3
              ELSE 0
          END)
          + 
          (CASE ---follow 
              WHEN _distinctorganicadsfollow >= 1 THEN 4
              ELSE 0
          END)
          +
          (CASE ---comment
              WHEN _distinctorganicadscomment >= 1 THEN 10
              ELSE 0
          END)
          +
          (CASE ---share
              WHEN _distinctorganicadsshare >= 1 THEN 15
              ELSE 0
          END)) AS _organic_social_score,
          #searchads 
          ((CASE ---searchads
              WHEN _distinctsearchads >= 1 THEN 20
              ELSE 0
          END)) AS _searchads_score,
          #formfill 
          ((CASE 
              WHEN _distinct_contactus_form >= 1 THEN 60
              ELSE 0
          END)) AS _formfilled_score


   
      FROM
        quarterly_contact_engagement
), contact_score_limit AS (
         SELECT * ,
 ( -- Setting of threshold for max of email score
IF(_email_score > 15, 15, _email_score ) ) AS _quarterly_email_score,
( -- Setting of threshold for max of paid social score
IF(_paid_social_score > 35, 35, _paid_social_score ) ) AS _quarterly_paidsocial_score,
( -- Setting of threshold for max of organic social score
IF(_organic_social_score > 35, 35, _organic_social_score) ) AS _quarterly_organic_social_score,
( -- Setting of threshold for max of search ads score
IF(_searchads_score > 20, 20, _searchads_score) ) AS _quarterly_search_ads_score,
( -- Setting of threshold for max of form filled
IF(_formfilled_score > 60, 60, _formfilled_score) ) AS _quarterly_formfilled_score,

FROM  quarterly_contact_scoring
) , quarterly_web_data AS (
        WITH web_activity AS (
  SELECT 
          _domain, 
          _visitorid,
          _timestamp, 
          EXTRACT(WEEK FROM _timestamp) AS _week,  
          EXTRACT(YEAR FROM _timestamp) AS _year, 
          -- _webActivity AS _pageName, 
          -- "Web Visit" AS _engagement, 
          CAST(_engagementtime AS NUMERIC) AS _website_time_spent,
          CAST(_totalsessionviews AS INT64) AS _website_page_view,
          _visitorid AS _website_visitor_count,
          _utmsource,
          _utmmedium
        FROM 
          `x-marketing.thelogicfactory.db_web_engagements_log` web 
        WHERE 
          --(NOT REGEXP_CONTAINS(LOWER(_utmsource), '6sense|linkedin|google|email') OR _utmsource IS NULL)
          --AND (NOT REGEXP_CONTAINS(_utmmedium, 'cpc|social') OR _utmmedium IS NULL)
          --AND 
        (NOT REGEXP_CONTAINS(LOWER(_page), 'unsubscribe|career')
        OR 
        NOT REGEXP_CONTAINS(LOWER(_page), '/privacy-statement/')
        OR
        NOT REGEXP_CONTAINS(LOWER(_page), '/careers/') OR
        NOT REGEXP_CONTAINS(LOWER(_page), '/careers/#jobs-section') OR
        NOT REGEXP_CONTAINS(LOWER(_page), '/about-us/faces-of-tlf/') OR
        NOT REGEXP_CONTAINS(LOWER(_page), '/faces-of-tlf-herbert-van-der-meij/') OR
        NOT REGEXP_CONTAINS(LOWER(_page), '/faces-of-tlf-claudia-mulder/') OR
        NOT REGEXP_CONTAINS(LOWER(_page), '/faces-of-tlf-avani-v-prajapati/') OR
        NOT REGEXP_CONTAINS(LOWER(_page), '/faces-of-tlf-wouter/') OR
        NOT REGEXP_CONTAINS(LOWER(_page), '/faces-of-tlf-darshan-panchal/') OR
        NOT REGEXP_CONTAINS(LOWER(_page), '/faces-of-tlf-dhrumandaxini/') OR
        NOT REGEXP_CONTAINS(LOWER(_page), '/faces-of-tlf-jess-ulan/') OR
        NOT REGEXP_CONTAINS(LOWER(_page), '/faces-of-tlf-luke-retout/') OR
        NOT REGEXP_CONTAINS(LOWER(_page), '/faces-of-tlf-malou-de-koning/') OR
        NOT REGEXP_CONTAINS(LOWER(_page), '/faces-of-tlf-nirav-parikh/') OR
        NOT REGEXP_CONTAINS(LOWER(_page), '/faces-of-tlf-prasat-mathawan/') OR
        NOT REGEXP_CONTAINS(LOWER(_page), '/faces-of-tlf-rahul-dolia/"') )
        ORDER BY
          _timestamp DESC, _totalsessionviews DESC
) SELECT
      _domain,
      --COALESCE(SUM(newsletter_subscription), 0) AS newsletter_subscription,
      COALESCE((SUM(_website_time_spent)), 0) AS _website_time_spent,
      COALESCE(SUM(_website_page_view), 0) AS _website_page_view,
      COALESCE(COUNT( DISTINCT _website_visitor_count), 0) AS _website_visitor_count,
      TRUE AS _visited_website,
      FROM  web_activity 
      WHERE
      (DATE(_timestamp) BETWEEN date_start AND date_end)
      AND 
        LENGTH(_domain) > 2
      GROUP BY
        1 
)
,
    --Get scores for web visits activity
quarterly_web_score AS (
      SELECT
        * EXCEPT(website_time_spent_score,
          website_page_view_score,
          website_visitor_count_score,
          visited_website_score),
          website_time_spent_score AS _website_time_spent_score,
          website_page_view_score AS _website_page_view_score,
          website_visitor_count_score AS _website_visitor_count_score,
          visited_website_score AS _visited_website_score,
          
          CASE
            WHEN (website_time_spent_score + website_page_view_score + website_visitor_count_score + visited_website_score ) > 30 THEN 30
            ELSE (website_time_spent_score + website_page_view_score + website_visitor_count_score + visited_website_score )
          END AS _quarterly_web_score
      FROM (
        SELECT
          *,
          CASE 
            WHEN _website_time_spent >= 120 THEN 20 #New
            -- WHEN _website_time_spent >= 120 THEN 20 #Old
            -- WHEN _website_time_spent < 120 THEN 10
            ELSE 0
          END
          + 
          CASE 
            WHEN _website_time_spent <= 120 THEN 10 #New
            -- WHEN _website_time_spent >= 120 THEN 20 #Old
            -- WHEN _website_time_spent < 120 THEN 10
            ELSE 0
          END
            AS website_time_spent_score,
          ( CASE 
              WHEN _website_page_view >= 5 THEN 15
              -- WHEN _website_page_view < 5 THEN 10
              ELSE 0
            END
            +
            CASE 
              WHEN _website_page_view <= 5 THEN 10
              -- WHEN _website_page_view < 5 THEN 10
              ELSE 0
            END
            )
            AS website_page_view_score,
            (CASE WHEN _website_visitor_count >= 3 THEN 10
              -- WHEN _website_visitor_count < 3 THEN 5
              ELSE 0
            END
            + 
            CASE WHEN _website_visitor_count < 3 THEN 5
              -- WHEN _website_visitor_count < 3 THEN 5
              ELSE 0
            END)
            AS website_visitor_count_score,
          5 AS visited_website_score
        FROM
          quarterly_web_data ) 
),final_scoring AS (
 SELECT all_accounts._domain,
COALESCE(_distinct_email_open,0) AS _distinct_email_open,
COALESCE(_distinct_email_click,0) AS _distinct_email_click,
COALESCE(_distinct_contactus_form,0) AS _distinct_contactus_form,
#paidadssocial
COALESCE(_distinctpaidads_clicks,0) AS _distinctpaidads_clicks,
COALESCE(_distinctpaidadsfollow,0) AS _distinctpaidadsfollow,
COALESCE(_distinctpaidadsshare,0) AS _distinctpaidadsshare,
COALESCE(_distinctpaidadscomment,0) AS _distinctpaidadscomment,
COALESCE(_distinctpaidadsvisit,0) AS _distinctpaidadsvisit,
#organic 
COALESCE(_distinctorganicadsclicks,0) AS _distinctorganicadsclicks,
COALESCE(_distinctorganicadsfollow,0) AS _distinctorganicadsfollow,
COALESCE(_distinctorganicadsshare,0) AS _distinctorganicadsshare,
COALESCE(_distinctorganicadscomment,0) AS _distinctorganicadscomment,
COALESCE(_distinctorganicadsvisit,0) AS _distinctorganicadsvisit,
#searchads
COALESCE(_distinctsearchads,0) AS _distinctsearchads,

COALESCE(_email_score,0) AS _email_score,
COALESCE(_paid_social_score,0) AS _paid_social_score,
COALESCE(_organic_social_score,0) AS _organic_social_score,
COALESCE(_searchads_score,0) AS _searchads_score,
COALESCE(_formfilled_score,0) AS _formfilled_score,

#quaeterly_score
COALESCE(_quarterly_email_score,0) AS _quarterly_email_score,
COALESCE(_quarterly_email_score,0) AS _quarterly_paidsocial_score,
COALESCE(_quarterly_organic_social_score,0) AS _quarterly_organic_social_score,
COALESCE(_quarterly_search_ads_score,0) AS _quarterly_search_ads_score,
COALESCE(_quarterly_formfilled_score,0) AS _quarterly_formfilled_score,

COALESCE(_website_time_spent,0) AS _website_time_spent,
COALESCE(_website_page_view,0) AS _website_page_view,
COALESCE(_website_visitor_count,0) AS _website_visitor_count,
IF(_visited_website IS NULL, false, _visited_website) AS _visited_website,
COALESCE(_website_time_spent_score, 0) AS _website_time_spent_score,
COALESCE(_website_page_view_score, 0) AS _website_page_view_score,
COALESCE(_website_visitor_count_score, 0) AS _website_visitor_count_score,
COALESCE(_visited_website_score, 0) AS _visited_website_score,
COALESCE(_quarterly_web_score, 0) AS _quarterly_web_score,


FROM 
all_accounts
LEFT JOIN 
contact_score_limit ON all_accounts._domain = contact_score_limit._domain
LEFT JOIN
quarterly_web_score ON all_accounts._domain = quarterly_web_score._domain
)
SELECT  *,
date_end AS _extract_date,
date_start AS _Tminus90_date 
FROM 
      final_scoring
      --WHERE _domain = 'ultra-ft.com'
    ORDER BY 
      _visited_website DESC
        ;
    -- Loop iteration
    SET index = index + 1;
  END;
END LOOP;