-- Declaring the loop index and variables for the min & max date
DECLARE index INT64 DEFAULT 0;
DECLARE date_ranges ARRAY<STRUCT<max_date DATE, min_date DATE>>;

DELETE FROM `3x.account_90days_score_new`  WHERE _domain IS NOT NULL;
-- Creating the date range array 
SET date_ranges = ARRAY(
  SELECT AS STRUCT
    DATE_TRUNC(_date, DAY) AS max_date, DATE_SUB(DATE_TRUNC(_date, DAY), INTERVAL 180 DAY) AS min_date
  FROM 
    UNNEST(GENERATE_DATE_ARRAY(CURRENT_DATE(), '2023-01-01', INTERVAL -1 DAY)) AS _date 
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
    
    INSERT INTO `3x.account_90days_score_new`

WITH all_accounts AS (
        SELECT DISTINCT _domain FROM `x-marketing.3x.db_consolidated_engagements_log` WHERE _domain IS NOT NULL
)
, quarterly_contact_engagement AS (
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
          COUNT(DISTINCT CASE WHEN _engagement = 'Form Filled' AND (_contentTitle = 'FM_2X_Microsite' OR REGEXP_CONTAINS(_contentTitle, 'Matching Labels')) THEN CONCAT(_email, _contentTitle) END ) AS _distinct_contactus_form,
          COUNT(DISTINCT CASE WHEN _engagement = 'Form Filled' AND REGEXP_CONTAINS(LOWER(_contentTitle), 'webinar|wbn') THEN CONCAT(_email, _contentTitle) END ) AS _distinct_webinar_form,
          COUNT(DISTINCT CASE WHEN _engagement = 'Form Filled' AND NOT REGEXP_CONTAINS(LOWER(_contentTitle), 'try now|demo|contact us|webinar|wbn') THEN CONCAT(_email, _contentTitle) END ) AS _distinct_gated_content,
          #6Sense
          --CAST(ROUND(AVG( CAST(CASE WHEN _engagement = '6Sense Clicks' THEN REGEXP_EXTRACT(_description, r'^(\d+) \(') END AS INT64) ) ) AS INT64) AS _distinct_6sense_clicks,
          COUNT(DISTINCT CASE WHEN _engagement = '6Sense New' THEN CONCAT(_domain, _utmcampaign) END )  AS _distinct_6sense_new,
          COUNT(DISTINCT CASE WHEN _engagement = '6Sense Increased' THEN CONCAT(_domain, _utmcampaign) END )  AS _distinct_6sense_increase,
          #Event
        COUNT(DISTINCT CASE WHEN  (_engagement = 'Events' AND _description = "Events - Bookmarked") THEN CONCAT(_email, _contentTitle) END ) AS _distincteventbookmarked,
          COUNT(DISTINCT CASE WHEN  (_engagement = 'Events' AND _description = "Events - Booth Visits") THEN CONCAT(_email, _contentTitle) END ) AS _distincteventboothvisit,
          COUNT(DISTINCT CASE WHEN  (_engagement = 'Events' AND _description = "Events - Downloaded Assets") THEN CONCAT(_email, _contentTitle) END ) AS _distincteventdownloadassets,
          COUNT(DISTINCT CASE WHEN  (_engagement = 'Events' AND _description = "Events - Inquiries") THEN CONCAT(_email, _contentTitle) END ) AS _distincteventinquiries,
          COUNT(DISTINCT CASE WHEN  (_engagement = 'Events' AND _description = "Events - Opt-Ins") THEN CONCAT(_email, _contentTitle) END )  AS _distincteventoptin,
          COUNT(DISTINCT CASE WHEN  (_engagement = 'Events' AND _description = "Events - Scheduled Meeting") THEN CONCAT(_email, _contentTitle) END )  AS _distincteventscheduledmeeting,
          COUNT(DISTINCT CASE WHEN  (_engagement = 'Events' AND _description = "Events - Attended Event") THEN CONCAT(_email, _contentTitle) END )  AS _distincteventattended,
          #Webinar
          COUNT(DISTINCT CASE WHEN  (_engagement = 'Webinar' AND _description = "Webinars - Attendees") THEN CONCAT(_email, _contentTitle) END )  AS _distinctWebinarattended,
          COUNT(DISTINCT CASE WHEN  (_engagement = 'Webinar' AND _description = "Webinars - Registrants") THEN CONCAT(_email, _contentTitle) END )  AS _distinctWebinarregistrants,
          #Content Syndication 
          COUNT(DISTINCT CASE WHEN  (_engagement = 'Content Syndication' AND _description = "Content Syndication") THEN CONCAT(_email, _contentTitle) END )  AS _distinctcontentsyndication,
          #PaidAds
          COUNT( CASE WHEN _engagement = 'Paid Ads Clicks' THEN CONCAT(_domain, _utmcampaign) END )  AS _distinct_paid_ads_clicks,
          COALESCE(CAST(NULL AS INT64), 0) AS _distinctpaidadsshare,
          COALESCE(CAST(NULL AS INT64), 0) AS _distinctpaidadscomment,
          COALESCE(CAST(NULL AS INT64), 0) AS _distinctpaidadsfollow,
          COALESCE(CAST(NULL AS INT64), 0) AS _distinctpaidadsvisit
          FROM
          (SELECT DISTINCT * FROM `x-marketing.3x.db_consolidated_engagements_log`)
      WHERE
      DATE(_timestamp) BETWEEN date_start AND date_end
        ---AND _domain IS NOT NULL
        GROUP BY
          1
      )
      ORDER BY 
        _distinct_contactus_form DESC, _distinct_webinar_form DESC, _distinct_gated_content DESC
)
,
quarterly_contact_scoring AS (
      SELECT
        DISTINCT *,
    #Calculating total email score
          (
          (CASE ---4 or more open
              WHEN _distinct_email_open >= 4 THEN 5
              --WHEN _distinct_email_open BETWEEN 1 AND 3 THEN 3
              ELSE 0
          END) + 
           (CASE --- 1-3 open
              --WHEN _distinct_email_open >= 4 THEN 5
              WHEN   _distinct_email_open >= 1 THEN 3
              ELSE 0
          END) + #Email click
          (CASE --- 4 or more open 
              WHEN _distinct_email_click >= 4 THEN 10
              ---WHEN _distinct_email_click BETWEEN 1 AND 3 THEN 5
              ELSE 0
          END ) + 
          (CASE --- 1-3 click
              --WHEN _distinct_email_click >= 4 THEN 10
              WHEN _distinct_email_click >= 1 THEN 5
              ELSE 0
          END )  #Email Click
        ) AS _email_score,
    #Calculating total unsubscribed score
        -- (CASE WHEN _unsubscribed > 0 THEN -10 ELSE 0 END ) AS _unsubscribed_score,
    #Calculating total content syndication score
        (
          (CASE
            WHEN _distinctcontentsyndication >= 1 THEN 25
            ELSE 0
          END)
        ) AS _content_synd_score,
    #Calculating total organic social score
        COALESCE(CAST(NULL AS INT64), 0) AS _organic_social_score,
    #Calculating form fill score
        (
          (CASE
            WHEN _distinct_contactus_form >= 1 THEN 60
            ELSE 0
          END)
        ) 
        AS _contactUs_form_score, #Contact Us Form
        (
          (CASE
            WHEN _distinct_webinar_form >= 1 THEN (_distinct_webinar_form * 15)
            ELSE 0
          END) + #Webinar Form
          (CASE
            WHEN _distinct_webinar_form >= 1 THEN (_distinct_webinar_form * 15)
            ELSE 0
          END) 
        ) AS _gated_or_webinar_form,
        (
          (CASE ---6sense click
              WHEN _distinct_6sense_clicks >= 1 THEN 2
              ELSE 0
          END) + 
           (CASE ---6sense click 
              WHEN _distinct_6sense_clicks > 1 THEN 4
              ELSE 0
          END) +  #6senseclick
          (CASE  --- newengaged
              WHEN _distinct_6sense_new >= 1 THEN 5*_distinct_6sense_new
              ELSE 0
          END ) + 
          (CASE --- increase6sense
              WHEN _distinct_6sense_increase >= 1 THEN 10*_distinct_6sense_increase
              ELSE 0
          END )  #6sense
        ) AS _6sense_score,
         ( 
            (CASE
              WHEN _distinctpaidadsshare >= 1 THEN 15
              ELSE 0
            END) + #paidadsshare
            (CASE
              WHEN _distinctpaidadscomment >= 1 THEN 10
              ELSE 0
            END)+ #paidadscomment
            (CASE
              WHEN _distinctpaidadsfollow >= 1 THEN 4
              ELSE 0
            END)+ #paidadsfollow
            (CASE
              WHEN _distinctpaidadsvisit >= 1 THEN 3
              ELSE 0
            END)+ #paidsadsvisit
            (CASE
              WHEN _distinct_paid_ads_clicks >= 1 THEN 3
              ELSE 0
            END) #paidsadsclick_like
           ) AS _paid_ads_score,
          (
            (CASE
              WHEN _distincteventscheduledmeeting >= 1 THEN 60*_distincteventscheduledmeeting
              ELSE 0
            END) +
            (CASE
              WHEN _distincteventinquiries >= 1 THEN 15*_distincteventinquiries
              ELSE 0
            END) +
            (CASE
              WHEN _distincteventbookmarked >= 1 THEN 10*_distincteventbookmarked
              ELSE 0
            END) +
            (CASE
              WHEN _distincteventoptin >= 1 THEN 1*_distincteventoptin
              ELSE 0
            END) +
            (CASE
              WHEN _distincteventboothvisit >= 1 THEN 1*_distincteventboothvisit
              ELSE 0
            END) +
            (CASE
              WHEN _distincteventdownloadassets >= 1 THEN 15*_distincteventdownloadassets
              ELSE 0
            END) + 
            (CASE
              WHEN _distincteventattended >= 1 THEN 1*_distincteventattended
              ELSE 0
            END)
           ) AS _event_score,
           (
             (CASE
              WHEN _distinctWebinarattended >= 1 THEN 30*_distinctWebinarattended
              ELSE 0
            END)
            +
            (CASE
              WHEN _distinctWebinarregistrants >= 1 THEN 15*_distinctWebinarregistrants
              ELSE 0
            END) 
           ) AS _webinar_score 
      FROM
        quarterly_contact_engagement 
)
,contact_score_limit AS (
        SELECT
          _domain,
          _distinct_email_open,
          _distinct_email_click,
          _distinct_contactus_form,
          _distinct_webinar_form,
          _distinct_gated_content,
          _distinct_6sense_clicks,
          _distinct_6sense_new,
          _distinct_6sense_increase,
          _distinct_paid_ads_clicks,
          _distinctpaidadsshare,
          _distinctpaidadscomment,
          _distinctpaidadsfollow,
          _distinctpaidadsvisit,
          _distinctcontentsyndication,
          _distinctWebinarregistrants,
          _distinctWebinarattended,
          _distincteventattended,
          _distincteventscheduledmeeting,
          _distincteventoptin,
          _distincteventinquiries,
          _distincteventdownloadassets,
          _distincteventboothvisit,
          _distincteventbookmarked,
          ( -- Setting of threshold for max of email score
            IF(_email_score > 15, 15, _email_score ) ) AS _quarterly_email_score,
          ( -- Setting of threshold for max of content synd score
            IF(_content_synd_score > 25, 25, _content_synd_score )  ) AS _quarterly_content_synd_score,
          ( -- Setting of threshold for max of organic social form score
            IF(_organic_social_score > 35, 35, _organic_social_score )  ) AS _quarterly_organic_social_score,
          ( -- Setting of threshold for max of gated/webinar form score
             IF (_contactUs_form_score > 60, 60, _contactUs_form_score ) ) AS _quarterly_form_fill_score,
          ( IF(_webinar_score > 60, 60, _webinar_score ) ) AS _quarterly_webinar_score, 
          (IF(_paid_ads_score > 35, 35, _paid_ads_score)  ) AS _quarterly_paid_ads_score,
          (IF(_6sense_score > 30, 30, _6sense_score)  ) AS _quarterly_6sense_score,
          (IF(_event_score > 60, 60, _event_score)  ) AS _quarterly_event_score,
        FROM
          quarterly_contact_scoring
),
    -- Get web visits data from mouseflow, tying with webtrack through IP address to get company's domain
    quarterly_web_data AS (
    WITH all_account AS ( 
  SELECT DISTINCT _domain FROM `3x.web_metrics`
  UNION DISTINCT (SELECT DISTINCT _domain FROM `x-marketing.3x.db_consolidated_engagements_log`
  WHERE _description = "Subscribed")
), web_activity AS (
  SELECT 
          _domain, 
          _visitorid,
          _timestamp, 
          EXTRACT(WEEK FROM _timestamp) AS _week,  
          EXTRACT(YEAR FROM _timestamp) AS _year, 
          -- _webActivity AS _pageName, 
          -- "Web Visit" AS _engagement, 
          _engagementtime AS _website_time_spent,
          CAST(_totalsessionviews AS INT64) AS _website_page_view,
          _visitorid AS _website_visitor_count,
          _utmsource,
          _utmmedium
        FROM 
          `3x.web_metrics` web 
        WHERE 
          --(NOT REGEXP_CONTAINS(LOWER(_utmsource), '6sense|linkedin|google|email') OR _utmsource IS NULL)
          --AND (NOT REGEXP_CONTAINS(_utmmedium, 'cpc|social') OR _utmmedium IS NULL)
          --AND 
          NOT REGEXP_CONTAINS(LOWER(_page), 'unsubscribe|career')
        ORDER BY
          _timestamp DESC, _totalsessionviews DESC
), subsribe AS (
  SELECT DISTINCT _domain, COUNT(DISTINCT CASE WHEN  (_engagement ='Website' AND _description = "Subscribed") THEN CONCAT(_email, _contentTitle) END )  AS _distinctsubsribe FROM `x-marketing.3x.db_consolidated_engagements_log`
  WHERE _description = "Subscribed"
  GROUP BY 1
) SELECT
      all_account._domain,
      --COALESCE(SUM(newsletter_subscription), 0) AS newsletter_subscription,
      COALESCE((SUM(_website_time_spent)), 0) AS _website_time_spent,
      COALESCE(SUM(_website_page_view), 0) AS _website_page_view,
      COALESCE(COUNT( DISTINCT _website_visitor_count), 0) AS _website_visitor_count,
      TRUE AS _visited_website,
      COALESCE((COUNT( DISTINCT _distinctsubsribe)), 0)  AS _distinctsubsribe
      FROM all_account 
      LEFT JOIN  web_activity ON all_account._domain = web_activity._domain
      LEFT JOIN subsribe ON all_account._domain = subsribe._domain
      WHERE
      (DATE(_timestamp) BETWEEN date_start AND date_end)
      AND 
        LENGTH(all_account._domain) > 2
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
          newsletter_subscription_score AS _newsletter_subscription_score,
          CASE
            WHEN (website_time_spent_score + website_page_view_score + website_visitor_count_score + visited_website_score + newsletter_subscription_score) > 30 THEN 30
            ELSE (website_time_spent_score + website_page_view_score + website_visitor_count_score + visited_website_score + newsletter_subscription_score)
          END AS _quarterly_web_score
      FROM (
        SELECT
          *,CASE WHEN _distinctsubsribe >= 1 THEN 20 ELSE 0 END AS  newsletter_subscription_score,
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
    SELECT 
        all_accounts._domain,
         COALESCE(_distinct_email_open,0) AS _distinct_email_open,
         COALESCE(_distinct_email_click,0) AS _distinct_email_click,
         COALESCE(_distinct_contactus_form,0) AS _distinct_contactus_form,
         COALESCE(_distinct_webinar_form,0) AS _distinct_webinar_form,
         COALESCE(_distinct_gated_content,0) AS _distinct_gated_content,
         COALESCE(_distinct_6sense_clicks,0) AS _distinct_6sense_clicks,
         COALESCE(_distinct_6sense_new,0) AS _distinct_6sense_new,
         COALESCE(_distinct_6sense_increase,0) AS _distinct_6sense_increase,
         COALESCE(_distinct_paid_ads_clicks,0) AS _distinct_paid_ads_clicks,
         COALESCE(_distinctpaidadsshare,0) AS _distinctpaidadsshare,
         COALESCE(_distinctpaidadscomment,0) AS _distinctpaidadscomment,
         COALESCE(_distinctpaidadsfollow,0) AS _distinctpaidadsfollow,
         COALESCE(_distinctpaidadsvisit,0) AS _distinctpaidadsvisit,
         COALESCE(_distinctcontentsyndication,0) AS _distinctcontentsyndication,
         COALESCE(_distinctWebinarregistrants,0) AS _distinctWebinarregistrants,
         COALESCE(_distinctWebinarattended,0) AS _distinctWebinarattended,
         COALESCE(_distincteventattended,0) AS _distincteventattended,
         COALESCE(_distincteventscheduledmeeting,0) AS _distincteventscheduledmeeting,
         COALESCE(_distincteventoptin,0) AS _distincteventoptin,
         COALESCE(_distincteventinquiries,0) AS  _distincteventinquiries,
         COALESCE(_distincteventdownloadassets,0) AS _distincteventdownloadassets,
         COALESCE(_distincteventboothvisit,0) AS _distincteventboothvisit,
         COALESCE(_distincteventbookmarked,0) AS _distincteventbookmarked,


         COALESCE(_quarterly_email_score,0) AS _quarterly_email_score,
         COALESCE(_quarterly_content_synd_score,0) AS _quarterly_content_synd_score,
         COALESCE(_quarterly_organic_social_score,0) AS _quarterly_organic_social_score,
         COALESCE(_quarterly_form_fill_score,0) AS _quarterly_form_fill_score,
         COALESCE(_quarterly_webinar_score,0) AS _quarterly_webinar_score,
         COALESCE(_quarterly_paid_ads_score,0) AS _quarterly_paid_ads_score,
         COALESCE(_quarterly_6sense_score,0) AS _quarterly_6sense_score,
         COALESCE(_quarterly_event_score,0) AS _quarterly_event_score,


         COALESCE(_website_time_spent, 0) AS _website_time_spent,
         COALESCE(_website_page_view, 0) AS _website_page_view,
         COALESCE(_website_visitor_count, 0) AS _website_visitor_count,
         IF(_visited_website IS NULL, false, _visited_website) AS _visited_website,
         COALESCE(_website_time_spent_score, 0) AS _website_time_spent_score,
         COALESCE(_website_page_view_score, 0) AS _website_page_view_score,
         COALESCE(_website_visitor_count_score, 0) AS _website_visitor_count_score,
         COALESCE(_visited_website_score, 0) AS _visited_website_score,
         COALESCE(_newsletter_subscription_score, 0) AS _newsletter_subscription_score,
         COALESCE(_quarterly_web_score, 0) AS _quarterly_web_score,


        FROM
          all_accounts
        LEFT JOIN
          contact_score_limit ON all_accounts._domain = contact_score_limit._domain
        LEFT JOIN
          quarterly_web_score ON all_accounts._domain = quarterly_web_score._domain
          --WHERE all_accounts._domain = "nvidia.com"
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




----------------------------------------old script-------------------------------------------------------------
/*-- Declaring the loop index and variables for the min & max date
DECLARE index INT64 DEFAULT 0;
DECLARE date_ranges ARRAY<STRUCT<max_date DATE, min_date DATE>>;

DELETE FROM `3x.account_90days_score` WHERE _domain IS NOT NULL;
-- Creating the date range array 
SET date_ranges = ARRAY(
  SELECT AS STRUCT
    DATE_TRUNC(_date, DAY) AS max_date, DATE_SUB(DATE_TRUNC(_date, DAY), INTERVAL 90 DAY) AS min_date
  FROM 
    UNNEST(GENERATE_DATE_ARRAY(CURRENT_DATE(), '2023-01-01', INTERVAL -1 DAY)) AS _date 
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

    INSERT INTO `3x.account_90days_score`
    WITH
    target_accounts AS (
      SELECT 
        DISTINCT _6sensedomain AS _domain, 
      FROM
        `webtrack_ipcompany.db_6sense_3x_segments`
      WHERE
        _segment != '3X_230109 (Bombora 60+)_Intent Segment'
      UNION DISTINCT
      SELECT 
        DISTINCT _domain
      FROM
          `x-marketing.3x.db_consolidated_engagements_log`
      WHERE
        _engagement NOT IN ('Bombora Report')
      UNION DISTINCT
      SELECT
        DISTINCT _6sensedomain
      FROM
        `webtrack_ipcompany.db_6sense_3x_campaign_accounts`
    ),
    icp_accounts AS (
      SELECT
        _domain, _target_accounts
      FROM
        target_accounts
      LEFT JOIN
        (SELECT DISTINCT _domain, _target_accounts FROM `3x.db_icp_database_log` WHERE _target_accounts = 1) USING(_domain)
    ),
    quarterly_contact_engagement AS (
      SELECT 
        *
      FROM (  
        SELECT
          DISTINCT
          _domain,
          COUNT(DISTINCT CASE WHEN _engagement = 'Email Opened' THEN CONCAT(_email, _contentTitle) END) AS _distinct_email_open,
          COUNT(DISTINCT  CASE WHEN _engagement = 'Email Clicked' THEN CONCAT(_email, _contentTitle) END) AS _distinct_email_click,
          COUNT(DISTINCT CASE WHEN _engagement = 'Form Filled' AND (_contentTitle = 'FM_2X_Microsite' OR REGEXP_CONTAINS(_contentTitle, 'Matching Labels')) THEN CONCAT(_email, _contentTitle) END ) AS _distinct_contactus_form,
          COUNT(DISTINCT CASE WHEN _engagement = 'Form Filled' AND REGEXP_CONTAINS(LOWER(_contentTitle), 'webinar|wbn') THEN CONCAT(_email, _contentTitle) END ) AS _distinct_webinar_form,
          COUNT(DISTINCT CASE WHEN _engagement = 'Form Filled' AND NOT REGEXP_CONTAINS(LOWER(_contentTitle), 'try now|demo|contact us|webinar|wbn') THEN CONCAT(_email, _contentTitle) END ) AS _distinct_gated_content,
        FROM
          (SELECT DISTINCT * FROM `x-marketing.3x.db_consolidated_engagements_log`)
        WHERE
          DATE(_timestamp) BETWEEN date_start AND date_end
          AND _domain IS NOT NULL
        GROUP BY
          1
      )
      ORDER BY 
        _distinct_contactus_form DESC, _distinct_webinar_form DESC, _distinct_gated_content DESC
    ),
    quarterly_contact_scoring AS (
      SELECT
        DISTINCT *,
    #Calculating total email score
        (
          (CASE
              WHEN _distinct_email_open >= 4 THEN 5
              WHEN _distinct_email_open BETWEEN 1 AND 3 THEN 3
              ELSE 0
          END) + #Email Open
          (CASE
              WHEN _distinct_email_click >= 4 THEN 10
              WHEN _distinct_email_click BETWEEN 1 AND 3 THEN 5
              ELSE 0
          END ) #Email Click
        ) AS _quarterly_email_score,
    #Calculating total unsubscribed score
        -- (CASE WHEN _unsubscribed > 0 THEN -10 ELSE 0 END ) AS _unsubscribed_score,
    #Calculating total content syndication score
        COALESCE(CAST(NULL AS INT64), 0) AS _quarterly_content_synd_score,
    #Calculating total organic social score
        COALESCE(CAST(NULL AS INT64), 0) AS _quarterly_organic_social_score,
    #Calculating form fill score
        (
          (CASE
            WHEN _distinct_contactus_form >= 1 THEN 60
            ELSE 0
          END)
        ) 
        AS _quarterly_contactUs_form_score, #Contact Us Form
        (
          (CASE
            WHEN _distinct_webinar_form >= 1 THEN (_distinct_webinar_form * 15)
            ELSE 0
          END) + #Webinar Form
          (CASE
            WHEN _distinct_webinar_form >= 1 THEN (_distinct_webinar_form * 15)
            ELSE 0
          END) 
        ) AS _quarterly_gated_or_webinar_form,
      FROM
        quarterly_contact_engagement 
    ),
    set_contact_score_limit AS (
        SELECT
          _domain,
          _distinct_email_open,
          _distinct_email_click,
          _distinct_contactus_form,
          _distinct_webinar_form,
          _distinct_gated_content,
          ( -- Setting of threshold for max of email score
            IF(_quarterly_email_score > 15, 15, _quarterly_email_score ) ) AS _quarterly_email_score,
          ( -- Setting of threshold for max of content synd score
            IF(_quarterly_content_synd_score > 30, 30, _quarterly_content_synd_score )  ) AS _quarterly_content_synd_score,
          ( -- Setting of threshold for max of organic social form score
            IF(_quarterly_organic_social_score > 35, 35, _quarterly_organic_social_score )  ) AS _quarterly_organic_social_score,
          ( -- Setting of threshold for max of gated/webinar form score
            IF(_quarterly_gated_or_webinar_form > 30, 30, _quarterly_gated_or_webinar_form ) + _quarterly_contactUs_form_score  ) AS _quarterly_form_fill_score,
        FROM
          quarterly_contact_scoring
    ),
    -- Get web visits data from mouseflow, tying with webtrack through IP address to get company's domain
    quarterly_web_data AS (
    SELECT
      _domain,
      -- COALESCE(SUM(newsletter_subscription), 0) AS newsletter_subscription,
      COALESCE((SUM(_website_time_spent)), 0) AS _website_time_spent,
      COALESCE(SUM(_website_page_view), 0) AS _website_page_view,
      COALESCE(COUNT( DISTINCT _website_visitor_count), 0) AS _website_visitor_count,
      TRUE AS _visited_website,
      -- MAX(_timestamp) AS last_engaged_date
    FROM (
        SELECT 
          _domain, 
          _visitorid,
          _timestamp, 
          EXTRACT(WEEK FROM _timestamp) AS _week,  
          EXTRACT(YEAR FROM _timestamp) AS _year, 
          -- _webActivity AS _pageName, 
          -- "Web Visit" AS _engagement, 
          _engagementtime AS _website_time_spent,
          CAST(_totalsessionviews AS INT64) AS _website_page_view,
          _visitorid AS _website_visitor_count,
          _utmsource,
          _utmmedium
        FROM 
          `3x.web_metrics` web 
        WHERE 
          (NOT REGEXP_CONTAINS(LOWER(_utmsource), '6sense|linkedin|google|email') OR _utmsource IS NULL)
          AND (NOT REGEXP_CONTAINS(_utmmedium, 'cpc|social') OR _utmmedium IS NULL)
          AND NOT REGEXP_CONTAINS(LOWER(_page), 'unsubscribe|career')
        ORDER BY
          _timestamp DESC, _totalsessionviews DESC
        )
      WHERE
        (DATE(_timestamp) BETWEEN date_start AND date_end)
        AND  LENGTH(_domain) > 2
      GROUP BY
        1 
    ),
    -- Get scores for web visits activity
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
            WHEN (website_time_spent_score + website_page_view_score + website_visitor_count_score + visited_website_score) > 30 THEN 30
            ELSE (website_time_spent_score + website_page_view_score + website_visitor_count_score + visited_website_score)
          END AS _quarterly_web_score
      FROM (
        SELECT
          *,
          CASE 
            WHEN _website_time_spent >= 120 THEN 10 #New
            -- WHEN _website_time_spent >= 120 THEN 20 #Old
            -- WHEN _website_time_spent < 120 THEN 10
            ELSE 0
          END
            AS website_time_spent_score,
          CASE 
            WHEN _website_page_view >= 5 THEN 15
            -- WHEN _website_page_view < 5 THEN 10
            ELSE 0
          END
            AS website_page_view_score,
          CASE WHEN _website_visitor_count >= 3 THEN 10
            -- WHEN _website_visitor_count < 3 THEN 5
            ELSE 0
          END
            AS website_visitor_count_score,
          1 AS visited_website_score
        FROM
          quarterly_web_data ) 
    ),
    quarterly_ad_clicks AS (
      SELECT
        DISTINCT _domain, 
        -- _week,
        -- _year,
        COUNT(DISTINCT CONCAT(_domain, _pageName)) OVER(PARTITION BY _domain*//* , _week  */
        --) 
        --AS _distinct_ads_clicks,
        #Calculating total ads score
        /*(CASE WHEN COUNT(DISTINCT CONCAT(_domain, _pageName)) OVER(PARTITION BY _domain*//* , _week  */
        --) >= 1 THEN 3 ELSE 0 END ) AS _quarterly_ads_score,
      /*FROM 
      (
        SELECT 
          DISTINCT
          _domain, 
          _timestamp, 
          -- EXTRACT(WEEK FROM _date) AS _week,  
          -- EXTRACT(YEAR FROM _date) AS _year, 
          _page AS _pageName, 
          CONCAT(_utmsource, "Ad Clicks") AS _engagement,
          -- _fullpage
        FROM 
          `3x.web_metrics` web
        WHERE 
          NOT REGEXP_CONTAINS(LOWER(_page), 'unsubscribe')
          AND REGEXP_CONTAINS(_utmmedium, 'cpc|social')
          AND (NOT REGEXP_CONTAINS(LOWER(_utmsource), '6sense|email') OR _utmsource IS NULL)
          AND LENGTH(_domain) > 1
      )
      WHERE
        DATE(_timestamp) BETWEEN date_start AND date_end
    ),
    quarterly_6sense_ads AS (
      SELECT
        DISTINCT _domain, 
        -- _week,
        -- _year,
        MAX(_clicks) OVER(PARTITION BY _domain) AS _distinct_6sense_clicks,
        MAX(_websiteengagement) OVER(PARTITION BY _domain ) AS _webEngagementTrend,
        #Calculating total ads score
        (
          CASE 
            WHEN MAX(_clicks) OVER(PARTITION BY _domain) = 1 THEN 2 
            WHEN MAX(_clicks) OVER(PARTITION BY _domain ) > 1 THEN 4
            ELSE 0 
          END 
          ) AS _6sense_click_score,
        (
          CASE 
            WHEN MAX(_websiteengagement) OVER(PARTITION BY _domain ) = 'New' THEN 5
            WHEN MAX(_websiteengagement) OVER(PARTITION BY _domain ) = 'Increased' THEN 10
            ELSE 0
          END
        ) AS _web_eng_trend_score
      FROM 
      (
        SELECT 
          DISTINCT _6sensedomain AS _domain,
          DATE(_date) AS _date,
          (CAST(_clicks AS INTEGER)) AS _clicks,
          _websiteengagement
        FROM 
          `webtrack_ipcompany.db_6sense_3x_campaign_accounts` 
        ORDER BY 
          _date DESC
      )
      WHERE
        _date BETWEEN date_start AND date_end
      ORDER BY 
        _distinct_6sense_clicks DESC
    )
    SELECT
      *,
      date_end AS _extract_date,
      date_start AS _Tminus90_date
    FROM
      icp_accounts
    LEFT JOIN
      set_contact_score_limit USING(_domain)
    LEFT JOIN
      quarterly_web_score USING(_domain)
    LEFT JOIN
      quarterly_ad_clicks USING(_domain)
    LEFT JOIN
      quarterly_6sense_ads USING(_domain)
    ORDER BY 
      _visited_website DESC
    ;
    -- Loop iteration
    SET index = index + 1;
  END;
END LOOP;*/