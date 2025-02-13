--------------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------ Account Scoring Backfill Script --------------------------------------------------------------
--------------------------------------------------------------------------------------------------------------------------------------------


/* 
  This script is used for backfilling account scores with a 90 days timefram of current timestamp
  CRM/Platform/Tools: -
  Data type: Account Scores
  Depedency Table: db_consolidated_engagements, report_tam_database, report_sent_to_sales, report_overview_engagement_opportunity, report_overview_account_opportunity
  Target table: db_account_scoring_90days
*/



DECLARE index INT64 DEFAULT 0;

DECLARE date_ranges ARRAY<STRUCT<max_date DATE, min_date DATE>>;

SET date_ranges = ARRAY(
  SELECT AS STRUCT
    DATE_TRUNC(_date, WEEK(MONDAY)) AS max_date, DATE_SUB(DATE_TRUNC(_date, WEEK(MONDAY)), INTERVAL 90 DAY) AS min_date
  FROM 
    UNNEST(GENERATE_DATE_ARRAY('2022-01-01',CURRENT_DATE(), INTERVAL 1 WEEK)) AS _date 
  ORDER BY 
    1 DESC
);


DELETE FROM `sandler.account_90days_score` WHERE _domain IS NOT NULL;

LOOP
  IF index = array_length(date_ranges) 
    THEN BREAK;
  END IF;
  BEGIN
    DECLARE date_end DATE DEFAULT date_ranges[OFFSET(index)].max_date;
    DECLARE date_start DATE DEFAULT date_ranges[OFFSET(index)].min_date;

    INSERT INTO `sandler.account_90days_score`
    WITH
      icp_accounts AS (
      SELECT
        _domain, _target_accounts
      FROM
        `sandler.db_tam_database`
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
            COUNT(DISTINCT CASE WHEN _engagement = 'Form Filled' AND REGEXP_CONTAINS(LOWER(_contentTitle), 'contact us|try now|demo') THEN CONCAT(_email, _contentTitle) END ) AS _distinct_contactus_form,
            COUNT(DISTINCT CASE WHEN _engagement = 'Form Filled' AND REGEXP_CONTAINS(LOWER(_contentTitle), 'webinar|wbn') THEN CONCAT(_email, _contentTitle) END ) AS _distinct_webinar_form,
            COUNT(DISTINCT CASE WHEN _engagement = 'Form Filled' AND NOT REGEXP_CONTAINS(LOWER(_contentTitle), 'try now|demo|contact us|webinar|wbn') THEN CONCAT(_email, _contentTitle) END ) AS _distinct_gated_content,
          FROM
            (SELECT DISTINCT * FROM `x-marketing.sandler.db_account_health_all`)
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
                WHEN _distinct_email_open >= 7 THEN 5
                WHEN _distinct_email_open BETWEEN 3 AND 6 THEN 3
                ELSE 0
            END) + #Email Open
            (CASE
                WHEN _distinct_email_click >= 7 THEN 10
                WHEN _distinct_email_click BETWEEN 3 AND 6 THEN 5
                ELSE 0
            END ) #Email Click
          ) AS _email_score,
      #Calculating total unsubscribed score
          -- (CASE WHEN _unsubscribed > 0 THEN -10 ELSE 0 END ) AS _unsubscribed_score,
      #Calculating total content syndication score
          COALESCE(CAST(NULL AS INT64), 0) AS _content_synd_score,
      #Calculating total organic social score
          COALESCE(CAST(NULL AS INT64), 0) AS _organic_social_score,
      #Calculating form fill score
          (
              (CASE
                WHEN _distinct_contactus_form >= 1 THEN 60
                ELSE 0
              END)
            ) AS _contactUs_form_score, #Contact Us Form
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
              IF(_email_score > 15, 15, _email_score ) ) AS _email_score,
            ( -- Setting of threshold for max of content synd score
              IF(_content_synd_score > 30, 30, _content_synd_score )  ) AS _content_synd_score,
            ( -- Setting of threshold for max of organic social form score
              IF(_organic_social_score > 35, 35, _organic_social_score )  ) AS _organic_social_score,
            ( -- Setting of threshold for max of gated/webinar form score
              IF(_gated_or_webinar_form > 30, 30, _gated_or_webinar_form ) + _contactUs_form_score  ) AS _form_fill_score,
          FROM
            quarterly_contact_scoring
      ),
      -- Get web visits data from mouseflow, tying with webtrack through IP address to get company's domain
      web_data AS (
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
            `sandler.db_web_engagements_log` web 
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
      web_score AS (
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
              WHEN (website_time_spent_score + website_page_view_score + website_visitor_count_score + visited_website_score) > 50 THEN 50
              ELSE (website_time_spent_score + website_page_view_score + website_visitor_count_score + visited_website_score)
            END AS _web_score
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
            web_data ) 
      ),
      ad_clicks AS (
        SELECT
          DISTINCT _domain, 
          -- _week,
          -- _year,
          COUNT(DISTINCT CONCAT(_domain, _pageName)) OVER(PARTITION BY _domain ) AS _distinct_ads_clicks,
          #Calculating total ads score
          (CASE WHEN COUNT(DISTINCT CONCAT(_domain, _pageName)) OVER(PARTITION BY _domain ) >= 1 THEN 3 ELSE 0 END ) AS _ads_score,
        FROM 
        (
          SELECT 
            DISTINCT 
            _domain, 
            _timestamp, 
            -- EXTRACT(WEEK FROM _date) AS _week,  
            -- EXTRACT(YEAR FROM _date) AS _year, 
            _page AS _pageName, 
            CONCAT(_utmsource, " Ad Clicks") AS _engagement,
            -- _fullpage
          FROM 
            `sandler.db_web_engagements_log` web
          WHERE 
            NOT REGEXP_CONTAINS(LOWER(_page), 'unsubscribe')
            AND REGEXP_CONTAINS(LOWER(_utmmedium), 'cpc|social|paid|banner|ppc')
            AND (NOT REGEXP_CONTAINS(LOWER(_utmsource), 'email') OR _utmsource IS NULL)
            AND LENGTH(_domain) > 1
        )
        WHERE
          DATE(_timestamp) BETWEEN date_start AND date_end
      )
      SELECT
        web_score._domain,
        _distinct_email_open,
        _distinct_email_click, 
        _distinct_contactus_form, 
        _distinct_webinar_form, 
        _distinct_gated_content, 
        _email_score, 
        _content_synd_score,
        _organic_social_score,
        _form_fill_score, 
        COALESCE(_website_time_spent, 0) AS _website_time_spent,
        COALESCE(_website_page_view, 0) AS _website_page_view,
        COALESCE(_website_visitor_count, 0) AS _website_visitor_count,
        IF(_visited_website IS NULL, false, _visited_website) AS _visited_website,
        COALESCE(_website_time_spent_score, 0) AS _website_time_spent_score,
        COALESCE(_website_page_view_score, 0) AS _website_page_view_score,
        COALESCE(_website_visitor_count_score, 0) AS _website_visitor_count_score,
        COALESCE(_visited_website_score, 0) AS _visited_website_score,
        COALESCE(_web_score, 0) AS _web_score,
        COALESCE(_distinct_ads_clicks, 0) AS _distinct_ads_clicks,
        COALESCE(_ads_score, 0) AS _ads_score,
        date_end AS _extract_date,
        date_start AS _Tminus90_date
      FROM
        icp_accounts
      LEFT JOIN
        set_contact_score_limit USING(_domain)  
      LEFT JOIN
        web_score USING(_domain)
      LEFT JOIN
        ad_clicks USING(_domain)
      WHERE 
        _domain <> ""
      ORDER BY 
        _visited_website DESC
    ;
    SET index = index + 1;
  END;
END LOOP;