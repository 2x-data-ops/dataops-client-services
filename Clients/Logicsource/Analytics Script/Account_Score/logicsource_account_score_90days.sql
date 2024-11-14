DECLARE index INT64 DEFAULT 0;

DECLARE date_ranges ARRAY<STRUCT<max_date DATE, min_date DATE>>;

SET date_ranges = ARRAY(
  SELECT AS STRUCT
    DATE_TRUNC(_date, DAY) AS max_date, DATE_SUB(DATE_TRUNC(_date, DAY), INTERVAL 180 DAY) AS min_date
  FROM 
    UNNEST(GENERATE_DATE_ARRAY(CURRENT_DATE(), '2023-01-01', INTERVAL -1 DAY)) AS _date 
  ORDER BY 
    1 DESC
);


DELETE FROM `logicsource.account_90days_score` WHERE _domain IS NOT NULL;

LOOP
  IF index = array_length(date_ranges) 
    THEN BREAK;
  END IF;
  BEGIN
    DECLARE date_end DATE DEFAULT date_ranges[OFFSET(index)].max_date;
    DECLARE date_start DATE DEFAULT date_ranges[OFFSET(index)].min_date;

    INSERT INTO `logicsource.account_90days_score`
WITH
      all_accounts AS (
        SELECT DISTINCT _domain FROM `x-marketing.logicsource.db_consolidated_engagements_log` WHERE _domain IS NOT NULL
      ),
      weekly_contact_engagement AS (
        SELECT 
          *
        FROM (  
          SELECT
            DISTINCT
            _domain,
            COUNT(DISTINCT CASE WHEN _engagement = 'Email Opened' THEN CONCAT(_email, _contentTitle) END) AS _distinctOpen,
            COUNT(DISTINCT  CASE WHEN _engagement = 'Email Clicked' THEN CONCAT(_email, _contentTitle) END) AS _distinctClick,
            COUNT(DISTINCT CASE WHEN (_engagement = 'Form Filled' AND REGEXP_CONTAINS(LOWER(_contentTitle), 'contact us|try now|demo|webinar')) OR (_engagement = 'Form Filled' AND _description = "Visited booth") THEN CONCAT(_email, _contentTitle) END ) AS _distinctContactUsForm,
            --COUNT(DISTINCT CASE WHEN _engagement = 'Form Filled' AND REGEXP_CONTAINS(LOWER(_contentTitle), 'webinar|wbn') THEN CONCAT(_email, _contentTitle) END ) 
             --COALESCE(CAST(NULL AS INT64), 0) 
              COUNT(DISTINCT CASE WHEN  (_engagement = 'Form Filled' AND _description = "Registered") THEN CONCAT(_email, _contentTitle) END ) AS _distinctWebinarForm,
             COUNT(DISTINCT CASE WHEN  (_engagement = 'Form Filled' AND _description = "Attended event") THEN CONCAT(_email, _contentTitle) END )  AS _distinctWebinarattended,
            COUNT(DISTINCT CASE WHEN _engagement = 'Form Filled' AND NOT REGEXP_CONTAINS(LOWER(_contentTitle), 'try now|demo|contact us|webinar|wbn') THEN CONCAT(_email, _contentTitle) END ) AS _distinctGatedContent,
            --COUNT(DISTINCT CASE WHEN _engagement = 'Content Engagement'  THEN CONCAT(_email, _contentTitle) END ) 
            COALESCE(CAST(NULL AS INT64), 0) AS _distinctcontentsync,
            COUNT(DISTINCT CASE WHEN _engagement = 'Paid Ads'  AND _contentTitle = 'Share' THEN CONCAT(_email, _contentTitle) END ) AS _distinctpaidadsshare,
            COUNT(DISTINCT CASE WHEN _engagement = 'Paid Ads'  AND _contentTitle = 'Comment' THEN CONCAT(_email, _contentTitle) END ) AS _distinctpaidadscomment,
            COUNT(DISTINCT CASE WHEN _engagement = 'Paid Ads'  AND _contentTitle = 'Follow' THEN CONCAT(_email, _contentTitle) END ) AS _distinctpaidadsfollow,
            COUNT(DISTINCT CASE WHEN _engagement = 'Paid Ads'  AND _contentTitle = 'Visit' THEN CONCAT(_email, _contentTitle) END ) AS _distinctpaidadsvisit,
            COUNT(DISTINCT CASE WHEN _engagement = 'Paid Ads'   AND SUBSTR(_description, 1,7) LIKE  '%Clicks%' OR SUBSTR(_description, 1,4) LIKE  '%Like%' THEN CONCAT(_email, _contentTitle) END ) AS _distinctpaidadsclick_like,
            COUNT(DISTINCT CASE WHEN _engagement = 'Organic Social'  AND _contentTitle = 'Share' THEN CONCAT(_email, _contentTitle) END ) AS _distinctorganicadsshare,
            COUNT(DISTINCT CASE WHEN _engagement = 'Organic Social'  AND _contentTitle = 'Comment' THEN CONCAT(_email, _contentTitle) END ) AS _distinctorganicadscomment,
            COUNT(DISTINCT CASE WHEN _engagement = 'Organic Social'  AND _contentTitle = 'Follow' THEN CONCAT(_email, _contentTitle) END ) AS _distinctorganicadsfollow,
            COUNT(DISTINCT CASE WHEN _engagement = 'Organic Social'  AND _contentTitle = 'Visit' THEN CONCAT(_email, _contentTitle) END ) AS _distinctorganicadsvisit,
            COUNT(DISTINCT CASE WHEN _engagement = 'Organic Social'  AND SUBSTR(_description, 1,7) LIKE  '%Clicks%' OR SUBSTR(_description, 1,4) LIKE  '%Like%' THEN CONCAT(_email, _contentTitle) END ) AS _distinctorganicadsclick_like,
          FROM
            (SELECT DISTINCT * FROM `logicsource.db_consolidated_engagements_log` WHERE _engagement IS NOT NULL)
          WHERE
          DATE( CAST(_date AS DATE)) BETWEEN date_start AND date_end
           AND 
            _domain IS NOT NULL
          GROUP BY
            1
        )
        ORDER BY 
          _distinctContactUsForm DESC, _distinctWebinarForm DESC, _distinctGatedContent DESC
      )
      ,weekly_contact_scoring AS (
        SELECT
          DISTINCT *,
      #Calculating total email score
          (
            (CASE
                WHEN _distinctClick >= 5 THEN 10
                ---WHEN _distinctOpen BETWEEN 3 AND 6 THEN 3
                ELSE 0
            END
            + 
            CASE
                WHEN _distinctOpen >= 5 THEN 5
                ---WHEN _distinctOpen BETWEEN 3 AND 6 THEN 3
                ELSE 0
            END
            ) + #Email Open
            (CASE
                WHEN _distinctClick >= 2 THEN 10
                --WHEN _distinctClick BETWEEN 3 AND 6 THEN 5
                ELSE 0
            END ) #Email Click
          ) AS _email_score,
      #Calculating total unsubscribed score
          -- (CASE WHEN _unsubscribed > 0 THEN -10 ELSE 0 END ) AS _unsubscribed_score,
      #Calculating total content syndication score
          --COALESCE(CAST(NULL AS INT64), 0) 
          CASE
              WHEN _distinctcontentsync >= 1 THEN 30
              ELSE 0
            END AS _content_synd_score,
      #Calculating total organic social score
          --COALESCE(CAST(NULL AS INT64), 0) 
           ( 
            (CASE
              WHEN _distinctorganicadsshare >= 1 THEN 15
              ELSE 0
            END) + #paidadsshare
            (CASE
              WHEN _distinctorganicadscomment >= 1 THEN 10
              ELSE 0
            END)+ #paidadscomment
            (CASE
              WHEN _distinctorganicadsfollow >= 1 THEN 4
              ELSE 0
            END)+ #paidadsfollow
            (CASE
              WHEN _distinctorganicadsclick_like >= 1 THEN 5
              ELSE 0
            END) #paidsadsclick_like
           ) AS _organic_social_score,
      #Calculating form fill score 
          (
            (CASE
              WHEN _distinctContactUsForm >= 1 THEN 50
              ELSE 0
            END)
          ) AS _contact_us_form_score, #Contact Us Form
          (  
            (CASE
              WHEN _distinctWebinarattended >= 1 THEN 5
              ELSE 0
            END) + #Webinar Form
            (CASE
              WHEN _distinctWebinarForm >= 1 THEN 15
              ELSE 0
            END) +
            (CASE
              WHEN _distinctGatedContent >= 1 THEN 20
              ELSE 0
            END) 
          ) AS _other_form_fill_score,
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
              WHEN _distinctpaidadsvisit >= 1 THEN 5
              ELSE 0
            END)+ #paidsadsvisit
            (CASE
              WHEN _distinctpaidadsclick_like >= 1 THEN 5
              ELSE 0
            END) #paidsadsclick_like
           ) AS _paid_ads_score,
        FROM
          weekly_contact_engagement
      )
      ,contact_score_limit AS (
        SELECT
          *,
          ( -- Setting of threshold for max of email score
            IF(_email_score > 20, 20, _email_score ) ) AS _email_score_total,
          ( -- Setting of threshold for max of content synd score
            IF(_content_synd_score > 30, 30, _content_synd_score )  ) AS _content_synd_score_total,
          ( -- Setting of threshold for max of organic social form score
            IF(_organic_social_score > 35, 35, _organic_social_score )  ) AS _organic_social_score_total,
           ( -- Setting of threshold for max of organic social form score
            IF(_paid_ads_score > 35, 35, _paid_ads_score)  ) AS _paid_ads_score_total,
          ( -- Setting of threshold for max of email score
            IF(_other_form_fill_score > 30, 30, _other_form_fill_score ) ) AS _other_form_fill_score_total,
          ( -- Setting of threshold for max of gated/webinar form score
            IF(_other_form_fill_score > 30, 30, _other_form_fill_score ) + _contact_us_form_score ) AS _form_fill_score_total,
        FROM
          weekly_contact_scoring
      ) -- Get web visits data from mouseflow, tying with webtrack through IP address to get company's domain
      ,weekly_web_data AS (
      SELECT
        _domain,
        -- _week,
        -- _year,
        -- COALESCE(SUM(newsletter_subscription), 0) AS newsletter_subscription,
        COALESCE((SUM(_website_time_spent)), 0) AS _website_time_spent,
        COALESCE(SUM(CASE WHEN _pageName IS NOT NULL THEN 1 END), 0) AS _website_page_view,
        COALESCE(COUNT(DISTINCT _visitorid), 0) AS _website_visitor_count,
        COALESCE(COUNT(DISTINCT CASE WHEN _pageName LIKE "%careers%" THEN _visitorid END), 0) AS _career_page,
        TRUE AS _visited_website,
        -- MAX(_timestamp) AS last_engaged_date
      FROM (
        /* SELECT
          DATE(_starttime) AS _timestamp,
          company._domain,
          SUM(CAST(_engagementtime AS INT64)) AS _website_time_spent,
          COUNT(DISTINCT(_page)) AS _website_page_view,
          COUNT(DISTINCT msflow._visitorid) AS _website_visitor_count,
          -- newsletter_subscription in the future,
        FROM
          `logicsource_mysql.mouseflow_pageviews` msflow
        LEFT JOIN (
          SELECT
            DISTINCT _ipaddr,
            _website AS _domain
          FROM
            `webtrack_ipcompany.webtrack_ipcompany_6sense`) company
          USING
            (_ipaddr)
        GROUP BY
          1, 2  */
          SELECT 
            _domain, 
            _visitorid,
            DATETIME(_timestamp) AS _timestamp, 
            EXTRACT(WEEK FROM _timestamp) AS _week,  
            EXTRACT(YEAR FROM _timestamp) AS _year, 
            _entrypage AS _pageName, 
            -- "Web Visit" AS _engagement, 
            CAST(_engagementtime AS INT64) AS _website_time_spent,
            _totalPages AS _website_page_view
          FROM 
            `logicsource.dashboard_mouseflow_kickfire` web 
          WHERE 
            NOT REGEXP_CONTAINS(LOWER(_source), 'linkedin|google|email') 
            AND _webactivity IS NOT NULL
          ORDER BY
            _timestamp DESC
          )
        WHERE
          (_timestamp BETWEEN date_start AND date_end)
        AND  
          LENGTH(_domain) > 2
        GROUP BY
          1 
     )
     -- Get scores for web visits activity
    , weekly_web_score AS (
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
              WHEN (website_time_spent_score + website_page_view_score + website_visitor_count_score + visited_website_score + career_page_score) > 40 THEN 40
              ELSE (website_time_spent_score + website_page_view_score + website_visitor_count_score + visited_website_score + career_page_score)
            END AS _quarterly_web_score
        FROM (
          SELECT
            *,
            COALESCE((_website_time_spent), 0)
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
              CASE WHEN _career_page > 1 THEN -5 ELSE 0 END AS career_page_score,
            5 AS visited_website_score
          FROM
            weekly_web_data ) 
      )
      ,final_scoring AS (
        SELECT 
        --*
          all_accounts._domain, 
          COALESCE(_distinctOpen, 0 ) AS _distinctOpen, 
          COALESCE(_distinctClick, 0 ) AS _distinctClick, 
          COALESCE(_distinctContactUsForm, 0 ) AS _distinctContactUsForm, 
          COALESCE(_distinctWebinarForm, 0 ) AS _distinctWebinarForm, 
          COALESCE(_distinctWebinarattended, 0 ) AS _distinctWebinarattended, 
          COALESCE(_distinctGatedContent, 0 ) AS _distinctGatedContent, 
          COALESCE(_distinctcontentsync, 0 ) AS _distinctcontentsync,
          COALESCE(_distinctpaidadsshare, 0 ) AS _distinctpaidadsshare,
          COALESCE(_distinctpaidadscomment, 0 ) AS _distinctpaidadscomment,
          COALESCE(_distinctpaidadsfollow, 0 ) AS _distinctpaidadsfollow,
          COALESCE(_distinctpaidadsvisit, 0 ) AS _distinctpaidadsvisit,
          COALESCE(_distinctpaidadsclick_like, 0 ) AS _distinctpaidadsclick_like,
          COALESCE(_distinctorganicadscomment, 0 ) AS _distinctorganicadscomment,
          COALESCE(_distinctorganicadsfollow, 0 ) AS _distinctorganicadsfollow,
          COALESCE(_distinctorganicadsvisit, 0 ) AS _distinctorganicadsvisit,
          COALESCE(_distinctorganicadsclick_like, 0 ) AS _distinctorganicadsclick_like,
          COALESCE(_email_score, 0 ) AS _email_score, 
          COALESCE(_content_synd_score, 0 ) AS  _content_synd_score, 
          COALESCE(_organic_social_score, 0 ) AS  _organic_social_score, 
          COALESCE(_contact_us_form_score, 0) AS  _contact_us_form_score,
          COALESCE(_other_form_fill_score, 0) AS _other_form_fill_score,
          COALESCE(_paid_ads_score, 0) AS  _paid_ads_score,
          COALESCE(_email_score_total, 0 ) AS _quarterly_email_score, 
          COALESCE(_content_synd_score_total, 0 ) AS  _quarterly_content_synd_score, 
          COALESCE(_paid_ads_score_total, 0 ) AS  _quarterly_paid_ads_score, 
          COALESCE(_form_fill_score_total, 0) AS  _quarterly_form_fill_score,
          COALESCE(_organic_social_score_total, 0) AS  _quarterly_organic_social_score,
          COALESCE(_website_time_spent, 0) AS _website_time_spent,
          COALESCE(_website_page_view, 0) AS _website_page_view,
          COALESCE(_website_visitor_count, 0) AS _website_visitor_count,
          COALESCE(_career_page, 0) AS _career_page_count,
          IF(_visited_website IS NULL, false, _visited_website) AS _visited_website,
          COALESCE(_website_time_spent_score, 0) AS _website_time_spent_score,
          COALESCE(_website_page_view_score, 0) AS _website_page_view_score,
          COALESCE(_website_visitor_count_score, 0) AS _website_visitor_count_score,
          COALESCE(career_page_score, 0) AS _career_page_score,
          COALESCE(_visited_website_score, 0) AS _visited_website_score,
          COALESCE(_quarterly_web_score, 0) AS _quarterly_web_score,
         --COALESCE(_distinctAdClicks, 0) AS _distinctAdClicks,
         -- COALESCE(_paid_ads_score_total, 0) AS _quarterly_ads_score,
        FROM
          all_accounts
        LEFT JOIN
          contact_score_limit ON all_accounts._domain = contact_score_limit._domain
        LEFT JOIN
          weekly_web_score ON all_accounts._domain = weekly_web_score._domain
      )    SELECT 
      *,
      date_end AS _extract_date,
      date_start AS _Tminus90_date 
    FROM 
      final_scoring
    ORDER BY 
      _visited_website DESC
      ;
    SET index = index + 1;
  END;
END LOOP;