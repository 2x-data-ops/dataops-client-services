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


DELETE FROM `terrasmart.account_90days_score` WHERE _domain IS NOT NULL;

LOOP
  IF index = array_length(date_ranges) 
    THEN BREAK;
  END IF;
  BEGIN
    DECLARE date_end DATE DEFAULT date_ranges[OFFSET(index)].max_date;
    DECLARE date_start DATE DEFAULT date_ranges[OFFSET(index)].min_date;

    INSERT INTO `terrasmart.account_90days_score`
    -- CREATE OR REPLACE TABLE `terrasmart.account_90days_score` AS
    WITH all_accounts AS (
            SELECT DISTINCT _domain FROM `x-marketing.terrasmart.db_account_health` WHERE _domain IS NOT NULL
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
                COUNT(DISTINCT CASE WHEN _engagement = 'Form Filled' AND REGEXP_CONTAINS(LOWER(_contentTitle), 'contact us|try now|demo') THEN CONCAT(_email, _contentTitle) END ) AS _distinctContactUsForm,
                COUNT(DISTINCT CASE WHEN _engagement = 'Form Filled' AND REGEXP_CONTAINS(LOWER(_contentTitle), 'webinar|wbn') THEN CONCAT(_email, _contentTitle) END ) AS _distinctWebinarForm,
                COUNT(DISTINCT CASE WHEN _engagement = 'Form Filled' AND NOT REGEXP_CONTAINS(LOWER(_contentTitle), 'try now|demo|contact us|webinar|wbn') THEN CONCAT(_email, _contentTitle) END ) AS _distinctGatedContent,
              FROM
                (SELECT DISTINCT * FROM `terrasmart.db_account_health` WHERE _engagement IS NOT NULL)
              WHERE
                DATE(_date) BETWEEN date_start AND date_end
                AND 
                _domain IS NOT NULL
              GROUP BY
                1
            )
            ORDER BY 
              _distinctContactUsForm DESC, _distinctWebinarForm DESC, _distinctGatedContent DESC
          ),
          weekly_contact_scoring AS (
            SELECT
              DISTINCT *,
          #Calculating total email score
              (
                (CASE
                    WHEN _distinctOpen >= 7 THEN 5
                    WHEN _distinctOpen BETWEEN 3 AND 6 THEN 3
                    ELSE 0
                END) + #Email Open
                (CASE
                    WHEN _distinctClick >= 7 THEN 10
                    WHEN _distinctClick BETWEEN 3 AND 6 THEN 5
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
                  WHEN _distinctContactUsForm >= 1 THEN 60
                  ELSE 0
                END)
              ) AS _contact_us_form_score, #Contact Us Form
              (  
                (CASE
                  WHEN _distinctWebinarForm >= 1 THEN (_distinctWebinarForm * 15)
                  ELSE 0
                END) + #Webinar Form
                (CASE
                  WHEN _distinctGatedContent >= 1 THEN (_distinctGatedContent * 15)
                  ELSE 0
                END) 
              ) AS _other_form_fill_score,
            FROM
              weekly_contact_engagement 
          ),
          -- Something is not right here, udpated script caused the pipeline influenced to go blank
          contact_score_limit AS (
            SELECT
              _domain,
              _distinctOpen,
              _distinctClick,
              _distinctContactUsForm,
              _distinctWebinarForm,
              _distinctGatedContent,
              ( -- Setting of threshold for max of email score
                IF(_email_score > 15, 15, _email_score ) ) AS _email_score,
              ( -- Setting of threshold for max of content synd score
                IF(_content_synd_score > 30, 30, _content_synd_score )  ) AS _content_synd_score,
              ( -- Setting of threshold for max of organic social form score
                IF(_organic_social_score > 35, 35, _organic_social_score )  ) AS _organic_social_score,
              ( -- Setting of threshold for max of gated/webinar form score
                IF(_other_form_fill_score > 30, 30, _other_form_fill_score ) + _contact_us_form_score ) AS _form_fill_score,
            FROM
              weekly_contact_scoring
          ),
          weekly_ads_engagement AS (
            SELECT 
                        *
                      FROM (  
                        SELECT DISTINCT 
                  CASE 
                      WHEN _accountdomain LIKE "%www.%"
                      THEN (
                          REGEXP_REPLACE(
                              SUBSTR(_accountdomain, INSTR(_accountdomain, '://') + 3),
                              '(www\\.|/+$)',
                              ''
                          )
                      )
                      ELSE (
                          REGEXP_REPLACE(
                              _accountdomain,
                              '(https?://|www\\.|/+$)',
                              ''
                          )
                      )
                  END AS _accountdomain,
                  RIGHT(_contactemail,LENGTH(_contactemail)-STRPOS(_contactemail,'@')) AS _domain,
                  _contactname,
                  EXTRACT(WEEK FROM DATE(_date)) AS _week,  
                  EXTRACT(YEAR FROM DATE(_date)) AS _year,
                  _engagementtype,
                  CASE
                      WHEN 'Like' IN UNNEST(SPLIT(_engagementtype, ','))
                      THEN COALESCE(
                          COUNT(DISTINCT CONCAT(_accountdomain, _campaignname)),
                          0
                      )
                      ELSE 0
                  END AS _distinctAdLikes,
                  CASE
                      WHEN 'Follow' IN UNNEST(SPLIT(_engagementtype, ','))
                      THEN COALESCE(
                          COUNT(DISTINCT CONCAT(_accountdomain, _campaignname)),
                          0
                      )
                      ELSE 0
                  END AS _distinctAdFollow,
                  CASE
                      WHEN 'Comment' IN UNNEST(SPLIT(_engagementtype, ','))
                      THEN COALESCE(
                          COUNT(DISTINCT CONCAT(_accountdomain, _campaignname)),
                          0
                      )
                      ELSE 0
                  END AS _distinctAdComment,
                  CASE
                      WHEN 'Share' IN UNNEST(SPLIT(_engagementtype, ','))
                      THEN COALESCE(
                          COUNT(DISTINCT CONCAT(_accountdomain, _campaignname)),
                          0
                      )
                      ELSE 0
                  END AS _distinctAdShare,
                  _accountName,
                  _campaignname
              FROM `x-marketing.gibraltar_mysql.db_airtable_abm_engagement`,
              UNNEST(SPLIT(_engagementtype, ',')) AS sepEngagementType
              WHERE TRIM(sepEngagementType) IN (
                  'Like','Share','Comment','Follow'
                  )
              AND RIGHT(_contactemail,LENGTH(_contactemail)-STRPOS(_contactemail,'@')) != '2x.marketing'
              AND DATE(_date) BETWEEN date_start AND date_end
              GROUP BY 1, 2, 3, 4, 5, _engagementtype,_accountName,sepEngagementType,_campaignname
                      )
                      -- ORDER BY 
                      --   _distinctContactUsForm DESC, _distinctWebinarForm DESC, _distinctGatedContent DESC
          ),
          weekly_account_scoring AS (
            SELECT
              DISTINCT *,
              (
                (CASE 
                  WHEN _distinctAdLikes >= 1 THEN 5 
                  ELSE 0 
                END)
                +
                (CASE 
                  WHEN _distinctAdFollow >= 1 THEN 5 
                  ELSE 0 
                END)
                +
                (CASE 
                  WHEN _distinctAdComment >= 1 THEN 10
                  ELSE 0 
                END)
                +
                (CASE 
                  WHEN _distinctAdShare >= 1 THEN 15
                  ELSE 0 
                END)
              ) AS _ads_score
              FROM 
                weekly_ads_engagement
          ),
          account_score_limit AS (
            SELECT
              _accountdomain,
              _distinctAdLikes,
              _distinctAdFollow,
              _distinctAdComment,
              _distinctAdShare,
              ( -- Setting of threshold for max of email score
                IF(_ads_score > 70, 70, _ads_score)
              ) AS _ads_score
            FROM
              weekly_account_scoring
          ),
          final_scoring AS (
            SELECT
              all_accounts._domain, 
              COALESCE(_distinctOpen, 0 ) AS _distinctOpen, 
              COALESCE(_distinctClick, 0 ) AS _distinctClick, 
              COALESCE(_distinctContactUsForm, 0 ) AS _distinctContactUsForm, 
              COALESCE(_distinctWebinarForm, 0 ) AS _distinctWebinarForm, 
              COALESCE(_distinctGatedContent, 0 ) AS _distinctGatedContent, 
              COALESCE(_email_score, 0 ) AS _quarterly_email_score, 
              COALESCE(_content_synd_score, 0 ) AS  _quarterly_content_synd_score, 
              COALESCE(_organic_social_score, 0 ) AS  _quarterly_organic_social_score, 
              COALESCE(_form_fill_score, 0) AS  _quarterly_form_fill_score,
              COALESCE(_distinctAdLikes, 0) AS _distinctAdLikes,
              COALESCE(_distinctAdFollow, 0) AS _distinctAdFollow,
              COALESCE(_distinctAdComment, 0) AS _distinctAdComment,
              COALESCE(_distinctAdShare, 0) AS _distinctAdShare,
              COALESCE(_ads_score, 0) AS _quarterly_ads_score,
              -- COALESCE(_website_time_spent, 0) AS _website_time_spent,
              -- COALESCE(_website_page_view, 0) AS _website_page_view,
              -- COALESCE(_website_visitor_count, 0) AS _website_visitor_count,
              -- IF(_visited_website IS NULL, false, _visited_website) AS _visited_website,
              -- COALESCE(_website_time_spent_score, 0) AS _website_time_spent_score,
              -- COALESCE(_website_page_view_score, 0) AS _website_page_view_score,
              -- COALESCE(_website_visitor_count_score, 0) AS _website_visitor_count_score,
              -- COALESCE(_visited_website_score, 0) AS _visited_website_score,
              -- COALESCE(_quarterly_web_score, 0) AS _quarterly_web_score,
              -- COALESCE(_distinctAdClicks, 0) AS _distinctAdClicks,
              -- COALESCE(_quarterly_ads_score, 0) AS _quarterly_ads_score,
            FROM
              all_accounts
            LEFT JOIN
              contact_score_limit ON all_accounts._domain = contact_score_limit._domain
            LEFT JOIN
              account_score_limit ON all_accounts._domain = account_score_limit._accountdomain
            -- LEFT JOIN
            --   weekly_web_score ON all_accounts._domain = weekly_web_score._domain
            -- LEFT JOIN
            --   weekly_ad_clicks ON all_accounts._domain = weekly_ad_clicks._domain
          )
        SELECT 
          *,
          date_end AS _extract_date,
          date_start AS _Tminus90_date 
        FROM 
          final_scoring;
        -- ORDER BY 
        --   _visited_website DESC
        SET index = index + 1;
  END;
END LOOP;
