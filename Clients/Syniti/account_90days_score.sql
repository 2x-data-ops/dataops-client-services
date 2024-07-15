CREATE OR REPLACE TABLE `x-marketing.syniti.account_90days_score` AS

WITH all_accounts AS (
  SELECT DISTINCT
    _domain, 
    -- _ebos, 
    -- _utilityprojects, 
    -- _tracker, 
    -- _acc_linkedinurl, 
    -- _canopy, 
    -- _prospect, 
    -- _fixedtilt, 
    -- _account, 
    -- _acc_persona, 
    -- _rep,
    -- _midwest ,
    -- "Other Account"  AS _account_segment,
    --  _type,  
  --       ROW_NUMBER() OVER(PARTITION BY LOWER(_domain) ORDER BY _account
  --  DESC) rowss
  FROM `syniti.db_consolidated_engagements_log`
),
quarterly_contact_engagement AS (
  SELECT *
  FROM (  
  SELECT DISTINCT
    _domain AS _domain,
  -- _month,
    #email
    COALESCE(SUM( CASE WHEN _engagement = 'Email Delivered' THEN 1 END),0) AS _distinct_email_delivered,
    COALESCE(SUM( CASE WHEN _engagement = 'Email Opened' THEN 1 END),0) AS _distinct_email_open,
    COALESCE(SUM(  CASE WHEN _engagement = 'Email Clicked' THEN 1 END),0) AS _distinct_email_click,

    #formfilled

    COALESCE(SUM( CASE WHEN _engagement = 'Email Form Filled' THEN 1 END),0) AS _distinct_contactus_form,

    #paid social 
    -- COALESCE(SUM( CASE WHEN _engagement = 'Paid Social Comment' THEN SAFE_CAST(_description AS INT64)  END ),0) AS _distinct_paid_social_commnet,
    -- COALESCE(SUM( CASE WHEN _engagement = 'Paid Social Follow' THEN SAFE_CAST(_description AS INT64)  END ),0) AS _distinct_paid_social_follow,
    -- COALESCE(SUM( CASE WHEN _engagement = 'Paid Social Like' THEN SAFE_CAST(_description AS INT64)  END ),0) AS _distinct_paid_social_like,
    -- COALESCE(SUM( CASE WHEN _engagement = 'Paid Social Share' THEN SAFE_CAST(_description AS INT64)  END ),0) AS _distinct_paid_social_share,
    -- COALESCE(SUM( CASE WHEN _engagement = 'Paid Social Click' THEN SAFE_CAST(_description AS INT64)  END ),0) AS _distinct_paid_social_click,
    -- COALESCE(SUM( CASE WHEN _engagement = 'Paid Social Visit' THEN SAFE_CAST(_description AS INT64)  END ),0) AS _distinct_paid_social_visit,
    -- COALESCE(SUM(CASE WHEN _engagement = 'Paid Social Form Filled' THEN 1 END ),0) AS _distinct_paid_social_form_filled,

    #organic social 

    -- COALESCE(SUM( CASE WHEN _engagement = 'Organic Social Comment' THEN SAFE_CAST(_description AS INT64)  END ),0) AS _distinct_organic_social_comment,
    -- COALESCE(SUM( CASE WHEN _engagement = 'Organic Social Follow' THEN SAFE_CAST(_description AS INT64)  END ),0) AS _distinct_organic_social_follow,
    -- COALESCE(SUM( CASE WHEN _engagement = 'Organic Social Like' THEN SAFE_CAST(_description AS INT64)  END ),0) AS _distinct_organic_social_like,
    -- COALESCE(SUM( CASE WHEN _engagement = 'Organic Social Share' THEN SAFE_CAST(_description AS INT64)  END ),0) AS _distinct_organic_social_share,
    -- COALESCE(SUM( CASE WHEN _engagement = 'Organic Social Click' THEN SAFE_CAST(_description AS INT64)  END ),0) AS _distinct_organic_social_click,
    -- COALESCE(SUM( CASE WHEN _engagement = 'organic Social Visit' THEN SAFE_CAST(_description AS INT64)  END ),0) AS _distinct_organic_social_visit,
    -- COALESCE(COUNT( CASE WHEN _engagement = 'Organic Social Form Filled' THEN CONCAT(_email, _contentTitle) END ),0) AS _distinct_organic_social_social_form_filled,

    # webinar 
    -- COALESCE(SUM( CASE WHEN _engagement = 'Webinar Sign Up' THEN 1 END ),0) AS _distinct_webinar_sing_up,
    -- COALESCE(SUM( CASE WHEN _engagement = 'Webinar Attendees' THEN 1 END ),0) AS _distinct_webinar_attendance,

    #event 
    -- COALESCE(SUM( CASE WHEN _description = 'Experience Event' THEN 1 END ),0) AS _distinct_experience_event_attendance,
    -- COALESCE(SUM( CASE WHEN _description = 'Educational Event' THEN 1 END ),0) AS _distinct_educational_event_attendance,

    #web visit
    -- COALESCE(COUNT( CASE WHEN _engagement = "Web Visit" THEN CONCAT(_email, _contentTitle,CAST(_timestamp AS STRING)) END ),0) AS _distinct_web_visit,
  FROM  (SELECT DISTINCT * FROM  `syniti.db_consolidated_engagements_log`)
          --  WHERE
          --      _month BETWEEN date_start AND date_end
          --      AND 
          --       _domain IS NOT NULL
              GROUP BY
                1
          --      -- ,2
                )
),
quarterly_contact_scoring AS (
  SELECT
    DISTINCT *,
    ( -- 2 clicks
      (CASE WHEN _distinct_email_click >= 1 THEN 10 ELSE 0 END ) +
      (CASE WHEN _distinct_email_click >= 2 THEN (_distinct_email_click - 1) * 5 ELSE 0 END) 
    ) AS _click_score, 
    ( -- 2 click
      (CASE WHEN _distinct_email_open >= 1 THEN _distinct_email_open * 5  ELSE 0 END )
    ) AS _open_score,
#Calculating total email score
      (
      (
        -- 2 click
        (CASE WHEN _distinct_email_open >= 1 THEN _distinct_email_open * 5 ELSE 0 END )
      ) + #Email click
      ( -- 2 click
      (CASE WHEN _distinct_email_click >= 1 THEN 10 ELSE 0 END ) +
      (CASE WHEN _distinct_email_click >= 2 THEN (_distinct_email_click - 1) * 5 ELSE 0 END)
      ) 
      #Email Click
    ) AS _email_score,
    CASE WHEN _distinct_email_delivered > 0  THEN ((
      (CASE --- 2 click
          WHEN _distinct_email_open >= 1 THEN _distinct_email_open * 5 ELSE 0 END )
      ) + #Email click
        ( 
      (CASE --- 2 click
          WHEN _distinct_email_click >= 1 THEN 10 ELSE 0 END ) +
      (CASE WHEN _distinct_email_click >= 2 THEN (_distinct_email_click -1 ) * 5 ELSE 0 END) 
      ) 
      #Email Click
        + 
    (CASE --- 1 additional 
          WHEN _distinct_contactus_form >= 1 THEN _distinct_contactus_form * 15 ELSE 0 END)
    )  / _distinct_email_delivered 
    ELSE 0 END
    
      AS _monthly_email_score
  FROM quarterly_contact_engagement
),
contact_score_limit AS (
   SELECT * ,
 --( -- Setting of threshold for max of email score
           -- IF(_monthly_email_score > 15, 15, _monthly_email_score) ) 
           _monthly_email_score AS _quarterly_email_score,
-- -- ( -- Setting of threshold for max of email score
-- --             IF(_paid_social_score > 4, 4, _paid_social_score ) ) 
--             _paid_social_score AS _quarterly_paid_social,
-- -- ( -- Setting of threshold for max of email score
-- --             IF(_organic_social_score > 8, 8, _organic_social_score) ) 
--             _organic_social_score AS _quarterly_organic_social,
-- -- ( -- Setting of threshold for max of email score
-- --             IF(_webinar_score > 37, 37, _webinar_score) )
--             _webinar_score AS _quarterly_webinar,
-- -- ( -- Setting of threshold for max of email score
-- --             IF(_event_score > 42, 42, _event_score) ) 
--             _event_score AS _quarterly_event_score,
-- --  ( -- Setting of threshold for max of email score
-- --             IF(_form_filled_score > 42, 42,_form_filled_score) ) 
--             _form_filled_score AS _quarterly_form_filled_score,
--             _web_visit_score AS _quaterly_web_visit_score,
 FROM quarterly_contact_scoring
),
final_scoring AS (
  SELECT
    all_accounts.*,
    COALESCE(_distinct_email_delivered, 0 ) AS _distinct_email_delivered,
    COALESCE(_distinct_email_open, 0 ) AS _distinct_email_open,
    COALESCE(_distinct_email_click, 0 ) AS _distinct_email_click,
    COALESCE(_distinct_contactus_form, 0 ) AS _distinct_contactus_form,
    -- COALESCE(_distinct_paid_social_commnet, 0 ) AS _distinct_paid_social_commnet,
    -- COALESCE(_distinct_paid_social_follow, 0 ) AS _distinct_paid_social_follow,
    -- COALESCE(_distinct_paid_social_like, 0 ) AS _distinct_paid_social_like,
    -- COALESCE(_distinct_paid_social_share, 0 ) AS _distinct_paid_social_share,
    -- COALESCE(_distinct_paid_social_click, 0 ) AS _distinct_paid_social_click,
    -- COALESCE(_distinct_paid_social_visit, 0 ) AS _distinct_paid_social_visit,
    -- COALESCE(_distinct_paid_social_form_filled, 0 ) AS _distinct_paid_social_form_filled,
    -- COALESCE(_distinct_organic_social_comment, 0 ) AS _distinct_organic_social_comment,
    -- COALESCE(_distinct_organic_social_follow, 0 ) AS _distinct_organic_social_follow,
    -- COALESCE(_distinct_organic_social_like, 0 ) AS _distinct_organic_social_like,
    -- COALESCE(_distinct_organic_social_share, 0 ) AS _distinct_organic_social_share,
    -- COALESCE(_distinct_organic_social_click, 0 ) AS _distinct_organic_social_click,
    -- COALESCE(_distinct_organic_social_visit, 0 ) AS _distinct_organic_social_visit,
    -- COALESCE(_distinct_organic_social_social_form_filled, 0 ) AS _distinct_organic_social_social_form_filled,
    -- COALESCE(_distinct_webinar_sing_up, 0 ) AS _distinct_webinar_sing_up,
    -- COALESCE(_distinct_webinar_attendance, 0 ) AS _distinct_webinar_attendance,
    -- COALESCE(_distinct_educational_event_attendance, 0 ) AS _distinct_educational_event_attendance,
    -- COALESCE(_distinct_experience_event_attendance, 0 ) AS _distinct_experience_event_attendance,
    COALESCE(_click_score, 0 ) AS _click_score,
    COALESCE(_open_score, 0 ) AS _open_score,
    COALESCE(_email_score, 0 ) AS _email_score,
    COALESCE(_monthly_email_score, 0 ) AS _monthly_email_score,
    -- COALESCE(_paid_social_share_score, 0 ) AS _paid_social_share_score,
    -- COALESCE(_paid_social_comment_score, 0 ) AS _paid_social_comment_score,
    -- COALESCE(_paid_social_follow_score, 0 ) AS _paid_social_follow_score,
    -- COALESCE(_paid_social_visit_score, 0 ) AS _paid_social_visit_score,
    -- COALESCE(_paid_social_click_score, 0 ) AS _paid_social_click_score,
    -- COALESCE(_paid_social_like_score, 0 ) AS _paid_social_like_score,
    -- COALESCE(_paid_social_score, 0 ) AS _paid_social_score,
    -- COALESCE(_organic_social_share_score, 0 ) AS _organic_social_share_score,
    -- COALESCE(_organic_social_comment_score, 0 ) AS _organic_social_comment_score,
    -- COALESCE(_organic_social_follow_score, 0 ) AS _organic_social_follow_score,
    -- COALESCE(_organic_social_visit_score, 0 ) AS _organic_social_visit_score,
    -- COALESCE(_organic_social_click_score, 0 ) AS _organic_social_click_score,
    -- COALESCE(_organic_social_like_score, 0 ) AS _organic_social_like_score,
    -- COALESCE(_organic_social_score, 0 ) AS _organic_social_score,
    -- COALESCE(_webinar_sing_up_score, 0 ) AS _webinar_sing_up_score,
    -- COALESCE(_webinar_attendance_score, 0 ) AS _webinar_attendance_score,
    -- COALESCE(_webinar_score, 0 ) AS _webinar_score,
    -- COALESCE(_educational_event_attendance_score, 0 ) AS _educational_event_attendance_score,
    -- COALESCE(_experience_event_attendance_score, 0 ) AS _experience_event_attendance_score,
    -- COALESCE(_event_score, 0 ) AS _event_score,
    -- COALESCE(_form_filled_score, 0 ) AS _form_filled_score,
    COALESCE(_quarterly_email_score, 0 ) AS _quarterly_email_score,
    -- COALESCE(_quarterly_paid_social, 0 ) AS _quarterly_paid_social,
    -- COALESCE(_quarterly_organic_social, 0 ) AS _quarterly_organic_social,
    -- COALESCE(_quarterly_webinar, 0 ) AS _quarterly_webinar,
    -- COALESCE(_quarterly_event_score, 0 ) AS _quarterly_event_score,
    -- COALESCE(_quarterly_form_filled_score, 0 ) AS _quarterly_form_filled_score,
    -- COALESCE(_distinct_web_visit, 0 ) AS _distinct_web_visit,
    -- COALESCE(_web_visit_score, 0 ) AS _quaterly_web_visit_score,
  FROM all_accounts
  LEFT JOIN  contact_score_limit
    ON all_accounts._domain = contact_score_limit._domain
),
get_total AS ( 
  SELECT  *,
    (_quarterly_email_score  /* _quarterly_paid_social */ /* + _quarterly_organic_social */  /* + _quarterly_webinar */ /* + _quarterly_event_score +  *//* _quaterly_web_visit_score */ ) AS _monthly_account_score,
  -- date_start AS _extract_date,
  -- date_end AS _Tminus90_date, 
  -- EXTRACT(YEAR FROM date_start) AS _year,
  -- _min_quater_date AS _min_quater_date,
  -- _max_quater_date AS _max_quater_date,
FROM final_scoring
)
SELECT
  get_total.*,
  0.0 AS _cumulative_monthly_engagement_score,
  0.0 AS _cumulative_quaterly_engagement_score,
  0.0 AS _cumulative_year_engagement_score,
FROM get_total;