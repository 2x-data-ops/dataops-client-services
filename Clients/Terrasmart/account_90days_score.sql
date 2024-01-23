DECLARE index INT64 DEFAULT 0;

DECLARE date_ranges ARRAY<STRUCT<max_date DATE, min_date DATE>>;

SET date_ranges = ARRAY(
  SELECT AS STRUCT
    DATE_SUB(_date, INTERVAL 1 DAY)  AS max_date, DATE_SUB(_date, INTERVAL 1 MONTH) AS min_date
  FROM
    UNNEST(GENERATE_DATE_ARRAY('2022-01-01',DATE_ADD(CURRENT_DATE(), INTERVAL 1 MONTH), INTERVAL 1 MONTH)) AS _date
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

    INSERT INTO  `terrasmart.account_90days_score` 
WITH all_accounts AS (
         SELECT _domain, _ebos, _utilityprojects, _tracker, _linkedinurl, _canopy, _prospect, _fixedtilt, _account, _persona, _rep, _midwest ,"Key Account" AS _account_segment
        FROM `x-marketing.terrasmart_mysql.db_key_accounts`
        WHERE _linkedinurl <> 'www.linkedin.com/company/centrica-business-solutions/'
        UNION DISTINCT 
        SELECT _domain, _ebos, _utilityprojects, _tracker, _acc_linkedinurl, _canopy, _prospect, _fixedtilt, _account, _acc_persona, _rep, _midwest ,"Other Account"  AS _account_segment
        FROM `terrasmart.db_consolidated_engagements_log` 
        WHERE _domain NOT IN (SELECT DISTINCT _domain FROM `x-marketing.terrasmart_mysql.db_key_accounts`)
)
, quarterly_contact_engagement AS (
  SELECT 
        *
      FROM (  
        SELECT
          DISTINCT
          _domain,
          #email

          COUNT(DISTINCT CASE WHEN _engagement = 'Email Delivered' THEN CONCAT(_email, _contentTitle) END) AS _distinct_email_delivered,
          COUNT(DISTINCT CASE WHEN _engagement = 'Email Opened' THEN CONCAT(_email, _contentTitle) END) AS _distinct_email_open,
          COUNT(DISTINCT  CASE WHEN _engagement = 'Email Clicked' THEN CONCAT(_email, _contentTitle) END) AS _distinct_email_click,

          #formfilled

          COUNT(DISTINCT CASE WHEN _engagement = 'Email Form Filled' THEN CONCAT(_email, _contentTitle) END ) AS _distinct_contactus_form,

          #paid social 

          COUNT(DISTINCT CASE WHEN _engagement = 'Paid Social Comment' THEN CONCAT(_email, _contentTitle) END ) AS _distinct_paid_social_commnet,
          COUNT(DISTINCT CASE WHEN _engagement = 'Paid Social Follow' THEN CONCAT(_email, _contentTitle) END ) AS _distinct_paid_social_follow,
          COUNT(DISTINCT CASE WHEN _engagement = 'Paid Social Like' THEN CONCAT(_email, _contentTitle) END ) AS _distinct_paid_social_like,
          COUNT(DISTINCT CASE WHEN _engagement = 'Paid Social Share' THEN CONCAT(_email, _contentTitle) END ) AS _distinct_paid_social_share,
          COUNT(DISTINCT CASE WHEN _engagement = 'Paid Social Click' THEN CONCAT(_email, _contentTitle) END ) AS _distinct_paid_social_click,
          COUNT(DISTINCT CASE WHEN _engagement = 'Paid Social Visit' THEN CONCAT(_email, _contentTitle) END ) AS _distinct_paid_social_visit,
          COUNT(DISTINCT CASE WHEN _engagement = 'Paid Social Form Filled' THEN CONCAT(_email, _contentTitle) END ) AS _distinct_paid_social_form_filled,

          #organic social 

         COUNT(DISTINCT CASE WHEN _engagement = 'Organic Social Comment' THEN CONCAT(_email, _contentTitle) END ) AS _distinct_organic_social_comment,
          COUNT(DISTINCT CASE WHEN _engagement = 'Organic Social Follow' THEN CONCAT(_email, _contentTitle) END ) AS _distinct_organic_social_follow,
          COUNT(DISTINCT CASE WHEN _engagement = 'Organic Social Like' THEN CONCAT(_email, _contentTitle) END ) AS _distinct_organic_social_like,
          COUNT(DISTINCT CASE WHEN _engagement = 'Organic Social Share' THEN CONCAT(_email, _contentTitle) END ) AS _distinct_organic_social_share,
          COUNT(DISTINCT CASE WHEN _engagement = 'Organic Social Click' THEN CONCAT(_email, _contentTitle) END ) AS _distinct_organic_social_click,
          COUNT(DISTINCT CASE WHEN _engagement = 'organic Social Visit' THEN CONCAT(_email, _contentTitle) END ) AS _distinct_organic_social_visit,
          COUNT(DISTINCT CASE WHEN _engagement = 'Organic Social Form Filled' THEN CONCAT(_email, _contentTitle) END ) AS _distinct_organic_social_social_form_filled,

          # webinar 
          COUNT(DISTINCT CASE WHEN _engagement = 'Webinar Sing Up' THEN CONCAT(_email, _contentTitle) END ) AS _distinct_webinar_sing_up,
          COUNT(DISTINCT CASE WHEN _engagement = 'Webinar Attendance' THEN CONCAT(_email, _contentTitle) END ) AS _distinct_webinar_attendance,

          #event 
          COUNT(DISTINCT CASE WHEN _engagement = 'Attendance' THEN CONCAT(_email, _contentTitle) END ) AS _distinct_event_attendance,

         FROM  (SELECT DISTINCT * FROM  `terrasmart.db_consolidated_engagements_log`)
           WHERE
                _month BETWEEN date_start AND date_end
                AND 
                _domain IS NOT NULL
              GROUP BY
                1
  )
      ORDER BY 
        _distinct_email_delivered DESC
)
,
 quarterly_contact_scoring AS (

  SELECT
        DISTINCT *,
        ( 
          (CASE --- 2 click
              --WHEN _distinct_email_click >= 4 THEN 10
              WHEN _distinct_email_click >= 2 THEN _distinct_email_click*5
              ELSE 0
          END ) + 
          (CASE --- 1 click
              --WHEN _distinct_email_click >= 4 THEN 10
              WHEN _distinct_email_click >= 1 THEN 10
              ELSE 0
          END ) #Email Click
          ) AS _click_score, 
        (
          (CASE -- 2 - 3 open
              WHEN _distinct_email_open >= 2 THEN _distinct_email_open*5
              --WHEN _distinct_email_open BETWEEN 1 AND 3 THEN 3
              ELSE 0
          END) + 
           (CASE --- 1-3 open
              --WHEN _distinct_email_open >= 4 THEN 5
              WHEN   _distinct_email_open >= 1 THEN 5
              ELSE 0
          END)) AS _open_score,
    #Calculating total email score
          (
             (CASE -- 2 - 3 open
              WHEN _distinct_email_open >= 2 THEN _distinct_email_open*5
              --WHEN _distinct_email_open BETWEEN 1 AND 3 THEN 3
              ELSE 0
          END) + 
           (CASE --- 1-3 open
              --WHEN _distinct_email_open >= 4 THEN 5
              WHEN   _distinct_email_open >= 1 THEN 5
              ELSE 0
          END) + #Email click
          (CASE --- 2 click
              --WHEN _distinct_email_click >= 4 THEN 10
              WHEN _distinct_email_click >= 2 THEN _distinct_email_click*5
              ELSE 0
          END ) + 
          (CASE --- 1 click
              --WHEN _distinct_email_click >= 4 THEN 10
              WHEN _distinct_email_click >= 1 THEN 10
              ELSE 0
          END ) #Email Click
        ) AS _email_score,
CASE WHEN    (
             (CASE -- 2 - 3 open
              WHEN _distinct_email_open >= 2 THEN _distinct_email_open*5
              --WHEN _distinct_email_open BETWEEN 1 AND 3 THEN 3
              ELSE 0
          END) + 
           (CASE --- 1-3 open
              --WHEN _distinct_email_open >= 4 THEN 5
              WHEN   _distinct_email_open >= 1 THEN 5
              ELSE 0
          END) + #Email click
          (CASE --- 2 click
              --WHEN _distinct_email_click >= 4 THEN 10
              WHEN _distinct_email_click >= 2 THEN _distinct_email_click*5
              ELSE 0
          END ) + 
          (CASE --- 1 click
              --WHEN _distinct_email_click >= 4 THEN 10
              WHEN _distinct_email_click >= 1 THEN 10
              ELSE 0
          END ) #Email Click
        ) > 0 AND _distinct_email_delivered > 0 THEN    (
            ( (CASE -- 2 - 3 open
              WHEN _distinct_email_open >= 2 THEN _distinct_email_open*5
              --WHEN _distinct_email_open BETWEEN 1 AND 3 THEN 3
              ELSE 0
          END) + 
           (CASE --- 1-3 open
              --WHEN _distinct_email_open >= 4 THEN 5
              WHEN   _distinct_email_open >= 1 THEN 5
              ELSE 0
          END) + #Email click
          (CASE --- 2 click
              --WHEN _distinct_email_click >= 4 THEN 10
              WHEN _distinct_email_click >= 2 THEN _distinct_email_click*5
              ELSE 0
          END ) + 
          (CASE --- 1 click
              --WHEN _distinct_email_click >= 4 THEN 10
              WHEN _distinct_email_click >= 1 THEN 10
              ELSE 0
          END ) #Email Click
        )) / _distinct_email_delivered
        ELSE 
           0 END AS _monthly_email_score,

        #paid social 
        (CASE --- 1 paid socoal share 
              WHEN _distinct_paid_social_share >= 1 THEN 1
              ELSE 0
          END) AS _paid_social_share_score,
        (CASE --- 1 paid socoal comment 
              WHEN _distinct_paid_social_commnet >= 1 THEN 1
              ELSE 0
          END) AS _paid_social_comment_score, 
        (CASE --- 1 paid socoal follow
              WHEN _distinct_paid_social_follow >= 1 THEN 0.5
              ELSE 0
          END) AS _paid_social_follow_score,
        (CASE --- 1 paid socoal visit 
              WHEN _distinct_paid_social_visit >= 1 THEN 0.5
              ELSE 0
          END) AS _paid_social_visit_score,
        (CASE --- 1 paid socoal click 
              WHEN _distinct_paid_social_click >= 1 THEN 0.5
              ELSE 0
          END) AS _paid_social_click_score, 
        (CASE --- 1 paid socoal like  
              WHEN _distinct_paid_social_like >= 1 THEN 0.5
              ELSE 0
          END) AS _paid_social_like_score,

        #paid social score
         ( (CASE --- 1 paid socoal share 
              WHEN _distinct_paid_social_share >= 1 THEN 1
              ELSE 0
          END) + (CASE --- 1 paid socoal comment 
              WHEN _distinct_paid_social_commnet >= 1 THEN 1
              ELSE 0
          END) + (CASE --- 1 paid socoal follow 
              WHEN _distinct_paid_social_follow >= 1 THEN 0.5
              ELSE 0
          END) + (CASE --- 1 paid socoal visit
              WHEN _distinct_paid_social_visit >= 1 THEN 0.5
              ELSE 0
          END) + (CASE --- 1 paid socoal click 
              WHEN _distinct_paid_social_click >= 1 THEN 0.5
              ELSE 0
          END) + (CASE --- 1 paid socoal like 
              WHEN _distinct_paid_social_like >= 1 THEN 0.5
              ELSE 0
          END) ) AS _paid_social_score,
        


        #organic_social 
        (CASE --- 1 paid socoal share 
              WHEN _distinct_organic_social_share >= 1 THEN 2
              ELSE 0
          END) AS _organic_social_share_score,
        (CASE --- 1 paid socoal comment 
              WHEN _distinct_organic_social_comment >= 1 THEN 2
              ELSE 0
          END) AS _organic_social_comment_score, 
        (CASE --- 1 paid socoal follow 
              WHEN _distinct_organic_social_follow >= 1 THEN 1
              ELSE 0
          END) AS _organic_social_follow_score,
         (CASE --- 1 paid socoal visit 
              WHEN _distinct_organic_social_visit >= 1 THEN 1
              ELSE 0
          END) AS _organic_social_visit_score,
          (CASE --- 1 paid socoal click
              WHEN _distinct_organic_social_click >= 1 THEN 1
              ELSE 0
          END) AS _organic_social_click_score, 
        (CASE --- 1 paid socoal like 
              WHEN _distinct_organic_social_like >= 1 THEN 1
              ELSE 0
          END) AS _organic_social_like_score, 
        
          #organic social score
         ( (CASE --- 1 paid socoal share 
              WHEN _distinct_organic_social_share >= 1 THEN 2
              ELSE 0
          END) + (CASE --- 1 paid socoal comment 
              WHEN _distinct_organic_social_comment >= 1 THEN 2
              ELSE 0
          END) + (CASE --- 1 paid socoal follow  
              WHEN _distinct_organic_social_follow >= 1 THEN 1
              ELSE 0
          END) + (CASE --- 1 paid socoal visit 
              WHEN _distinct_organic_social_visit >= 1 THEN 1
              ELSE 0
          END) + (CASE --- 1 paid socoal click 
              WHEN _distinct_organic_social_click >= 1 THEN 1
              ELSE 0
          END) + (CASE --- 1 paid socoal like 
              WHEN _distinct_organic_social_like >= 1 THEN 1
              ELSE 0
          END)) AS _organic_social_score,

          #webinar 
          (CASE --- attendance 
              WHEN _distinct_webinar_sing_up >= 1 THEN 10
              ELSE 0
          END) AS _webinar_sing_up_score,
          (
            (CASE --- attendance
              WHEN _distinct_webinar_attendance >= 1 THEN 25
              ELSE 0
          END)
          + 
          (CASE --- attendance
              WHEN _distinct_webinar_attendance >= 1 THEN _distinct_webinar_attendance*2
              ELSE 0
          END)
          ) AS _webinar_attendance_score,

          #webinar score 
          (CASE --- attendance
              WHEN _distinct_webinar_sing_up >= 1 THEN 10
              ELSE 0
          END) + (
            (CASE --- attendance 
              WHEN _distinct_webinar_attendance >= 1 THEN 25
              ELSE 0
          END)
          + 
          (CASE --- attendance 
              WHEN _distinct_webinar_attendance >= 1 THEN _distinct_webinar_attendance*2
              ELSE 0
          END)
          ) AS _webinar_score,

          #event
          (
            (CASE --- 1 event 
              WHEN _distinct_event_attendance >= 1 THEN 40
              ELSE 0
          END)
          + 
          (CASE --- 1 additional 
              WHEN _distinct_event_attendance >= 1 THEN _distinct_event_attendance*2
              ELSE 0
          END)
          ) AS _event_score,

          #form filled
          (CASE --- 1 additional 
              WHEN _distinct_contactus_form >= 1 THEN _distinct_contactus_form*15
              ELSE 0
          END) AS _form_filled_score


        FROM
        quarterly_contact_engagement 

 )
 ,contact_score_limit AS (
   SELECT * ,
 ( -- Setting of threshold for max of email score
            IF(_monthly_email_score > 15, 15, _monthly_email_score) ) AS _quarterly_email_score,
( -- Setting of threshold for max of email score
            IF(_paid_social_score > 4, 4, _paid_social_score ) ) AS _quarterly_paid_social,
( -- Setting of threshold for max of email score
            IF(_organic_social_score > 8, 8, _organic_social_score) ) AS _quarterly_organic_social,
( -- Setting of threshold for max of email score
            IF(_webinar_score > 37, 37, _webinar_score) ) AS _quarterly_webinar,
( -- Setting of threshold for max of email score
            IF(_event_score > 42, 42, _event_score) ) AS _quarterly_event_score,
 ( -- Setting of threshold for max of email score
            IF(_form_filled_score > 42, 42,_form_filled_score) ) AS _quarterly_form_filled_score,
 FROM quarterly_contact_scoring
 )
 ,final_scoring AS (
   SELECT all_accounts._domain,
   _ebos, _utilityprojects, _tracker, _linkedinurl, _canopy, _prospect, _fixedtilt, _account, _persona, _rep, _midwest ,_account_segment,
 COALESCE(_distinct_email_delivered, 0 ) AS _distinct_email_delivered,
 COALESCE(_distinct_email_open, 0 ) AS _distinct_email_open,
 COALESCE(_distinct_email_click, 0 ) AS _distinct_email_click,
 COALESCE(_distinct_contactus_form, 0 ) AS _distinct_contactus_form,
 COALESCE(_distinct_paid_social_commnet, 0 ) AS _distinct_paid_social_commnet,
 COALESCE(_distinct_paid_social_follow, 0 ) AS _distinct_paid_social_follow,
 COALESCE(_distinct_paid_social_like, 0 ) AS _distinct_paid_social_like,
 COALESCE(_distinct_paid_social_share, 0 ) AS _distinct_paid_social_share,
 COALESCE(_distinct_paid_social_click, 0 ) AS _distinct_paid_social_click,
 COALESCE(_distinct_paid_social_visit, 0 ) AS _distinct_paid_social_visit,
 COALESCE(_distinct_paid_social_form_filled, 0 ) AS _distinct_paid_social_form_filled,
 COALESCE(_distinct_organic_social_comment, 0 ) AS _distinct_organic_social_comment,
 COALESCE(_distinct_organic_social_follow, 0 ) AS _distinct_organic_social_follow,
 COALESCE(_distinct_organic_social_like, 0 ) AS _distinct_organic_social_like,
 COALESCE(_distinct_organic_social_share, 0 ) AS _distinct_organic_social_share,
 COALESCE(_distinct_organic_social_click, 0 ) AS _distinct_organic_social_click,
 COALESCE(_distinct_organic_social_visit, 0 ) AS _distinct_organic_social_visit,
 COALESCE(_distinct_organic_social_social_form_filled, 0 ) AS _distinct_organic_social_social_form_filled,
 COALESCE(_distinct_webinar_sing_up, 0 ) AS _distinct_webinar_sing_up,
 COALESCE(_distinct_webinar_attendance, 0 ) AS _distinct_webinar_attendance,
 COALESCE(_distinct_event_attendance, 0 ) AS _distinct_event_attendance,
 COALESCE(_click_score, 0 ) AS _click_score,
 COALESCE(_open_score, 0 ) AS _open_score,
 COALESCE(_email_score, 0 ) AS _email_score,
 COALESCE(_monthly_email_score, 0 ) AS _monthly_email_score,
 COALESCE(_paid_social_share_score, 0 ) AS _paid_social_share_score,
 COALESCE(_paid_social_comment_score, 0 ) AS _paid_social_comment_score,
 COALESCE(_paid_social_follow_score, 0 ) AS _paid_social_follow_score,
 COALESCE(_paid_social_visit_score, 0 ) AS _paid_social_visit_score,
 COALESCE(_paid_social_click_score, 0 ) AS _paid_social_click_score,
 COALESCE(_paid_social_like_score, 0 ) AS _paid_social_like_score,
 COALESCE(_paid_social_score, 0 ) AS _paid_social_score,
 COALESCE(_organic_social_share_score, 0 ) AS _organic_social_share_score,
 COALESCE(_organic_social_comment_score, 0 ) AS _organic_social_comment_score,
 COALESCE(_organic_social_follow_score, 0 ) AS _organic_social_follow_score,
 COALESCE(_organic_social_visit_score, 0 ) AS _organic_social_visit_score,
 COALESCE(_organic_social_click_score, 0 ) AS _organic_social_click_score,
 COALESCE(_organic_social_like_score, 0 ) AS _organic_social_like_score,
 COALESCE(_organic_social_score, 0 ) AS _organic_social_score,
 COALESCE(_webinar_sing_up_score, 0 ) AS _webinar_sing_up_score,
 COALESCE(_webinar_attendance_score, 0 ) AS _webinar_attendance_score,
 COALESCE(_webinar_score, 0 ) AS _webinar_score,
 COALESCE(_event_score, 0 ) AS _event_score,
 COALESCE(_form_filled_score, 0 ) AS _form_filled_score,
 COALESCE(_quarterly_email_score, 0 ) AS _quarterly_email_score,
 COALESCE(_quarterly_paid_social, 0 ) AS _quarterly_paid_social,
 COALESCE(_quarterly_organic_social, 0 ) AS _quarterly_organic_social,
 COALESCE(_quarterly_webinar, 0 ) AS _quarterly_webinar,
 COALESCE(_quarterly_event_score, 0 ) AS _quarterly_event_score,
 COALESCE(_quarterly_form_filled_score, 0 ) AS _quarterly_form_filled_score,
 FROM all_accounts
 LEFT JOIN 
 contact_score_limit ON all_accounts._domain = contact_score_limit._domain
 ) SELECT  *,
 (_quarterly_email_score + _quarterly_paid_social + _quarterly_organic_social  + _quarterly_webinar + _quarterly_event_score + _quarterly_form_filled_score) AS _monthly_account_score,
date_end AS _extract_date,
date_start AS _Tminus90_date 
FROM 
      final_scoring;
        -- ORDER BY 
        --   _visited_website DESC
        SET index = index + 1;
  END;
END LOOP;