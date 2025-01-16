------------------------------------------------------------------------------------------------------------
---------------------------------account score--------------------------------------------------------------
------------------------------------------------------------------------------------------------------------
DECLARE index INT64 DEFAULT 0;

DECLARE date_ranges ARRAY<STRUCT<max_date DATE, min_date DATE, first_1_month DATE ,first_2_month DATE ,
 first_3_month DATE, first_4_month DATE,
first_5_month DATE ,
 first_6_month DATE,_min_quater_date DATE,_max_quater_date DATE >>;

SET date_ranges = ARRAY(
  SELECT AS STRUCT
    DATE_SUB(_date, INTERVAL 1 DAY)  AS max_date, 
    DATE_SUB(_date, INTERVAL 1 MONTH) AS min_date,
    DATE_SUB(_date, INTERVAL 1 MONTH)  AS first_1_month,
    DATE_SUB(_date, INTERVAL 2 MONTH) AS first_2_month,
    DATE_SUB(_date, INTERVAL 3 MONTH) AS first_3_month,
    DATE_SUB(_date, INTERVAL 4 MONTH) AS first_4_month,
    DATE_SUB(_date, INTERVAL 5 MONTH) AS first_5_month,
    DATE_SUB(_date, INTERVAL 6 MONTH) AS first_6_month,
    DATE_TRUNC(DATE_SUB(_date, INTERVAL 1 MONTH), QUARTER) AS _min_quater_date,
    DATE_ADD(DATE_TRUNC(DATE_SUB(_date, INTERVAL 1 MONTH), QUARTER),INTERVAL 1 QUARTER)-1 AS _max_quater_date,
    
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
    DECLARE first_1_month DATE DEFAULT date_ranges[OFFSET(index)].first_1_month;
    DECLARE first_2_month DATE DEFAULT date_ranges[OFFSET(index)].first_2_month;
    DECLARE first_3_month DATE DEFAULT date_ranges[OFFSET(index)].first_3_month;
    DECLARE first_4_month DATE DEFAULT date_ranges[OFFSET(index)].first_4_month;
    DECLARE first_5_month DATE DEFAULT date_ranges[OFFSET(index)].first_5_month;
    DECLARE first_6_month DATE DEFAULT date_ranges[OFFSET(index)].first_6_month;
    DECLARE _min_quater_date DATE DEFAULT date_ranges[OFFSET(index)]._min_quater_date;
    DECLARE _max_quater_date DATE DEFAULT date_ranges[OFFSET(index)]._max_quater_date;

    INSERT INTO  `terrasmart.account_90days_score` 
    
     WITH key_account AS (
SELECT * EXCEPT (rowss)
FROM (
SELECT 
      _domain AS _domain, 
      _ebos, 
      _utilityprojects, 
      _tracker, 
      _linkedinurl, 
      _canopy, 
      _prospect, 
      _fixedtilt, 
      _account, 
      _persona, 
      _rep, 
      _midwest,
      "Key Account" AS _account_segment,
       _type,
      ROW_NUMBER() OVER(PARTITION BY LOWER(_domain),LOWER(_account) ORDER BY _account
 DESC) rowss
      FROM `x-marketing.terrasmart_mysql_2.db_key_accounts`
) WHERE rowss = 1
) , other_account AS (
      SELECT * EXCEPT (rowss)
FROM ( SELECT 
      _domain, 
      _ebos, 
      _utilityprojects, 
      _tracker, 
      _acc_linkedinurl, 
      _canopy, 
      _prospect, 
      _fixedtilt, 
      _account, 
      _acc_persona, 
      _rep, _midwest ,
      "Other Account"  AS _account_segment,
       _type,
      ROW_NUMBER() OVER(PARTITION BY LOWER(_domain) ORDER BY _account
 DESC) rowss
      FROM `terrasmart.db_consolidated_engagements_log` 
        WHERE _domain NOT IN (SELECT DISTINCT _domain AS _domain FROM `x-marketing.terrasmart_mysql_2.db_key_accounts`)

        ) WHERE rowss = 1
)
, all_accounts AS (
SELECT * FROM key_account
UNION ALL 
SELECT * FROM other_account
        ), quarterly_contact_engagement AS (
          SELECT 
          *
          FROM (  
            SELECT
            DISTINCT
            _domain AS _domain,
           -- _month,
            #email
            COALESCE(SUM( CASE WHEN _engagement = 'Email Delivered' THEN 1 END),0) AS _distinct_email_delivered,
            COALESCE(SUM( CASE WHEN _engagement = 'Email Opened' THEN 1 END),0) AS _distinct_email_open,
           COALESCE(SUM(  CASE WHEN _engagement = 'Email Clicked' THEN 1 END),0) AS _distinct_email_click,

          #formfilled

          COALESCE(SUM( CASE WHEN _engagement = 'Email Form Filled' THEN 1 END),0) AS _distinct_contactus_form,

          #paid social 

          COALESCE(SUM( CASE WHEN _engagement = 'Paid Social Comment' THEN SAFE_CAST(_description AS INT64)  END ),0) AS _distinct_paid_social_commnet,
          COALESCE(SUM( CASE WHEN _engagement = 'Paid Social Follow' THEN SAFE_CAST(_description AS INT64)  END ),0) AS _distinct_paid_social_follow,
          COALESCE(SUM( CASE WHEN _engagement = 'Paid Social Like' THEN SAFE_CAST(_description AS INT64)  END ),0) AS _distinct_paid_social_like,
          COALESCE(SUM( CASE WHEN _engagement = 'Paid Social Share' THEN SAFE_CAST(_description AS INT64)  END ),0) AS _distinct_paid_social_share,
          COALESCE(SUM( CASE WHEN _engagement = 'Paid Social Click' THEN SAFE_CAST(_description AS INT64)  END ),0) AS _distinct_paid_social_click,
          COALESCE(SUM( CASE WHEN _engagement = 'Paid Social Visit' THEN SAFE_CAST(_description AS INT64)  END ),0) AS _distinct_paid_social_visit,
          COALESCE(SUM(CASE WHEN _engagement = 'Paid Social Form Filled' THEN 1 END ),0) AS _distinct_paid_social_form_filled,

          #organic social 

         COALESCE(SUM( CASE WHEN _engagement = 'Organic Social Comment' THEN SAFE_CAST(_description AS INT64)  END ),0) AS _distinct_organic_social_comment,
          COALESCE(SUM( CASE WHEN _engagement = 'Organic Social Follow' THEN SAFE_CAST(_description AS INT64)  END ),0) AS _distinct_organic_social_follow,
          COALESCE(SUM( CASE WHEN _engagement = 'Organic Social Like' THEN SAFE_CAST(_description AS INT64)  END ),0) AS _distinct_organic_social_like,
          COALESCE(SUM( CASE WHEN _engagement = 'Organic Social Share' THEN SAFE_CAST(_description AS INT64)  END ),0) AS _distinct_organic_social_share,
          COALESCE(SUM( CASE WHEN _engagement = 'Organic Social Click' THEN SAFE_CAST(_description AS INT64)  END ),0) AS _distinct_organic_social_click,
          COALESCE(SUM( CASE WHEN _engagement = 'organic Social Visit' THEN SAFE_CAST(_description AS INT64)  END ),0) AS _distinct_organic_social_visit,
          COALESCE(COUNT( CASE WHEN _engagement = 'Organic Social Form Filled' THEN CONCAT(_email, _contentTitle) END ),0) AS _distinct_organic_social_social_form_filled,

          # webinar 
          COALESCE(SUM( CASE WHEN _engagement = 'Webinar Sign Up' THEN 1 END ),0) AS _distinct_webinar_sing_up,
          COALESCE(SUM( CASE WHEN _engagement = 'Webinar Attendees' THEN 1 END ),0) AS _distinct_webinar_attendance,

          #event 
          COALESCE(SUM( CASE WHEN _description = 'Experience Event' THEN 1 END ),0) AS _distinct_experience_event_attendance,
         COALESCE(SUM( CASE WHEN _description = 'Educational Event' THEN 1 END ),0) AS _distinct_educational_event_attendance,

          #web visit
          COALESCE(COUNT( CASE WHEN _engagement = "Web Visit" THEN CONCAT(_email, _contentTitle,CAST(_timestamp AS STRING)) END ),0) AS _distinct_web_visit,
         FROM  (SELECT DISTINCT * FROM  `terrasmart.db_consolidated_engagements_log`)
           WHERE
               _month BETWEEN date_start AND date_end
               AND 
                _domain IS NOT NULL
              GROUP BY
                1
               -- ,2
                )
                ORDER BY 
                _distinct_email_delivered DESC
      ),quarterly_contact_scoring AS (
        SELECT
        DISTINCT *,
        ( 
          (CASE --- 2 click
              WHEN _distinct_email_click >= 1 THEN 10
              
              ELSE 0
          END )+
          (CASE WHEN _distinct_email_click >= 2 THEN (_distinct_email_click-1)*5 
          ELSE 0
          END) 
          ) AS _click_score, 
        (
          (CASE --- 2 click
              WHEN _distinct_email_open >= 1 THEN _distinct_email_open*5
              
              ELSE 0
          END )
        ) AS _open_score,
    #Calculating total email score
         ( (
          (CASE --- 2 click
              WHEN _distinct_email_open >= 1 THEN _distinct_email_open*5
              
              ELSE 0
          END )
        ) + #Email click
           ( 
          (CASE --- 2 click
              WHEN _distinct_email_click >= 1 THEN 10
              
              ELSE 0
          END )+
          (CASE WHEN _distinct_email_click >= 2 THEN (_distinct_email_click-1)*5 
          ELSE 0
          END) 
          ) 
          #Email Click
        ) AS _email_score,
         CASE WHEN _distinct_email_delivered > 0  THEN ((
          (CASE --- 2 click
              WHEN _distinct_email_open >= 1 THEN _distinct_email_open*5
              
              ELSE 0
          END )
        ) + #Email click
           ( 
          (CASE --- 2 click
              WHEN _distinct_email_click >= 1 THEN 10
              
              ELSE 0
          END )+
          (CASE WHEN _distinct_email_click >= 2 THEN (_distinct_email_click-1)*5 
          ELSE 0
          END) 
          ) 
          #Email Click
           + 
        (CASE --- 1 additional 
              WHEN _distinct_contactus_form >= 1 THEN _distinct_contactus_form*15
              ELSE 0
          END)
        )  / _distinct_email_delivered 
        ELSE 0 END
        
         AS _monthly_email_score,


        #paid social 
       (CASE --- 1 paid socoal share 
              WHEN _distinct_paid_social_share >= 1 THEN 1*_distinct_paid_social_share
              ELSE 0
          END) AS _paid_social_share_score,
        (CASE --- 1 paid socoal comment 
              WHEN _distinct_paid_social_commnet >= 1 THEN 1*_distinct_paid_social_commnet
              ELSE 0
          END) AS _paid_social_comment_score, 
        (CASE --- 1 paid socoal follow
              WHEN _distinct_paid_social_follow >= 1 THEN 0.5*_distinct_paid_social_follow
              ELSE 0
          END) AS _paid_social_follow_score,
        (CASE --- 1 paid socoal visit 
              WHEN _distinct_paid_social_visit >= 1 THEN 0.5*_distinct_paid_social_visit
              ELSE 0
          END) AS _paid_social_visit_score,
        (CASE --- 1 paid socoal click 
              WHEN _distinct_paid_social_click >= 1 THEN 0.5*_distinct_paid_social_click
              ELSE 0
          END) AS _paid_social_click_score, 
        (CASE --- 1 paid socoal like  
              WHEN _distinct_paid_social_like >= 1 THEN 0.5*_distinct_paid_social_like
              ELSE 0
          END) AS _paid_social_like_score,

        #paid social score
         ( (CASE --- 1 paid socoal share 
              WHEN _distinct_paid_social_share >= 1 THEN 1*_distinct_paid_social_share
              ELSE 0
          END) + (CASE --- 1 paid socoal comment 
              WHEN _distinct_paid_social_commnet >= 1 THEN 1*_distinct_paid_social_commnet
              ELSE 0
          END) + (CASE --- 1 paid socoal follow 
              WHEN _distinct_paid_social_follow >= 1 THEN 0.5*_distinct_paid_social_follow
              ELSE 0
          END) + (CASE --- 1 paid socoal visit
              WHEN _distinct_paid_social_visit >= 1 THEN 0.5*_distinct_paid_social_visit
              ELSE 0
          END) + (CASE --- 1 paid socoal click 
              WHEN _distinct_paid_social_click >= 1 THEN 0.5*_distinct_paid_social_click
              ELSE 0
          END) + (CASE --- 1 paid socoal like 
              WHEN _distinct_paid_social_like >= 1 THEN 0.5*_distinct_paid_social_like
              ELSE 0
          END) ) AS _paid_social_score,
        


        #organic_social 
        (CASE --- 1 paid socoal share 
              WHEN _distinct_organic_social_share >= 1 THEN 2*_distinct_organic_social_share
              ELSE 0
          END) AS _organic_social_share_score,
        (CASE --- 1 paid socoal comment 
              WHEN _distinct_organic_social_comment >= 1 THEN 2*_distinct_organic_social_comment
              ELSE 0
          END) AS _organic_social_comment_score, 
        (CASE --- 1 paid socoal follow 
              WHEN _distinct_organic_social_follow >= 1 THEN 1*_distinct_organic_social_follow
              ELSE 0
          END) AS _organic_social_follow_score,
         (CASE --- 1 paid socoal visit 
              WHEN _distinct_organic_social_visit >= 1 THEN 1*_distinct_organic_social_visit
              ELSE 0
          END) AS _organic_social_visit_score,
          (CASE --- 1 paid socoal click
              WHEN _distinct_organic_social_click >= 1 THEN 1*_distinct_organic_social_click
              ELSE 0
          END) AS _organic_social_click_score, 
        (CASE --- 1 paid socoal like 
              WHEN _distinct_organic_social_like >= 1 THEN 1*_distinct_organic_social_like
              ELSE 0
          END) AS _organic_social_like_score, 
        
          #organic social score
         ( (CASE --- 1 paid socoal share 
              WHEN _distinct_organic_social_share >= 1 THEN 2*_distinct_organic_social_share
              ELSE 0
          END) + (CASE --- 1 paid socoal comment 
              WHEN _distinct_organic_social_comment >= 1 THEN 2*_distinct_organic_social_comment
              ELSE 0
          END) + (CASE --- 1 paid socoal follow  
              WHEN _distinct_organic_social_follow >= 1 THEN 1*_distinct_organic_social_follow
              ELSE 0
          END) + (CASE --- 1 paid socoal visit 
              WHEN _distinct_organic_social_visit >= 1 THEN 1*_distinct_organic_social_visit
              ELSE 0
          END) + (CASE --- 1 paid socoal click 
              WHEN _distinct_organic_social_click >= 1 THEN 1*_distinct_organic_social_click
              ELSE 0
          END) + (CASE --- 1 paid socoal like 
              WHEN _distinct_organic_social_like >= 1 THEN 1*_distinct_organic_social_like
              ELSE 0
          END)) AS _organic_social_score,
          #webinar 
          (CASE --- attendance 
              WHEN _distinct_webinar_sing_up >= 1 THEN 10*_distinct_webinar_sing_up
              ELSE 0
          END) AS _webinar_sing_up_score,
          (
            (CASE --- attendance
              WHEN _distinct_webinar_attendance >= 1 THEN 25
              ELSE 0
          END)
          + 
          (CASE --- attendance
              WHEN _distinct_webinar_attendance >= 2 THEN (_distinct_webinar_attendance-1)*2
              ELSE 0
          END)
          ) AS _webinar_attendance_score,

          #webinar score 
          (CASE --- attendance
              WHEN _distinct_webinar_sing_up >= 1 THEN 10*_distinct_webinar_sing_up
              ELSE 0
          END) + (
             (CASE --- attendance
              WHEN _distinct_webinar_attendance >= 1 THEN 25
              ELSE 0
          END)
          + 
          (CASE --- attendance
              WHEN _distinct_webinar_attendance >= 2 THEN (_distinct_webinar_attendance-1)*2
              ELSE 0
          END)
          ) AS _webinar_score,
          (CASE --- 1 event 
              WHEN _distinct_educational_event_attendance >= 1 THEN 40
              ELSE 0
          END) AS _educational_event_attendance_score,

          (CASE --- 1 additional 
              WHEN _distinct_experience_event_attendance >= 1 THEN 25
              ELSE 0
          END) AS _experience_event_attendance_score,

          #event
          (
            (CASE --- 1 event 
              WHEN _distinct_educational_event_attendance >= 1 THEN 40
              ELSE 0
          END)
          + 
          (CASE --- 1 additional 
              WHEN _distinct_experience_event_attendance >= 1 THEN 25
              ELSE 0
          END)
          ) AS _event_score,

          #form filled
          (CASE --- 1 additional 
              WHEN _distinct_contactus_form >= 1 THEN _distinct_contactus_form*15
              ELSE 0
          END) AS _form_filled_score,

           #form filled
          (CASE --- 1 additional 
              WHEN _distinct_web_visit >= 1 THEN _distinct_web_visit*0.5
              ELSE 0
          END) AS _web_visit_score


        FROM
        quarterly_contact_engagement 
  ),contact_score_limit AS (
   SELECT * ,
 --( -- Setting of threshold for max of email score
           -- IF(_monthly_email_score > 15, 15, _monthly_email_score) ) 
           _monthly_email_score AS _quarterly_email_score,
-- ( -- Setting of threshold for max of email score
--             IF(_paid_social_score > 4, 4, _paid_social_score ) ) 
            _paid_social_score AS _quarterly_paid_social,
-- ( -- Setting of threshold for max of email score
--             IF(_organic_social_score > 8, 8, _organic_social_score) ) 
            _organic_social_score AS _quarterly_organic_social,
-- ( -- Setting of threshold for max of email score
--             IF(_webinar_score > 37, 37, _webinar_score) )
            _webinar_score AS _quarterly_webinar,
-- ( -- Setting of threshold for max of email score
--             IF(_event_score > 42, 42, _event_score) ) 
            _event_score AS _quarterly_event_score,
--  ( -- Setting of threshold for max of email score
--             IF(_form_filled_score > 42, 42,_form_filled_score) ) 
            _form_filled_score AS _quarterly_form_filled_score,
            _web_visit_score AS _quaterly_web_visit_score,
 FROM quarterly_contact_scoring
 ), campaign_score AS (
  SELECT _account, _domain, _account_segment,  SUM(_monthly_email_score) AS _monthly_email_score_campaign, SUM(_webinar_score) AS _webinar_score_campaign
FROM `terrasmart.campaign_90days_score`
 WHERE _extract_date BETWEEN date_start AND date_end
               
GROUP BY _account, _domain, _account_segment
 )
 ,final_scoring AS (
   SELECT 
   --_month,
   all_accounts._domain,
   _ebos, 
   _utilityprojects, 
   _tracker, 
   _linkedinurl, 
   _canopy, 
   _prospect, 
   _fixedtilt, 
   all_accounts._account, 
   _persona, 
   _rep, 
   _midwest,
   all_accounts._account_segment,
   _type,
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
   COALESCE(_distinct_educational_event_attendance, 0 ) AS _distinct_educational_event_attendance,
   COALESCE(_distinct_experience_event_attendance, 0 ) AS _distinct_experience_event_attendance,
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
   COALESCE(_educational_event_attendance_score, 0 ) AS _educational_event_attendance_score,
   COALESCE(_experience_event_attendance_score, 0 ) AS _experience_event_attendance_score,
   COALESCE(_event_score, 0 ) AS _event_score,
   COALESCE(_form_filled_score, 0 ) AS _form_filled_score,
   COALESCE(_quarterly_email_score, 0 ) AS _quarterly_email_score,
   COALESCE(_quarterly_paid_social, 0 ) AS _quarterly_paid_social,
   COALESCE(_quarterly_organic_social, 0 ) AS _quarterly_organic_social,
   COALESCE(_quarterly_webinar, 0 ) AS _quarterly_webinar,
   COALESCE(_quarterly_event_score, 0 ) AS _quarterly_event_score,
   COALESCE(_quarterly_form_filled_score, 0 ) AS _quarterly_form_filled_score,
   COALESCE(_distinct_web_visit, 0 ) AS _distinct_web_visit,
   COALESCE(_web_visit_score, 0 ) AS _quaterly_web_visit_score,
   _monthly_email_score_campaign,
   _webinar_score_campaign
   FROM all_accounts
   LEFT JOIN 
   contact_score_limit ON all_accounts._domain = contact_score_limit._domain
   LEFT JOIN 
   campaign_score ON all_accounts._domain = campaign_score._domain AND all_accounts._account = campaign_score._account
AND all_accounts._account_segment = campaign_score._account_segment
 )
 , get_total AS ( 
  SELECT  * ,
 (_monthly_email_score_campaign + _quarterly_paid_social + _quarterly_organic_social  + _webinar_score_campaign + _quarterly_event_score + _quaterly_web_visit_score ) AS _monthly_account_score,
date_start AS _extract_date,
date_end AS _Tminus90_date, 
EXTRACT(YEAR FROM date_start) AS _year,
_min_quater_date AS _min_quater_date,
_max_quater_date AS _max_quater_date,
FROM final_scoring
 ) , key_account_year AS (
  SELECT DISTINCT _domain, CAST(_key_account_year AS INT64) AS _key_account_year FROM `x-marketing.terrasmart_mysql_2.db_key_accounts`
 ) , all_score AS (
     SELECT get_total.*EXCEPT (_monthly_email_score_campaign,
   _webinar_score_campaign),
     0.0 AS _cumulative_monthly_engagement_score,
      0.0 AS _cumulative_quaterly_engagement_score,
     0.0 AS _cumulative_year_engagement_score,
    _monthly_email_score_campaign,
   _webinar_score_campaign
FROM get_total
 ) ,   acc AS (
  SELECT
    name,
    id AS accountid,
    LEFT(main.id, LENGTH(main.id) - 3) AS account_id,
    website,
    type AS prospect_type,
    new_vs_existing_customer__c AS new_vs_existing_customer
  FROM
    `x-marketing.terrasmart_salesforce_alt.Account` main 
    ) , new_vs_existing_customer AS (
SELECT
  DISTINCT _domain,
  new_vs_existing_customer
FROM
  acc
LEFT JOIN
  `x-marketing.terrasmart_googlesheet.Standardized_SFDC_Account_Name` key_account
ON
  acc.account_id = key_account.account_id
LEFT JOIN
  all_score  domain
ON
  COALESCE(database_account_name__standardized_, name) = domain._account
QUALIFY
  ROW_NUMBER() OVER(PARTITION BY _domain ORDER BY new_vs_existing_customer DESC) = 1
  ) SELECT  all_score.*, 
 0 AS _key_account_year,
 new_vs_existing_customer,
 first_2_month AS _first_2_month, 
 0.0 AS _score_first_2_month,
  first_3_month AS _first_3_month,
  0.0 AS  _score_first_3_month, 
 first_4_month AS  _first_4_month, 
  0.0 AS _score_first_4_month, 
  first_5_month  AS _first_5_month, 
  0.0 AS _score_first_5_month, 
  first_6_month AS _first_6_month, 
  0.0 AS _score_first_6_month,
  CAST(0.0 AS NUMERIC) AS _sum_rolling_6_month,
CAST(0.0 AS NUMERIC) AS _rolling_6_month_avg,
CAST( 0.0 AS NUMERIC) AS _rolling_6_month
 FROM all_score 
 LEFT JOIN new_vs_existing_customer existing_customer ON all_score._domain = existing_customer._domain;
        -- ORDER BY 
        --   _visited_website DESC
        SET index = index + 1;
  END;
END LOOP;


UPDATE `x-marketing.terrasmart.account_90days_score` origin  
SET
origin._cumulative_monthly_engagement_score = updates ._cumulative_monthly_engagement_score,
origin._cumulative_year_engagement_score = updates ._cumulative_year_engagement_score
FROM (
WITH cummulative_score AS (
  SELECT 
  _domain,
  _year,
  _extract_date,
  _account,
  _monthly_account_score,
 SUM (_monthly_account_score) OVER (PARTITION BY _domain,_year ORDER BY _extract_date) AS _cumulative_monthly_engagement_score
FROM `x-marketing.terrasmart.account_90days_score` 
 ) SELECT *,
MAX (_cumulative_monthly_engagement_score) OVER (PARTITION BY _domain,_year ORDER BY _domain) AS _cumulative_year_engagement_score,

FROM cummulative_score
ORDER BY _extract_date DESC
) updates 
WHERE origin._domain = updates._domain
AND origin._account = updates._account
AND origin._year = updates._year
AND origin._extract_date = updates._extract_date; 

UPDATE `x-marketing.terrasmart.account_90days_score` origin  
SET
origin._cumulative_quaterly_engagement_score = updates ._cumulative_quaterly_engagement_score
FROM (
WITH cummulative_score AS (
  SELECT 
  _domain,
  _year,
  _min_quater_date ,
  _account,
 SUM (_monthly_account_score) AS _cumulative_quaterly_engagement_score
FROM `x-marketing.terrasmart.account_90days_score` 

GROUP BY 1,2,3,4
 ) SELECT *

FROM cummulative_score
ORDER BY _min_quater_date DESC
) updates 
WHERE origin._domain = updates._domain
AND origin._account = updates._account
AND origin._year = updates._year
AND origin._min_quater_date = updates._min_quater_date; 

UPDATE `x-marketing.terrasmart.account_90days_score` origin  
SET
origin._score_first_2_month = updates .score_first_2_month,
origin._score_first_3_month = updates .score_first_3_month,
origin._score_first_4_month = updates .score_first_4_month,
origin._score_first_5_month = updates .score_first_5_month,
origin._score_first_6_month = updates .score_first_6_month,
origin._sum_rolling_6_month = updates ._sum_rolling_6_month,
origin._rolling_6_month_avg = updates ._rolling_6_month_avg,
origin._rolling_6_month = updates .rolling_6_month
FROM (

WITH dates AS (
  SELECT 
    DATE_SUB(_date, INTERVAL 1 MONTH)  AS first_1_month,
    DATE_SUB(_date, INTERVAL 2 MONTH) AS first_2_month,
    DATE_SUB(_date, INTERVAL 3 MONTH) AS first_3_month,
    DATE_SUB(_date, INTERVAL 4 MONTH) AS first_4_month,
    DATE_SUB(_date, INTERVAL 5 MONTH) AS first_5_month,
    DATE_SUB(_date, INTERVAL 6 MONTH) AS first_6_month,
    DATE_SUB(_date, INTERVAL 1 MONTH)  AS _extract_date
  FROM  UNNEST(GENERATE_DATE_ARRAY('2022-01-01',DATE_ADD(CURRENT_DATE(), INTERVAL 1 MONTH), INTERVAL 1 MONTH)) AS _date
),
all_data AS (
  SELECT * FROM `terrasmart.account_90days_score`
),
aggregated_data AS (
  SELECT
    a._domain,
    a._account,
    a._account_segment,
    d._extract_date,
    d.first_1_month,
    AVG(CASE WHEN a._extract_date = d.first_1_month THEN a._monthly_account_score END) AS score_first_1_month,
    d.first_2_month,
    AVG(CASE WHEN a._extract_date = d.first_2_month THEN a._monthly_account_score END) AS score_first_2_month,
    d.first_3_month,
    AVG(CASE WHEN a._extract_date = d.first_3_month THEN a._monthly_account_score END) AS score_first_3_month,
    d.first_4_month,
    AVG(CASE WHEN a._extract_date = d.first_4_month THEN a._monthly_account_score END) AS score_first_4_month,
    d.first_5_month,
    AVG(CASE WHEN a._extract_date = d.first_5_month THEN a._monthly_account_score END) AS score_first_5_month,
    d.first_6_month,
    AVG(CASE WHEN a._extract_date = d.first_6_month THEN a._monthly_account_score END) AS score_first_6_month
  FROM all_data a
  JOIN dates d
  ON a._extract_date <= d._extract_date
  GROUP BY a._domain, a._account, a._account_segment, d._extract_date, d.first_1_month, d.first_2_month, d.first_3_month, d.first_4_month, d.first_5_month, d.first_6_month
)
SELECT
  ad._account,
  _account_segment,
  ad._domain,
  ad._extract_date,
    ad.first_2_month,
  COALESCE(ad.score_first_2_month,0) AS score_first_2_month,
    ad.first_3_month,
  COALESCE(ad.score_first_3_month,0) AS score_first_3_month,
    ad.first_4_month,
  COALESCE(ad.score_first_4_month,0) AS score_first_4_month,
    ad.first_5_month,
  COALESCE(ad.score_first_5_month,0) AS score_first_5_month,
  ad.first_6_month,
  COALESCE(ad.score_first_6_month,0) AS score_first_6_month,


CAST(COALESCE(ad.score_first_1_month,0)+COALESCE(ad.score_first_2_month,0)+COALESCE(ad.score_first_3_month,0)+COALESCE(ad.score_first_4_month,0)+COALESCE(ad.score_first_5_month,0)+COALESCE(ad.score_first_6_month,0) AS NUMERIC) AS _sum_rolling_6_month,
CAST((COALESCE(ad.score_first_1_month,0)+COALESCE(ad.score_first_2_month,0)+COALESCE(ad.score_first_3_month,0)+COALESCE(ad.score_first_4_month,0)+COALESCE(ad.score_first_5_month,0)+COALESCE(ad.score_first_6_month,0))/6 AS NUMERIC) AS _rolling_6_month_avg,

  CAST(AVG(ad.score_first_6_month) OVER (PARTITION BY ad._domain ORDER BY ad._extract_date ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS NUMERIC)AS rolling_6_month
FROM aggregated_data ad

ORDER BY ad._extract_date DESC
) updates 
WHERE origin._domain = updates._domain
AND origin._account = updates._account
AND origin._account_segment = updates._account_segment
AND origin._extract_date = updates._extract_date;

-- UPDATE `x-marketing.terrasmart.account_90days_score` origin  
-- SET
-- origin._monthly_email_score_campaign = updates ._monthly_email_score_campaign,
-- origin._webinar_score_campaign = updates ._webinar_score_campaign
-- FROM (
-- SELECT _account, _domain, _account_segment, _extract_date, SUM(_monthly_email_score) AS _monthly_email_score_campaign, SUM(_webinar_score) AS _webinar_score_campaign
-- FROM `terrasmart.campaign_90days_score`
-- GROUP BY _account, _domain, _account_segment, _extract_date
-- ) updates 
-- WHERE origin._domain = updates._domain
-- AND origin._account = updates._account
-- AND origin._account_segment = updates._account_segment
-- AND origin._extract_date = updates._extract_date;
