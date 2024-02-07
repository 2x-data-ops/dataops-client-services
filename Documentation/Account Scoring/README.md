# Intent Driven Marketing - Account Scoring

Created by: Nazzatul Farahiayah Mohd Nazziri
Created time: February 2, 2024 11:36 AM

# Intent Driven Marketing

1. Intent-based marketing uses individual online behavior data to deliver targeted advertising at the time when buyers are most likely to purchase, within the right context.
2. The purposeful delivery of marketing content to a purchase-ready audience aligns your messaging to the uncanny answer to their problem, eliminating untimely or distracting ads.

# Account Scoring

Account scoring, a part of [account-based marketing](https://www.factors.ai/blog/what-is-account-based-marketing-abm), helps you rank potential customers from the most to the least valuable.

# Criteria of account scoring

![Untitled](Intent%20Driven%20Marketing%20-%20Account%20Scoring%202c7313075fa84664b94e1f83f96a4570/Untitled.png)

![Untitled](Intent%20Driven%20Marketing%20-%20Account%20Scoring%202c7313075fa84664b94e1f83f96a4570/Untitled%201.png)

First party Intent Data

Prospective buyer activity tracked on websites and digital properties that you **own**, sponsor, or control.

# Account Scoring/ Engagements Scoring Model

![Untitled](Intent%20Driven%20Marketing%20-%20Account%20Scoring%202c7313075fa84664b94e1f83f96a4570/Untitled%202.png)

# Engagements Scoring Rubrics

**Example of rubrics**

![Untitled](Intent%20Driven%20Marketing%20-%20Account%20Scoring%202c7313075fa84664b94e1f83f96a4570/Untitled%203.png)

**TLF Engagements Scoring** 

![Untitled](Intent%20Driven%20Marketing%20-%20Account%20Scoring%202c7313075fa84664b94e1f83f96a4570/Untitled%204.png)

**3x Engagement  Scoring**

![Untitled](Intent%20Driven%20Marketing%20-%20Account%20Scoring%202c7313075fa84664b94e1f83f96a4570/Untitled%205.png)

**Terrasmart account engagement scoring**

![Untitled](Intent%20Driven%20Marketing%20-%20Account%20Scoring%202c7313075fa84664b94e1f83f96a4570/Untitled%206.png)

# Data flow of account scoring

High Level of account scoring

![Untitled](Intent%20Driven%20Marketing%20-%20Account%20Scoring%202c7313075fa84664b94e1f83f96a4570/Untitled%207.png)

Detail of scoring

![Untitled](Intent%20Driven%20Marketing%20-%20Account%20Scoring%202c7313075fa84664b94e1f83f96a4570/Untitled%208.png)

# BigQuery Script Preparation

Engagement consolidation table preparation. 

1. Create Engagement consolidation table where we combine all engagement from different platforms into 1 table. 
2. List of data inside engagement consolidation

![Untitled](Intent%20Driven%20Marketing%20-%20Account%20Scoring%202c7313075fa84664b94e1f83f96a4570/Untitled%209.png)

- Data Ops : all low level table such as email engagement log, web engagement long, tam and opportunity already being prepared.
- Know what type engagement that being score and where is the data coming from.
- for paid ads/ social ads please note that the data can only be done manually.

| _email | _domain | _timestamp | _week | _year | _contentTitle | _engagement | _description | _utmsource | _utmcampaign | _utmmedium | _utmcontent | _fullurl | _frequency |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| a.bright@f5.com | f5.com | 2023-11-09 14:09:33.218000 UTC | 45 | 2023 | EM_2023-11-09_3X_Case-Study-Jasper_Why-Maas_W5 | Email Clicked |  |  |  |  |  |  |  |
| a.bright@f5.com | f5.com | 2023-11-09 14:09:33.218000 UTC | 45 | 2023 | EM_2023-11-09_3X_Case-Study-Jasper_Why-Maas_W5 | Email Opened |  |  |  |  |  |  |  |
| a.bright@f5.com | f5.com | 2023-08-09 14:14:27.497000 UTC | 32 | 2023 | EM_2023-08-08_3X_Marketo-Email-Services-Page_W5 | Web Visit |  |  |  |  |  |  |  |
| a.bright@f5.com | f5.com | 2023-08-09 14:14:27.497000 UTC | 32 | 2023 | EM_2023-08-08_3X_Marketo-Email-Services-Page_W5 | Form Filled |  |  |  |  |  |  |  |
| a.bright@f5.com | f5.com | 2023-08-03 14:11:14.996000 UTC | 31 | 2023 | EM_2023-08-03_3X_AI-Webinar_Email-Invite | Paid Ads Click |  |  |  |  |  |  |  |
| a.bright@f5.com | f5.com | 2023-08-03 14:11:14.996000 UTC | 31 | 2023 | EM_2023-08-03_3X_AI-Webinar_Email-Invite | Social Ads Click |  |  |  |  |  |  |  |
| a.bright@f5.com | f5.com | 2023-08-01 14:03:34.143000 UTC | 31 | 2023 | EM_2023-08-01_3X_Marketo-Email-Blog_W4 | Events |  |  |  |  |  |  |  |
| a.bright@f5.com | f5.com | 2023-08-01 14:03:34.143000 UTC | 31 | 2023 | EM_2023-08-01_3X_Marketo-Email-Blog_W4 | Webinar |  |  |  |  |  |  |  |
| a.bright@f5.com | f5.com | 2023-07-26 14:02:52.927000 UTC | 30 | 2023 | EM_2023-07-26_3X_Marketo-Email-Blog_W3 | Email Clicked |  |  |  |  |  |  |  |

Account scoring table preparation 

1. Every action in every category is being calculate according to the score. 
2. Each action is correspond to every domain in the engagement consolidation. 

![Untitled](Intent%20Driven%20Marketing%20-%20Account%20Scoring%202c7313075fa84664b94e1f83f96a4570/Untitled%2010.png)

## pseudo code

```
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

DELETE FROM `**account_90days_score`** WHERE _domain IS NOT NULL;

LOOP
  IF index = array_length(date_ranges) 
    THEN BREAK;
  END IF;
  BEGIN
    DECLARE date_end DATE DEFAULT date_ranges[OFFSET(index)].max_date;
    DECLARE date_start DATE DEFAULT date_ranges[OFFSET(index)].min_date;

    INSERT INTO  `**account_90days_score**` 
    WITH all_accounts AS (
engagement scoring rubrinc,      
date_start AS _extract_date,
  date_end AS _Tminus90_date 
FROM 
      final_scoring;
        -- ORDER BY 
        --   _visited_website DESC
        SET index = index + 1;
  END;
END LOOP;
```

**Date Range Initialization:**

- The script begins by initializing variables for managing the date ranges. The **`index`** variable is a counter used in a loop, and the **`date_ranges`** array is populated with date ranges generated using the **`GENERATE_DATE_ARRAY`** function.
1. **Data Deletion:**
    - Records from the **`account_90days_score`** table where **`_domain`** is not null are deleted. This step ensures that the table is refreshed before new data is inserted.
2. **Loop Structure:**
    - A loop is initiated to iterate through the date ranges specified in the **`date_ranges`** array. The loop continues until all date ranges have been processed.
3. **Data Insertion and Scoring:**
    - For each date range, the script inserts data into the **`account_90days_score`** table by joining information from **`terrasmart.db_consolidated_engagements_log`**. The goal is to consolidate engagement data for scoring purposes.
    - Various engagement types are considered, including email opens, clicks, social media interactions, webinars, events, form submissions, and web visits. Counts for each type of engagement are calculated using **`COUNT`** and **`CASE`** statements.
    - Scores are assigned based on the counts of engagements. The scoring logic is extensive and considers multiple conditions for different engagement types.
4. **Threshold Setting:**
    - After calculating scores, there is a section that limits the scores for email, paid social, organic social, webinars, events, and form submissions to certain thresholds. This step helps ensure that scores do not exceed predefined limits.
5. **Final Scoring:**
    - The script joins the calculated scores with additional information from the **`all_accounts`** table. This table likely contains information about accounts, such as domain, region, and other attributes.
    - Quarterly scores are calculated for each engagement type, and additional scores are derived based on the calculated scores (e.g., total email score, paid social score, organic social score, etc.).
    - The final result set includes various scores for different engagement types, along with information about the accounts.
6. **Result Set:**
    - The final results are returned, providing a detailed breakdown of scores for each account, along with the quarterly and monthly scores.
7. **Loop Increment and Termination:**
    - After processing each date range in the loop, the **`index`** variable is incremented to move to the next date range.
    - The loop terminates when all date ranges have been processed.

example date : 

| max_date | min_date |
| --- | --- |
| 2024-02-29 | 2024-02-01 |
| 2024-01-31 | 2024-01-01 |
| 2023-12-31 | 2023-12-01 |
| 2023-11-30 | 2023-11-01 |
| 2023-10-31 | 2023-10-01 |
| 2023-09-30 | 2023-09-01 |
| 2023-08-31 | 2023-08-01 |
| 2023-07-31 | 2023-07-01 |
| 2023-06-30 | 2023-06-01 |
| 2023-05-31 | 2023-05-01 |
| 2023-04-30 | 2023-04-01 |
| 2023-03-31 | 2023-03-01 |
| 2023-02-28 | 2023-02-01 |
| 2023-01-31 | 2023-01-01 |
| 2022-12-31 | 2022-12-01 |

## SQL Query for Detailed Overview engagement Scoring

Assign the score base on the rubric scoring given. Example 

| Scoring |  |
| --- | --- |
| Email |  |
| Open | 5 |
| Click | 10 |
| 2 opens | 10 |
| 2 clicks | 15 |
| 3 opens | 15 |
| 3 clicks | 20 |
1. **Click Score (`_click_score`):**
    - Assigns a score based on the number of distinct email clicks.
    - If there is only one click, assigns a score of 10; if there are two or more clicks, assigns a score of **`_distinct_email_click * 5`**; otherwise, assigns a score of 0.
2. **Open Score (`_open_score`):**
    - Assigns a score based on the number of distinct email opens.
    - If there is only one open, assigns a score of 5; if there are two or more opens, assigns a score of **`_distinct_email_open * 5`**; otherwise, assigns a score of 0.
3. **Email Score (`_email_score`):**
    - Combines the click and open scores to calculate the total email score.

# Final Table

This table represents the final output of a process that scores accounts based on their engagement. Each row corresponds to a specific domain (i.e., account) and a specific month.

The columns of the table include:

- `_domain`: The domain or account that the row pertains to.
- `_distinct_email_delivered`: The number of unique email deliveries for the account in the specific month.
- `_distinct_email_open`: The number of unique email opens for the account in the specific month.
- `_distinct_email_click`: The number of unique email clicks for the account in the specific month.
- `_distinct_contactus_form`: The number of unique 'contact us' form submissions for the account in the specific month.
- `_monthly_account_score`: The final account score calculated for the account for the specific month. This score is a representation of the account's engagement and is calculated based on the other columns in the table.
- `_extract_date`: The start date for the month that the row pertains to.
- `_Tminus90_date`: The end date for the month that the row pertains to.

In this specific example, all the columns relating to engagement (email deliveries, opens, clicks, and 'contact us' form submissions) have a value of 0, which indicates that there was no recorded engagement for the `quantapower.net` account across all the months listed.

| _domain | _distinct_email_delivered | _distinct_email_open | _distinct_email_click | _distinct_contactus_form | _monthly_account_score | _extract_date | _Tminus90_date |
| --- | --- | --- | --- | --- | --- | --- | --- |
| quantapower.net | 0 | 0 | 0 | 0 | 0 | 2024-02-01 | 2024-02-29 |
| quantapower.net | 1 | 0 | 0 | 0 | 0 | 2024-01-01 | 2024-01-31 |
| quantapower.net | 0 | 0 | 0 | 0 | 0 | 2023-12-01 | 2023-12-31 |
| quantapower.net | 0 | 0 | 0 | 0 | 0 | 2023-11-01 | 2023-11-30 |
| quantapower.net | 0 | 0 | 0 | 0 | 0 | 2023-10-01 | 2023-10-31 |
| quantapower.net | 0 | 0 | 0 | 0 | 0 | 2023-09-01 | 2023-09-30 |
| quantapower.net | 0 | 0 | 0 | 0 | 0 | 2023-08-01 | 2023-08-31 |
| quantapower.net | 0 | 0 | 0 | 0 | 0 | 2023-07-01 | 2023-07-31 |
| quantapower.net | 0 | 0 | 0 | 0 | 0 | 2023-06-01 | 2023-06-30 |
| quantapower.net | 0 | 0 | 0 | 0 | 0 | 2023-05-01 | 2023-05-31 |
| quantapower.net | 0 | 0 | 0 | 0 | 0 | 2023-04-01 | 2023-04-30 |
| quantapower.net | 0 | 0 | 0 | 0 | 0 | 2023-03-01 | 2023-03-31 |
| quantapower.net | 0 | 0 | 0 | 0 | 0 | 2023-02-01 | 2023-02-28 |
| quantapower.net | 0 | 0 | 0 | 0 | 0 | 2023-01-01 | 2023-01-31 |
| quantapower.net | 0 | 0 | 0 | 0 | 0 | 2022-12-01 | 2022-12-31 |
| quantapower.net | 0 | 0 | 0 | 0 | 0 | 2022-11-01 | 2022-11-30 |
| quantapower.net | 0 | 0 | 0 | 0 | 0 | 2022-10-01 | 2022-10-31 |
| quantapower.net | 0 | 0 | 0 | 0 | 0 | 2022-09-01 | 2022-09-30 |
| quantapower.net | 0 | 0 | 0 | 0 | 0 | 2022-08-01 | 2022-08-31 |
| quantapower.net | 0 | 0 | 0 | 0 | 0 | 2022-07-01 | 2022-07-31 |
| quantapower.net | 0 | 0 | 0 | 0 | 0 | 2022-06-01 | 2022-06-30 |
| quantapower.net | 0 | 0 | 0 | 0 | 0 | 2022-05-01 | 2022-05-31 |
| quantapower.net | 0 | 0 | 0 | 0 | 0 | 2022-04-01 | 2022-04-30 |
| quantapower.net | 0 | 0 | 0 | 0 | 0 | 2022-03-01 | 2022-03-31 |
| quantapower.net | 0 | 0 | 0 | 0 | 0 | 2022-02-01 | 2022-02-28 |
| quantapower.net | 0 | 0 | 0 | 0 | 0 | 2022-01-01 | 2022-01-31 |
| quantapower.net | 0 | 0 | 0 | 0 | 0 | 2021-12-01 | 2021-12-31 |

# Summary

This document provides a detailed overview of the concept of "Intent Driven Marketing" and "Account Scoring".

Intent Driven Marketing is a strategy that uses individual online behavior data to deliver targeted advertising when buyers are most likely to make a purchase. The document emphasizes the alignment of marketing content with a purchase-ready audience, which helps to avoid untimely or distracting ads.

Account Scoring is a component of account-based marketing. It assists in ranking potential customers from the most valuable to the least, using specific criteria.

The document further highlights the use of First Party Intent Data, which involves tracking prospective buyer activity on websites and digital properties that the organization owns, sponsors, or controls.

A comprehensive scoring model is presented which includes rubrics, engagement scoring, and account scoring, using a variety of engagement metrics like email clicks, website visits, form fills, and event attendance, etc.

The document also explains in detail the data flow of account scoring and how to prepare for BigQuery Script to manage and analyze this data. It provides a step-by-step guide for creating a pseudo code which includes data deletion, loop structure, data insertion and scoring, threshold setting, and result set.

The final section covers account scoring table preparation. It describes how various actions are calculated according to the score and how this data corresponds to each domain in the engagement consolidation. An example date range and an SQL query for a detailed overview of the engagement scoring is provided.

The document concludes with an example of a final table that represents the final output of a process that scores accounts based on their engagement. Each row corresponds to a specific domain and a specific month.

# Example Dashboard

| Client | Report Type | Dashboard Link | Script |
| --- | --- | --- | --- |
| Terrasmart | Account Engagement Scoring | https://lookerstudio.google.com/reporting/e42b728f-8398-461f-bf16-4922349da4ad/page/p_iiflujjk6c | terrasmart_account_score_90days |
| 3x | Scoreboard | https://lookerstudio.google.com/reporting/34dc6a10-a5ed-41aa-99a1-8238122748e5/page/p_hhsosojt0c | 3x_account_90days_score,3x_account_score_90days |
| Logicsource | Account Intent | https://lookerstudio.google.com/reporting/68eb0889-bf1b-4cb3-9327-5e207e0a67f6/page/p_ullf2vauuc | logicsource_account_score_90days |
| The LogicFactory | Account Engagement Scoring | https://lookerstudio.google.com/reporting/8b3aa23f-f883-4b2f-a7f2-02e1a44fc88d | thelogicfactory_account_90days_score |