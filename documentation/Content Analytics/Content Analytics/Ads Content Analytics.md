# Ads Content Analytics

## ERD Process

![Untitled](Ads%20Content%20Analytics/Untitled.png)

## Data Dictionary

### **db_ad_content_analytics**

| Field Name | Source | Data Type | Description | Example |
| --- | --- | --- | --- | --- |
| ad_id | LinkedIn | INT64 | Ad ID | 214629313 |
| ad_group_id | LinkedIn | INT64 | Ad Group ID | 624395653 |
| day | LinkedIn | TIMESTAMP | Timestamp | 2022-10-23 00:00:00 UTC |
| spent | LinkedIn | FLOAT64 | Ad Spent | 16.58 |
| impressions | LinkedIn | INT64 | Ad Impression | 92 |
| clicks | LinkedIn | INT64 | Ad Click | 5 |
| _advariation | LinkedIn | STRING | Ad variation | Do the Math - Services |
| _content | LinkedIn | STRING | Ad content | urn:li:share:7026658140121108481 |
| _screenshot | LinkedIn | STRING | Ad Screenshot | https://dp.2x.marketing/airtable-images/n/[2023-11-16-02-33-40]___HC2.jpg |
| _reportinggroup | LinkedIn | STRING | Reporting Group | Brand Awareness Building |
| _campaign | LinkedIn | STRING | Campaign ID | 213331843 |
| _source | LinkedIn | STRING | Ad Source | Linkedin |
| _medium | LinkedIn | STRING | Ad medium | Paid |
| _platform | LinkedIn | STRING | Ad platform | Linkedin |
| _asset | LinkedIn | STRING | Ad asset |  |
| _landingpageurl | LinkedIn | STRING | Landing Page | https://logicsource.com/tylp/evaluating-procurement-technology-look-beyond-legacy-providers/?utm_source=linkedin&utm_campaign=2023-07-25_EB1&utm_content=OM-Brand.A_V1 |
| _campaignname | LinkedIn | STRING | Campaign Name | CLM Debate |
| _stage | LinkedIn | STRING | Ad Stage | Awareness |
| adnum | LinkedIn | INT64 | Count of Ad ID per day and ad group id | 2 |
| pageviews | Web | INT64 | Page Views | 1 |
| reduced_pageviews | Web | FLOAT64 | Page Views / ad Count | 5.0 |
| visitors | Web | INT64 | Visitors | 1 |
| reduced_visitors | Web | FLOAT64 | Visitors / Ad Count | 1.0 |
| _contentitem | Content Airtable | STRING | Content Item | Fighting Inflation with Better Buying video LP. |
| _contenttype | Content Airtable | STRING | Content Type | Guide |
| _gatingstrategy | Content Airtable | STRING | Gating Strategy | No |
| _homeurl | Content Airtable | STRING | Home URL Content | https://logicsource.com/lp/optimizing-priority-99/ |
| _summary | Content Airtable | STRING | Content Summary | KEY FACT: Non-clinical spending takes up a staggering 20% to 25% of net patient revenue |
| _status | Content Airtable | STRING | Content Status | In planning |
| _buyerstage | Content Airtable | STRING | Buyer Stage | Awareness |
| _vertical | Content Airtable | STRING | Vertical | HEALTHCARE |
| _persona | Content Airtable | STRING | Persona | CEO,CPO,COO,CFO,CTO |