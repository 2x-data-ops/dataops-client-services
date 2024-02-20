# Email Content Analytics

## ERD Process

![Untitled](Email%20Content%20Analytics/Untitled.png)

## Data Dictionary

### **db_email_content_analytics**

| FieldName | Source | DataType | Description | Example |
| --- | --- | --- | --- | --- |
| _sdc_sequence | Hubspot | INT64 | Stitchdata’s sequence | 1699493484205787037 |
| _campaignID | Hubspot | STRING | Campaign’s ID | 281703889 |
| _contentTitle | Hubspot | STRING | Content’s title | @November 8, 2023_NT-12_TOFU01_OM |
| _subject | Hubspot | STRING | Campaign’s subject | Showcase Procurement’s True Value with Analytics |
| _email | Hubspot | STRING | Prospect’s email | mailto:bruce.pomazal@cdk.com |
| _description | Hubspot | STRING | Campaign’s description | https://logicsource.com/lp/optimizing-priority-99/?utm_campaign=2023-10-19_EM-05_WC-01&_hsmi=279018602&utm_content=ST02_LSI-HI_cta_img02&utm_source=email |
| _device_type | Hubspot | STRING | Campaign device type | COMPUTER |
| _linkid | Hubspot | STRING | Campaign link id | 0 |
| _duration | Hubspot | STRING | Campaign duration | 10643 |
| _timestamp | Hubspot | TIMESTAMP | Engagement’s timestamp | @November 8, 2023 14:02:25.822000 UTC |
| _engagement | Hubspot | STRING | Prospect’s engagement | Delivered |
| _response | Hubspot | STRING | Email response | 250 2.0.0 Ok: 26219 bytes queued as A837018005D |
| _prospectID | Hubspot | STRING | Prospect’s ID | 454935 |
| _name | Hubspot | STRING | Prospect’s Name | Toni Houck |
| _domain | Hubspot | STRING | Email domain | http://generalinsulation.com/ |
| _title | Hubspot | STRING | Prospect’s title | Director, Credit |
| _function | Hubspot | STRING | Prospect’s function | Medical Administration |
| _seniority | Hubspot | STRING | Prospect Seniority | Director |
| _phone | Hubspot | STRING | Prospect phone | (503) 402-3383 |
| _company | Hubspot | STRING | Prospect company | CDK Global |
| _revenue | Hubspot | STRING | Prospect company revenue | 1960100000 |
| _industry | Hubspot | STRING | Prospect company industry | Software |
| _city | Hubspot | STRING | Prospect company city | Portland |
| _state | Hubspot | STRING | Prospect company state | Oregon |
| _country | Hubspot | STRING | Prospect company country | US |
| _lifecycleStage | Hubspot | STRING | Prospect lifecycle stage | marketingqualifiedlead |
| isPageView | Hubspot | BOOL | Is page viewed | True |
| _isBot | Hubspot | STRING | If “bot” user | yes |
| _showExport | Hubspot | STRING | Show Exported | Yes |
| _dropped | Hubspot | STRING | If dropped | True |
| _notSent | Hubspot | STRING | If not sent | True |
| _falseDelivered | Hubspot | STRING | False Delivered | True |
| _leadscore | Hubspot | INT64 | Lead Score | 0 |
| _hubspotscore | Hubspot | FLOAT64 | Hubspot Score | 80.0 |
| _company_id | Hubspot | INT64 | Company ID | 16392854421 |
| _job_role | Hubspot | STRING | Job Role | Procurement |
| _mql_date | Hubspot | TIMESTAMP | Marketing Qualified Date | @August 23, 2023 14:04:12.192000 UTC |
| _source | Hubspot | STRING | Source | OFFLINE |
| _latest_source | Hubspot | STRING | Latest email source | EMAIL_MARKETING |
| _ipqc_check | Hubspot | STRING | - | Valid |
| _company_segment | Hubspot | STRING | Company Segment | LE |
| _lead_segment | Hubspot | STRING | - | OneMarket |
| _segment | Hubspot | STRING | Segment | LE |
| _property_leadstatus | Hubspot | STRING | Lead Status | Working |
| _companylinkedinbio | Hubspot | STRING | Company Linkedin Bio | CDK Global Inc. is an American multinational corporation based in Hoffman Estates, Illinois, providing data and technology to the automotive, heavy truck, recreation, and heavy equipment industries. |
| _company_linkedin | Hubspot | STRING | Company Linkedin | https://www.linkedin.com/company/cdknorthamerica |
| _employee_range | Hubspot | STRING | Employee Range | 10000+ |
| _numberofemployees | Hubspot | NUMERIC | Num of Employees | 882 |
| _annualrevenue | Hubspot | NUMERIC | Annual Revenue | 639130000 |
| _annual_revenue_range | Hubspot | STRING | Annual Revenue Range | Over $5 bil. |
| _salesforceaccountid | Hubspot | STRING | Salesforce Acc ID | 0014W00002ruaNzQAI |
| _salesforceleadid | Hubspot | STRING | Salesforce Lead ID | 00Q4W00001OcP1dUAF |
| _salesforcecontactid | Hubspot | STRING | Salesforce Contact ID | 0034W000036RY39QAG |
| _assettitle | Hubspot | STRING | Asset Title | How to Fix Failing Procurement Technology Before it’s Too Late |
| _assettype | Hubspot | STRING | Asset Type | Recap |
| _livedate | Hubspot | DATE | Campaign Live Date | @November 8, 2023 |
| _employee_range | Hubspot | STRING | Employee Range | https://dp.2x.marketing/airtable-images/n/%5B2023-11-15-00-33-39%5D___image.png |
| _requestername | Hubspot | STRING | Requester Name | Colin |
| _emailsegment | Hubspot | STRING | Email Segment | OM TOFU01 #12 |
| _campaignsegment | Hubspot | STRING | Campaign Segment | OM: Multiple Industries |
| _emailid | Hubspot | INT64 | Email ID | 144296746034 |
| _totalPageViews | Hubspot | INT64 | Total Page Views | 3 |
| _averagePageViews | Hubspot | FLOAT64 | Average Page Views | 3.0 |
| _senddate | Hubspot | DATE | Campaign Send Date | @November 8, 2023 |
| _campaignCode | Hubspot | STRING | Campaign Code | @November 8, 2023_NT-12_TOFU01_OM |
| _contentitem | Content Airtable | STRING | Content Item | Fighting Inflation with Better Buying video LP. |
| _contenttype | Content Airtable | STRING | Content Type | Guide |
| _gatingstrategy | Content Airtable | STRING | Gating Strategy | No |
| _homeurl | Content Airtable | STRING | Home URL Content | https://logicsource.com/lp/optimizing-priority-99/ |
| _summary | Content Airtable | STRING | Content Summary | KEY FACT: Non-clinical spending takes up a staggering 20% to 25% of net patient revenue |
| _status | Content Airtable | STRING | Content Status | In planning |
| _buyerstage | Content Airtable | STRING | Buyer Stage | Awareness |
| _vertical | Content Airtable | STRING | Vertical | HEALTHCARE |
| _persona | Content Airtable | STRING | Persona | CEO,CPO,COO,CFO,CTO |