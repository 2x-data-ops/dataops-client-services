# Google Ads and Google Analytics 4

***Comparison in different connection***

### **Different between Google Ads and Google Analytics 4 (GA4)**

| Google Ads | Google Analytics 4 (GA4) |
| --- | --- |
| Used for creating and managing online advertising campaigns | Tracking analyze the website performance |
| It enables businesses to create targeted advertisements that appear in search results when users search for specific keywords related | Google Analytics provides data on various metrics such as the number of visitors, their geographical locations, the devices they use, the pages they visit, the time they spend on the website, and the actions they take, among others |
| Helps businesses reach their target audience through advertising | Provides valuable insights into user behavior, helping businesses make informed decisions about their online strategies |

More on Google Analytics: [Click Here](https://2xmarketing.sharepoint.com/:p:/s/DataPlayground/EUm3NGkCPjVNnPe2z1Pz7FkBFASEpJI5XOIrDog7FmsAvw?e=NW4Rir)

# **Google Ads**

### **Differentiation in data flow**

![Untitled](Google%20Ads%20and%20Google%20Analytics%204%20421f0e376c2644b1a0db90d1b199b570/Untitled.png)

### Direct Connection vs Stitch Integration

| Direct Connection |  | Stitch Integration to BigQuery |
| --- | --- | --- |
| 1 account | Number of account per data source | Can go more than 1 account (Example: Fastmarkets, BRP) |
| 12 hours as default | Data Refresh Interval time | 24 hours depends on schedule in stitch integration and Big Query |
| Low possibility of having a discrepancy – but if having it, its totally on the Google Ads itself | Troubleshooting | Need to troubleshoot if having a discrepancy (Data Ops Support) |
| No credit consume | Credit consuming | Have the credit limit per month and additional charge on Big Query data that connect to Looker studio |
| Not available and need to blend with airtable | Ad Graphic / Screenshot | Not available and need to join with airtable via Big Query |
| Cover almost all basic field including  <ul> <li> Basic metrics (Impressions, Clicks, Spent, Budget, Conversions etc) </li> <li> Calculated field (CPC, CTR, CPM, Bounce rate, Conversion rate, etc) </li> <li> Video performance metrics (Video played, views, YouTube Earned view etc) </li> <li> Website visit parameter <ul> | Metrics available <ul> | Cover almost all basic field including </li> <li> Basic metrics (Impressions, Clicks, Spent, Budget, Conversions etc) <li> Calculated field (CPC, CTR, CPM, Bounce rate, Conversion rate, etc) <li> Video performance metrics (Video played and view) <li> Active viewable metrics |
| Metrics that only available in Direct Connection </li> <li> YouTube Earned metrics (Likes, Shared, Subscribers, Views) </li> <li> Website visit parameter </li> <li> Any custom field or extended field from Google Tag Manager | Metrics available (only for specific connection) | Metrics that only available in Big Query </li> <li> Active viewable metrics </li> <li> https://support.google.com/admanager/answer/6233478?hl=en#:~:text=Active%20View%20measurement%20terminology&text=Measurable%20impressions%3A%20Impressions%20that%20can,a%20subset%20of%20measurable%20impressions. |
| <li> Google SEM (Search) Campaign </li> <li> Google Display Campaign </li> <li> Google Search Keyword </li> <li> Google Display Keyword </li> <li> Google Ads Variation Search </li> <li> Google Ads Variation Display </li> <li> Google Video Performance </li> <li> Google Search Query | Report Type | <li> Google SEM (Search) Campaign </li> <li> Google Display Campaign </li> <li> Google Search Keyword </li> <li> Google Ads Variation Search </li> <li> Google Video Performance </li> <li> Google Search Query <ul> |
| Available performance report for all ads, campaign and ad group level | Google Display performance report | Only available for campaign and ad group level |

![Direct Connection](Google%20Ads%20and%20Google%20Analytics%204%20421f0e376c2644b1a0db90d1b199b570/Untitled%201.png)

Direct Connection

![Stitch Integration to BigQuery](Google%20Ads%20and%20Google%20Analytics%204%20421f0e376c2644b1a0db90d1b199b570/Untitled%202.png)

Stitch Integration to BigQuery

| Same as Google ads, it’s a real time and the historical data can be retrieve using date filter | Data Structure | Data are transparent as can see the value of historical data in BigQuery |
| --- | --- | --- |
| Contain all essential dimension except </li> <li> Ad Name </li> <li> Ad Graphic | Dimension | Contain all essential dimension except </li> <li> Ads name (for Google Search) </li> <li> Ad Graphic |
| It already having a fixed field and having its own aggregated metrics and difficult to create a new formulated field | Field Customization | Can be customized in Big Query since we can formulate it using base parameter |
| Not possible to combined across various platform | Combination ads performance in various platform | Possible to combined across various platform such as LinkedIn and 6sense |

### FAQ

| Question | Answer |
| --- | --- |
| If I have found out the discrepancy during connect the data via direct connection, should I raise up the data ops ticket for troubleshooting? | No need to raise up as the direct connection is totally direct from Google Ads. The problem might be happened because of the Google Ads itself or it doesn’t reach the refresh period yet especially for the active campaign |
| Is there any alternative to retrieved the ads name seem its not really pop up using direct connection and stitch integration? | Can include the information in airtable hence we can join it into the respective table |
| If my client having a multiple account, how do I differentiate the performance for each account? | There is 1 segment field named as “descriptive_name” that can segregate the performance by account |

### Recommendation

| Condition | Recommendation |
| --- | --- |
| The client has multiple Google Ads accounts and wishes to have them combined into one data source | Stitch Integration |
| The client want to construct standard reporting, and the client only has one account in Google Ads | Direct Connection |
| The client has multiple reports that they want to retrieve in a dashboard, including SEM, Display, and Keywords | Direct Connection |

### Dashboard Example

Choice 1: Direct Connection

| Client | Report Type | Dashboard Link |
| --- | --- | --- |
| Tecsys | Google Display Performance | https://lookerstudio.google.com/reporting/12942b55-cd4e-4bed-b979-6deb10001311 |
| Fastmarkets | Google Search Performance | https://lookerstudio.google.com/reporting/c4d2c386-5534-4951-a352-0fdecf7d9666 |

Choice 2: Stitch Integration to BigQuery

| Client | Report Type | Dashboard Link |
| --- | --- | --- |
| Brightcove | <li> Google Search and Video Performance </li> <li> Google Search Keywords | https://lookerstudio.google.com/reporting/7eb071bf-3f05-4f34-9d54-8f4518065145 |
| Quantum | Google Search (Ad & Campaign Level) / Display (Campaign Level) | https://lookerstudio.google.com/reporting/cc025a98-7d12-468b-a4cd-7bc90f4edc31 |

# **Google Analytics 4 (GA4)**

### Differentiation in data flow

![Untitled](Google%20Ads%20and%20Google%20Analytics%204%20421f0e376c2644b1a0db90d1b199b570/Untitled%203.png)

![Untitled](Google%20Ads%20and%20Google%20Analytics%204%20421f0e376c2644b1a0db90d1b199b570/Untitled%204.png)

### Choice 1: GA4 Direct Connection

**Pro(s)**

1. Template from Google is available (Cover on Summary & Trends, Device Category, Top Traffic Sources, Events & Landing Pages, Conversions & Top Pages and eCommerce)

![Untitled](Google%20Ads%20and%20Google%20Analytics%204%20421f0e376c2644b1a0db90d1b199b570/Untitled%205.png)

1. The result and outcome was directly from GA4
2. Data freshness is on 12 hours interval (it can be customized)

![Untitled](Google%20Ads%20and%20Google%20Analytics%204%20421f0e376c2644b1a0db90d1b199b570/Untitled%206.png)

1. Does not consume credit or additional cost

**Con(s)**

1. Having a quota error issue especially when having too many charts at one data source

![Untitled](Google%20Ads%20and%20Google%20Analytics%204%20421f0e376c2644b1a0db90d1b199b570/Untitled%207.png)

2. The daily token limit is set at 25,000. Therefore, making too many requests or accessing the system simultaneously may exceed this limit, resulting in quota errors

![Untitled](Google%20Ads%20and%20Google%20Analytics%204%20421f0e376c2644b1a0db90d1b199b570/Untitled%208.png)

3. If GA4 does not display the data or if there is a discrepancy, it will also impact the direct connection in the dashboard. In such cases, please contact your campaign team for assistance

![Untitled](Google%20Ads%20and%20Google%20Analytics%204%20421f0e376c2644b1a0db90d1b199b570/Untitled%209.png)

### GA4 Stitch Integration

**Pro(s)**

1. The historical data being store in tabulated

![Untitled](Google%20Ads%20and%20Google%20Analytics%204%20421f0e376c2644b1a0db90d1b199b570/Untitled%2010.png)

2. Can integrate the custom report based on GA4

![Untitled](Google%20Ads%20and%20Google%20Analytics%204%20421f0e376c2644b1a0db90d1b199b570/Untitled%2011.png)

**Con(s)**

1. Can only select up to 10 metrics and 9 dimensions per report. Hence only simple report may be works

![Untitled](Google%20Ads%20and%20Google%20Analytics%204%20421f0e376c2644b1a0db90d1b199b570/Untitled%2012.png)

2. Metric and dimension combinations are subject to Google’s compatibility rules

Refer this link to check the compatibility rules: [ga-dev-tools.google/ga4/dimensions-metrics-explorer/](https://ga-dev-tools.google/ga4/dimensions-metrics-explorer/)

3. Only 1 account per integration is allowed

![Untitled](Google%20Ads%20and%20Google%20Analytics%204%20421f0e376c2644b1a0db90d1b199b570/Untitled%2013.png)

4. There tends to be a daily quota limit, which can result in delays in updating the data

![Untitled](Google%20Ads%20and%20Google%20Analytics%204%20421f0e376c2644b1a0db90d1b199b570/Untitled%2014.png)

### GA4 Big Query Integration

**Pro(s)**

1. Can retrieved the daily performance data (but minus 1 day)

![Untitled](Google%20Ads%20and%20Google%20Analytics%204%20421f0e376c2644b1a0db90d1b199b570/Untitled%2015.png)

2. Consume less credit compared to Stitch integration
3. Suitable to be used if you want to obtain extended information by joining with other CRM tools such as Salesforce

**Con(s)**

1. The historical data is only available from the day it is integrated. Integration requires admin access from the super admin account in Big Query. The Data Ops manager needs access to the client's Google Analytics account for integration

![Untitled](Google%20Ads%20and%20Google%20Analytics%204%20421f0e376c2644b1a0db90d1b199b570/Untitled%2016.png)

2. Naming convention issue

![Untitled](Google%20Ads%20and%20Google%20Analytics%204%20421f0e376c2644b1a0db90d1b199b570/Untitled%2017.png)

### Recommendation

| Condition | Recommendation |
| --- | --- |
| If want to create an overview dashboard for GA4 including (Summary, Device Category, Top Traffic Sources, Events & Landing Pages, Conversions & Top Pages, eCommerce etc.) | Direct Connection |
| If want to create a detail dashboard via connection with CRM | Big Query Integration |

![Untitled](Google%20Ads%20and%20Google%20Analytics%204%20421f0e376c2644b1a0db90d1b199b570/Untitled%2018.png)

### Dashboard Example

**Choice 1: Direct Connection**

| Client | Report Type | Dashboard Link |
| --- | --- | --- |
| Pareto | Website Overview/Blog | https://lookerstudio.google.com/reporting/aa688ebb-77ad-4b74-a5ad-d5cc9ee54b7b/page/p_nm56qwvr7c/edit |
| Sandler | Website Overview/Landing Page performance | https://lookerstudio.google.com/reporting/9951f43a-307b-4eb0-a800-580982280916 |

**Choice 2: Stitch Integration to Big Query**

| Client | Report Type | Dashboard Link |
| --- | --- | --- |
| BRP | Form Submission/Landing Page performance | https://lookerstudio.google.com/reporting/a3d9aaaa-f9b0-4b19-8a89-c5f16d1c2422 |

**Choice 3: Big Query Integration**

| Client | Report Type | Dashboard Link |
| --- | --- | --- |
| PCS | Website Visit (Info linked with Salesforce) | https://lookerstudio.google.com/reporting/42fb3e94-9930-4f34-9d8e-efc4ee3ead92/page/p_drj1anocbd |

The End!

![Untitled](Google%20Ads%20and%20Google%20Analytics%204%20421f0e376c2644b1a0db90d1b199b570/Untitled%2019.png)