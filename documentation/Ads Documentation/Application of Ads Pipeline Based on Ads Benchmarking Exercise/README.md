# Campaign Benchmarking Exercise

### Objective

- Provide an assessment for campaign team to analyzing the effectiveness of current marketing campaigns and how to optimize it.
- To compare the ads/campaign performance across all 2X client in all respective platform
- To determine the performance across platform in different campaign objectives

### How the data gathered?

![Untitled](Campaign%20Benchmarking%20Exercise/Untitled.png)

### Summary of data extracted via Big Query

![Untitled](Campaign%20Benchmarking%20Exercise/Untitled%201.png)

### Data Dictionary

| Field Name | Data Type | Data Format | Description  | Example |
| --- | --- | --- | --- | --- |
| day | Timestamp | YYYY-MM-DD | The daily performance for respective ads/campaign |  |
| campaign_id | String |  | The unique identification for each campaign |  |
| ad_group_id | String |  | The unique identification for each ad group |  |
| ad_id | String |  | The unique identification for each ad |  |
| ad_group_name | String |  | The ad group name |  |
| campaign_name | String |  | The campaign name |  |
| ad_name | String |  | The ad name |  |
| start_date | Timestamp | YYYY-MM-DD | The start date for specific campaign |  |
| end_date | Timestamp | YYYY-MM-DD | The end date for specific campaign |  |
| platform | String |  | The platform used for online advertising | LinkedIn, 6sense, Google SEM/Display |
| client | String |  | Client’s name | EPAM, Hyland, Quantum |
| type | String |  | Type of data | Ad, campaign |
| screenshot | String |  | The graphical image for each ads |  |
| landing_page_url | String |  | The URL for the specific landing page |  |
| ad_type | String |  | The type of ad | Image ad, text ad |
| spent | Float |  | The spent for each campaign based on the budget allocated |  |
| impressions | Integer |  | The number of times the content displayed for each campaign/ad |  |
| clicks | Integer |  | The number of clicks for each campaign/ad |  |
| conversions | Integer |  | Action a user takes in response to an ad |  |
| leads | Integer |  | The number of leads generated from specific campaign/ads |  |
| landing_page_clicks | Integer |  | The clicks towards landing page (website visit only) |  |

### **Full ERD – Campaign Benchmarking Exercise**

https://docs.google.com/presentation/d/1YPDxa90pUb9sXvkILzFGGpg7F0S_4BwE3ImPHGAPJnY/edit?usp=sharing

![Untitled](Campaign%20Benchmarking%20Exercise/Untitled%202.png)

### Expected Questions

| Question | Answer |
| --- | --- |
| Why do Google Display does not contain the ad level performance? | In Google Ads API, the performance for Display campaign were only covered by campaign and ad group level |
| Why need to separate the performance by ads and campaign? | In term of aggregation, we know that the total performance of ads can produce the performance for each campaign respectively. However, since API provide the separate data source for ad and campaign level, then to be safe we do it separately since the benchmarking exercise will evaluate the performance from campaign first and then followed by ads |
| Why the calculated field was not directly scripted in the data source? | It’s more appropriate to do calculated field in the dashboard by using the formula from root metrics such as impressions, spent, clicks etc to ensure the data was flexible and accurate when do a comparison |
| Since airtable is the part of the data source to support some information that not available in API, how about for the client that does not provide the airtable? | To ensure our backend structure was sync, hence the dummy data for airtable will provided for those client that does not have the airtable information. </li> Hence, some of the information related to airtable will be blank |