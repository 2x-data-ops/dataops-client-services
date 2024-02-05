# Ads Optimization

### Objectives

- Standardized A/B Testing Framework and Articulation of Impact Testing vs no Testing
- The campaigns team normally monitors their ongoing campaigns and with this initiative, the data team is going to support them by analyzing their campaign performances and providing campaign optimization recommendations.
- The data ops will support in the development of the data pipeline from 6sense and LinkedIn and connect it with the respective airtable that store the brief information regarding the ads respectively.

### Process Flow

![Untitled](Ads%20Optimization%207ad358db148e44b59d2c0b6c5ad894b3/Untitled.png)

### **Data Dictionary for final ads optimization dataset**

| Field Name | Data Type | Data Format | Description | Example |
| --- | --- | --- | --- | --- |
| Ad Name | String |  | Ad name for by respective campaigns |  |
| Campaign ID | String |  | Unique ID for each campaigns |  |
| Campaign Date | Date | YYYY-MM-DD | The daily performance date/Extract date for every campaign |  |
| Spent | Float | $ | The spent for each campaign based on the budget allocated |  |
| Clicks | Integer |  | The number of clicks for each campaign/ad |  |
| Impressions | Integer |  | The number of times the content displayed for each campaign/ad |  |
| Ad Copy | String |  | Ad text or caption for the specific ad/campaign | Eliminate Distribution Inefficiencies with 5 Key Secrets |
| CTA Copy | String |  | Prompt for the reader to engage | Learn More, Read Now |
| Design Template | String |  | Set of layout | Copy Design |
| Size | String |  | Size of the ad image for respective ad | 300x250, 160x600 |
| Platform | String |  | The platform of the specific campaign | 6sense, LinkedIn |
| Segment | String |  | The segment for specific campaign that set by the client/internal team | Consideration, Decision & Purchase |
| Design Color | String |  | Color of the ad image design | Light, Dark |
| Design Blurb | String |  | Additional text in the design image | With, Without |
| Logos | String |  | The company logos | With, Without |
| Copy Messaging | String |  | The theme and type of message from the ad copy | Solution, Problem, Benefit |
| Copy Asset Type | String |  | Type of asset type | Guide, Infographic, Case Study, Video |
| Copy Tone | String |  | The tone of the message | Aspirational, FOMO |
| Copy Product Company Name | String |  | The company’s name mentioned in the copy | Yes, No |
| Copy Statistic Proof Point | String |  | The statistics mentioned in the copy | Yes, No |
| CTA Copy Soft/Hard | String |  | The type of call to action | Soft CTA, Hard CTA |
| Screenshot | String |  | The graphic for specific ad | https://dp.2x.marketing/airtable-images/o/%5B2023-10-30-01-55-22%5D___Content-A-336X280.png |

### ERD - Ads Optimization

[https://docs.google.com/presentation/d/1XdfCeKghXDJuWt-nUinzFCQbBG9l4DCcRskffSGaL_k/edit?usp=sharing](https://docs.google.com/presentation/d/1XdfCeKghXDJuWt-nUinzFCQbBG9l4DCcRskffSGaL_k/edit?usp=sharing)

### Action that should be taken

| Platform | Condition | Action |
| --- | --- | --- |
| 6sense | The ads performance were extracted weekly using extract date | <li> Make sure the ad name in the ads performance data were same as in airtable (unless ad ID is provided in the ads performance data) <li> Make sure to confirm with the data analyst on <ul> </li> <li> How they extract the ads overview data <li> How frequent they update the latest data? <li> Are they truncate the new data from old data? |
| 6sense | The ads performance data were taken by daily performance | <li> Make sure consistently upload the data based on planned to reduce the backlog data in dashboard <li> Make sure to confirm with the data analyst on <ul> <li> How they extract the ads overview data? <li> How frequent they update the latest data? <li> Are they truncate the new data from old data? |
| LinkedIn | The LinkedIn performance were measure using LinkedIn campaign manager | Make sure to share the LinkedIn campaign manager access (confirm it with data ops if its already being integrated or not) |
| LinkedIn | The LinkedIn performance were measure using 6sense | Make sure to include in 6sense ID in the airtable since the ad ID in 6sense is not the same as the ad ID in LinkedIn campaign manager |

### Key Takeaways

*Data Ops*

- Make sure to check the ad name in ads overview 6sense table is same as the one in airtable (It’s applicable if the ads overview data from 6sense not contain ad ID – streamlit upload)
- Make sure to confirm with data analyst where they measure the LinkedIn ads performance either it’s on the LinkedIn Campaign Manager or the campaign in 6sense)
- If the data analyst measure their LinkedIn performance data via 6sense, make sure the field ‘6senseID’ were added in ***linkedin_optimization_airtable_ads*** airtable since the ad ID from LinkedIn campaign manager was not same as ad ID from 6sense

*Data Analyst*

- Make sure after the airtable been updated, check the updated airtable data in MySQL playground. If the data is not updated in MySQL Playground, then inform Zi Yan to troubleshoot it
- Ensure all the data populated in the airtable have no human error such as typing error or pasting wrong ads copy (images) into different sizes
- Ads overview upload in CSV2SQL will be having extract date, kindly ensure all extract date reflect accordingly to the campaign. This is important to build a proper visualization for the engagements

### Dashboard Example

| Client | Report Type | Dashboard Link | Script |
| --- | --- | --- | --- |
| Tecsys | 6sense only | https://lookerstudio.google.com/reporting/ae2843e7-7cee-42dd-8fe5-914c28c31925 | tecsys_dashboard_optimization_ads |
| Sandler | 6sense and LinkedIn | https://lookerstudio.google.com/reporting/02f569d4-6426-4281-b0c2-5e7bcfeb3e8b | sandler_dashboard_optimization_ads |
| Diligent | 6sense only | https://lookerstudio.google.com/reporting/309212c8-69ec-46c5-af19-a0c1f51cc831 | diligent_dashboard_optimization_ads |
| Influitive | 6sense only | https://lookerstudio.google.com/reporting/1438f32a-e859-4e72-81e3-13c4e1b29738 | influitive_dashboard_optimization_ads |
| 3X | 6sense and LinkedIn (campaign in 6sense) | https://lookerstudio.google.com/reporting/1438f32a-e859-4e72-81e3-13c4e1b29738 | 3x_dashboard_optimization_ads |