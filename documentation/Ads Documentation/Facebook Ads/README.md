# Facebook Ads

# About Facebook Ads

Facebook Ads are paid advertisements through which you can advertise your products and target customers according to their gender, age, language, purchasing decisions

# Comparison between the ads

| Characteristics | LinkedIn Ads | Google Ads | Facebook Ads |
| --- | --- | --- | --- |
| Audience | Highly perfect for B2B company | It may helpful for B2B and B2C company | Can create personalized ads according to ad type |
| Targeting Options | Good for ABM implementation - when people sign up, the entire details such as job title, industry, company name, company size provided | Offer customer matches - data taken from search, youtube, gmail accounts | Have broader reach and targets bulk customers - Most of the people have Facebook account especially during rise of smartphone |

# Facebook Ads Structure

# ERD - Facebook Ads

[https://docs.google.com/presentation/d/1zzv-_91RYEWhCGiqKW7rTmEIX8Bvx6INFMPo8iLynvk/edit?usp=sharing](https://docs.google.com/presentation/d/1zzv-_91RYEWhCGiqKW7rTmEIX8Bvx6INFMPo8iLynvk/edit?usp=sharing)

# Key Takeaways

1. Some of the performance metrics were in array and need to unnest it to retrieved the information such as:
    - Landing Page Click
    - Photo View
    - Conversions
    - Page Engagements
2. Campaigns is the highest level while ads is the lowest level

![Untitled](Facebook%20Ads%2098fa71acd8cf4b73bc27e88948fc4d57/Untitled.png)

1. Contain the performance by demographic

![Untitled](Facebook%20Ads%2098fa71acd8cf4b73bc27e88948fc4d57/Untitled%201.png)

# Different in data structure between Facebook, Google and LinkedIn ads

| Subject | Facebook ads | Google ads | LinkedIn ads |
| --- | --- | --- | --- |
| Ads Hierarchy | Campaign > Ad Set > Ad | Ad Group > Campaign > Ad | Ad Group > Campaign > Ad |
| Timestamp for ads performance | Daily and contain 1 day, 7 days and 28 days performance projection | Daily | Daily |
| Aggregate performance based on segment | <li> Yes (only for Age/Gender, Country, Designated Market Area (state), Platform or Device, Region) <li> Supported by the API | <li> No | <li> Yes (by company, industry, job function, seniority, company size etc) <li> Not supported by API. Need to use csv2sql |
| Social media performance | Not supported by API | Not supported by API | Not supported by API |