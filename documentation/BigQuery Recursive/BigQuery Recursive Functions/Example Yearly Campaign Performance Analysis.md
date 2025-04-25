# Example : Yearly Campaign Performance Analysis

### **Example Data and Results**

### **Sample Data**

```sql
INSERT INTO campaign_performance (campaign_id, campaign_name, year, parent_campaign_id, performance_metric) VALUES
(1, 'Campaign A', 2021, NULL, 1000),
(1, 'Campaign A', 2022, NULL, 1500),
(1, 'Campaign A', 2023, NULL, 2000),
(2, 'Campaign B', 2021, 1, 800),
(2, 'Campaign B', 2022, 1, 1200),
(3, 'Campaign C', 2022, 1, 500),
(3, 'Campaign C', 2023, 1, 700),
(4, 'Campaign D', 2023, 2, 300);

```

### **Results**

After running the recursive query, you might get results like this:

| campaign_id | campaign_name | year | parent_campaign_id | performance_metric | start_year |
| --- | --- | --- | --- | --- | --- |
| 1 | Campaign A | 2021 | NULL | 1000 | 2021 |
| 1 | Campaign A | 2022 | NULL | 1500 | 2021 |
| 1 | Campaign A | 2023 | NULL | 2000 | 2021 |
| 2 | Campaign B | 2021 | 1 | 800 | 2021 |
| 2 | Campaign B | 2022 | 1 | 1200 | 2021 |
| 3 | Campaign C | 2022 | 1 | 500 | 2022 |
| 3 | Campaign C | 2023 | 1 | 700 | 2022 |
| 4 | Campaign D | 2023 | 2 | 300 | 2023 |

### **Insights from the Results**

1. **Campaign Performance Over Time**:
    - Track the annual performance of each campaign.
    - Understand how the performance of campaigns changes year-over-year.
2. **Parent-Child Relationships**:
    - Analyze how parent campaigns influence the performance of their child campaigns over time.
    - See if there are any trends or patterns in the performance of related campaigns.
3. **Performance Metrics**:
    - Evaluate the effectiveness of campaigns based on the performance metric (e.g., revenue, leads).
    - Identify high-performing campaigns and those that need optimization.

### **Conclusion**

By using recursive functions in BigQuery to analyze campaign performance over different years, you can gain valuable insights into the long-term effectiveness of your marketing efforts. This analysis helps you understand trends, optimize resource allocation, and make data-driven decisions to enhance your marketing strategies.