# Example : Campaign Performance

**Example Data and Results**

### **Sample Data**

```sql
sqlCopy code
INSERT INTO campaigns (campaign_id, campaign_name, parent_campaign_id) VALUES
(1, 'Campaign A', NULL),
(2, 'Campaign B', 1),
(3, 'Campaign C', 1),
(4, 'Campaign D', 2),
(5, 'Campaign E', 2),
(6, 'Campaign F', 3);

```

### **Results**

After running the recursive query, you might get results like this:

| campaign_id | campaign_name | parent_campaign_id | level |
| --- | --- | --- | --- |
| 1 | Campaign A | NULL | 0 |
| 2 | Campaign B | 1 | 1 |
| 3 | Campaign C | 1 | 1 |
| 4 | Campaign D | 2 | 2 |
| 5 | Campaign E | 2 | 2 |
| 6 | Campaign F | 3 | 2 |

### **Insights from the Results**

1. **Campaign Hierarchy**:
    - **`Campaign A`** is the top-level campaign.
    - **`Campaign B`** and **`Campaign C`** are child campaigns of **`Campaign A`**.
    - **`Campaign D`** and **`Campaign E`** are child campaigns of **`Campaign B`**.
    - **`Campaign F`** is a child campaign of **`Campaign C`**.
2. **Depth of Campaigns**:
    - The hierarchy has three levels (0, 1, and 2).
    - You can see how far each campaign extends in terms of levels.
3. **Reach and Influence**:
    - Analyze the performance metrics (e.g., impressions, clicks, conversions) at each level to understand the effectiveness of the parent campaigns.
    - Identify which campaigns drive the most engagement and conversions down the hierarchy.

### **Conclusion**

Using recursive functions in BigQuery to analyze campaign performance can provide valuable insights into the structure and effectiveness of your marketing efforts. By understanding the hierarchical relationships and measuring the reach and influence of each campaign, you can optimize your marketing strategies, allocate resources more effectively, and ultimately drive better results.