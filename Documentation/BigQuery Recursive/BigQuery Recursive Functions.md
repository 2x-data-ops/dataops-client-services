# BigQuery Recursive Functions

## **Overview**

[Work with recursive CTEs  |  BigQuery  |  Google Cloud](https://cloud.google.com/bigquery/docs/recursive-ctes)

Recursive functions in BigQuery allow you to perform hierarchical and iterative queries. They enable you to compute values across a set of rows related to the current row without collapsing them into a single output row. This is particularly useful for tasks such as analyzing hierarchical data, calculating running totals, or generating cumulative sums.

## **Key Concepts**

- **Recursive CTE (Common Table Expression)**: A CTE that references itself to process hierarchical data or perform iterative calculations.
- **Anchor Member**: The initial query that serves as the starting point for the recursion.
- **Recursive Member**: The query that references the CTE, allowing for repeated execution until a termination condition is met.

## **Syntax**

```sql
WITH RECURSIVE cte_name AS (
  -- Anchor member
  initial_query

  UNION ALL

  -- Recursive member
  recursive_query
)
SELECT * FROM cte_name;

```

- **cte_name**: The name of the CTE.
- **initial_query**: The base query that initializes the recursive process.
- **recursive_query**: The query that references the CTE to build upon the results of the initial query.

### **Example : Campaign Performance Analysis**

### **Scenario**

Analyze the hierarchical structure of marketing campaigns and measure their reach.

### **Table Structure**

```sql
CREATE TABLE campaigns (
  campaign_id INT64,
  campaign_name STRING,
  parent_campaign_id INT64
);

```

### **Query**

```sql
WITH RECURSIVE campaign_hierarchy AS (
  -- Anchor member: Select top-level campaigns
  SELECT
    campaign_id,
    campaign_name,
    parent_campaign_id,
    0 AS level
  FROM
    campaigns
  WHERE
    parent_campaign_id IS NULL

  UNION ALL

  -- Recursive member: Select child campaigns and increment the level
  SELECT
    c.campaign_id,
    c.campaign_name,
    c.parent_campaign_id,
    ch.level + 1 AS level
  FROM
    campaigns c
  JOIN
    campaign_hierarchy ch
  ON
    c.parent_campaign_id = ch.campaign_id
)
SELECT
  campaign_id,
  campaign_name,
  parent_campaign_id,
  level
FROM
  campaign_hierarchy
ORDER BY
  level, campaign_id;

```

[Example Data](BigQuery%20Recursive%20Functions/Example%20Campaign%20Performance.md)

## **Benefits of Using Recursive Functions**

- **Simplify Complex Hierarchical Queries**: Easily manage and query hierarchical data structures.
- **Improve Data Analysis**: Gain deeper insights into data patterns and relationships.

## **Conclusion**

Recursive functions in BigQuery are powerful tools that can help you perform advanced data analysis and gain valuable insights into hierarchical data structures. 

# Bonus

### **Example : Yearly Campaign Performance Analysis**

### **Scenario**

You want to analyze the performance of your marketing campaigns over different years, identifying how each campaign contributes to the overall performance annually.

### **Table Structure**

Let's assume you have a table **`campaign_performance`** with the following structure:

```sql
CREATE TABLE campaign_performance (
  campaign_id INT64,
  campaign_name STRING,
  year INT64,
  parent_campaign_id INT64,
  performance_metric FLOAT64
);

```

This table includes information about each campaign, the year it was run, its parent campaign (if any), and a performance metric (such as revenue generated, number of leads, etc.).

### **Recursive Query**

Here's the recursive query to analyze the performance of campaigns over different years:

```sql
WITH RECURSIVE yearly_campaign_performance AS (
  -- Anchor member: Select the initial year for each campaign
  SELECT
    campaign_id,
    campaign_name,
    year,
    parent_campaign_id,
    performance_metric,
    year AS start_year
  FROM
    campaign_performance
  WHERE
    year = (SELECT MIN(year) FROM campaign_performance cp WHERE cp.campaign_id = campaign_performance.campaign_id)

  UNION ALL

  -- Recursive member: Add subsequent years' performance for each campaign
  SELECT
    cp.campaign_id,
    cp.campaign_name,
    cp.year,
    cp.parent_campaign_id,
    cp.performance_metric,
    ycp.start_year
  FROM
    campaign_performance cp
  JOIN
    yearly_campaign_performance ycp
  ON
    cp.campaign_id = ycp.campaign_id
  WHERE
    cp.year = ycp.year + 1
)
SELECT
  campaign_id,
  campaign_name,
  year,
  parent_campaign_id,
  performance_metric,
  start_year
FROM
  yearly_campaign_performance
ORDER BY
  campaign_id, year;

```

### **Explanation**

1. **Anchor Member**:
    - Selects the initial year for each campaign.
    - Initializes the recursive process with the first year a campaign was run.
2. **Recursive Member**:
    - Joins subsequent years' performance data to the initial campaign data.
    - Ensures that the subsequent year is exactly one year after the current year (**`cp.year = ycp.year + 1`**).
3. **Final Selection**:
    - Selects all columns from the recursive CTE.
    - Orders the results by campaign ID and year to show the performance progression clearly.
    
[Example Data](BigQuery%20Recursive%20Functions/Example%20Yearly%20Campaign%20Performance%20Analysis.md)

[**Example : Campaign Performance**](BigQuery%20Recursive%20Functions/Example%20Campaign%20Performance.md)

[**Example : Yearly Campaign Performance Analysis**](BigQuery%20Recursive%20Functions%20/Example%20Yearly%20Campaign%20Performance%20Analysis.md)