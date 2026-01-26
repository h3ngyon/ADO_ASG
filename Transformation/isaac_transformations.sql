USE ROLE TRAINING_ROLE;
USE WAREHOUSE CAT_WH;
USE DATABASE GRP1_ASG;
USE SCHEMA ASG_TRANSFORMATIONS;

--------------------------------------[Transformation for Customer Analytics]-----------------------------------------------------------------
--- RFM Analysis: Value of Customers---
CREATE OR REPLACE TABLE ANALYTICS_CUSTOMER_RFM AS
WITH customer_aggregates AS (
    SELECT 
        CustomerKey,
        MAX(OrderDate) AS Last_Purchase_Date,
        COUNT(DISTINCT SalesOrderNumber) AS Frequency,
        SUM(SalesAmount) AS Monetary_Value
    FROM ASG_CLEAN.FACTINTERNETSALES_CLEAN
    GROUP BY CustomerKey
),
rfm_scores AS (
    SELECT 
        CustomerKey,
        -- Reference ASG_CLEAN for the Max date calculation
        DATEDIFF('day', Last_Purchase_Date, (SELECT MAX(OrderDate) FROM ASG_CLEAN.FACTINTERNETSALES_CLEAN)) AS Recency_Days,
        Frequency,
        Monetary_Value,
        -- Scoring from 1 to 5 (5 is best)
        NTILE(5) OVER (ORDER BY Recency_Days DESC) AS R_Score,
        NTILE(5) OVER (ORDER BY Frequency ASC) AS F_Score,
        NTILE(5) OVER (ORDER BY Monetary_Value ASC) AS M_Score
    FROM customer_aggregates
)
SELECT 
    *,
    (R_Score + F_Score + M_Score) AS Total_RFM_Score,
    CASE 
        WHEN Total_RFM_Score >= 13 THEN 'Champions'
        WHEN Total_RFM_Score >= 10 THEN 'Loyal Customers'
        WHEN Total_RFM_Score >= 7 THEN 'At Risk'
        ELSE 'About to Sleep/Lost'
    END AS Customer_Segment
FROM rfm_scores;

SELECT * FROM ANALYTICS_CUSTOMER_RFM;

--- Customer 360 View: Provides a high definition view of each customer ---

CREATE OR REPLACE VIEW ANALYTICS_CUSTOMER_360 AS
SELECT 
    c.CustomerKey,
    c.FirstName || ' ' || c.LastName AS FullName,
    c.Gender,
    c.YearlyIncome,
    c.EnglishEducation AS Education,
    c.EnglishOccupation AS Occupation,
    g.City,
    g.StateProvinceName,
    g.EnglishCountryRegionName AS Country,
    rfm.Customer_Segment,
    rfm.Monetary_Value AS Lifetime_Value,
    rfm.Frequency AS Total_Orders,
    c.DateFirstPurchase
FROM ASG_CLEAN.DIMCUSTOMER_CLEAN c
JOIN ASG_CLEAN.DIMGEOGRAPHY_CLEAN g ON c.GeographyKey = g.GeographyKey
LEFT JOIN ANALYTICS_CUSTOMER_RFM rfm ON c.CustomerKey = rfm.CustomerKey;

SELECT * FROM ANALYTICS_CUSTOMER_360;

--- Analytics Prospective Priority: Which customers are more likely to spend money ---
CREATE OR REPLACE VIEW ANALYTICS_PROSPECT_PRIORITY AS
WITH target_profile AS (
    -- Identify the average income of your top-tier customers
    SELECT AVG(YearlyIncome) as Champion_Avg_Income
    FROM ANALYTICS_CUSTOMER_360
    WHERE Customer_Segment = 'Champions'
)
SELECT 
    p.FIRSTNAME,
    p.LAST_NAME,
    p.EMAILADDRESS,
    p.YEARLYINCOME,
    p.EDUCATION,
    p.OCCUPATION,
    CASE 
        WHEN p.YEARLYINCOME >= (SELECT Champion_Avg_Income FROM target_profile) THEN 'High Priority'
        WHEN p.YEARLYINCOME >= (SELECT Champion_Avg_Income * 0.7 FROM target_profile) THEN 'Medium Priority'
        ELSE 'Low Priority'
    END AS Lead_Score
FROM ASG_CLEAN.PROSPECTIVEBUYER_CLEAN p;

SELECT * FROM ANALYTICS_PROSPECT_PRIORITY;

--- Analytics Product Interest: Products frequently bought together as well as relationship between customer and products bought ---
CREATE OR REPLACE TABLE ANALYTICS_PRODUCT_INTEREST AS
SELECT 
    c.Customer_Segment,
    s.PRODUCTCATEGORY,
    COUNT(*) AS Interest_Count
FROM ASG_CLEAN.FACTSURVEYRESPONSE_CLEAN s
JOIN ANALYTICS_CUSTOMER_360 c ON s.CUSTOMERKEY = c.CustomerKey
GROUP BY 1, 2
ORDER BY 1, 3 DESC;

SELECT * FROM ANALYTICS_PRODUCT_INTEREST LIMIT 10;
----------------------------------------------------------------------------------------------------------------------------------------------