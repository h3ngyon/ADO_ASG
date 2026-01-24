USE ROLE TRAINING_ROLE;
USE WAREHOUSE FERRET_WH;
USE DATABASE GRP1_ASG;
USE SCHEMA ASG;

CREATE OR REPLACE TABLE ANALYTICS_CUSTOMER_RFM AS
WITH customer_aggregates AS (
    SELECT 
        CustomerKey,
        MAX(OrderDate) AS Last_Purchase_Date,
        COUNT(DISTINCT SalesOrderNumber) AS Frequency,
        SUM(SalesAmount) AS Monetary_Value
    FROM ASG.FACTINTERNETSALES_CLEAN
    GROUP BY CustomerKey
),
rfm_scores AS (
    SELECT 
        CustomerKey,
        DATEDIFF('day', Last_Purchase_Date, (SELECT MAX(OrderDate) FROM ASG.FACTINTERNETSALES_CLEAN)) AS Recency_Days,
        Frequency,
        Monetary_Value,
        -- Divide into 5 groups (5 is best, 1 is worst)
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
        ELSE 'Lost'
    END AS Customer_Segment
FROM rfm_scores;

SELECT * FROM ANALYTICS_CUSTOMER_RFM LIMIT 10;