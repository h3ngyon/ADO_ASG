-----[Initialize Connection]-----
USE WAREHOUSE CAT_WH;
USE DATABASE GRP1_ASG;
USE SCHEMA ASG;
ALTER WAREHOUSE CAT_WH
SET AUTO_SUSPEND = 600;
-----[Inspection of Tables]-----
DESC TABLE ProspectiveBuyer;
SELECT * FROM ProspectiveBuyer;
DESC TABLE FactSurveyResponse;
SELECT * FROM FactSurveyResponse;
DESC TABLE FactSalesQuota;
SELECT * FROM FactSalesQuota;
DESC TABLE FactResellerSales;
SELECT * FROM FactResellerSales;
DESC TABLE FactInternetSalesReason;
SELECT * FROM FactInternetSalesReason;
DESC TABLE FactInternetSales;
SELECT * FROM FactInternetSales;
---------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------[Data Cleansing]-----------------------------------------------------------------
-- Check NULL Values for ProspectiveBuyer
SELECT COUNT(*) AS ProspectiveBuyer_Nulls
FROM ProspectiveBuyer
WHERE "ProspectiveBuyerKey" IS NULL;

-- Check Duplicate PK for ProspectiveBuyer
SELECT "ProspectiveBuyerKey", COUNT(*) AS Count
FROM ProspectiveBuyer
GROUP BY "ProspectiveBuyerKey"
HAVING COUNT(*) > 1;

--Check Duplicate Alternate Key for ProspectiveBuyer
SELECT "ProspectAlternateKey", COUNT(*) AS Count
FROM ProspectiveBuyer
GROUP BY "ProspectAlternateKey"
HAVING COUNT(*) > 1;

-- Cleaned Prospective Buyer
CREATE OR REPLACE TABLE PROSPECTIVEBUYER_CLEAN AS
SELECT
    "ProspectiveBuyerKey" AS PROSPECTIVEBUYER_KEY,
    "ProspectAlternateKey" AS PROSPECTALTERNATEKEY,
    TRIM("FirstName") AS FIRSTNAME,
    COALESCE(TRIM("MiddleName"), '') AS MIDDLENAME,
    TRIM("LastName") AS LAST_NAME,
    TRY_TO_DATE("BirthDate") AS BIRTHDATE,
    CASE 
        WHEN "MaritalStatus" = 'M' THEN 'Married'
        WHEN "MaritalStatus" = 'S' THEN 'Single'
        ELSE 'Unknown'
    END AS MARITALSTATUS,
    CASE 
        WHEN "Gender" = 'M' THEN 'Male'
        WHEN "Gender" = 'F' THEN 'Female'
        ELSE 'Other'
    END AS GENDER,
    LOWER(TRIM("EmailAddress")) AS EMAILADDRESS,
    "YearlyIncome" AS YEARLYINCOME,
    "TotalChildren" AS TOTALCHILDREN,
    "NumberChildrenAtHome" AS CHILDRENATHOME,
    "Education" AS EDUCATION,
    "Occupation" AS OCCUPATION,
    "HouseOwnerFlag"::BOOLEAN AS ISHOUSEOWNER,
    "NumberCarsOwned" AS CARSOWNED,
    TRIM("AddressLine1") AS ADDRESSLINE1,
    COALESCE(TRIM("AddressLine2"), '') AS ADDRESSLINE2,
    "City" AS CITY,
    "StateProvinceCode" AS STATEPROVINCECODE,
    "PostalCode" AS POSTALCODE,
    "Phone" AS PHONE
FROM ProspectiveBuyer
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY "ProspectAlternateKey" -- Use the Alternate Key here
    ORDER BY "BirthDate" DESC
) = 1;

SELECT * FROM PROSPECTIVEBUYER_CLEAN;

-- Verify Table Cleansing 
SELECT COUNT(*) AS ROWS_SOURCE FROM ProspectiveBuyer;           -- COUNT: 2059 --
SELECT COUNT(*) AS ROWS_CLEAN FROM PROSPECTIVEBUYER_CLEAN;      -- COUNT: 2053 --
----------------------------------------------------------------------------------------------------------------------------------
-- Check NULL Values for FactSurveyResponse
SELECT COUNT(*) AS FactSurveyResponse_Nulls
FROM FactSurveyResponse
WHERE "SurveyResponseKey" IS NULL;

-- Check Duplicate PK for FactSurveyResponse
SELECT "SurveyResponseKey", COUNT(*) AS Count
FROM FactSurveyResponse
GROUP BY "SurveyResponseKey"
HAVING COUNT(*) > 1;

--Check Duplicate DateKey for FactSurveyResponse
SELECT "DateKey", COUNT(*) AS Count
FROM FactSurveyResponse
GROUP BY "DateKey"
HAVING COUNT(*) > 1;

--Check Duplicate CustomerKey for FactSurveyResponse
SELECT "CustomerKey", COUNT(*) AS Count
FROM FactSurveyResponse
GROUP BY "CustomerKey"
HAVING COUNT(*) > 1;

-- Cleaned Fact Survery Response 

CREATE OR REPLACE TABLE FACTSURVEYRESPONSE_CLEAN AS
SELECT
    "SurveyResponseKey" AS SURVEYRESPONSEKEY,
    TRY_TO_DATE(TO_VARCHAR("DateKey"), 'YYYYMMDD') AS SURVEYDATE,
    "CustomerKey" AS CUSTOMERKEY,
    "ProductCategoryKey" AS PRODUCTCATEGORYKEY,
    COALESCE(TRIM("EnglishProductCategoryName"), '') AS PRODUCTCATEGORY,
    "ProductSubcategoryKey" AS PRODUCTSUBCATEGORYKEY,
    COALESCE(TRIM("EnglishProductSubcategoryName"), '') AS PRODUCTSUBCATEGORY
FROM FactSurveyResponse;
SELECT * FROM FACTSURVEYRESPONSE_CLEAN;

-- Verify Table Cleansing
SELECT COUNT(*) AS ROWS_SOURCE FROM FactSurveyResponse;           -- COUNT: 2727 --
SELECT COUNT(*) AS ROWS_CLEAN FROM FACTSURVEYRESPONSE_CLEAN;      -- COUNT: 2727 --
----------------------------------------------------------------------------------------------------------------------------------
-- Check NULL Values for FactSalesQuota
SELECT COUNT(*) AS FactSalesQuota_Nulls
FROM FactSalesQuota
WHERE "SalesQuotaKey" IS NULL;

-- Check Duplicate PK for FactSalesQuota
SELECT "SalesQuotaKey", COUNT(*) AS Count
FROM FactSalesQuota
GROUP BY "SalesQuotaKey"
HAVING COUNT(*) > 1;

--Check Duplicate Employee Key for FactSalesQuota
-- Does not have to be fixed as data is to log how much employee makes each date
SELECT "EmployeeKey", COUNT(*) AS Count
FROM FactSalesQuota
GROUP BY "EmployeeKey"
HAVING COUNT(*) > 1;
-- Cleaned Fact Sales Quota
CREATE OR REPLACE TABLE FACTSALESQUOTA_CLEAN AS
SELECT
    "SalesQuotaKey" AS SALESQUOTAKEY,
    "EmployeeKey" AS EMPLOYEEKEY,
    
    -- Convert Integer type to Date type
    TRY_TO_DATE(TO_VARCHAR("DateKey"), 'YYYYMMDD') AS QUOTADATE,  
    
    "CalendarYear" AS CALENDARYEAR,
    "CalendarQuarter" AS CALENDARQUARTER,
    
    -- Change data type from FLOAT to DECIMAL
    CAST("SalesAmountQuota" AS DECIMAL(18,2)) AS SALESAMOUNTQUOTA
FROM FactSalesQuota;
SELECT * FROM FACTSALESQUOTA_CLEAN;

-- Verify Table Cleansing
SELECT COUNT(*) AS ROWS_SOURCE FROM FactSalesQuota;           -- COUNT: 163 --
SELECT COUNT(*) AS ROWS_CLEAN FROM FACTSALESQUOTA_CLEAN;      -- COUNT: 163 --
-----------------------------------------------------------------------------------------------------------------------------------
-- Clean Fact Reseller Sales
CREATE OR REPLACE TABLE FACTRESELLERSALES_CLEAN AS
SELECT
    "ProductKey" AS ProductKey,

    -- Convert Integer type (YYYYMMDD) to Date type
    TRY_TO_DATE(TO_VARCHAR("OrderDateKey"), 'YYYYMMDD') AS OrderDate,
    TRY_TO_DATE(TO_VARCHAR("DueDateKey"), 'YYYYMMDD') AS DueDate,
    TRY_TO_DATE(TO_VARCHAR("ShipDateKey"), 'YYYYMMDD') AS ShipDate,
    
    "ResellerKey" AS ResellerKey,
    "EmployeeKey" AS EmployeeKey,
    "PromotionKey" AS PromotionKey,
    "CurrencyKey" AS CurrencyKey,
    "SalesTerritoryKey" AS SalesTerritoryKey,
    
    TRIM("SalesOrderNumber") AS SalesOrderNumber,
    "SalesOrderLineNumber" AS SalesOrderLineNumber,
    "RevisionNumber" AS RevisionNumber,
    "OrderQuantity" AS OrderQuantity,
     
    -- Change numeric data types to DECIMAL(18,4) for financial precision
    CAST("UnitPrice" AS DECIMAL(18,4)) AS UnitPrice,
    CAST("ExtendedAmount" AS DECIMAL(18,4)) AS ExtendedAmount,
    CAST("UnitPriceDiscountPct" AS DECIMAL(18,4)) AS UnitPriceDiscountPct,
    CAST("DiscountAmount" AS DECIMAL(18,4)) AS DiscountAmount,
    CAST("ProductStandardCost" AS DECIMAL(18,4)) AS ProductStandardCost,
    CAST("TotalProductCost" AS DECIMAL(18,4)) AS TotalProductCost,
    CAST("SalesAmount" AS DECIMAL(18,4)) AS SalesAmount,
    CAST("TaxAmt" AS DECIMAL(18,4)) AS TaxAmount,
    CAST("Freight" AS DECIMAL(18,4)) AS Freight,
    
    -- Clean string fields and handle NULLs/empty values
    COALESCE(NULLIF(TRIM("CarrierTrackingNumber"), ''), 'Unknown') AS CarrierTrackingNumber,
    COALESCE(NULLIF(TRIM("CustomerPONumber"), ''), 'Unknown') AS CustomerPONumber
FROM ASG.FactResellerSales;

-- Verify Table Cleansing
SELECT COUNT(*) AS ROWS_SOURCE FROM ASG.FactResellerSales;           -- COUNT: 60855 --
SELECT COUNT(*) AS ROWS_CLEAN FROM FACTRESELLERSALES_CLEAN;          -- COUNT: 60855 --
----------------------------------------------------------------------------------------------------------------------------------
-- Check NULL Values for FactInternetSalesReason
SELECT COUNT(*) AS FactInternetSalesReason_Nulls
FROM FactInternetSalesReason
WHERE "SalesOrderNumber" IS NULL;

-- Check Duplicate records for FactInternetSalesReason
-- Duplicate records remain as each row is unique to the sales reason key. Hence use "SELECT DISTINCT" when doing sales related data
SELECT "SalesOrderNumber", COUNT(*) AS Count
FROM FactInternetSalesReason
GROUP BY "SalesOrderNumber"
HAVING COUNT(*) > 1;

-- Clean Fact Internet Sales Reason

CREATE OR REPLACE TABLE FACTINTERNETSALESREASON_CLEAN AS
SELECT DISTINCT
    TRIM("SalesOrderNumber") AS SalesOrderNumber,
    "SalesOrderLineNumber" AS SalesOrderLineNumber,
    "SalesReasonKey" AS SalesReasonKey
FROM FactInternetSalesReason;

SELECT * FROM FACTINTERNETSALESREASON_CLEAN;

-- Verify Table Cleansing
SELECT COUNT(*) AS ROWS_SOURCE FROM FactInternetSalesReason;           -- COUNT: 64515
SELECT COUNT(*) AS ROWS_CLEAN FROM FACTINTERNETSALESREASON_CLEAN;      -- COUNT: 64515 --
----------------------------------------------------------------------------------------------------------------------------------  
-- Clean Fact Internet Sales
CREATE OR REPLACE TABLE FACTINTERNETSALES_CLEAN AS
SELECT
    "ProductKey" AS ProductKey,

    TRY_TO_DATE(TO_VARCHAR("OrderDateKey"), 'YYYYMMDD') AS OrderDate,
    TRY_TO_DATE(TO_VARCHAR("DueDateKey"), 'YYYYMMDD') AS DueDate,
    TRY_TO_DATE(TO_VARCHAR("ShipDateKey"), 'YYYYMMDD') AS ShipDate,
    
    "CustomerKey" AS CustomerKey,
    "PromotionKey" AS PromotionKey,
    "CurrencyKey" AS CurrencyKey,
    "SalesTerritoryKey" AS SalesTerritoryKey,
    
    "SalesOrderNumber" AS SalesOrderNumber,
    "SalesOrderLineNumber" AS SalesOrderLineNumber,
    "RevisionNumber" AS RevisionNumber,
    "OrderQuantity" AS OrderQuantity,
     
    -- Change data type from FLOAT to DECIMAL
    CAST("UnitPrice" AS DECIMAL(18,4)) AS UnitPrice,
    CAST("ExtendedAmount" AS DECIMAL(18,4)) AS ExtendedAmount,
    CAST("UnitPriceDiscountPct" AS DECIMAL(18,4)) AS UnitPriceDiscountPct,
    CAST("DiscountAmount" AS DECIMAL(18,4)) AS DiscountAmount,
    CAST("ProductStandardCost" AS DECIMAL(18,4)) AS ProductStandardCost,
    CAST("TotalProductCost" AS DECIMAL(18,4)) AS TotalProductCost,
    CAST("SalesAmount" AS DECIMAL(18,4)) AS SalesAmount,
    
    CAST("TaxAmt" AS DECIMAL(18,4)) AS TaxAmount,
    CAST("Freight" AS DECIMAL(18,4)) AS Freight,
    
    -- Carrier Tracking Number and Customer PO Number 
    COALESCE("CarrierTrackingNumber", '') AS CarrierTrackingNumber,
    COALESCE("CustomerPONumber", '') AS CustomerPONumber
FROM FactInternetSales;

SELECT * FROM FACTINTERNETSALES_CLEAN;
----------------------------------------------------------------------------------------------------------------------------------------------
--------------------------------------[Transformation for Customer Analytics]-----------------------------------------------------------------
--- RFM Analysis: Value of Customers---
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
FROM ASG.DIMCUSTOMER_CLEAN c
JOIN ASG.DIMGEOGRAPHY_CLEAN g ON c.GeographyKey = g.GeographyKey
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
FROM ASG.PROSPECTIVEBUYER_CLEAN p;

SELECT * FROM ANALYTICS_PROSPECT_PRIORITY;

--- Analytics Product Interest: Products frequently bought together as well as relationship between customer and products bought ---
CREATE OR REPLACE TABLE ANALYTICS_PRODUCT_INTEREST AS
SELECT 
    c.Customer_Segment,
    s.PRODUCTCATEGORY,
    COUNT(*) AS Interest_Count
FROM FACTSURVEYRESPONSE_CLEAN s
JOIN ANALYTICS_CUSTOMER_360 c ON s.CUSTOMERKEY = c.CustomerKey
GROUP BY 1, 2
ORDER BY 1, 3 DESC;

SELECT * FROM ANALYTICS_PRODUCT_INTEREST LIMIT 10;
----------------------------------------------------------------------------------------------------------------------------------------------