-----[Initialize Connection]-----
USE WAREHOUSE CAT_WH;
USE DATABASE GRP1_ASG;
USE SCHEMA ASG_CLEAN;
ALTER WAREHOUSE CAT_WH
SET AUTO_SUSPEND = 600;

-----[Inspection of Tables]-----
DESC TABLE ASG_RAW.ProspectiveBuyer;
SELECT * FROM ASG_RAW.ProspectiveBuyer;
DESC TABLE ASG_RAW.FactSurveyResponse;
SELECT * FROM ASG_RAW.FactSurveyResponse;
DESC TABLE ASG_RAW.FactSalesQuota;
SELECT * FROM ASG_RAW.FactSalesQuota;
DESC TABLE ASG_RAW.FactResellerSales;
SELECT * FROM ASG_RAW.FactResellerSales;
DESC TABLE ASG_RAW.FactInternetSalesReason;
SELECT * FROM ASG_RAW.FactInternetSalesReason;
DESC TABLE ASG_RAW.FactInternetSales;
SELECT * FROM ASG_RAW.FactInternetSales;

---------------------------------------------------------------------------------------------------------------------------------------
------------------------------------------------------[Data Cleansing]-----------------------------------------------------------------
-- Check NULL Values for ProspectiveBuyer
SELECT COUNT(*) AS ProspectiveBuyer_Nulls
FROM ASG_RAW.ProspectiveBuyer
WHERE "ProspectiveBuyerKey" IS NULL;

-- Check Duplicate PK for ProspectiveBuyer
SELECT "ProspectiveBuyerKey", COUNT(*) AS Count
FROM ASG_RAW.ProspectiveBuyer
GROUP BY "ProspectiveBuyerKey"
HAVING COUNT(*) > 1;

--Check Duplicate Alternate Key for ProspectiveBuyer
SELECT "ProspectAlternateKey", COUNT(*) AS Count
FROM ASG_RAW.ProspectiveBuyer
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
FROM ASG_RAW.ProspectiveBuyer
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY "ProspectAlternateKey" -- Use the Alternate Key here
    ORDER BY "BirthDate" DESC
) = 1;

SELECT * FROM PROSPECTIVEBUYER_CLEAN;

-- Verify Table Cleansing 
SELECT COUNT(*) AS ROWS_SOURCE FROM ASG_RAW.ProspectiveBuyer;           -- COUNT: 2059 --
SELECT COUNT(*) AS ROWS_CLEAN FROM PROSPECTIVEBUYER_CLEAN;      -- COUNT: 2053 --

----------------------------------------------------------------------------------------------------------------------------------
-- Check NULL Values for FactSurveyResponse
SELECT COUNT(*) AS FactSurveyResponse_Nulls
FROM ASG_RAW.FactSurveyResponse
WHERE "SurveyResponseKey" IS NULL;

-- Check Duplicate PK for FactSurveyResponse
SELECT "SurveyResponseKey", COUNT(*) AS Count
FROM ASG_RAW.FactSurveyResponse
GROUP BY "SurveyResponseKey"
HAVING COUNT(*) > 1;

--Check Duplicate DateKey for FactSurveyResponse
SELECT "DateKey", COUNT(*) AS Count
FROM ASG_RAW.FactSurveyResponse
GROUP BY "DateKey"
HAVING COUNT(*) > 1;

--Check Duplicate CustomerKey for FactSurveyResponse
SELECT "CustomerKey", COUNT(*) AS Count
FROM ASG_RAW.FactSurveyResponse
GROUP BY "CustomerKey"
HAVING COUNT(*) > 1;

-- Cleaned Fact Survey Response 
CREATE OR REPLACE TABLE FACTSURVEYRESPONSE_CLEAN AS
SELECT
    "SurveyResponseKey" AS SURVEYRESPONSEKEY,
    TRY_TO_DATE(TO_VARCHAR("DateKey"), 'YYYYMMDD') AS SURVEYDATE,
    "CustomerKey" AS CUSTOMERKEY,
    "ProductCategoryKey" AS PRODUCTCATEGORYKEY,
    COALESCE(TRIM("EnglishProductCategoryName"), '') AS PRODUCTCATEGORY,
    "ProductSubcategoryKey" AS PRODUCTSUBCATEGORYKEY,
    COALESCE(TRIM("EnglishProductSubcategoryName"), '') AS PRODUCTSUBCATEGORY
FROM ASG_RAW.FactSurveyResponse;
SELECT * FROM FACTSURVEYRESPONSE_CLEAN;

-- Verify Table Cleansing
SELECT COUNT(*) AS ROWS_SOURCE FROM ASG_RAW.FactSurveyResponse;           -- COUNT: 2727 --
SELECT COUNT(*) AS ROWS_CLEAN FROM FACTSURVEYRESPONSE_CLEAN;      -- COUNT: 2727 --

----------------------------------------------------------------------------------------------------------------------------------
-- Check NULL Values for FactSalesQuota
SELECT COUNT(*) AS FactSalesQuota_Nulls
FROM ASG_RAW.FactSalesQuota
WHERE "SalesQuotaKey" IS NULL;

-- Check Duplicate PK for FactSalesQuota
SELECT "SalesQuotaKey", COUNT(*) AS Count
FROM ASG_RAW.FactSalesQuota
GROUP BY "SalesQuotaKey"
HAVING COUNT(*) > 1;

--Check Duplicate Employee Key for FactSalesQuota
-- Does not have to be fixed as data is to log how much employee makes each date
SELECT "EmployeeKey", COUNT(*) AS Count
FROM ASG_RAW.FactSalesQuota
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
FROM ASG_RAW.FactSalesQuota;
SELECT * FROM FACTSALESQUOTA_CLEAN;

-- Verify Table Cleansing
SELECT COUNT(*) AS ROWS_SOURCE FROM ASG_RAW.FactSalesQuota;           -- COUNT: 163 --
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
FROM ASG_RAW.FactResellerSales;

-- Verify Table Cleansing
SELECT COUNT(*) AS ROWS_SOURCE FROM ASG_RAW.FactResellerSales;           -- COUNT: 60855 --
SELECT COUNT(*) AS ROWS_CLEAN FROM FACTRESELLERSALES_CLEAN;           -- COUNT: 60855 --

----------------------------------------------------------------------------------------------------------------------------------
-- Check NULL Values for FactInternetSalesReason
SELECT COUNT(*) AS FactInternetSalesReason_Nulls
FROM ASG_RAW.FactInternetSalesReason
WHERE "SalesOrderNumber" IS NULL;

-- Check Duplicate records for FactInternetSalesReason
SELECT "SalesOrderNumber", COUNT(*) AS Count
FROM ASG_RAW.FactInternetSalesReason
GROUP BY "SalesOrderNumber"
HAVING COUNT(*) > 1;

-- Clean Fact Internet Sales Reason
CREATE OR REPLACE TABLE FACTINTERNETSALESREASON_CLEAN AS
SELECT DISTINCT
    TRIM("SalesOrderNumber") AS SalesOrderNumber,
    "SalesOrderLineNumber" AS SalesOrderLineNumber,
    "SalesReasonKey" AS SalesReasonKey
FROM ASG_RAW.FactInternetSalesReason;

SELECT * FROM FACTINTERNETSALESREASON_CLEAN;

-- Verify Table Cleansing
SELECT COUNT(*) AS ROWS_SOURCE FROM ASG_RAW.FactInternetSalesReason;           -- COUNT: 64515
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
FROM ASG_RAW.FactInternetSales;

SELECT * FROM FACTINTERNETSALES_CLEAN;

-- Verify Table Cleansing
SELECT COUNT(*) AS ROWS_SOURCE FROM ASG_RAW.FactInternetSales;           -- Optional Verification
SELECT COUNT(*) AS ROWS_CLEAN FROM FACTINTERNETSALES_CLEAN;
----------------------------------------------------------------------------------------------------------------------------------------------
