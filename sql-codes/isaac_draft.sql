USE WAREHOUSE CAT_WH;
USE DATABASE GRP1_ASG;
USE SCHEMA ASG;
ALTER WAREHOUSE CAT_WH
SET AUTO_SUSPEND = 600;

SELECT * FROM ProspectiveBuyer;

SELECT * FROM FactSurveyResponse;

SELECT * FROM FactSalesQuota;

SELECT * FROM FactResellerSales;

SELECT * FROM FactInternetSalesReason;

SELECT * FROM FactInternetSales;

-- Cleaned Prospective Buyer
CREATE OR REPLACE TABLE PROSPECTIVE_BUYER_CLEAN AS
SELECT
    "ProspectiveBuyerKey" AS PROSPECTIVE_BUYER_KEY,
    "ProspectAlternateKey" AS PROSPECT_ALTERNATE_KEY,
    TRIM("FirstName") AS FIRST_NAME,
    COALESCE(TRIM("MiddleName"), '') AS MIDDLE_NAME,
    TRIM("LastName") AS LAST_NAME,
    TRY_TO_DATE("BirthDate") AS BIRTH_DATE,
    CASE 
        WHEN "MaritalStatus" = 'M' THEN 'Married'
        WHEN "MaritalStatus" = 'S' THEN 'Single'
        ELSE 'Unknown'
    END AS MARITAL_STATUS,
    CASE 
        WHEN "Gender" = 'M' THEN 'Male'
        WHEN "Gender" = 'F' THEN 'Female'
        ELSE 'Other'
    END AS GENDER,
    LOWER(TRIM("EmailAddress")) AS EMAIL_ADDRESS,
    "YearlyIncome" AS YEARLY_INCOME,
    "TotalChildren" AS TOTAL_CHILDREN,
    "NumberChildrenAtHome" AS CHILDREN_AT_HOME,
    "Education" AS EDUCATION,
    "Occupation" AS OCCUPATION,
    "HouseOwnerFlag"::BOOLEAN AS IS_HOUSE_OWNER,
    "NumberCarsOwned" AS CARS_OWNED,
    TRIM("AddressLine1") AS ADDRESS_LINE_1,
    COALESCE(TRIM("AddressLine2"), '') AS ADDRESS_LINE_2,
    "City" AS CITY,
    "StateProvinceCode" AS STATE_PROVINCE_CODE,
    "PostalCode" AS POSTAL_CODE,
    "Phone" AS PHONE
FROM ProspectiveBuyer
QUALIFY ROW_NUMBER() OVER (PARTITION BY "ProspectiveBuyerKey" ORDER BY "BirthDate" DESC) = 1;

SELECT * FROM PROSPECTIVE_BUYER_CLEAN;

-- Cleaned Fact Survery Response 

CREATE OR REPLACE TABLE FACT_SURVEY_RESPONSE_CLEAN AS
SELECT
    "SurveyResponseKey" AS SURVEY_RESPONSE_KEY,
    TRY_TO_DATE(TO_VARCHAR("DateKey"), 'YYYYMMDD') AS SURVEY_DATE,
    "CustomerKey" AS CUSTOMER_KEY,
    "ProductCategoryKey" AS PRODUCT_CATEGORY_KEY,
    COALESCE(TRIM("EnglishProductCategoryName"), '') AS PRODUCT_CATEGORY,
    "ProductSubcategoryKey" AS PRODUCT_SUBCATEGORY_KEY,
    COALESCE(TRIM("EnglishProductSubcategoryName"), '') AS PRODUCT_SUBCATEGORY
FROM FactSurveyResponse;
SELECT * FROM FACT_SURVEY_RESPONSE_CLEAN;
