--Initialise Connection Settings
USE WAREHOUSE CHEETAH_WH;
USE DATABASE GRP1_ASG;
USE SCHEMA ASG;
ALTER WAREHOUSE CHEETAH_WH
SET AUTO_SUSPEND = 600;

-- 1. Inspection of tables
DESC TABLE DIMPRODUCTSUBCATEGORY;
SELECT * FROM DIMPROMOTION;
DESC TABLE DIMRESELLER;
SELECT * FROM DIMSALESREASON;
DESC TABLE DIMSALESTERRITORY;

----- [DimProductSubcategory] -----
-- Checking of NULL values for PK
SELECT COUNT(*) AS ProductSubcategoryKey_Null_Count
FROM DimProductSubcategory
WHERE "ProductSubcategoryKey" IS NULL;

-- Checking of duplicate PK
SELECT "ProductSubcategoryKey", COUNT(*) AS Count
FROM DimProductSubcategory
GROUP BY "ProductSubcategoryKey"
HAVING COUNT(*) > 1;

-- Duplicate Business Key checks.
SELECT "ProductSubcategoryAlternateKey", COUNT(*) AS Count
FROM DimProductSubcategory
GROUP BY "ProductSubcategoryAlternateKey"
HAVING COUNT(*) > 1;

-- Cleanse DimProductSubcategory
CREATE OR REPLACE TABLE DimProductSubcategory_Cleaned AS
WITH base AS (
  SELECT
    "ProductSubcategoryKey" AS ProductSubcategoryKey,
    "ProductSubcategoryAlternateKey" AS ProductSubcategoryAlternateKey,
    TRIM("EnglishProductSubcategoryName") AS EnglishProductSubcategoryName,
    TRIM("SpanishProductSubcategoryName") AS SpanishProductSubcategoryName,
    TRIM("FrenchProductSubcategoryName") AS FrenchProductSubcategoryName,
    "ProductCategoryKey" AS ProductCategoryKey
  FROM DimProductSubcategory
  WHERE "ProductSubcategoryKey" IS NOT NULL
),
dedup AS (
  SELECT *
  FROM base
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ProductSubcategoryKey
    ORDER BY
      (IFF(EnglishProductSubcategoryName IS NOT NULL AND EnglishProductSubcategoryName <> '', 1, 0) +
       IFF(ProductCategoryKey IS NOT NULL, 1, 0)
      ) DESC
  ) = 1
)
SELECT * FROM dedup;

-- 3) Optional: quick validation outputs
SELECT COUNT(*) AS ROWS_SOURCE FROM DimProductSubcategory;
SELECT COUNT(*) AS ROWS_CLEAN FROM DimProductSubcategory_Cleaned; -- Verified, both Counts gives <37>. 

-- Verify transformations
SELECT
  ProductSubcategoryKey,
  EnglishProductSubcategoryName,
  ProductCategoryKey
FROM DimProductSubcategory_Cleaned
ORDER BY ProductSubcategoryKey
LIMIT 25;


----- [DimReseller] -----
-- Checking of NULL values for PK
SELECT COUNT(*) AS ResellerKey_Null_Count
FROM DimReseller
WHERE "ResellerKey" IS NULL;

-- Checking of duplicate PK
SELECT "ResellerKey", COUNT(*) AS Count
FROM DimReseller
GROUP BY "ResellerKey"
HAVING COUNT(*) > 1;

-- Duplicate Business Key checks.
SELECT "ResellerAlternateKey", COUNT(*) AS Count
FROM DimReseller
GROUP BY "ResellerAlternateKey"
HAVING COUNT(*) > 1;

-- Cleanse DimReseller
CREATE OR REPLACE TABLE DimReseller_Cleaned AS
WITH base AS (
  SELECT
    "ResellerKey" AS ResellerKey,
    "GeographyKey" AS GeographyKey,
    TRIM("ResellerAlternateKey") AS ResellerAlternateKey,
    TRIM("Phone") AS Phone,
    TRIM("BusinessType") AS BusinessType,
    TRIM("ResellerName") AS ResellerName,
    "NumberEmployees" AS NumberEmployees,
    TRIM("OrderFrequency") AS OrderFrequency,

    -- replace NULL -> 0 for Order Metrics
    COALESCE("OrderMonth", 0) AS OrderMonth,
    COALESCE("FirstOrderYear", 0) AS FirstOrderYear,
    COALESCE("LastOrderYear", 0) AS LastOrderYear,

    TRIM("ProductLine") AS ProductLine,
    TRIM("AddressLine1") AS AddressLine1,

    -- replace NULL -> '' (Empty String) for AddressLine2
    COALESCE(TRIM("AddressLine2"), '') AS AddressLine2,

    "AnnualSales" AS AnnualSales,
    TRIM("BankName") AS BankName,

    -- replace NULL -> 0 for Payment Logic
    COALESCE("MinPaymentType", 0) AS MinPaymentType,
    COALESCE("MinPaymentAmount", 0) AS MinPaymentAmount,

    "AnnualRevenue" AS AnnualRevenue,
    "YearOpened" AS YearOpened

  FROM DimReseller
  WHERE "ResellerKey" IS NOT NULL
),
dedup AS (
  SELECT *
  FROM base
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ResellerKey
    ORDER BY
      (IFF(ResellerAlternateKey IS NOT NULL AND ResellerAlternateKey <> '', 1, 0) +
       IFF(ResellerName IS NOT NULL AND ResellerName <> '', 1, 0) +
       IFF(Phone IS NOT NULL AND Phone <> '', 1, 0) +
       IFF(AddressLine1 IS NOT NULL AND AddressLine1 <> '', 1, 0) +
       IFF(BankName IS NOT NULL AND BankName <> '', 1, 0)
      ) DESC
  ) = 1
)
SELECT * FROM dedup;

-- 3) Optional: quick validation outputs
SELECT COUNT(*) AS ROWS_SOURCE FROM DimReseller;
SELECT COUNT(*) AS ROWS_CLEAN FROM DimReseller_Cleaned; -- Verified, both Counts gives 701. 

-- Verify transformations
SELECT
  ResellerKey,
  ResellerName,
  AddressLine2,
  MinPaymentAmount,
  OrderMonth,
  FirstOrderYear
FROM DimReseller_Cleaned
ORDER BY ResellerKey
LIMIT 25;

----- [DimPromotion] -----
-- Checking of NULL values for PK
SELECT COUNT(*) AS PromotionKey_Null_Count
FROM DimPromotion
WHERE "PromotionKey" IS NULL;

-- Checking of duplicate PK
SELECT "PromotionKey", COUNT(*) AS Count
FROM DimPromotion
GROUP BY "PromotionKey"
HAVING COUNT(*) > 1;

-- Duplicate Business Key checks.
SELECT "PromotionAlternateKey", COUNT(*) AS Count
FROM DimPromotion
GROUP BY "PromotionAlternateKey"
HAVING COUNT(*) > 1;

-- Cleanse DimPromotion
CREATE OR REPLACE TABLE DimPromotion_Cleaned AS
WITH base AS (
  SELECT
    "PromotionKey" AS PromotionKey,
    "PromotionAlternateKey" AS PromotionAlternateKey,
    TRIM("EnglishPromotionName") AS EnglishPromotionName,
    TRIM("SpanishPromotionName") AS SpanishPromotionName,
    TRIM("FrenchPromotionName") AS FrenchPromotionName,
    "DiscountPct" AS DiscountPct,
    TRIM("EnglishPromotionType") AS EnglishPromotionType,
    TRIM("SpanishPromotionType") AS SpanishPromotionType,
    TRIM("FrenchPromotionType") AS FrenchPromotionType,
    TRIM("EnglishPromotionCategory") AS EnglishPromotionCategory,
    TRIM("SpanishPromotionCategory") AS SpanishPromotionCategory,
    TRIM("FrenchPromotionCategory") AS FrenchPromotionCategory,
    "StartDate" AS StartDate,
    "EndDate" AS EndDate,
    "MinQty" AS MinQty,

    -- replace NULL -> 999999 (No Limit) for MaxQty
    COALESCE("MaxQty", 999999) AS MaxQty

  FROM DimPromotion
  WHERE "PromotionKey" IS NOT NULL
),
dedup AS (
  SELECT *
  FROM base
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY PromotionKey
    ORDER BY
      (IFF(EnglishPromotionName IS NOT NULL AND EnglishPromotionName <> '', 1, 0) +
       IFF(EnglishPromotionType IS NOT NULL AND EnglishPromotionType <> '', 1, 0) +
       IFF(EnglishPromotionCategory IS NOT NULL AND EnglishPromotionCategory <> '', 1, 0) +
       IFF(DiscountPct IS NOT NULL, 1, 0)
      ) DESC
  ) = 1
)
SELECT * FROM dedup;

-- 3) Optional: quick validation outputs
SELECT COUNT(*) AS ROWS_SOURCE FROM DimPromotion;
SELECT COUNT(*) AS ROWS_CLEAN FROM DimPromotion_Cleaned; -- Verified, both Counts gives <16>. 

-- Verify transformations
SELECT
  PromotionKey,
  EnglishPromotionName,
  MinQty,
  MaxQty,
  DiscountPct
FROM "DimPromotion_Cleaned"
ORDER BY PromotionKey
LIMIT 25;

----- [DimSalesReason] -----
-- Checking of NULL values for PK
SELECT COUNT(*) AS SalesReasonKey_Null_Count
FROM DimSalesReason
WHERE "SalesReasonKey" IS NULL;

-- Checking of duplicate PK
SELECT "SalesReasonKey", COUNT(*) AS Count
FROM DimSalesReason
GROUP BY "SalesReasonKey"
HAVING COUNT(*) > 1;

-- Duplicate Business Key checks.
SELECT "SalesReasonAlternateKey", COUNT(*) AS Count
FROM DimSalesReason
GROUP BY "SalesReasonAlternateKey"
HAVING COUNT(*) > 1;

-- Cleanse DimSalesReason
CREATE OR REPLACE TABLE DimSalesReason_Cleaned AS
WITH base AS (
  SELECT
    "SalesReasonKey" AS SalesReasonKey,
    "SalesReasonAlternateKey" AS SalesReasonAlternateKey,
    TRIM("SalesReasonName") AS SalesReasonName,
    TRIM("SalesReasonReasonType") AS SalesReasonReasonType
  FROM DimSalesReason
  WHERE "SalesReasonKey" IS NOT NULL
),
dedup AS (
  SELECT *
  FROM base
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SalesReasonKey
    ORDER BY
      (IFF(SalesReasonName IS NOT NULL AND SalesReasonName <> '', 1, 0) +
       IFF(SalesReasonReasonType IS NOT NULL AND SalesReasonReasonType <> '', 1, 0)
      ) DESC
  ) = 1
)
SELECT * FROM dedup;

-- 3) Optional: quick validation outputs
SELECT COUNT(*) AS ROWS_SOURCE FROM DimSalesReason;
SELECT COUNT(*) AS ROWS_CLEAN FROM DimSalesReason_Cleaned; -- Verified, both Counts gives <insert actual count>. 

-- Verify transformations
SELECT
  SalesReasonKey,
  SalesReasonName,
  SalesReasonReasonType
FROM DimSalesReason_Cleaned
ORDER BY SalesReasonKey
LIMIT 25;

----- [DimSalesTerritory] -----
-- Checking of NULL values for PK
SELECT COUNT(*) AS SalesTerritoryKey_Null_Count
FROM DimSalesTerritory
WHERE "SalesTerritoryKey" IS NULL;

-- Checking of duplicate PK
SELECT "SalesTerritoryKey", COUNT(*) AS Count
FROM DimSalesTerritory
GROUP BY "SalesTerritoryKey"
HAVING COUNT(*) > 1;

-- Duplicate Business Key checks.
SELECT "SalesTerritoryAlternateKey", COUNT(*) AS Count
FROM DimSalesTerritory
GROUP BY "SalesTerritoryAlternateKey"
HAVING COUNT(*) > 1;

-- Cleanse DimSalesTerritory
CREATE OR REPLACE TABLE DimSalesTerritory_Cleaned AS
WITH base AS (
  SELECT
    "SalesTerritoryKey" AS SalesTerritoryKey,
    "SalesTerritoryAlternateKey" AS SalesTerritoryAlternateKey,
    TRIM("SalesTerritoryRegion") AS SalesTerritoryRegion,
    TRIM("SalesTerritoryCountry") AS SalesTerritoryCountry,
    TRIM("SalesTerritoryGroup") AS SalesTerritoryGroup
  FROM DimSalesTerritory
  WHERE "SalesTerritoryKey" IS NOT NULL
),
dedup AS (
  SELECT *
  FROM base
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SalesTerritoryKey
    ORDER BY
      (IFF(SalesTerritoryRegion IS NOT NULL AND SalesTerritoryRegion <> '', 1, 0) +
       IFF(SalesTerritoryCountry IS NOT NULL AND SalesTerritoryCountry <> '', 1, 0)
      ) DESC
  ) = 1
)
SELECT * FROM dedup;

-- 3) Optional: quick validation outputs
SELECT COUNT(*) AS ROWS_SOURCE FROM DimSalesTerritory;
SELECT COUNT(*) AS ROWS_CLEAN FROM DimSalesTerritory_Cleaned; -- Verified, both Counts gives <insert actual count>. 

-- Verify transformations
SELECT
  SalesTerritoryKey,
  SalesTerritoryRegion,
  SalesTerritoryCountry,
  SalesTerritoryGroup
FROM DimSalesTerritory_Cleaned
ORDER BY SalesTerritoryKey
LIMIT 25;