USE WAREHOUSE CATFISH_WH;
USE DATABASE GRP1_ASG;
USE SCHEMA ASG;
ALTER WAREHOUSE CATFISH_WH
SET AUTO_SUSPEND = 600;

----- [DIMEMPLOYEE] -----
-- Checking of NULL values for PK
SELECT COUNT(*) AS EmployeeKey_Null_Count
FROM DIMEMPLOYEE
WHERE "EmployeeKey" IS NULL;

-- Checking of duplicate PK
SELECT "EmployeeKey", COUNT(*) AS Count
FROM DIMEMPLOYEE
GROUP BY "EmployeeKey"
HAVING COUNT(*) > 1;

-- Duplicate Business Key checks.
SELECT "EmployeeNationalIDAlternateKey", COUNT(*) AS Count
FROM DIMEMPLOYEE
GROUP BY "EmployeeNationalIDAlternateKey"
HAVING COUNT(*) > 1;

-- Cleanse DIMEMPLOYEE
CREATE OR REPLACE TABLE DIMEMPLOYEE_CLEAN AS
WITH base AS (
  SELECT
    "EmployeeKey" AS EmployeeKey,
    "ParentEmployeeKey" AS ParentEmployeeKey,
    "ParentEmployeeNationalIDAlternateKey" AS ParentEmployeeNationalIDAlternateKey,
    TRIM("EmployeeNationalIDAlternateKey") AS EmployeeNationalIDAlternateKey,
    TRIM("FirstName") AS FirstName,
    TRIM("LastName") AS LastName,

    -- replace NULL -> ''
    COALESCE(TRIM("MiddleName"), '') AS MiddleName,
    
    TRIM("Title") AS Title,
    TRIM("EmailAddress") AS EmailAddress,
    TRIM("Phone") AS Phone,
    TRIM("LoginID") AS LoginID,
    TRIM("MaritalStatus") AS MaritalStatus,
    TRIM("Gender") AS Gender,

    "HireDate" AS HireDate,
    "BirthDate" AS BirthDate,

    -- EndDate: NULL means active; set '9999-12-31' date for easier downstream filtering
    COALESCE("EndDate", DATE '9999-12-31') AS EndDate,

    -- Status: NULL -> 'Unknown'
    COALESCE(NULLIF(TRIM("Status"), ''), 'Unknown') AS Status,

    "SalariedFlag" AS SalariedFlag,
    "VacationHours" AS VacationHours,
    "SickLeaveHours" AS SickLeaveHours
  FROM DIMEMPLOYEE
  WHERE "EmployeeKey" IS NOT NULL
),
dedup AS (
  SELECT *
  FROM base
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY EmployeeKey
    ORDER BY
      (IFF(EmployeeNationalIDAlternateKey IS NOT NULL AND EmployeeNationalIDAlternateKey <> '', 1, 0) +
       IFF(FirstName IS NOT NULL AND FirstName <> '', 1, 0) +
       IFF(LastName IS NOT NULL AND LastName <> '', 1, 0) +
       IFF(Title IS NOT NULL AND Title <> '', 1, 0) +
       IFF(LoginID IS NOT NULL AND LoginID <> '', 1, 0) +
       IFF(Status IS NOT NULL AND Status <> '', 1, 0)
      ) DESC
  ) = 1
)
SELECT * FROM dedup;

-- Validation outputs
SELECT COUNT(*) AS ROWS_DIMEMPLOYEE FROM DIMEMPLOYEE;
SELECT COUNT(*) AS ROWS_DIMEMPLOYEE_CLEAN FROM DIMEMPLOYEE_CLEAN; -- Verified, both Counts gives 296. 

-- Verify transformations
SELECT
  EmployeeKey,
  FirstName, MiddleName, LastName,
  Status,
  EndDate,
  ParentEmployeeNationalIDAlternateKey
FROM DIMEMPLOYEE_CLEAN
ORDER BY EmployeeKey
LIMIT 25;

--------------------



----- [DIMGEOGRAPHY] -----
-- Checking of NULL values for PK
SELECT COUNT(*) AS GeographyKey_Null_Count
FROM DIMGEOGRAPHY
WHERE "GeographyKey" IS NULL;

-- Checking of duplicate PK
SELECT "GeographyKey", COUNT(*) AS Count
FROM DIMGEOGRAPHY
GROUP BY "GeographyKey"
HAVING COUNT(*) > 1;

-- Duplicate Business Key checks (no explicit AlternateKey in this table)
-- Use a practical natural key combo check: City + StateProvinceCode + PostalCode
SELECT
  TRIM("City") AS City,
  TRIM("StateProvinceCode") AS StateProvinceCode,
  TRIM("PostalCode") AS PostalCode,
  COUNT(*) AS Count
FROM DIMGEOGRAPHY
GROUP BY 1,2,3
HAVING COUNT(*) > 1;

-- Cleanse DIMGEOGRAPHY
CREATE OR REPLACE TABLE DIMGEOGRAPHY_CLEAN AS
WITH base AS (
  SELECT
    "GeographyKey" AS GeographyKey,

    -- Trim all text columns
    TRIM("City") AS City,
    TRIM("StateProvinceCode") AS StateProvinceCode,
    TRIM("StateProvinceName") AS StateProvinceName,
    TRIM("CountryRegionCode") AS CountryRegionCode,
    TRIM("EnglishCountryRegionName") AS EnglishCountryRegionName,
    TRIM("SpanishCountryRegionName") AS SpanishCountryRegionName,
    TRIM("FrenchCountryRegionName") AS FrenchCountryRegionName,
    TRIM("PostalCode") AS PostalCode,

    "SalesTerritoryKey" AS SalesTerritoryKey
  FROM DIMGEOGRAPHY
  WHERE "GeographyKey" IS NOT NULL
),
fix_nulls AS (
  SELECT
    GeographyKey,

    -- Geography text fields are safe to fill for grouping/maps
    COALESCE(NULLIF(City, ''), 'Unknown') AS City,
    COALESCE(NULLIF(StateProvinceCode, ''), 'Unknown') AS StateProvinceCode,
    COALESCE(NULLIF(StateProvinceName, ''), 'Unknown') AS StateProvinceName,
    COALESCE(NULLIF(CountryRegionCode, ''), 'Unknown') AS CountryRegionCode,
    COALESCE(NULLIF(EnglishCountryRegionName, ''), 'Unknown') AS EnglishCountryRegionName,
    COALESCE(NULLIF(SpanishCountryRegionName, ''), 'Unknown') AS SpanishCountryRegionName,
    COALESCE(NULLIF(FrenchCountryRegionName, ''), 'Unknown') AS FrenchCountryRegionName,
    COALESCE(NULLIF(PostalCode, ''), 'Unknown') AS PostalCode,

    SalesTerritoryKey
  FROM base
),
dedup AS (
  SELECT *
  FROM fix_nulls
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY GeographyKey
    ORDER BY
      (IFF(City IS NOT NULL AND City <> '', 1, 0) +
       IFF(StateProvinceName IS NOT NULL AND StateProvinceName <> '', 1, 0) +
       IFF(EnglishCountryRegionName IS NOT NULL AND EnglishCountryRegionName <> '', 1, 0) +
       IFF(PostalCode IS NOT NULL AND PostalCode <> '', 1, 0)
      ) DESC
  ) = 1
)
SELECT * FROM dedup;

-- Validation outputs
SELECT COUNT(*) AS ROWS_DIMGEOGRAPHY FROM DIMGEOGRAPHY;
SELECT COUNT(*) AS ROWS_DIMGEOGRAPHY_CLEAN FROM DIMGEOGRAPHY_CLEAN; -- Verified, both Counts at 655

-- Verify transformations
SELECT *
FROM DIMGEOGRAPHY_CLEAN
ORDER BY GeographyKey
LIMIT 25;

---------------------


----- [DIMORGANIZATION] -----
-- Checking of NULL values for PK
SELECT COUNT(*) AS OrganizationKey_Null_Count
FROM DIMORGANIZATION
WHERE "OrganizationKey" IS NULL;

-- Checking of duplicate PK
SELECT "OrganizationKey", COUNT(*) AS Count
FROM DIMORGANIZATION
GROUP BY "OrganizationKey"
HAVING COUNT(*) > 1;

-- Duplicate Business Key checks (no AlternateKey provided)
-- Check duplicates on OrganizationName as a practical business identifier
SELECT TRIM("OrganizationName") AS OrganizationName, COUNT(*) AS Count
FROM DIMORGANIZATION
GROUP BY 1
HAVING COUNT(*) > 1;

-- Cleanse DIMORGANIZATION
CREATE OR REPLACE TABLE DIMORGANIZATION_CLEAN AS
WITH base AS (
  SELECT
    "OrganizationKey" AS OrganizationKey,
    "ParentOrganizationKey" AS ParentOrganizationKey,  -- allow NULL 
    "PercentageOfOwnership" AS PercentageOfOwnership,
    TRIM("OrganizationName") AS OrganizationName,
    "CurrencyKey" AS CurrencyKey
  FROM DIMORGANIZATION
  WHERE "OrganizationKey" IS NOT NULL
),
fix_nulls AS (
  SELECT
    OrganizationKey,
    ParentOrganizationKey,
    PercentageOfOwnership,

    -- OrganisationName is safe to fill
    COALESCE(NULLIF(OrganizationName, ''), 'Unknown') AS OrganizationName,

    CurrencyKey
  FROM base
),
dedup AS (
  SELECT *
  FROM fix_nulls
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY OrganizationKey
    ORDER BY
      (IFF(OrganizationName IS NOT NULL AND OrganizationName <> '', 1, 0) +
       IFF(CurrencyKey IS NOT NULL, 1, 0) +
       IFF(PercentageOfOwnership IS NOT NULL, 1, 0)
      ) DESC
  ) = 1
)
SELECT * FROM dedup;

-- Validation outputs
SELECT COUNT(*) AS ROWS_DIMORGANIZATION FROM DIMORGANIZATION;
SELECT COUNT(*) AS ROWS_DIMORGANIZATION_CLEAN FROM DIMORGANIZATION_CLEAN; -- Verified, both counts at 14

-- Verify transformations
SELECT *
FROM DIMORGANIZATION_CLEAN
ORDER BY OrganizationKey
LIMIT 25;

---------------------



----- [DIMPRODUCT] -----
-- Checking of NULL values for PK
SELECT COUNT(*) AS ProductKey_Null_Count
FROM DIMPRODUCT
WHERE "ProductKey" IS NULL;

-- Checking of duplicate PK
SELECT "ProductKey", COUNT(*) AS Count
FROM DIMPRODUCT
GROUP BY "ProductKey"
HAVING COUNT(*) > 1;

-- Duplicate Business Key checks.
SELECT "ProductAlternateKey", COUNT(*) AS Count
FROM DIMPRODUCT
GROUP BY "ProductAlternateKey"
HAVING COUNT(*) > 1;

-- Cleanse DIMPRODUCT
CREATE OR REPLACE TABLE DIMPRODUCT_CLEAN AS
WITH base AS (
  SELECT
    "ProductKey" AS ProductKey,
    TRIM("ProductAlternateKey") AS ProductAlternateKey,
    "ProductSubcategoryKey" AS ProductSubcategoryKey,
    TRIM("WeightUnitMeasureCode") AS WeightUnitMeasureCode,
    TRIM("SizeUnitMeasureCode") AS SizeUnitMeasureCode,
    TRIM("EnglishProductName") AS EnglishProductName,
    TRIM("SpanishProductName") AS SpanishProductName,
    TRIM("FrenchProductName") AS FrenchProductName,
    "StandardCost" AS StandardCost,
    "FinishedGoodsFlag" AS FinishedGoodsFlag,
    TRIM("Color") AS Color,
    "SafetyStockLevel" AS SafetyStockLevel,
    "ReorderPoint" AS ReorderPoint,
    "ListPrice" AS ListPrice,
    TRIM("Size") AS Size,
    TRIM("SizeRange") AS SizeRange,
    "Weight" AS Weight,
    "DaysToManufacture" AS DaysToManufacture,
    TRIM("ProductLine") AS ProductLine,
    "DealerPrice" AS DealerPrice,
    TRIM("Class") AS Class,
    TRIM("Style") AS Style,
    TRIM("ModelName") AS ModelName,
    "LargePhoto" AS LargePhoto,
    TRIM("EnglishDescription") AS EnglishDescription,
    TRIM("FrenchDescription") AS FrenchDescription,
    TRIM("ChineseDescription") AS ChineseDescription,
    TRIM("ArabicDescription") AS ArabicDescription,
    TRIM("HebrewDescription") AS HebrewDescription,
    TRIM("ThaiDescription") AS ThaiDescription,
    TRIM("GermanDescription") AS GermanDescription,
    TRIM("JapaneseDescription") AS JapaneseDescription,
    TRIM("TurkishDescription") AS TurkishDescription,
    "StartDate" AS StartDate,

    -- EndDate: NULL means active; set '9999-12-31' for easier downstream filtering (same as DIMEMPLOYEE)
    COALESCE("EndDate", DATE '9999-12-31') AS EndDate,

    -- Status: NULL -> 'Unknown'
    COALESCE(NULLIF(TRIM("Status"), ''), 'Unknown') AS Status
  FROM DIMPRODUCT
  WHERE "ProductKey" IS NOT NULL
),
fix_nulls AS (
  SELECT
    ProductKey,
    ProductAlternateKey,
    ProductSubcategoryKey,
    WeightUnitMeasureCode,
    SizeUnitMeasureCode,

    -- Safe descriptive fill for names & descriptors
    COALESCE(NULLIF(EnglishProductName, ''), 'Unknown Product') AS EnglishProductName,
    SpanishProductName,
    FrenchProductName,
    StandardCost,
    FinishedGoodsFlag,
    
    COALESCE(NULLIF(Color, ''), 'Unknown') AS Color,
    SafetyStockLevel,
    ReorderPoint,
    ListPrice,
    
    COALESCE(NULLIF(Size, ''), 'Unknown') AS Size,
    SizeRange,
    Weight,
    DaysToManufacture,
    ProductLine,
    DealerPrice,
    Class,
    Style,
    ModelName,
    LargePhoto,
    -- Leave translation/descriptions NULL if not provided (no fake translations)
    EnglishDescription,
    FrenchDescription,
    ChineseDescription,
    ArabicDescription,
    HebrewDescription,
    ThaiDescription,
    GermanDescription,
    JapaneseDescription,
    TurkishDescription,
    
    StartDate,
    EndDate,
    Status
  FROM base
),
dedup AS (
  SELECT *
  FROM fix_nulls
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ProductKey
    ORDER BY
      (IFF(ProductAlternateKey IS NOT NULL AND ProductAlternateKey <> '', 1, 0) +
       IFF(EnglishProductName IS NOT NULL AND EnglishProductName <> '', 1, 0) +
       IFF(ListPrice IS NOT NULL, 1, 0) +
       IFF(StandardCost IS NOT NULL, 1, 0) +
       IFF(Status IS NOT NULL AND Status <> '', 1, 0)
      ) DESC
  ) = 1
)
SELECT * FROM dedup;

-- Validation outputs
SELECT COUNT(*) AS ROWS_DIMPRODUCT FROM DIMPRODUCT;
SELECT COUNT(*) AS ROWS_DIMPRODUCT_CLEAN FROM DIMPRODUCT_CLEAN; -- Verified, both Counts at 606

-- Verify transformations
SELECT
  ProductKey,
  ProductAlternateKey,
  EnglishProductName,
  Color,
  Size,
  Status,
  EndDate
FROM DIMPRODUCT_CLEAN
ORDER BY ProductKey
LIMIT 25;

--------------------



----- [DIMPRODUCTCATEGORY] -----
-- Checking of NULL values for PK
SELECT COUNT(*) AS ProductCategoryKey_Null_Count
FROM DIMPRODUCTCATEGORY
WHERE "ProductCategoryKey" IS NULL;

-- Checking of duplicate PK
SELECT "ProductCategoryKey", COUNT(*) AS Count
FROM DIMPRODUCTCATEGORY
GROUP BY "ProductCategoryKey"
HAVING COUNT(*) > 1;

-- Duplicate Business Key checks.
SELECT "ProductCategoryAlternateKey", COUNT(*) AS Count
FROM DIMPRODUCTCATEGORY
GROUP BY "ProductCategoryAlternateKey"
HAVING COUNT(*) > 1;

-- Cleanse DIMPRODUCTCATEGORY
CREATE OR REPLACE TABLE DIMPRODUCTCATEGORY_CLEAN AS
WITH base AS (
  SELECT
    "ProductCategoryKey" AS ProductCategoryKey,
    TRIM("ProductCategoryAlternateKey") AS ProductCategoryAlternateKey,
    TRIM("EnglishProductCategoryName") AS EnglishProductCategoryName,
    TRIM("SpanishProductCategoryName") AS SpanishProductCategoryName,
    TRIM("FrenchProductCategoryName") AS FrenchProductCategoryName
  FROM DIMPRODUCTCATEGORY
  WHERE "ProductCategoryKey" IS NOT NULL
),
fix_nulls AS (
  SELECT
    ProductCategoryKey,
    ProductCategoryAlternateKey,

    -- Safe to fill category name for reporting
    COALESCE(NULLIF(EnglishProductCategoryName, ''), 'Unknown Category') AS EnglishProductCategoryName,

    -- Leave translations NULL if not provided
    SpanishProductCategoryName,
    FrenchProductCategoryName
  FROM base
),
dedup AS (
  SELECT *
  FROM fix_nulls
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ProductCategoryKey
    ORDER BY
      (IFF(ProductCategoryAlternateKey IS NOT NULL AND ProductCategoryAlternateKey <> '', 1, 0) +
       IFF(EnglishProductCategoryName IS NOT NULL AND EnglishProductCategoryName <> '', 1, 0)
      ) DESC
  ) = 1
)
SELECT * FROM dedup;

-- Validation outputs
SELECT COUNT(*) AS ROWS_DIMPRODUCTCATEGORY FROM DIMPRODUCTCATEGORY;
SELECT COUNT(*) AS ROWS_DIMPRODUCTCATEGORY_CLEAN FROM DIMPRODUCTCATEGORY_CLEAN; -- Verified, both Counts at 4

-- Verify transformations
SELECT *
FROM DIMPRODUCTCATEGORY_CLEAN
ORDER BY ProductCategoryKey
LIMIT 25;

--------------------










