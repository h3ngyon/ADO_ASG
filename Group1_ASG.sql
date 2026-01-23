------------------------------------------------------------
USE WAREHOUSE FERRET_WH;
USE DATABASE GRP1_ASG;
USE SCHEMA ASG_ClEAN;
ALTER WAREHOUSE FERRET_WH
SET AUTO_SUSPEND = 600;


--- Cleansing of Tables (Handling NULL values, etc.) --- 


CREATE OR REPLACE TABLE DIMACCOUNT_CLEAN AS
WITH base AS (
  SELECT
    "AccountKey" AS AccountKey,
    "ParentAccountKey"AS ParentAccountkey,
    TRIM("AccountCodeAlternateKey")      AS AccountCodeAlternateKey,
    TRIM("AccountDescription")           AS AccountDescription,
    TRIM("AccountType")                  AS AccountType,
    TRIM("Operator")                     AS Operator,
    TRIM("CustomMembers")                AS CustomMembers,
    TRIM("CustomMemberOptions")          AS CustomMemberOptions,
    "ValueType" AS ValueType,
  FROM ASG.DIMACCOUNT
  WHERE AccountKey IS NOT NULL
),
dedup AS (
  SELECT *
  FROM base
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY AccountKey
    ORDER BY
      (IFF(AccountCodeAlternateKey IS NOT NULL AND AccountCodeAlternateKey <> '', 1, 0) +
       IFF(AccountDescription IS NOT NULL AND AccountDescription <> '', 1, 0) +
       IFF(AccountType IS NOT NULL AND AccountType <> '', 1, 0)
      ) DESC
  ) = 1
)
SELECT * FROM dedup;

CREATE OR REPLACE TABLE DIMCURRENCY_CLEAN AS
WITH base AS (
  SELECT
    "CurrencyKey" AS CurrencyKey,
    UPPER(TRIM("CurrencyAlternateKey")) AS CurrencyAlternateKey,
    INITCAP(TRIM("CurrencyName"))       AS CurrencyName
  FROM ASG.DIMCURRENCY
  WHERE "CurrencyKey" IS NOT NULL
),
dedup AS (
  SELECT *
  FROM base
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CurrencyKey
    ORDER BY IFF(CurrencyName IS NOT NULL AND CurrencyName <> '', 1, 0) DESC
  ) = 1
)
SELECT * FROM dedup;


CREATE OR REPLACE TABLE DIMCUSTOMER_CLEAN AS
WITH base AS (
  SELECT
    "CustomerKey"                          AS CustomerKey,
    "GeographyKey"                         AS GeographyKey,
    NULLIF(TRIM("CustomerAlternateKey"), '') AS CustomerAlternateKey,

    NULLIF(INITCAP(TRIM("Title")), '')     AS Title,
    NULLIF(INITCAP(TRIM("FirstName")), '') AS FirstName,
    NULLIF(INITCAP(TRIM("MiddleName")), '') AS MiddleName,
    NULLIF(INITCAP(TRIM("LastName")), '')  AS LastName,
    TRY_TO_BOOLEAN("NameStyle")            AS NameStyle,
    NULLIF(INITCAP(TRIM("Suffix")), '')    AS Suffix,

    TRY_TO_DATE("BirthDate")               AS BirthDate,

    CASE
      WHEN UPPER(TRIM("Gender")) IN ('M','MALE') THEN 'M'
      WHEN UPPER(TRIM("Gender")) IN ('F','FEMALE') THEN 'F'
      ELSE NULL
    END                                    AS Gender,

    CASE
      WHEN UPPER(TRIM("MaritalStatus")) IN ('M','MARRIED') THEN 'Married'
      WHEN UPPER(TRIM("MaritalStatus")) IN ('S','SINGLE') THEN 'Single'
      ELSE NULL
    END                                    AS MaritalStatus,

    NULLIF(LOWER(TRIM("EmailAddress")), '') AS EmailAddress,
    NULLIF(TRIM("Phone"), '')              AS Phone,

    "YearlyIncome"                         AS YearlyIncome,
    "TotalChildren"                        AS TotalChildren,
    "NumberChildrenAtHome"                 AS NumberChildrenAtHome,
    NULLIF(INITCAP(TRIM("EnglishEducation")), '')  AS EnglishEducation,
    NULLIF(INITCAP(TRIM("SpanishEducation")), '')  AS SpanishEducation,
    NULLIF(INITCAP(TRIM("FrenchEducation")), '')   AS FrenchEducation,
    NULLIF(INITCAP(TRIM("EnglishOccupation")), '') AS EnglishOccupation,
    NULLIF(INITCAP(TRIM("SpanishOccupation")), '') AS SpanishOccupation,
    NULLIF(INITCAP(TRIM("FrenchOccupation")), '')  AS FrenchOccupation,
    "HouseOwnerFlag"                       AS HouseOwnerFlag,
    "NumberCarsOwned"                      AS NumberCarsOwned,

    NULLIF(TRIM("AddressLine1"), '')       AS AddressLine1,
    NULLIF(TRIM("AddressLine2"), '')       AS AddressLine2,

    TRY_TO_DATE("DateFirstPurchase")       AS DateFirstPurchase,
    NULLIF(TRIM("CommuteDistance"), '')    AS CommuteDistance
  FROM ASG.DIMCUSTOMER
  WHERE "CustomerKey" IS NOT NULL
),
dedup_pk AS (
  SELECT *
  FROM base
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CustomerKey
    ORDER BY
      ( IFF(CustomerAlternateKey IS NOT NULL, 1, 0)
      + IFF(FirstName IS NOT NULL, 1, 0)
      + IFF(LastName IS NOT NULL, 1, 0)
      + IFF(DateFirstPurchase IS NOT NULL, 1, 0)
      ) DESC
  ) = 1
)
SELECT *
FROM dedup_pk;


CREATE OR REPLACE TABLE DIMDATE_CLEAN AS
WITH base AS (
  SELECT
    "DateKey"                           AS DateKey,
    TRY_TO_DATE("FullDateAlternateKey") AS FullDateAlternateKey,
    "DayNumberOfWeek"                   AS DayNumberOfWeek,
    NULLIF(INITCAP(TRIM("EnglishDayNameOfWeek")), '') AS EnglishDayNameOfWeek,
    NULLIF(INITCAP(TRIM("SpanishDayNameOfWeek")), '') AS SpanishDayNameOfWeek,
    NULLIF(INITCAP(TRIM("FrenchDayNameOfWeek")), '')  AS FrenchDayNameOfWeek,
    "DayNumberOfMonth"                  AS DayNumberOfMonth,
    "DayNumberOfYear"                   AS DayNumberOfYear,
    "WeekNumberOfYear"                  AS WeekNumberOfYear,
    NULLIF(INITCAP(TRIM("EnglishMonthName")), '') AS EnglishMonthName,
    NULLIF(INITCAP(TRIM("SpanishMonthName")), '') AS SpanishMonthName,
    NULLIF(INITCAP(TRIM("FrenchMonthName")), '')  AS FrenchMonthName,
    "MonthNumberOfYear"                 AS MonthNumberOfYear,
    "CalendarQuarter"                   AS CalendarQuarter,
    "CalendarYear"                      AS CalendarYear,
    "CalendarSemester"                  AS CalendarSemester,
    "FiscalQuarter"                     AS FiscalQuarter,
    "FiscalYear"                        AS FiscalYear,
    "FiscalSemester"                    AS FiscalSemester
  FROM ASG.DIMDATE
  WHERE "DateKey" IS NOT NULL
),
dedup_pk AS (
  SELECT *
  FROM base
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY DateKey
    ORDER BY
      ( IFF(FullDateAlternateKey IS NOT NULL, 1, 0)
      + IFF(EnglishDayNameOfWeek IS NOT NULL, 1, 0)
      + IFF(EnglishMonthName IS NOT NULL, 1, 0)
      ) DESC
  ) = 1
)
SELECT *
FROM dedup_pk;


CREATE OR REPLACE TABLE DIMDEPARTMENTGROUP_CLEAN AS
WITH base AS (
  SELECT
    "DepartmentGroupKey"       AS DepartmentGroupKey,
    "ParentDepartmentGroupKey" AS ParentDepartmentGroupKey,
    NULLIF(INITCAP(TRIM("DepartmentGroupName")), '') AS DepartmentGroupName
  FROM ASG.DIMDEPARTMENTGROUP
  WHERE "DepartmentGroupKey" IS NOT NULL
),
dedup_pk AS (
  SELECT *
  FROM base
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY DepartmentGroupKey
    ORDER BY IFF(DepartmentGroupName IS NOT NULL, 1, 0) DESC
  ) = 1
)
SELECT *
FROM dedup_pk;





















----- [DIMEMPLOYEE] -----

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
  FROM ASG.DIMEMPLOYEE
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

--------------------



----- [DIMGEOGRAPHY] -----

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
  FROM ASG.DIMGEOGRAPHY
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


---------------------


----- [DIMORGANIZATION] -----

-- Cleanse DIMORGANIZATION
CREATE OR REPLACE TABLE DIMORGANIZATION_CLEAN AS
WITH base AS (
  SELECT
    "OrganizationKey" AS OrganizationKey,
    "ParentOrganizationKey" AS ParentOrganizationKey,  -- allow NULL 
    "PercentageOfOwnership" AS PercentageOfOwnership,
    TRIM("OrganizationName") AS OrganizationName,
    "CurrencyKey" AS CurrencyKey
  FROM ASG.DIMORGANIZATION
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

---------------------



----- [DIMPRODUCT] -----

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
  FROM ASG.DIMPRODUCT
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

--------------------


----- [DIMPRODUCTCATEGORY] -----

-- Cleanse DIMPRODUCTCATEGORY
CREATE OR REPLACE TABLE DIMPRODUCTCATEGORY_CLEAN AS
WITH base AS (
  SELECT
    "ProductCategoryKey" AS ProductCategoryKey,
    TRIM("ProductCategoryAlternateKey") AS ProductCategoryAlternateKey,
    TRIM("EnglishProductCategoryName") AS EnglishProductCategoryName,
    TRIM("SpanishProductCategoryName") AS SpanishProductCategoryName,
    TRIM("FrenchProductCategoryName") AS FrenchProductCategoryName
  FROM asg.DIMPRODUCTCATEGORY
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

----- [DimProductSubcategory] -----
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

----- [DimPromotion] -----
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

----- [DimSalesReason] -----
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

----- [DimSalesTerritory] -----
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


------------------------------------------------------------
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
FROM ASG.ProspectiveBuyer
QUALIFY ROW_NUMBER() OVER (PARTITION BY "ProspectiveBuyerKey" ORDER BY "BirthDate" DESC) = 1;

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
FROM ASG.FactSurveyResponse;

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
FROM ASG.FactSalesQuota;

--  Clean Fact Reseller Sales

CREATE OR REPLACE TABLE FACTSALESQUOTA_CLEAN AS
SELECT
    "SalesQuotaKey" AS SalesQuotaKey,
    "EmployeeKey" AS EmployeeKey,
    TRY_TO_DATE(TO_VARCHAR("DateKey"), 'YYYYMMDD') AS QuotaDate,
    "CalendarYear" AS CalendarYear,
    "CalendarQuarter" AS CalendarQuarter,
    CAST("SalesAmountQuota" AS DECIMAL(18,2)) AS SalesAmountQuota
FROM ASG.FactSalesQuota;


-- Clean Fact Internet Sales Reason
CREATE OR REPLACE TABLE FACTINTERNETSALESREASON_CLEAN AS
SELECT DISTINCT
    TRIM("SalesOrderNumber") AS SalesOrderNumber,
    "SalesOrderLineNumber" AS SalesOrderLineNumber,
    "SalesReasonKey" AS SalesReasonKey
FROM ASG.FactInternetSalesReason;

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
FROM ASG.FactInternetSales;

















------------------------------------------------------------
-- Clean DimScenario Table
CREATE OR REPLACE TABLE DIMSCENARIO_CLEAN AS
SELECT 
    "ScenarioKey" as ScenarioKey,
    TRIM("ScenarioName") as ScenarioName
FROM 
ASG.DIMSCENARIO;


-- Clean FactFinance Table
CREATE OR REPLACE TABLE FACTFINANCE_CLEAN AS
SELECT
  "FinanceKey" as FinanceKey,
  "DateKey"    as DateKey,
  "OrganizationKey" as OrgranizationKey,
  "DepartmentGroupKey" as DepartmentGroupKey,
  "ScenarioKey" as ScenarioKey,
  "AccountKey" as AccountKey,
  "Amount" as Amount
FROM 
ASG.FACTFINANCE;

-- CLEAN FACTADDITIONALINTERNATIONALPRODUCTDESCRIPTION
CREATE OR REPLACE TABLE FACTADDITIONALINTERNATIONALPRODUCTDESCRIPTION_CLEAN AS
SELECT 
  "ProductKey" as ProductKey,
  "CultureName" as CultureName,
  "ProductDescription" as ProductDescription
FROM
ASG.FACTADDITIONALINTERNATIONALPRODUCTDESCRIPTION;

-- CLEAN FACTCURRENCYRATE
CREATE OR REPLACE TABLE FACTCURRENCYRATE_CLEAN AS
SELECT
 f."CurrencyKey" as CurrencyKey,
 TRY_TO_DATE(d."FullDateAlternateKey")  as Date,
 CAST(f."AverageRate" AS DECIMAL (15,5)) as AverageRate,
 CAST(f."EndOfDayRate" AS DECIMAL (15,5)) as EndOfDayRate
 FROM 
 ASG.FACTCURRENCYRATE f
 JOIN ASG.DIMDATE d on 
 d."DateKey" = f."DateKey";

-- CLEAN FACTCALLCENTER
CREATE OR REPLACE TABLE FACTCALLCENTER_CLEAN AS
SELECT
  "FactCallCenterID" as FactCallCenterID,
  TRY_TO_DATE(d."FullDateAlternateKey") as Date,
  TRIM("WageType") as WageType,
  TRIM("Shift") as Shift,
  "LevelOneOperators" as LevelOneOperators,
  "LevelTwoOperators" as LevelTwoOperators,
  "TotalOperators" as TotalOperators,
  "Calls" as Calls,
  "AutomaticResponses" as AutomaticResponses,
  "Orders" as Orders,
  "IssuesRaised" as IssuesRaised,
  "AverageTimePerIssue" as AverageTimePerIssue,
  "ServiceGrade" as ServiceGrade
FROM 
ASG.FACTCALLCENTER f
JOIN ASG.DIMDATE d on
d."DateKey" = f."DateKey";
