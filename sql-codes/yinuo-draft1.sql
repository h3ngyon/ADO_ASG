--Initialise Connection Settings
USE WAREHOUSE GECKO_WH;
USE DATABASE GRP1_ASG;
USE SCHEMA ASG;
ALTER WAREHOUSE GECKO_WH
SET AUTO_SUSPEND = 600;

-- 1. Inspection of tables
DESC TABLE DIMACCOUNT;
SELECT * FROM DIMACCOUNT;
DESC TABLE DIMCURRENCY;
SELECT * FROM DIMCURRENCY;
DESC TABLE DIMCUSTOMER;

SELECT * FROM DIMCUSTOMER;
DESC TABLE DIMDATE;
SELECT * FROM DIMDATE;

DESC TABLE DIMDEPARTMENTGROUP;
SELECT * FROM DIMDEPARTMENTGROUP;


-- [Table: DIMACCOUNT] 1.1: Checking of NULL values for PK.
SELECT COUNT(*) AS NULL_AccountKey_Count
FROM DIMACCOUNT
WHERE "ParentAccountKey" IS NULL; --3 NULL values for PK

-- [Table: DIMACCOUNT] 1.2: Checking of duplicate PKs.
SELECT "ParentAccountKey", COUNT(*) AS cnt
FROM DIMACCOUNT
GROUP BY "ParentAccountKey"
HAVING COUNT(*) > 1; --Multiple duplicates detected

-- [Table: DIMACCOUNT] 1.3: Duplicate Business Key checks.
SELECT "AccountCodeAlternateKey", COUNT(*) AS cnt
FROM DIMACCOUNT
GROUP BY "AccountCodeAlternateKey"  
HAVING COUNT(*) > 1; -- None detected

--Notes: AlternateKey is a surrogate key, used for joins.
--Code below shows what each surrogate key represents.
SELECT
  "AccountCodeAlternateKey",
  "AccountDescription"
FROM DIMACCOUNT
ORDER BY "AccountCodeAlternateKey";

--[Table: DIMACCOUNTS] 1.4 Cleans DIMACCOUNTS by removing null AccountKey, deduplicating AccountKey and choosing
-- to keep the row with the most information. It also standardises the text(trims it.)
--Creates new table "DIMACCOUNT_CLEAN" to store the cleaned data.

CREATE OR REPLACE TABLE "DIMACCOUNT_CLEAN" AS
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
  FROM DIMACCOUNT
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

-- Optional: Show changes for the new table.
SELECT * FROM DIMACCOUNT_CLEAN;

-- DIMACCOUNT is marked as cleaned after the aforementioned steps.


--[Table: DIMCURRENCY] 2.1 Checking of NULL PKs.
SELECT COUNT(*) AS NULL_CURRENCYKEY_COUNT
FROM DIMCURRENCY
WHERE "CurrencyKey" IS NULL; --No NULL PKs.

-- [Table: DIMCURRENCY] 2.2 Checking of Duplicate PKs.
SELECT "CurrencyKey", COUNT(*) AS DUPLICATE_PK_COUNT
FROM DIMCURRENCY
GROUP BY "CurrencyKey"
HAVING COUNT(*) > 1; --No duplicates

-- [Table: DIMCURRENCY] 2.3 Duplicate Currency Code checks.
SELECT "CurrencyAlternateKey", COUNT(*) AS cnt
FROM DIMCURRENCY
GROUP BY "CurrencyAlternateKey"
HAVING COUNT(*) > 1; --No duplicates

--[Table: DIMCURRENCY] 2.4 Cleans table by removing NULL primary keys, standardising text values and removes duplicate rows safely.
CREATE OR REPLACE TABLE DIMCURRENCY_CLEAN AS
WITH base AS (
  SELECT
    "CurrencyKey" AS CurrencyKey,
    UPPER(TRIM("CurrencyAlternateKey")) AS CurrencyAlternateKey,
    INITCAP(TRIM("CurrencyName"))       AS CurrencyName
  FROM DIMCURRENCY
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

SELECT * FROM DIMCURRENCY;

-- Optional: Show changes for the new table.
SELECT * FROM DIMCURRENCY_CLEAN;

-- DIMACCOUNT is marked as cleaned after the aforementioned steps.

-- [Table: DIMCUSTOMER] 2.1 NULL PK checks
SELECT COUNT(*) AS NULL_CUSTOMERKEY_COUNT
FROM DIMCUSTOMER
WHERE "CustomerKey" IS NULL; -- No nulls

-- [Table: DIMCUSTOMER] 2.2 Duplicate PK check
SELECT "CustomerKey", COUNT(*) AS DUPLICATE_CUSTOMERKEY_COUNT
FROM DIMCUSTOMER
GROUP BY "CustomerKey"
HAVING COUNT(*) > 1; --No duplicates

-- [Table: DIMCUSTOMER] 2.3 Duplicate Business Key check
SELECT "CustomerAlternateKey", COUNT(*) AS cnt
FROM DIMCUSTOMER
GROUP BY "CustomerAlternateKey"
HAVING COUNT(*) > 1; --No duplicates present

-- Misc: Checks if any email addresses are duplicated
SELECT
  LOWER(TRIM("EmailAddress")) AS email_norm,
  COUNT(*) AS cnt
FROM DIMCUSTOMER
WHERE "EmailAddress" IS NOT NULL AND TRIM("EmailAddress") <> ''
GROUP BY LOWER(TRIM("EmailAddress"))
HAVING COUNT(*) > 1
ORDER BY cnt DESC;

SELECT * FROM DIMCUSTOMER
LIMIT 5;


--Code beyond here is still work in progress.
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
  FROM DIMCUSTOMER
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

SELECT * FROM DIMCUSTOMER
LIMIT 10;
SELECT * FROM DIMCUSTOMER_CLEAN
LIMIT 10;


SELECT * FROM "DIMDATE";

-- [Table: DIMDATE] 3.1 NULL PK checks
SELECT COUNT(*) AS NULL_DATEKEY_COUNT
FROM DIMDATE
WHERE "DateKey" IS NULL;

-- [Table: DIMDATE] 3.2 Duplicate PK checks
SELECT "DateKey", COUNT(*) AS DUPLICATE_DATEKEY_COUNT
FROM DIMDATE
GROUP BY "DateKey"
HAVING COUNT(*) > 1;

-- [Table: DIMDATE] 3.3 Duplicate FullDate checks
SELECT "FullDateAlternateKey", COUNT(*) AS cnt
FROM DIMDATE
GROUP BY "FullDateAlternateKey"
HAVING COUNT(*) > 1;

-- [Table: DIMDATE] 3.4 Clean table by removing NULL PKs, standardising text fields, and deduplicating.
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
  FROM DIMDATE
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

-- Optional: Show changes for the new table.
SELECT * FROM DIMDATE_CLEAN;

-- [Table: DIMDEPARTMENTGROUP] 4.1 NULL PK checks
SELECT COUNT(*) AS NULL_DEPARTMENTGROUPKEY_COUNT
FROM DIMDEPARTMENTGROUP
WHERE "DepartmentGroupKey" IS NULL;

-- [Table: DIMDEPARTMENTGROUP] 4.2 Duplicate PK checks
SELECT "DepartmentGroupKey", COUNT(*) AS DUPLICATE_DEPARTMENTGROUPKEY_COUNT
FROM DIMDEPARTMENTGROUP
GROUP BY "DepartmentGroupKey"
HAVING COUNT(*) > 1;

-- [Table: DIMDEPARTMENTGROUP] 4.3 Duplicate DepartmentGroupName checks
SELECT "DepartmentGroupName", COUNT(*) AS cnt
FROM DIMDEPARTMENTGROUP
GROUP BY "DepartmentGroupName"
HAVING COUNT(*) > 1;

-- [Table: DIMDEPARTMENTGROUP] 4.4 Clean table by removing NULL PKs, standardising text fields, and deduplicating.
CREATE OR REPLACE TABLE DIMDEPARTMENTGROUP_CLEAN AS
WITH base AS (
  SELECT
    "DepartmentGroupKey"       AS DepartmentGroupKey,
    "ParentDepartmentGroupKey" AS ParentDepartmentGroupKey,
    NULLIF(INITCAP(TRIM("DepartmentGroupName")), '') AS DepartmentGroupName
  FROM DIMDEPARTMENTGROUP
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

-- Optional: Show changes for the new table.
SELECT * FROM DIMDEPARTMENTGROUP_CLEAN;

