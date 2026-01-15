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
    "CustomerKey" AS CustomerKey,
    TRIM("CustomerAlternateKey") AS CustomerAlternateKey,

    -- Names (common columns)
    INITCAP(TRIM("FirstName"))   AS FirstName,
    INITCAP(TRIM("LastName"))    AS LastName,

    -- Email normalization (if exists)
    LOWER(TRIM("EmailAddress"))  AS EmailAddress,

    -- Gender normalization (examples: 'M','F','Male','Female')
    CASE
      WHEN UPPER(TRIM("Gender")) IN ('M','MALE') THEN 'M'
      WHEN UPPER(TRIM("Gender")) IN ('F','FEMALE') THEN 'F'
      ELSE NULL
    END AS Gender,

    -- Marital status normalization (examples: 'M','S','Married','Single')
    CASE
      WHEN UPPER(TRIM("MaritalStatus")) IN ('M','MARRIED') THEN 'Married'
      WHEN UPPER(TRIM("MaritalStatus")) IN ('S','SINGLE') THEN 'Single'
      ELSE NULL
    END AS MaritalStatus,

    -- Geography (common columns)
    INITCAP(TRIM("City"))              AS City,
    INITCAP(TRIM("StateProvinceName")) AS StateProvinceName,
    INITCAP(TRIM("CountryRegionName")) AS CountryRegionName,

    -- Keep everything else (add columns you have)
    DateFirstPurchase,
    BirthDate,
    YearlyIncome,
    TotalChildren,
    NumberChildrenAtHome,
    EnglishEducation,
    EnglishOccupation
  FROM DIMCUSTOMER
  WHERE CustomerKey IS NOT NULL
),
dedup_pk AS (
  SELECT *
  FROM base
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CustomerKey
    ORDER BY
      (IFF(EmailAddress IS NOT NULL AND EmailAddress <> '', 1, 0) +
       IFF(FirstName IS NOT NULL AND FirstName <> '', 1, 0) +
       IFF(LastName IS NOT NULL AND LastName <> '', 1, 0)
      ) DESC
  ) = 1
),
dedup_people AS (
  -- Optional: also dedupe by email if present (keeps best row per email)
  SELECT *
  FROM dedup_pk
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY EmailAddress
    ORDER BY
      (IFF(DateFirstPurchase IS NOT NULL, 1, 0) +
       IFF(BirthDate IS NOT NULL, 1, 0) +
       IFF(YearlyIncome IS NOT NULL, 1, 0)
      ) DESC
  ) = 1
)
SELECT * FROM dedup_people;



