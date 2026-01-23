USE ROLE TRAINING_ROLE;
USE WAREHOUSE FERRET_WH;
USE DATABASE GRP1_ASG;
USE SCHEMA ASG;

LIST @GRP1_ASG.ASG.asg_stage;

-- DIMSCENARIO   okay
-- FACTADDITIONALINTERNATIONALPRODUCTDESCRIPTION remove, data provides description of products in other languages
-- FACTCALLCENTER       Okay
-- FACTCURRENCYRATE     Okay    
-- FACTFINANCE          Okay

SELECT * FROM DIMSCENARIO ;
SELECT * FROM FACTADDITIONALINTERNATIONALPRODUCTDESCRIPTION LIMIT 10;
SELECT * FROM FACTCALLCENTER LIMIT 10;
SELECT * FROM FACTCURRENCYRATE LIMIT 10;
SELECT * FROM FACTFINANCE LIMIT 10;

-- Create file format
---------------------------
ls @asg_stage;


------------------------------------------------------------
-- Clean DimScenario Table
CREATE OR REPLACE TABLE DIMSCENARIO_CLEAN AS
SELECT 
    "ScenarioKey" as ScenarioKey,
    TRIM("ScenarioName") as ScenarioName
FROM 
DIMSCENARIO;


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
FACTFINANCE;

-- CLEAN FACTADDITIONALINTERNATIONALPRODUCTDESCRIPTION
CREATE OR REPLACE TABLE FACTADDITIONALINTERNATIONALPRODUCTDESCRIPTION_CLEAN AS
SELECT 
  "ProductKey" as ProductKey,
  "CultureName" as CultureName,
  "ProductDescription" as ProductDescription
FROM
FACTADDITIONALINTERNATIONALPRODUCTDESCRIPTION;

-- CLEAN FACTCURRENCYRATE
CREATE OR REPLACE TABLE FACTCURRENCYRATE_CLEAN AS
SELECT
 f."CurrencyKey" as CurrencyKey,
 TRY_TO_DATE(d."FullDateAlternateKey")  as Date,
 CAST(f."AverageRate" AS DECIMAL (15,5)) as AverageRate,
 CAST(f."EndOfDayRate" AS DECIMAL (15,5)) as EndOfDayRate
 FROM FACTCURRENCYRATE f
 JOIN DIMDATE d on 
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
FACTCALLCENTER f
JOIN DIMDATE d on
d."DateKey" = f."DateKey";