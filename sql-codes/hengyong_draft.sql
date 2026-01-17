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


-- DIMSCENARIO_CLEANED
--------------------------------
SELECT * from DIMSCENARIO limit 10;

CREATE OR REPLACE TABLE DIMSCENARIO_CLEAN AS
SELECT 
    "ScenarioKey" as ScenarioKey,
    TRIM("ScenarioName") as ScenarioName
FROM 
DIMSCENARIO;
SELECT * FROM DIMSCENARIO_CLEAN LIMIT 10;



-- FACTFINANCE_CLEANED
---------------------------------------------
SELECT * FROM FACTFINANCE LIMIT 10;
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
 
-- check data in cleaned factfinance table
SELECT * FROM FACTFINANCE_CLEAN LIMIT 10;