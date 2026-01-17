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
CREATE OR REPLACE FILE FORMAT csv_ff_header
  TYPE = CSV
  FIELD_DELIMITER = ','
  PARSE_HEADER = TRUE
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE
  NULL_IF = ('NULL', '');

CREATE OR REPLACE FILE FORMAT csv_ff
  TYPE = CSV
  FIELD_DELIMITER = ','
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  TRIM_SPACE = TRUE
  NULL_IF = ('NULL', '');




ls @asg_stage_cleaned;

-- DIMSCENARIO_CLEANED
--------------------------------
-- creates table in stage as .gz format
COPY INTO @asg_stage_cleaned/DimScenario_cleaned
FROM
(
    SELECT
    *
    FROM DIMSCENARIO
)
FILE_FORMAT = (TYPE = 'CSV')
HEADER = TRUE                  -- Include column names in the file
OVERWRITE = TRUE;

-- Create an empty table in the database for the cleaned data
CREATE OR REPLACE TABLE DimScenario_cleaned
USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(INFER_SCHEMA(
      LOCATION => '@asg_stage_cleaned/DimScenario_cleaned_0_0_0.csv.gz',
      FILE_FORMAT => 'csv_ff_header'
  ))
);

-- Copy cleaned data into the empty table created
COPY INTO DimScenario_cleaned
FROM @asg_stage_cleaned/DimScenario_cleaned_0_0_0.csv.gz
FILE_FORMAT = (FORMAT_NAME = 'csv_ff');

SELECT * FROM DIMSCENARIO_CLEANED LIMIT 10;



-- FACTFINANCE_CLEANED
---------------------------------------------
SELECT * FROM FACTFINANCE LIMIT 10;

COPY INTO @asg_stage_cleaned/FactFinance_cleaned
FROM
(
    SELECT
    *
    FROM FACTFINANCE
)
FILE_FORMAT = (TYPE = 'CSV')
HEADER = TRUE                  -- Include column names in the file
OVERWRITE = TRUE;


CREATE OR REPLACE TABLE FACTFINANCE_cleaned
USING TEMPLATE (
  SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
  FROM TABLE(INFER_SCHEMA(
      LOCATION => '@asg_stage_cleaned/FactFinance_cleaned_0_0_0.csv.gz',
      FILE_FORMAT => 'csv_ff_header'
  ))
);
COPY INTO FACTFINANCE_cleaned
FROM @asg_stage_cleaned/FactFinance_cleaned_0_0_0.csv.gz
FILE_FORMAT = (FORMAT_NAME = 'csv_ff');

-- check data in cleaned factfinance table
SELECT * FROM FACTFINANCE_CLEANED LIMIT 10;