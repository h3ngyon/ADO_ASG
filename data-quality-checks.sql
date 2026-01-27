-- Data Quality Checks for GRP1_ASG.ASG_CLEAN.*_CLEAN
-- Fails the script if any check fails.
USE ROLE CICD_ROLE;
USE WAREHOUSE FERRET_WH;
USE DATABASE GRP1_ASG;
USE SCHEMA ASG_TRANSFORMATIONS;

BEGIN
  -------------------------------------------------------------------
  -- helper: fail-fast via divide-by-zero
  -------------------------------------------------------------------
  LET ok NUMBER := 1;

  -------------------------------------------------------------------
  -- 1) DIMDATE_CLEAN: not empty, DateKey unique + not null
  -------------------------------------------------------------------
  IF (EXISTS (
      SELECT 1
      FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = 'ASG_CLEAN' AND TABLE_NAME = 'DIMDATE_CLEAN'
  )) THEN
    LET cnt_dimdate NUMBER := (SELECT COUNT(*) FROM DIMDATE_CLEAN);
    SELECT IFF(:cnt_dimdate > 0, 1, 1/0) AS check_dimdate_not_empty;

    LET dup_dimdate NUMBER := (
      SELECT COUNT(*)
      FROM (
        SELECT DATEKEY
        FROM DIMDATE_CLEAN
        GROUP BY DATEKEY
        HAVING COUNT(*) > 1
      )
    );
    SELECT IFF(:dup_dimdate = 0, 1, 1/0) AS check_dimdate_datekey_unique;

    LET null_dimdate NUMBER := (SELECT COUNT(*) FROM DIMDATE_CLEAN WHERE DATEKEY IS NULL);
    SELECT IFF(:null_dimdate = 0, 1, 1/0) AS check_dimdate_datekey_not_null;
  END IF;

  -------------------------------------------------------------------
  -- 2) DIMCUSTOMER_CLEAN: not empty, CustomerKey unique + not null
  -------------------------------------------------------------------
  IF (EXISTS (
      SELECT 1
      FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = 'ASG_CLEAN' AND TABLE_NAME = 'DIMCUSTOMER_CLEAN'
  )) THEN
    LET cnt_dimcust NUMBER := (SELECT COUNT(*) FROM DIMCUSTOMER_CLEAN);
    SELECT IFF(:cnt_dimcust > 0, 1, 1/0) AS check_dimcustomer_not_empty;

    LET dup_dimcust NUMBER := (
      SELECT COUNT(*)
      FROM (
        SELECT CUSTOMERKEY
        FROM DIMCUSTOMER_CLEAN
        GROUP BY CUSTOMERKEY
        HAVING COUNT(*) > 1
      )
    );
    SELECT IFF(:dup_dimcust = 0, 1, 1/0) AS check_dimcustomer_customerkey_unique;

    LET null_dimcust NUMBER := (SELECT COUNT(*) FROM DIMCUSTOMER_CLEAN WHERE CUSTOMERKEY IS NULL);
    SELECT IFF(:null_dimcust = 0, 1, 1/0) AS check_dimcustomer_customerkey_not_null;
  END IF;

  -------------------------------------------------------------------
  -- 3) DIMPRODUCT_CLEAN: not empty, ProductKey unique + not null
  -------------------------------------------------------------------
  IF (EXISTS (
      SELECT 1
      FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = 'ASG_CLEAN' AND TABLE_NAME = 'DIMPRODUCT_CLEAN'
  )) THEN
    LET cnt_dimprod NUMBER := (SELECT COUNT(*) FROM DIMPRODUCT_CLEAN);
    SELECT IFF(:cnt_dimprod > 0, 1, 1/0) AS check_dimproduct_not_empty;

    LET dup_dimprod NUMBER := (
      SELECT COUNT(*)
      FROM (
        SELECT PRODUCTKEY
        FROM DIMPRODUCT_CLEAN
        GROUP BY PRODUCTKEY
        HAVING COUNT(*) > 1
      )
    );
    SELECT IFF(:dup_dimprod = 0, 1, 1/0) AS check_dimproduct_productkey_unique;

    LET null_dimprod NUMBER := (SELECT COUNT(*) FROM DIMPRODUCT_CLEAN WHERE PRODUCTKEY IS NULL);
    SELECT IFF(:null_dimprod = 0, 1, 1/0) AS check_dimproduct_productkey_not_null;
  END IF;

  -------------------------------------------------------------------
  -- 4) FACTINTERNETSALES_CLEAN: not empty,
  --    PK uniqueness on (SalesOrderNumber, SalesOrderLineNumber),
  --    FK existence: CustomerKey, ProductKey, OrderDateKey in corresponding dims if present
  -------------------------------------------------------------------
  IF (EXISTS (
      SELECT 1
      FROM INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = 'ASG_CLEAN' AND TABLE_NAME = 'FACTINTERNETSALES_CLEAN'
  )) THEN
    LET cnt_fact NUMBER := (SELECT COUNT(*) FROM FACTINTERNETSALES_CLEAN);
    SELECT IFF(:cnt_fact > 0, 1, 1/0) AS check_factinternetsales_not_empty;

    -- PK uniqueness check: (SalesOrderNumber, SalesOrderLineNumber) is the standard unique key
    LET dup_fact_pk NUMBER := (
      SELECT COUNT(*)
      FROM (
        SELECT SALESORDERNUMBER, SALESORDERLINENUMBER
        FROM FACTINTERNETSALES_CLEAN
        GROUP BY SALESORDERNUMBER, SALESORDERLINENUMBER
        HAVING COUNT(*) > 1
      )
    );
    SELECT IFF(:dup_fact_pk = 0, 1, 1/0) AS check_factinternetsales_pk_unique;

    -- FK: CustomerKey exists in DimCustomer (only if DimCustomer exists)
    IF (EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA='ASG_CLEAN' AND TABLE_NAME='DIMCUSTOMER_CLEAN'
    )) THEN
      LET bad_fk_cust NUMBER := (
        SELECT COUNT(*)
        FROM FACTINTERNETSALES_CLEAN f
        LEFT JOIN DIMCUSTOMER_CLEAN d
          ON f.CUSTOMERKEY = d.CUSTOMERKEY
        WHERE d.CUSTOMERKEY IS NULL
      );
      SELECT IFF(:bad_fk_cust = 0, 1, 1/0) AS check_fact_customerkey_fk;
    END IF;

    -- FK: ProductKey exists in DimProduct (only if DimProduct exists)
    IF (EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA='ASG_CLEAN' AND TABLE_NAME='DIMPRODUCT_CLEAN'
    )) THEN
      LET bad_fk_prod NUMBER := (
        SELECT COUNT(*)
        FROM FACTINTERNETSALES_CLEAN f
        LEFT JOIN DIMPRODUCT_CLEAN d
          ON f.PRODUCTKEY = d.PRODUCTKEY
        WHERE d.PRODUCTKEY IS NULL
      );
      SELECT IFF(:bad_fk_prod = 0, 1, 1/0) AS check_fact_productkey_fk;
    END IF;

    -- FK: OrderDateKey exists in DimDate (only if DimDate exists)
    IF (EXISTS (
        SELECT 1 FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA='ASG_CLEAN' AND TABLE_NAME='DIMDATE_CLEAN'
    )) THEN
      LET bad_fk_date NUMBER := (
        SELECT COUNT(*)
        FROM FACTINTERNETSALES_CLEAN f
        LEFT JOIN DIMDATE_CLEAN d
          ON f.ORDERDATEKEY = d.DATEKEY
        WHERE d.DATEKEY IS NULL
      );
      SELECT IFF(:bad_fk_date = 0, 1, 1/0) AS check_fact_orderdatekey_fk;
    END IF;

  END IF;

END;
