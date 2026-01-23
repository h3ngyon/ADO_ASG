USE ROLE TRAINING_ROLE;
USE WAREHOUSE FERRET_WH;
USE DATABASE GRP1_ASG;
USE SCHEMA ASG_TRANSFORMATIONS;

-- Create tables suitable to make a dashboard for a sales manager
-- tables should answer: - sales figures by area, product category, product subcategory
-- FACTINTERNETSALES

CREATE OR REPLACE TABLE SalesManager AS
SELECT
    fis.ProductKey AS ProductKey,
    dp."EnglishProductName" as ProductName,
    COALESCE(dpc.ENGLISHPRODUCTCATEGORYNAME, 'Uncategorized') AS Category,
    COALESCE(dpsc."EnglishProductSubcategoryName", 'Uncategorized') AS Subcategory,
    

    fis.OrderDate AS OrderDate,
    YEAR(fis.ORDERDATE) AS OrderYear,
    MONTH(fis.ORDERDATE) AS OrderMonth,
    TO_VARCHAR(fis.OrderDate, 'YYYY-MM') AS YearMonth,

    
    fis.CustomerKey AS CustomerKey,
    dc.currencyalternatekey AS Currency,
    dst."SalesTerritoryCountry" AS Country,
    dst."SalesTerritoryGroup" AS Continent,

    
    fis.SalesOrderNumber AS SalesOrderNumber,
    fis.SalesOrderLineNumber AS SalesOrderLineNumber,
    fis.OrderQuantity AS OrderQuantity,
    CAST(fis.UnitPrice AS DECIMAL(18,4)) AS UnitPrice,
     
    -- Change data type from FLOAT to DECIMAL
    CAST(fis.SalesAmount AS DECIMAL(18,4)) AS SalesAmount,
    CAST(fis.DiscountAmount AS DECIMAL(18,4)) AS DiscountAmount,
    CAST(fis.TotalProductCost AS DECIMAL(18,4)) AS TotalProductCost,
    
    CAST((fis.SalesAmount - fis.TaxAmount - fis.Freight - fis.TotalProductCost - fis.DiscountAmount) AS DECIMAL(10,2)) as GrossProfit,

    

    
FROM ASG_CLEAN.FACTINTERNETSALES_CLEAN fis
JOIN ASG_CLEAN.DIMCURRENCY_CLEAN dc on
fis.CurrencyKey = dc.currencykey

JOIN ASG.DIMPRODUCT dp on
fis.ProductKey = dp."ProductKey"

JOIN ASG.DIMSALESTERRITORY dst on
fis.SalesTerritoryKey = dst."SalesTerritoryKey"

LEFT JOIN ASG.DIMPRODUCTSUBCATEGORY dpsc on
dp."ProductSubcategoryKey" = dpsc."ProductSubcategoryKey"

LEFT JOIN ASG_CLEAN.DIMPRODUCTCATEGORY_CLEAN dpc on
dpc.PRODUCTCATEGORYKEY = dpsc."ProductCategoryKey";


SELECT * FROM SalesManager LIMIT 10;






