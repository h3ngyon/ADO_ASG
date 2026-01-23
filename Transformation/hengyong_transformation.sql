USE ROLE TRAINING_ROLE;
USE WAREHOUSE FERRET_WH;
USE DATABASE GRP1_ASG;
USE SCHEMA ASG;
-- Create tables suitable to make a dashboard for a sales manager
-- tables should answer: - sales figures by area, product category, product subcategory
-- FACTINTERNETSALES




select * from DIMPRODUCTSUBCATEGORY_CLEANED LIMIT 10;

CREATE OR REPLACE TEMPORARY TABLE Consolidated_Products AS
SELECT
    dp."ProductKey" as ProductKey,
    dps.ENGLISHPRODUCTSUBCATEGORYNAME as Subcategory

FROM DIMPRODUCT dp
LEFT JOIN DIMPRODUCTSUBCATEGORY_CLEANED dps on
dp."ProductSubcategoryKey" = dps.productsubcategorykey; 

SELECT * FROM Consolidated_Products ;



CREATE OR REPLACE TEMPORARY TABLE new_fisc AS
SELECT
    fis.ProductKey AS ProductKey,
    dp."EnglishProductName" as ProductName,
    COALESCE(dpc.ENGLISHPRODUCTCATEGORYNAME, 'Uncategorized') AS Category,
    COALESCE(dpsc.ENGLISHPRODUCTSUBCATEGORYNAME, 'Uncategorized') AS Subcategory,
    

    fis.OrderDate AS OrderDate,
    YEAR(fis.ORDERDATE) AS OrderYear,
    MONTH(fis.ORDERDATE) AS OrderMonth,
    TO_VARCHAR(fis.OrderDate, 'YYYY-MM') AS YearMonth,

    
    fis.CustomerKey AS CustomerKey,
    dc.currencyalternatekey AS Currency,
    dst.SalesTerritoryCountry AS Country,
    dst.SalesTerritoryGroup AS Continent,

    
    fis.SalesOrderNumber AS SalesOrderNumber,
    fis.SalesOrderLineNumber AS SalesOrderLineNumber,
    fis.OrderQuantity AS OrderQuantity,
    CAST(fis.UnitPrice AS DECIMAL(18,4)) AS UnitPrice,
     
    -- Change data type from FLOAT to DECIMAL
    CAST(fis.SalesAmount AS DECIMAL(18,4)) AS SalesAmount,
    CAST(fis.DiscountAmount AS DECIMAL(18,4)) AS DiscountAmount,
    CAST(fis.TotalProductCost AS DECIMAL(18,4)) AS TotalProductCost,
    
    CAST((fis.SalesAmount - fis.TaxAmount - fis.Freight - fis.TotalProductCost - fis.DiscountAmount) AS DECIMAL(10,2)) as GrossProfit,

    

    
FROM FACTINTERNETSALES_CLEAN fis
JOIN DIMCURRENCY_CLEAN dc on
fis.CurrencyKey = dc.currencykey
JOIN DIMPRODUCT dp on
fis.ProductKey = dp."ProductKey"
JOIN DIMSALESTERRITORY_CLEANED dst on
fis.SalesTerritoryKey = dst.salesterritorykey
LEFT JOIN DIMPRODUCTSUBCATEGORY_CLEANED dpsc on
dp."ProductSubcategoryKey" = dpsc.productsubcategorykey
LEFT JOIN DIMPRODUCTCATEGORY_CLEAN dpc on
dpc.PRODUCTCATEGORYKEY = dpsc.productcategorykey;


SELECT * FROM new_fisc LIMIT 10;






