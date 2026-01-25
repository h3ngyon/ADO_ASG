--Initialise Connection Settings
USE WAREHOUSE CHEETAH_WH;
USE DATABASE GRP1_ASG;
USE SCHEMA ASG_TRANSFORMATIONS;
ALTER WAREHOUSE CHEETAH_WH
SET AUTO_SUSPEND = 600;

-- CREATING NEW TABLES FOR ASSIGNED SECTOR (PRODUCT PERFORMANCE ANALYSIS)
CREATE TABLE MAP_PRODUCT_HIERARCHY AS
SELECT 
    p.ProductKey,
    p.EnglishProductName AS ProductName,
    p.ModelName,
    s.EnglishProductSubcategoryName AS SubcategoryName,
    c.EnglishProductCategoryName AS CategoryName
FROM ASG_CLEAN.DIMPRODUCT_CLEAN p
LEFT JOIN ASG_CLEAN.DIMPRODUCTSUBCATEGORY_CLEAN s ON p.ProductSubcategoryKey = s.ProductSubcategoryKey
LEFT JOIN ASG_CLEAN.DIMPRODUCTCATEGORY_CLEAN c ON s.ProductCategoryKey = c.ProductCategoryKey;



CREATE TABLE AGG_PRODUCT_SALES_SUMMARY AS
WITH CombinedSales AS (
    SELECT ProductKey, OrderQuantity, SalesAmount, TotalProductCost FROM FACTINTERNETSALES_CLEAN
    UNION ALL
    SELECT ProductKey, OrderQuantity, SalesAmount, TotalProductCost FROM FACTRESELLERSALES_CLEAN
)
SELECT 
    ProductKey,
    SUM(OrderQuantity) AS TotalUnitsSold,
    SUM(SalesAmount) AS TotalRevenue,
    SUM(SalesAmount) - SUM(TotalProductCost) AS TotalGrossProfit,
    (SUM(SalesAmount) - SUM(TotalProductCost)) / NULLIF(SUM(SalesAmount), 0) AS ProfitMargin
FROM CombinedSales
GROUP BY ProductKey;


CREATE TABLE FACT_PRODUCT_PROMO_EFFECTIVENESS AS
SELECT 
    s.ProductKey,
    p.EnglishPromotionName,
    p.DiscountPct,
    SUM(s.SalesAmount) AS PromoSalesAmount,
    SUM(s.OrderQuantity) AS PromoUnitsSold
FROM FACTINTERNETSALES_CLEAN s
JOIN DIMPROMOTION p ON s.PromotionKey = p.PromotionKey
WHERE p.PromotionKey <> 1 -- Excluding 'No Discount'
GROUP BY s.ProductKey, p.EnglishPromotionName, p.DiscountPct;


CREATE TABLE DIM_SLOW_MOVING_INVENTORY AS
SELECT 
    p.ProductKey,
    p.EnglishProductName,
    p.SafetyStockLevel,
    p.ListPrice
FROM DIMPRODUCT_CLEAN p
LEFT JOIN (
    SELECT ProductKey FROM FACTINTERNETSALES_CLEAN
    UNION 
    SELECT ProductKey FROM FACTRESELLERSALES_CLEAN
) s ON p.ProductKey = s.ProductKey
WHERE s.ProductKey IS NULL; -- Products with zero sales records