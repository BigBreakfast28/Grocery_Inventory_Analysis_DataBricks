-- ============================================================================
-- PHASE 6: DASHBOARD CREATION
-- SQL Queries for 5 Dashboard Visualizations
-- ============================================================================
-- This adds Date_Received, Product_ID, and Reorder_Level to grocery_inventory_adjusted
CREATE OR REPLACE TABLE grocery_inventory_adjusted AS
SELECT 
  gi.Product_Name,
  gi.Category,
  gi.Warehouse_Location,
  DATE_ADD(gi.Expiration_Date, 649) AS Expiration_Date,
  DATEDIFF(DATE_ADD(gi.Expiration_Date, 649), CURRENT_DATE()) AS days_until_expiration,
  gi.Stock_Quantity,
  gi.Unit_Price,
  gi.Sales_Volume,
  gi.Inventory_Turnover_Rate,
  DATE_ADD(gi.Last_Order_Date, 649) AS Last_Order_Date,
  gi.Status,
  gi.Supplier_Name,
  gi.Product_ID,
  gi.Reorder_Level,
  DATE_ADD(gi.Date_Received, 649) AS Date_Received,
  (gi.Stock_Quantity * gi.Unit_Price) AS inventory_value_at_risk
FROM grocery_inventory gi
ORDER BY Expiration_Date;
-- ============================================================================
-- VISUAL 1: KPI TILES (3 Queries)
-- ============================================================================

-- KPI 1: Total Inventory Value
SELECT 
  SUM(Stock_Quantity * Unit_Price) AS total_inventory_value
FROM grocery_inventory_adjusted;

-- KPI 2: Number of Low-Stock Products
SELECT 
  COUNT(*) AS low_stock_count
FROM grocery_inventory_adjusted
WHERE Stock_Quantity < Reorder_Level;

-- KPI 3: Average Turnover Rate
SELECT 
  AVG(Inventory_Turnover_Rate) AS avg_turnover_rate
FROM grocery_inventory_adjusted;

-- ============================================================================
-- VISUAL 2: BAR CHART - Top 10 Products by Sales
-- ============================================================================

SELECT 
  Product_Name,
  Sales_Volume
FROM grocery_inventory_adjusted
ORDER BY Sales_Volume DESC
LIMIT 10;

-- ============================================================================
-- VISUAL 3: LINE CHART - Monthly Inventory Value Trend
-- ============================================================================

SELECT 
  DATE_TRUNC('month', Date_Received) AS month,
  SUM(Stock_Quantity * Unit_Price) AS total_inventory_value
FROM grocery_inventory_adjusted
GROUP BY DATE_TRUNC('month', Date_Received)
ORDER BY month;

-- ============================================================================
-- VISUAL 4: TABLE - Low Stock Alerts
-- ============================================================================

SELECT 
  Product_Name,
  Category,
  Stock_Quantity,
  Reorder_Level,
  (Reorder_Level - Stock_Quantity) AS shortage_amount
FROM grocery_inventory_adjusted
WHERE Stock_Quantity < Reorder_Level
ORDER BY shortage_amount DESC;

-- ============================================================================
-- VISUAL 5: PIE CHART - Inventory by Category (% of total)
-- ============================================================================

WITH category_values AS (
  SELECT 
    Category,
    SUM(Stock_Quantity * Unit_Price) AS category_value
  FROM grocery_inventory_adjusted
  GROUP BY Category
),
total_value AS (
  SELECT SUM(Stock_Quantity * Unit_Price) AS total_value
  FROM grocery_inventory_adjusted
)
SELECT 
  cv.Category,
  cv.category_value,
  ROUND((cv.category_value * 100.0 / tv.total_value), 2) AS percentage_of_total
FROM category_values cv
CROSS JOIN total_value tv
ORDER BY cv.category_value DESC;
