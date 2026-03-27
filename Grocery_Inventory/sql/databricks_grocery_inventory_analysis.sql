-- ============================================================================
-- DATABRICKS GROCERY INVENTORY ANALYTICS PROJECT
-- Phases 0-4: Data Ingestion, Validation, EDA, and Delta Lake Operations
-- ============================================================================
--
-- EXECUTION INSTRUCTIONS:
-- 1. Upload "Grocery_Inventory new v1.csv" to Databricks via UI:
--    - Go to Data > Create Table > Upload File
--    - Upload to /FileStore/grocery_inventory.csv
--    - OR create table directly via UI and skip to Phase 2
--
-- 2. If table already exists via UI, you can skip the CREATE TABLE section
--    and go directly to Phase 2 (Data Validation)
--
-- 3. Execute queries sequentially, section by section
-- 4. Document key findings as you go
--
-- ============================================================================

-- ============================================================================
-- PHASE 0: PRE-WORK - DATASET UNDERSTANDING
-- ============================================================================
-- Dataset: Grocery_Inventory new v1.csv
-- Expected rows: ~990 products
-- Key columns: Product_Name, Catagory (note: typo in original), Supplier_Name, 
--              Warehouse_Location, Status, Product_ID, Supplier_ID,
--              Date_Received, Last_Order_Date, Expiration_Date,
--              Stock_Quantity, Reorder_Level, Reorder_Quantity,
--              Unit_Price, Sales_Volume, Inventory_Turnover_Rate, percentage

-- Business Questions:
-- Pillar 1: Sales Tracking & Analytics
--   1. Which product categories have the highest total sales volume?
--   2. Which products are consistently low-performing (sales < 30 units)?
--   3. What is the monthly trend of total inventory value?

-- Pillar 2: Inventory Turnover & Stocking History
--   4. Which products have inventory turnover rates > 80% (fast movers)?
--   5. Which warehouses have the most products below reorder level?
--   6. What is the average time between Date_Received and Last_Order_Date?

-- Pillar 3: Predictive ML
--   7. Can we forecast which products will need restocking in the next 30 days?
--   8. What factors most influence inventory turnover rate?

-- ============================================================================
-- PHASE 1: DATA INGESTION
-- ============================================================================

-- Step 1.1: Upload CSV to Databricks (done via UI to /FileStore/)
-- Step 1.2: Create Bronze Delta Table (Delta Operation #1)

-- OPTION A: If CSV uploaded via UI to /FileStore/, use COPY INTO:
-- COPY INTO grocery_inventory
-- FROM '/FileStore/grocery_inventory.csv'
-- FILEFORMAT = CSV
-- FORMAT_OPTIONS ('header' = 'true', 'inferSchema' = 'true');

-- OPTION B: If table created via UI, use this to convert to Delta:
-- CREATE OR REPLACE TABLE grocery_inventory
-- USING DELTA
-- AS
-- SELECT * FROM grocery_inventory_temp;

-- OPTION C: Direct Delta table creation (after CSV upload):
-- First create external table, then convert to Delta
CREATE OR REPLACE TABLE grocery_inventory_temp
USING CSV
OPTIONS (
  path '/FileStore/grocery_inventory.csv',
  header 'true',
  inferSchema 'true'
);

-- Convert to Delta table with cleaned schema (Delta Operation #1)
CREATE OR REPLACE TABLE grocery_inventory
USING DELTA
AS
SELECT 
  Product_Name,
  Catagory AS Category,  -- Fix typo in column name
  Supplier_Name,
  Warehouse_Location,
  Status,
  Product_ID,
  Supplier_ID,
  TO_DATE(Date_Received, 'M/d/yyyy') AS Date_Received,
  TO_DATE(Last_Order_Date, 'M/d/yyyy') AS Last_Order_Date,
  TO_DATE(Expiration_Date, 'M/d/yyyy') AS Expiration_Date,
  CAST(Stock_Quantity AS INT) AS Stock_Quantity,
  CAST(Reorder_Level AS INT) AS Reorder_Level,
  CAST(Reorder_Quantity AS INT) AS Reorder_Quantity,
  CAST(REPLACE(Unit_Price, '$', '') AS DOUBLE) AS Unit_Price,
  CAST(Sales_Volume AS INT) AS Sales_Volume,
  CAST(Inventory_Turnover_Rate AS INT) AS Inventory_Turnover_Rate,
  CAST(REPLACE(percentage, '%', '') AS DOUBLE) AS Percentage
FROM grocery_inventory_temp;

-- Drop temporary table
DROP TABLE IF EXISTS grocery_inventory_temp;

-- Verify table creation
SELECT COUNT(*) AS total_rows FROM grocery_inventory;
SELECT * FROM grocery_inventory LIMIT 10;

-- ============================================================================
-- PHASE 2: DATA VALIDATION & CLEANING
-- ============================================================================

-- Query 1: Count total rows
SELECT COUNT(*) AS total_products FROM grocery_inventory;

-- Query 2: Check for nulls in critical columns
SELECT 
  COUNT(*) AS total_rows,
  SUM(CASE WHEN Product_ID IS NULL THEN 1 ELSE 0 END) AS null_product_id,
  SUM(CASE WHEN Stock_Quantity IS NULL THEN 1 ELSE 0 END) AS null_stock_quantity,
  SUM(CASE WHEN Sales_Volume IS NULL THEN 1 ELSE 0 END) AS null_sales_volume,
  SUM(CASE WHEN Unit_Price IS NULL THEN 1 ELSE 0 END) AS null_unit_price
FROM grocery_inventory;

-- Query 3: Identify products with Stock_Quantity < 0 (data errors)
SELECT 
  Product_ID,
  Product_Name,
  Stock_Quantity
FROM grocery_inventory
WHERE Stock_Quantity < 0;

-- Query 4: Check date formats (are dates parseable?)
SELECT 
  COUNT(*) AS total_rows,
  SUM(CASE WHEN Date_Received IS NULL THEN 1 ELSE 0 END) AS null_date_received,
  SUM(CASE WHEN Last_Order_Date IS NULL THEN 1 ELSE 0 END) AS null_last_order_date,
  SUM(CASE WHEN Expiration_Date IS NULL THEN 1 ELSE 0 END) AS null_expiration_date
FROM grocery_inventory;

-- Query 5: Count distinct categories, suppliers, warehouses
SELECT 
  COUNT(DISTINCT Category) AS distinct_categories,
  COUNT(DISTINCT Supplier_Name) AS distinct_suppliers,
  COUNT(DISTINCT Warehouse_Location) AS distinct_warehouses,
  COUNT(DISTINCT Status) AS distinct_statuses
FROM grocery_inventory;

-- Step 2.2: Handle Basic Issues (Delta Operation #2)
-- Delete invalid records: negative stock quantities and null Product_IDs

DELETE FROM grocery_inventory
WHERE Stock_Quantity < 0 OR Product_ID IS NULL;

-- Verify cleanup
SELECT COUNT(*) AS remaining_rows_after_cleanup FROM grocery_inventory;

-- ============================================================================
-- PHASE 3: SQL-BASED EDA (20-25 Queries)
-- ============================================================================

-- ============================================================================
-- BLOCK A: Sales Tracking & Analytics (Queries 6-12)
-- ============================================================================

-- Query 6: Top 10 Products by Sales Volume
-- Purpose: Identify best-selling products
SELECT 
  Product_Name,
  Category,
  Sales_Volume,
  Unit_Price,
  (Sales_Volume * Unit_Price) AS Total_Revenue
FROM grocery_inventory
ORDER BY Sales_Volume DESC
LIMIT 10;

-- Query 7: Bottom 10 Products by Sales Volume
-- Purpose: Identify slow-moving inventory
SELECT 
  Product_Name,
  Category,
  Sales_Volume,
  Unit_Price,
  Status
FROM grocery_inventory
ORDER BY Sales_Volume ASC
LIMIT 10;

-- Query 8: Total Sales by Category
-- Purpose: Understand category performance
SELECT 
  Category,
  SUM(Sales_Volume) AS total_sales_volume,
  AVG(Sales_Volume) AS avg_sales_volume,
  COUNT(*) AS product_count
FROM grocery_inventory
GROUP BY Category
ORDER BY total_sales_volume DESC;

-- Query 9: Total Inventory Value by Category
-- Purpose: Calculate capital tied up in inventory
SELECT 
  Category,
  SUM(Stock_Quantity * Unit_Price) AS total_inventory_value,
  AVG(Stock_Quantity * Unit_Price) AS avg_product_value,
  COUNT(*) AS product_count
FROM grocery_inventory
GROUP BY Category
ORDER BY total_inventory_value DESC;

-- Query 10: Monthly Sales Trends
-- Purpose: Detect seasonality
SELECT 
  DATE_TRUNC('month', Date_Received) AS month,
  SUM(Sales_Volume) AS total_sales_volume,
  SUM(Stock_Quantity * Unit_Price) AS total_inventory_value,
  COUNT(*) AS products_received
FROM grocery_inventory
GROUP BY DATE_TRUNC('month', Date_Received)
ORDER BY month;

-- Query 11: Products with High Unit Price but Low Sales
-- Purpose: Identify overpriced items
SELECT 
  Product_Name,
  Category,
  Unit_Price,
  Sales_Volume,
  Status
FROM grocery_inventory
WHERE Unit_Price > 10 AND Sales_Volume < 20
ORDER BY Unit_Price DESC;

-- Query 12: Total Revenue by Product
-- Purpose: Calculate actual revenue impact
SELECT 
  Product_Name,
  Category,
  Unit_Price,
  Sales_Volume,
  (Unit_Price * Sales_Volume) AS total_revenue
FROM grocery_inventory
ORDER BY total_revenue DESC
LIMIT 20;

-- ============================================================================
-- BLOCK B: Inventory Turnover & Stocking (Queries 13-18)
-- ============================================================================

-- Query 13: Products Below Reorder Level
-- Purpose: Generate restocking alerts
SELECT 
  Product_ID,
  Product_Name,
  Category,
  Warehouse_Location,
  Stock_Quantity,
  Reorder_Level,
  (Reorder_Level - Stock_Quantity) AS units_below_reorder,
  Status
FROM grocery_inventory
WHERE Stock_Quantity < Reorder_Level
ORDER BY (Reorder_Level - Stock_Quantity) DESC;

-- Query 14: Average Inventory Turnover by Category
-- Purpose: Compare category efficiency
SELECT 
  Category,
  AVG(Inventory_Turnover_Rate) AS avg_turnover_rate,
  MIN(Inventory_Turnover_Rate) AS min_turnover_rate,
  MAX(Inventory_Turnover_Rate) AS max_turnover_rate,
  COUNT(*) AS product_count
FROM grocery_inventory
GROUP BY Category
ORDER BY avg_turnover_rate DESC;

-- Query 15: Products with Turnover > 80%
-- Purpose: Identify fast-moving items
SELECT 
  Product_Name,
  Category,
  Inventory_Turnover_Rate,
  Sales_Volume,
  Stock_Quantity,
  Warehouse_Location
FROM grocery_inventory
WHERE Inventory_Turnover_Rate > 80
ORDER BY Inventory_Turnover_Rate DESC;

-- Query 16: Warehouse Inventory Distribution
-- Purpose: Compare stock levels across locations
SELECT 
  Warehouse_Location,
  COUNT(*) AS product_count,
  SUM(Stock_Quantity) AS total_stock_quantity,
  SUM(Stock_Quantity * Unit_Price) AS total_inventory_value,
  AVG(Stock_Quantity) AS avg_stock_per_product,
  COUNT(CASE WHEN Stock_Quantity < Reorder_Level THEN 1 END) AS products_below_reorder
FROM grocery_inventory
GROUP BY Warehouse_Location
ORDER BY total_inventory_value DESC;

-- Query 17: Supplier Performance
-- Purpose: Count products per supplier
SELECT 
  Supplier_Name,
  COUNT(*) AS product_count,
  SUM(Sales_Volume) AS total_sales_volume,
  AVG(Inventory_Turnover_Rate) AS avg_turnover_rate,
  COUNT(CASE WHEN Status = 'Backordered' THEN 1 END) AS backordered_count
FROM grocery_inventory
GROUP BY Supplier_Name
ORDER BY product_count DESC
LIMIT 20;

-- Query 18: Expired or Expiring Products
-- Purpose: Identify waste risk
SELECT 
  Product_Name,
  Category,
  Warehouse_Location,
  Expiration_Date,
  DATEDIFF(Expiration_Date, CURRENT_DATE()) AS days_until_expiration,
  Stock_Quantity,
  Unit_Price,
  (Stock_Quantity * Unit_Price) AS inventory_value_at_risk
FROM grocery_inventory
WHERE Expiration_Date <= DATE_ADD(CURRENT_DATE(), 30)
ORDER BY Expiration_Date;

-- ============================================================================
-- BLOCK C: Advanced Analytics (Queries 19-25)
-- ============================================================================

-- Query 19: Stock-to-Sales Ratio
-- Purpose: Measure inventory efficiency
SELECT 
  Product_Name,
  Category,
  Stock_Quantity,
  Sales_Volume,
  CASE 
    WHEN Sales_Volume > 0 THEN ROUND(Stock_Quantity / Sales_Volume, 2)
    ELSE NULL
  END AS stock_to_sales_ratio,
  CASE 
    WHEN Sales_Volume > 0 AND (Stock_Quantity / Sales_Volume) > 5 THEN 'Overstocked'
    WHEN Sales_Volume > 0 AND (Stock_Quantity / Sales_Volume) BETWEEN 1 AND 2 THEN 'Healthy'
    WHEN Sales_Volume > 0 AND (Stock_Quantity / Sales_Volume) < 1 THEN 'Understocked'
    ELSE 'No Sales Data'
  END AS inventory_status
FROM grocery_inventory
ORDER BY stock_to_sales_ratio DESC;

-- Query 20: Days Since Last Order
-- Purpose: Identify stale inventory
SELECT 
  Product_Name,
  Category,
  Last_Order_Date,
  DATEDIFF(CURRENT_DATE(), Last_Order_Date) AS days_since_last_order,
  Stock_Quantity,
  Sales_Volume,
  Status
FROM grocery_inventory
WHERE DATEDIFF(CURRENT_DATE(), Last_Order_Date) >= 60
ORDER BY days_since_last_order DESC;

-- Query 21: Window Function - Rank Categories by Turnover
-- Purpose: Use RANK() to compare categories
SELECT 
  Category,
  AVG(Inventory_Turnover_Rate) AS avg_turnover_rate,
  RANK() OVER (ORDER BY AVG(Inventory_Turnover_Rate) DESC) AS turnover_rank
FROM grocery_inventory
GROUP BY Category
ORDER BY turnover_rank;

-- Query 22: Window Function - Moving Average of Sales (by month)
-- Purpose: Calculate monthly average sales trend
WITH monthly_sales AS (
  SELECT 
    DATE_TRUNC('month', Date_Received) AS month,
    SUM(Sales_Volume) AS monthly_sales
  FROM grocery_inventory
  GROUP BY DATE_TRUNC('month', Date_Received)
)
SELECT 
  month,
  monthly_sales,
  AVG(monthly_sales) OVER (
    ORDER BY month 
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ) AS moving_avg_3_months
FROM monthly_sales
ORDER BY month;

-- Query 23: Products with Status = 'Backordered'
-- Purpose: Track supply chain issues
SELECT 
  Product_Name,
  Category,
  Supplier_Name,
  Warehouse_Location,
  Stock_Quantity,
  Reorder_Level,
  Status
FROM grocery_inventory
WHERE Status = 'Backordered'
ORDER BY Stock_Quantity;

-- Query 24: Price Range Analysis
-- Purpose: Segment products by price tier
SELECT 
  CASE 
    WHEN Unit_Price < 5 THEN '<$5'
    WHEN Unit_Price BETWEEN 5 AND 10 THEN '$5-$10'
    WHEN Unit_Price BETWEEN 10 AND 20 THEN '$10-$20'
    ELSE '>$20'
  END AS price_tier,
  COUNT(*) AS product_count,
  SUM(Sales_Volume) AS total_sales_volume,
  AVG(Sales_Volume) AS avg_sales_volume,
  SUM(Stock_Quantity * Unit_Price) AS total_inventory_value
FROM grocery_inventory
GROUP BY 
  CASE 
    WHEN Unit_Price < 5 THEN '<$5'
    WHEN Unit_Price BETWEEN 5 AND 10 THEN '$5-$10'
    WHEN Unit_Price BETWEEN 10 AND 20 THEN '$10-$20'
    ELSE '>$20'
  END
ORDER BY 
  CASE 
    WHEN Unit_Price < 5 THEN 1
    WHEN Unit_Price BETWEEN 5 AND 10 THEN 2
    WHEN Unit_Price BETWEEN 10 AND 20 THEN 3
    ELSE 4
  END;

-- Query 25: Supplier Reliability Score
-- Purpose: Calculate % of products NOT backordered per supplier
SELECT 
  Supplier_Name,
  COUNT(*) AS total_products,
  COUNT(CASE WHEN Status = 'Backordered' THEN 1 END) AS backordered_count,
  COUNT(CASE WHEN Status != 'Backordered' THEN 1 END) AS active_count,
  ROUND(
    (COUNT(CASE WHEN Status != 'Backordered' THEN 1 END) * 100.0 / COUNT(*)), 
    2
  ) AS reliability_score_percent
FROM grocery_inventory
GROUP BY Supplier_Name
HAVING COUNT(*) >= 5  -- Only suppliers with 5+ products
ORDER BY reliability_score_percent DESC;

-- ============================================================================
-- PHASE 4: DELTA LAKE OPERATIONS
-- ============================================================================

-- Delta Operation #3: UPDATE Stock Quantities
-- Purpose: Simulate receiving a shipment
-- Business Case: Increase stock for 5 products by 100 units each

-- First, let's see current stock for some products
SELECT 
  Product_ID,
  Product_Name,
  Stock_Quantity,
  Reorder_Level
FROM grocery_inventory
WHERE Status = 'Active'
ORDER BY Stock_Quantity ASC
LIMIT 5;

-- Update stock quantities (simulating shipment received)
UPDATE grocery_inventory
SET Stock_Quantity = Stock_Quantity + 100
WHERE Product_ID IN (
  SELECT Product_ID 
  FROM grocery_inventory 
  WHERE Status = 'Active'
  ORDER BY Stock_Quantity ASC
  LIMIT 5
);

-- Verify the update
SELECT 
  Product_ID,
  Product_Name,
  Stock_Quantity,
  Reorder_Level
FROM grocery_inventory
WHERE Product_ID IN (
  SELECT Product_ID 
  FROM grocery_inventory 
  WHERE Status = 'Active'
  ORDER BY Stock_Quantity DESC
  LIMIT 5
);

-- Delta Operation #4: DESCRIBE HISTORY
-- Purpose: View version history of the table
-- Business Value: Audit trail for compliance

DESCRIBE HISTORY grocery_inventory;

-- Delta Operation #5: Time Travel Query
-- Purpose: Compare current inventory to initial state
-- Business Value: Analyze how inventory changed over time

-- Current state
SELECT 
  'Current' AS version,
  COUNT(*) AS product_count,
  SUM(Stock_Quantity) AS total_stock,
  SUM(Stock_Quantity * Unit_Price) AS total_inventory_value
FROM grocery_inventory;

-- Initial state (Version 0)
SELECT 
  'Version 0' AS version,
  COUNT(*) AS product_count,
  SUM(Stock_Quantity) AS total_stock,
  SUM(Stock_Quantity * Unit_Price) AS total_inventory_value
FROM grocery_inventory VERSION AS OF 0;

-- Compare specific products (using JOIN for better performance)
WITH current_data AS (
  SELECT 
    Product_ID,
    Product_Name,
    Stock_Quantity AS current_stock
  FROM grocery_inventory
  WHERE Status = 'Active'
  ORDER BY Stock_Quantity DESC
  LIMIT 5
),
initial_data AS (
  SELECT 
    Product_ID,
    Stock_Quantity AS initial_stock
  FROM grocery_inventory VERSION AS OF 0
)
SELECT 
  c.Product_ID,
  c.Product_Name,
  c.current_stock,
  i.initial_stock,
  (c.current_stock - i.initial_stock) AS stock_change
FROM current_data c
LEFT JOIN initial_data i ON c.Product_ID = i.Product_ID;

-- ============================================================================
-- SUMMARY STATISTICS FOR DOCUMENTATION
-- ============================================================================

-- Overall dataset summary
SELECT 
  COUNT(*) AS total_products,
  COUNT(DISTINCT Category) AS total_categories,
  COUNT(DISTINCT Supplier_Name) AS total_suppliers,
  COUNT(DISTINCT Warehouse_Location) AS total_warehouses,
  SUM(Stock_Quantity * Unit_Price) AS total_inventory_value,
  SUM(Sales_Volume) AS total_sales_volume,
  AVG(Inventory_Turnover_Rate) AS avg_turnover_rate,
  COUNT(CASE WHEN Stock_Quantity < Reorder_Level THEN 1 END) AS products_below_reorder,
  COUNT(CASE WHEN Status = 'Backordered' THEN 1 END) AS backordered_products
FROM grocery_inventory;

