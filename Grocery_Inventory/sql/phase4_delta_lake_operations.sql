-- ============================================================================
-- PHASE 4: DELTA LAKE OPERATIONS
-- Databricks Grocery Inventory Analytics Project
-- ============================================================================
--
-- Prerequisites: 
-- - Phase 1: Delta table 'grocery_inventory' must exist
-- - Phase 2: Data cleaning (DELETE operation) should be completed
-- - Phase 3: SQL queries executed (optional, but recommended)
--
-- This phase completes the 5 required Delta Lake operations:
--   #1: CREATE TABLE (completed in Phase 1)
--   #2: DELETE (completed in Phase 2)
--   #3: UPDATE (this file)
--   #4: DESCRIBE HISTORY (this file)
--   #5: Time Travel Query (this file)
--
-- ============================================================================

-- ============================================================================
-- DELTA OPERATION #3: UPDATE Stock Quantities
-- ============================================================================
-- Purpose: Simulate receiving a supplier shipment
-- Business Justification: Demonstrate transactional UPDATE capability for inventory management
-- Expected Outcome: 5 products receive +100 units each

-- Step 1: Identify products to update (lowest stock, Active status)
SELECT 
  Product_ID,
  Product_Name,
  Category,
  Stock_Quantity AS current_stock,
  Reorder_Level,
  (Reorder_Level - Stock_Quantity) AS units_below_reorder
FROM grocery_inventory
WHERE Status = 'Active'
ORDER BY Stock_Quantity ASC
LIMIT 5;

-- Step 2: Execute UPDATE operation (Delta Operation #3)
-- Option A: Using subquery (works in most Databricks SQL contexts)
UPDATE grocery_inventory
SET Stock_Quantity = Stock_Quantity + 100
WHERE Product_ID IN (
  SELECT Product_ID 
  FROM grocery_inventory 
  WHERE Status = 'Active'
  ORDER BY Stock_Quantity ASC
  LIMIT 5
);

-- Option B: Alternative approach using CTE (if Option A fails)
-- WITH products_to_update AS (
--   SELECT Product_ID 
--   FROM grocery_inventory 
--   WHERE Status = 'Active'
--   ORDER BY Stock_Quantity ASC
--   LIMIT 5
-- )
-- UPDATE grocery_inventory
-- SET Stock_Quantity = Stock_Quantity + 100
-- WHERE Product_ID IN (SELECT Product_ID FROM products_to_update);

-- Option C: Explicit Product_IDs (use if both above fail)
-- First run Step 1 query, note the Product_IDs, then:
-- UPDATE grocery_inventory
-- SET Stock_Quantity = Stock_Quantity + 100
-- WHERE Product_ID IN ('ID1', 'ID2', 'ID3', 'ID4', 'ID5');

-- Step 3: Verify the update
SELECT 
  Product_ID,
  Product_Name,
  Category,
  Stock_Quantity AS updated_stock,
  Reorder_Level,
  CASE 
    WHEN Stock_Quantity >= Reorder_Level THEN 'Above Reorder Level'
    ELSE 'Below Reorder Level'
  END AS stock_status
FROM grocery_inventory
WHERE Product_ID IN (
  SELECT Product_ID 
  FROM grocery_inventory 
  WHERE Status = 'Active'
  ORDER BY Stock_Quantity DESC
  LIMIT 5
);

-- Document: "We updated inventory after receiving a supplier shipment. 
-- 5 products received +100 units each, bringing them above reorder level."

-- ============================================================================
-- DELTA OPERATION #4: DESCRIBE HISTORY
-- ============================================================================
-- Purpose: View version history of the Delta table
-- Business Value: Audit trail for compliance (who changed what, when)
-- Expected Outcome: Table showing versions 0, 1, 2, etc.

DESCRIBE HISTORY grocery_inventory;

-- Expected versions:
-- Version 0: Initial table creation (CREATE TABLE)
-- Version 1: Data cleaning (DELETE invalid records)
-- Version 2: Stock update (UPDATE shipment)

-- Document: "Version history shows:
-- - Version 0: Initial load of grocery inventory data
-- - Version 1: Deleted invalid records (negative stock, null Product_IDs)
-- - Version 2: Updated stock quantities after receiving shipment"

-- ============================================================================
-- DELTA OPERATION #5: Time Travel Query
-- ============================================================================
-- Purpose: Compare current inventory state to previous versions
-- Business Value: Analyze how inventory changed over time, audit changes
-- Expected Outcome: Side-by-side comparison showing inventory value changes

-- Step 1: Compare aggregate statistics - Current vs Version 0
SELECT 
  'Current' AS version,
  COUNT(*) AS product_count,
  SUM(Stock_Quantity) AS total_stock_quantity,
  ROUND(SUM(Stock_Quantity * Unit_Price), 2) AS total_inventory_value,
  AVG(Stock_Quantity) AS avg_stock_per_product
FROM grocery_inventory

UNION ALL

SELECT 
  'Version 0 (Initial)' AS version,
  COUNT(*) AS product_count,
  SUM(Stock_Quantity) AS total_stock_quantity,
  ROUND(SUM(Stock_Quantity * Unit_Price), 2) AS total_inventory_value,
  AVG(Stock_Quantity) AS avg_stock_per_product
FROM grocery_inventory VERSION AS OF 0;

-- Step 2: Calculate percentage change in inventory value
WITH current_stats AS (
  SELECT 
    SUM(Stock_Quantity * Unit_Price) AS current_value,
    SUM(Stock_Quantity) AS current_stock
  FROM grocery_inventory
),
initial_stats AS (
  SELECT 
    SUM(Stock_Quantity * Unit_Price) AS initial_value,
    SUM(Stock_Quantity) AS initial_stock
  FROM grocery_inventory VERSION AS OF 0
)
SELECT 
  ROUND(c.current_value, 2) AS current_inventory_value,
  ROUND(i.initial_value, 2) AS initial_inventory_value,
  ROUND(c.current_value - i.initial_value, 2) AS value_change,
  ROUND(((c.current_value - i.initial_value) / i.initial_value * 100), 2) AS percent_change,
  c.current_stock AS current_total_stock,
  i.initial_stock AS initial_total_stock,
  (c.current_stock - i.initial_stock) AS stock_quantity_change
FROM current_stats c
CROSS JOIN initial_stats i;

-- Step 3: Compare specific products (before/after update)
-- This shows which products were updated and their stock changes
WITH current_data AS (
  SELECT 
    Product_ID,
    Product_Name,
    Category,
    Stock_Quantity AS current_stock,
    Unit_Price,
    (Stock_Quantity * Unit_Price) AS current_value
  FROM grocery_inventory
  WHERE Status = 'Active'
  ORDER BY Stock_Quantity DESC
  LIMIT 10
),
initial_data AS (
  SELECT 
    Product_ID,
    Stock_Quantity AS initial_stock,
    (Stock_Quantity * Unit_Price) AS initial_value
  FROM grocery_inventory VERSION AS OF 0
)
SELECT 
  c.Product_ID,
  c.Product_Name,
  c.Category,
  i.initial_stock,
  c.current_stock,
  (c.current_stock - i.initial_stock) AS stock_change,
  ROUND(i.initial_value, 2) AS initial_value,
  ROUND(c.current_value, 2) AS current_value,
  ROUND(c.current_value - i.initial_value, 2) AS value_change
FROM current_data c
LEFT JOIN initial_data i ON c.Product_ID = i.Product_ID
ORDER BY stock_change DESC;

-- Document: "Time travel analysis shows:
-- - Total inventory value increased by X% after restocking
-- - 5 products received +100 units each
-- - Inventory value change: $X increase from initial state"

-- ============================================================================
-- PHASE 4 SUMMARY
-- ============================================================================
-- All 5 Delta Lake operations completed:
-- ✅ Operation #1: CREATE TABLE (Phase 1)
-- ✅ Operation #2: DELETE (Phase 2)
-- ✅ Operation #3: UPDATE (Phase 4 - this file)
-- ✅ Operation #4: DESCRIBE HISTORY (Phase 4 - this file)
-- ✅ Operation #5: Time Travel Query (Phase 4 - this file)

-- Business Value Demonstrated:
-- 1. ACID transactions: Updates are atomic and consistent
-- 2. Audit trail: Full history of all changes
-- 3. Time travel: Ability to query any previous version
-- 4. Compliance: Complete change tracking for regulatory requirements

