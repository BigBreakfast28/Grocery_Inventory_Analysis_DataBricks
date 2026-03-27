-- ============================================================================
-- PHASE 5: MACHINE LEARNING
-- Feature Preparation, Training Data Export, Testing, Predictions, Performance, Forecast
-- ============================================================================
-- Prerequisites: grocery_inventory_adjusted Delta table must exist

-- ============================================================================
-- STEP 5.1: FEATURE PREPARATION
-- ============================================================================
-- Create Days_Until_Stockout and Needs_Restock target variable

CREATE OR REPLACE TABLE grocery_inventory_features AS
SELECT 
  Product_Name,
  Category,
  Inventory_Turnover_Rate,
  Stock_Quantity,
  Sales_Volume,
  Unit_Price,
  CASE 
    WHEN Sales_Volume > 0 THEN ROUND(Stock_Quantity / (Sales_Volume / 30.0), 2)
    ELSE NULL
  END AS Days_Until_Stockout,
  CASE 
    WHEN Sales_Volume > 0 AND (Stock_Quantity / (Sales_Volume / 30.0)) < 30 THEN 1
    ELSE 0
  END AS Needs_Restock
FROM grocery_inventory_adjusted
WHERE Status = 'Active'
  AND Sales_Volume IS NOT NULL
  AND Sales_Volume > 0
  AND Stock_Quantity IS NOT NULL
  AND Inventory_Turnover_Rate IS NOT NULL;

-- Verify feature preparation
SELECT 
  COUNT(*) AS total_records,
  SUM(Needs_Restock) AS products_needing_restock,
  COUNT(*) - SUM(Needs_Restock) AS products_not_needing_restock
FROM grocery_inventory_features;

-- ============================================================================
-- STEP 5.2: TRAINING DATA EXPORT
-- ============================================================================
-- 70/30 train/test split using hash-based sampling

CREATE OR REPLACE TABLE grocery_inventory_train AS
SELECT *
FROM grocery_inventory_features
WHERE MOD(ABS(HASH(Product_Name)), 10) < 7;

CREATE OR REPLACE TABLE grocery_inventory_test AS
SELECT *
FROM grocery_inventory_features
WHERE MOD(ABS(HASH(Product_Name)), 10) >= 7;

-- Verify split
SELECT 
  'Training' AS dataset,
  COUNT(*) AS record_count,
  SUM(Needs_Restock) AS positive_cases,
  ROUND(AVG(Needs_Restock) * 100, 2) AS positive_rate_percent
FROM grocery_inventory_train
UNION ALL
SELECT 
  'Testing' AS dataset,
  COUNT(*) AS record_count,
  SUM(Needs_Restock) AS positive_cases,
  ROUND(AVG(Needs_Restock) * 100, 2) AS positive_rate_percent
FROM grocery_inventory_test;

-- ============================================================================
-- STEP 5.3: TESTING DATA QUERY
-- ============================================================================
-- Query to evaluate predictions on test set
-- NOTE: This query works with current data (before model training)

SELECT 
  Product_Name,
  Category,
  Needs_Restock AS actual,
  Days_Until_Stockout,
  Stock_Quantity,
  Sales_Volume
FROM grocery_inventory_test
ORDER BY Days_Until_Stockout ASC;

-- AFTER MODEL TRAINING: Use this query when inventory_forecast table exists
-- SELECT 
--   t.Product_Name,
--   t.Category,
--   t.Needs_Restock AS actual,
--   p.Predicted_Restock_Needed AS predicted
-- FROM grocery_inventory_test t
-- LEFT JOIN inventory_forecast p ON t.Product_Name = p.Product_Name;

-- ============================================================================
-- STEP 5.4: PREDICTION RESULTS QUERY
-- ============================================================================
-- Query to retrieve model predictions from inventory_forecast table
-- NOTE: Requires inventory_forecast table created after ML model training

-- BEFORE MODEL TRAINING: Use feature-based forecast
SELECT 
  Product_Name,
  Category,
  Stock_Quantity,
  Sales_Volume,
  Days_Until_Stockout,
  Needs_Restock AS predicted_needs_restock,
  CASE 
    WHEN Days_Until_Stockout < 15 THEN 'Urgent'
    WHEN Days_Until_Stockout < 30 THEN 'Moderate'
    ELSE 'Low Priority'
  END AS restock_priority
FROM grocery_inventory_features
WHERE Needs_Restock = 1
ORDER BY Days_Until_Stockout ASC;

-- AFTER MODEL TRAINING: Use this query when inventory_forecast table exists
-- SELECT 
--   Product_Name,
--   Category,
--   Stock_Quantity,
--   Sales_Volume,
--   Days_Until_Stockout,
--   Predicted_Restock_Needed,
--   Prediction_Probability
-- FROM inventory_forecast
-- WHERE Predicted_Restock_Needed = 1
-- ORDER BY Prediction_Probability DESC;

-- ============================================================================
-- STEP 5.5: MODEL PERFORMANCE QUERY
-- ============================================================================
-- Standard statistical prediction evaluations: Accuracy, Precision, Recall, F1-Score
-- NOTE: Requires inventory_forecast table created after ML model training

-- BEFORE MODEL TRAINING: Baseline performance using feature-based predictions
WITH baseline_predictions AS (
  SELECT
    Needs_Restock AS actual,
    Needs_Restock AS predicted
  FROM grocery_inventory_test
),
metrics AS (
  SELECT
    COUNT(*) AS total_predictions,
    SUM(CASE WHEN actual = 1 AND predicted = 1 THEN 1 ELSE 0 END) AS true_positives,
    SUM(CASE WHEN actual = 0 AND predicted = 0 THEN 1 ELSE 0 END) AS true_negatives,
    SUM(CASE WHEN actual = 0 AND predicted = 1 THEN 1 ELSE 0 END) AS false_positives,
    SUM(CASE WHEN actual = 1 AND predicted = 0 THEN 1 ELSE 0 END) AS false_negatives
  FROM baseline_predictions
)
SELECT
  total_predictions,
  true_positives,
  true_negatives,
  false_positives,
  false_negatives,
  ROUND((true_positives + true_negatives) * 100.0 / total_predictions, 2) AS accuracy_percent,
  ROUND(true_positives * 100.0 / NULLIF(true_positives + false_positives, 0), 2) AS precision_percent,
  ROUND(true_positives * 100.0 / NULLIF(true_positives + false_negatives, 0), 2) AS recall_percent,
  ROUND(2.0 * (true_positives * 1.0 / NULLIF(true_positives + false_positives, 0)) * 
        (true_positives * 1.0 / NULLIF(true_positives + false_negatives, 0)) / 
        NULLIF((true_positives * 1.0 / NULLIF(true_positives + false_positives, 0)) + 
               (true_positives * 1.0 / NULLIF(true_positives + false_negatives, 0)), 0) * 100, 2) AS f1_score_percent
FROM metrics;

-- AFTER MODEL TRAINING: Use this query when inventory_forecast table exists
-- WITH model_results AS (
--   SELECT
--     t.Needs_Restock AS actual,
--     p.Predicted_Restock_Needed AS predicted
--   FROM grocery_inventory_test t
--   JOIN inventory_forecast p ON t.Product_Name = p.Product_Name
-- ),
-- metrics AS (
--   SELECT
--     COUNT(*) AS total_predictions,
--     SUM(CASE WHEN actual = 1 AND predicted = 1 THEN 1 ELSE 0 END) AS true_positives,
--     SUM(CASE WHEN actual = 0 AND predicted = 0 THEN 1 ELSE 0 END) AS true_negatives,
--     SUM(CASE WHEN actual = 0 AND predicted = 1 THEN 1 ELSE 0 END) AS false_positives,
--     SUM(CASE WHEN actual = 1 AND predicted = 0 THEN 1 ELSE 0 END) AS false_negatives
--   FROM model_results
-- )
-- SELECT
--   total_predictions,
--   true_positives,
--   true_negatives,
--   false_positives,
--   false_negatives,
--   ROUND((true_positives + true_negatives) * 100.0 / total_predictions, 2) AS accuracy_percent,
--   ROUND(true_positives * 100.0 / NULLIF(true_positives + false_positives, 0), 2) AS precision_percent,
--   ROUND(true_positives * 100.0 / NULLIF(true_positives + false_negatives, 0), 2) AS recall_percent,
--   ROUND(2.0 * (true_positives * 1.0 / NULLIF(true_positives + false_positives, 0)) * 
--         (true_positives * 1.0 / NULLIF(true_positives + false_negatives, 0)) / 
--         NULLIF((true_positives * 1.0 / NULLIF(true_positives + false_positives, 0)) + 
--                (true_positives * 1.0 / NULLIF(true_positives + false_negatives, 0)), 0) * 100, 2) AS f1_score_percent
-- FROM metrics;

-- ============================================================================
-- STEP 5.6: PRODUCTS FORECAST QUERY
-- ============================================================================
-- Lists products predicted to need restocking, ordered by urgency

-- BEFORE MODEL TRAINING: Use feature-based forecast
SELECT
  Product_Name,
  Category,
  Stock_Quantity,
  Sales_Volume,
  Days_Until_Stockout,
  Needs_Restock AS predicted_needs_restock,
  CASE 
    WHEN Days_Until_Stockout < 15 THEN 'Urgent'
    WHEN Days_Until_Stockout < 30 THEN 'Moderate'
    ELSE 'Low Priority'
  END AS restock_priority
FROM grocery_inventory_features
WHERE Needs_Restock = 1
ORDER BY Days_Until_Stockout ASC;

-- Forecast summary statistics
SELECT 
  COUNT(*) AS products_needing_restock,
  COUNT(CASE WHEN Days_Until_Stockout < 15 THEN 1 END) AS urgent_restock,
  COUNT(CASE WHEN Days_Until_Stockout BETWEEN 15 AND 30 THEN 1 END) AS moderate_restock,
  SUM(Stock_Quantity * Unit_Price) AS total_inventory_value_at_risk
FROM grocery_inventory_features
WHERE Needs_Restock = 1;

-- AFTER MODEL TRAINING: Use this query when inventory_forecast table exists
-- SELECT
--   fi.Product_Name,
--   fi.Category,
--   fi.Stock_Quantity,
--   fi.Sales_Volume,
--   fi.Days_Until_Stockout,
--   fp.Predicted_Restock_Needed,
--   CASE 
--     WHEN fi.Days_Until_Stockout < 15 THEN 'Urgent'
--     WHEN fi.Days_Until_Stockout < 30 THEN 'Moderate'
--     ELSE 'Low Priority'
--   END AS restock_priority
-- FROM grocery_inventory_features fi
-- JOIN inventory_forecast fp ON fi.Product_Name = fp.Product_Name
-- WHERE fp.Predicted_Restock_Needed = 1
-- ORDER BY fi.Days_Until_Stockout ASC;
