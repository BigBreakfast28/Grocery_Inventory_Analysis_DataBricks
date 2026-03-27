# Grocery_Inventory_Analysis_DataBricks
This project analyzes grocery inventory data to optimize stock levels, reduce stockouts, and improve supply chain efficiency using **Databricks, SQL, Delta Lake, and machine learning**.

---

## 🎯 Business Problem

A regional grocery retailer needs a data-driven approach to:
- Monitor inventory health  
- Reduce stock shortages  
- Improve inventory turnover  
- Identify top-performing and underperforming products  
- Forecast restocking needs  

Without proper analytics, the business risks:
- Lost revenue due to stockouts  
- Excess capital tied up in overstocked items  
- Inefficient supplier management  

---

## 📂 Dataset Overview

The dataset contains inventory and sales information for grocery products, including:

- Product Name, Category, Supplier  
- Stock Quantity & Reorder Level  
- Unit Price & Sales Volume  
- Inventory Turnover Rate  
- Warehouse Location  
- Order & Expiration Dates  

- ~990 unique products  
- ~55,000 total inventory units  
- Multiple suppliers and product categories  

---

## ⚙️ Technologies Used

- **Databricks**
- **SQL**
- **Delta Lake** (ACID transactions, versioning, time travel)
- **Python / PySpark**
- **Machine Learning (Forecasting)**
- **Dashboarding & Visualization**

---

## 🧠 Project Components

### 1. SQL-Based Analysis
- Identified top-selling and low-performing products  
- Calculated inventory turnover rates  
- Detected products below reorder levels  
- Analyzed supplier performance and stock trends  

### 2. Delta Lake Operations
- Created Delta table from raw dataset  
- Performed UPDATE and DELETE operations  
- Tracked version history of data  
- Used **time travel queries** to compare historical states  

### 3. Dashboard Development
Built an interactive dashboard to monitor:
- Total inventory value  
- Number of low-stock products  
- Average turnover rate  
- Monthly inventory trends  
- Category distribution  
- Low-stock alerts  

### 4. Machine Learning Forecasting
- Built a predictive model to identify products requiring restocking  
- Features used:
  - Stock quantity  
  - Sales volume  
  - Inventory turnover rate  
- Generated predictions for future inventory needs  

---

## 📊 Key Insights

- Identified **455 products below reorder level**, indicating widespread stock shortages  
- Average inventory turnover rate of **~50%**, showing moderate efficiency  
- High-demand categories (e.g., seafood, dairy) are at risk of stockouts  
- Supplier reliability analysis suggests potential **supply chain disruption**  
- Inventory levels fluctuate significantly, indicating inconsistent stocking patterns  

---

## 📈 Dashboard Preview

![Dashboard](images/dashboard.png)

---

## 🚀 Business Recommendations

- Prioritize restocking of high-demand, low-stock products  
- Adjust reorder levels based on turnover rates  
- Reduce or eliminate low-performing products  
- Improve supplier selection and reliability tracking  
- Implement predictive alerts for proactive inventory management  

---

## 💡 Key Takeaway

This project demonstrates how **data analytics, SQL, and machine learning** can be combined to optimize inventory systems, reduce operational risk, and improve decision-making in retail environments.
