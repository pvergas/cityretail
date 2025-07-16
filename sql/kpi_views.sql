-- View: Sales by Region
CREATE OR REPLACE VIEW v_sales_by_region AS
SELECT
    s.Region,
    SUM(f.revenue) AS total_revenue,
    SUM(f.QtySold) AS total_quantity
FROM factsales f
JOIN dimstore s ON f.StoreID = s.StoreID
GROUP BY s.Region;

-- View: Monthly Sales Trends (Enhanced with WeekNumber)
CREATE OR REPLACE VIEW v_monthly_sales AS
SELECT
    d.year,
    d.month,
    MIN(d.WeekNumber) AS first_week_of_month,
    MAX(d.WeekNumber) AS last_week_of_month,
    SUM(f.Revenue) AS total_revenue,
    SUM(f.QtySold) AS total_quantity
FROM factsales f
JOIN dimdate d ON f.DateID = d.DateID
GROUP BY d.year, d.month
ORDER BY d.year, d.month;

--View: Weekly Sales Trends
CREATE OR REPLACE VIEW v_weekly_sales AS
SELECT
    d.year,
    d.weeknumber,
    SUM(f.Revenue) AS total_revenue,
    SUM(f.QtySold) AS total_quantity
FROM factsales f
JOIN dimdate d ON f.DateID = d.DateID
GROUP BY d.year, d.weeknumber
ORDER BY d.year, d.weeknumber;

-- View: Weekend vs Weekday Sales Comparison
CREATE OR REPLACE VIEW v_weekend_sales_comparison AS
SELECT
    d.IsWeekend,
    d.Month,  
    SUM(f.Revenue) AS total_revenue,
    SUM(f.QtySold) AS total_quantity
FROM factsales f
JOIN dimdate d ON f.DateID = d.DateID
GROUP BY d.IsWeekend, d.Month 
ORDER BY d.Month, d.IsWeekend DESC;

-- View: Product Performance by Category
CREATE OR REPLACE VIEW v_product_performance AS
SELECT
    p.ProductID,
    p.ProductName,
    p.Category,
    p.Subcategory,
    SUM(f.Revenue) AS total_revenue,
    SUM(f.QtySold) AS total_quantity,
    ROUND(AVG((p.SalePrice - p.CostPrice) / NULLIF(p.SalePrice, 0)), 2) AS avg_profit_margin
FROM factsales f
JOIN dimproduct p ON f.ProductID = p.ProductID
GROUP BY p.ProductID, p.ProductName, p.Category, p.Subcategory;

-- View: Monthly Sales by Category
CREATE OR REPLACE VIEW v_monthly_sales_by_category AS
SELECT
    d.year,
    d.month,
    p.Category,
    p.Subcategory,
    SUM(f.Revenue) AS total_revenue,
    SUM(f.QtySold) AS total_quantity
FROM factsales f
JOIN dimdate d ON f.DateID = d.DateID
JOIN dimproduct p ON f.ProductID = p.ProductID
GROUP BY d.year, d.month, p.Category, p.Subcategory
ORDER BY d.year, d.month;

-- View: Top Region by Revenue
CREATE OR REPLACE VIEW v_top_region AS
SELECT Region, SUM(f.Revenue) AS total_revenue
FROM factsales f
JOIN dimstore s ON f.StoreID = s.StoreID
GROUP BY Region
ORDER BY total_revenue DESC
LIMIT 1;

-- Deprecated View: Profit Margin by Product
-- (Kept for reference, but superseded by v_product_performance)
CREATE OR REPLACE VIEW v_profit_margin_by_product AS
SELECT
    ProductID,
    ProductName,
    ROUND((SalePrice - CostPrice) / NULLIF(SalePrice, 0), 2) AS profit_margin
FROM dimproduct;
