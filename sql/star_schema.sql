
-- Dimension: Product
CREATE TABLE DimProduct (
    ProductID INT PRIMARY KEY,
    ProductName VARCHAR(100),
    Category VARCHAR(50),
    Subcategory VARCHAR(50),
    CostPrice DECIMAL(10,2),
    SalePrice DECIMAL(10,2)
);

-- Dimension: Store
CREATE TABLE DimStore (
    StoreID INT PRIMARY KEY,
    StoreName VARCHAR(100),
    City VARCHAR(50),
    Region VARCHAR(50)
);

-- Dimension: Date
CREATE TABLE DimDate (
    DateID INT PRIMARY KEY,
    Date DATE,
    Year INT,
    Quarter INT,
    Month INT,
    Day INT,
    Weekday VARCHAR(15),
    WeekNumber INT,          
    IsWeekend BOOLEAN        
);

-- Fact: Sales
CREATE TABLE FactSales (
    SalesID INT PRIMARY KEY,
    DateID INT,
    ProductID INT,
    StoreID INT,
    QtySold INT,
    Revenue DECIMAL(10,2),
    FOREIGN KEY (DateID) REFERENCES DimDate(DateID),
    FOREIGN KEY (ProductID) REFERENCES DimProduct(ProductID),
    FOREIGN KEY (StoreID) REFERENCES DimStore(StoreID)
);
