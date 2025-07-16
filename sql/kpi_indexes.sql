-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_factsales_store_id ON factsales(StoreID);
CREATE INDEX IF NOT EXISTS idx_factsales_date_id ON factsales(DateID);
CREATE INDEX IF NOT EXISTS idx_dimstore_region ON dimstore(Region);
CREATE INDEX IF NOT EXISTS idx_dimdate_month_year ON dimdate(Month, Year);

-- Indexes for enhanced views
CREATE INDEX IF NOT EXISTS idx_dimdate_weeknumber ON dimdate(WeekNumber);
CREATE INDEX IF NOT EXISTS idx_dimdate_isweekend ON dimdate(IsWeekend);