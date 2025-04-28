
-- Original Table Creation
CREATE TABLE IF NOT EXISTS online_retail (
    InvoiceNo VARCHAR(20),
    StockCode VARCHAR(20),
    Description TEXT,
    Quantity INT,
    InvoiceDate TIMESTAMP,
    UnitPrice DECIMAL(10,2),
    CustomerID VARCHAR(20),
    Country VARCHAR(50),
    TotalAmount DECIMAL(10,2),
    PRIMARY KEY (InvoiceNo, StockCode)
)

-- QUERY OPTIMIZATION

-- Create a new Partitioned table based on 'InvoiceDate'
CREATE TABLE online_retail_partitioned (
	InvoiceNo VARCHAR(20),
	StockCode VARCHAR(20),
	Description TEXT,
	Quantity INT,
	InvoiceDate TIMESTAMP,
	UnitPrice DECIMAL(10,2),
	CustomerID VARCHAR(20),
	Country VARCHAR(50),
	TotalAmount DECIMAL(10,2),
	PRIMARY KEY (InvoiceNo, StockCode, InvoiceDate)
) PARTITION BY RANGE (InvoiceDate);


-- Function to generate monthly partitions
CREATE OR REPLACE FUNCTION create_monthly_partition(start_date DATE, end_date DATE)
RETURNS void AS $$
DECLARE
    curr_date DATE := start_date;
    next_month DATE;
    partition_name TEXT;
BEGIN
    WHILE curr_date < end_date LOOP
        next_month := curr_date + INTERVAL '1 month';
        partition_name := 'online_retail_' || TO_CHAR(curr_date, 'YYYY_MM');

        EXECUTE format($f$
            CREATE TABLE IF NOT EXISTS %I PARTITION OF online_retail_partitioned
            FOR VALUES FROM (%L) TO (%L)
        $f$, partition_name, curr_date, next_month);

        curr_date := next_month;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Calling the function
SELECT create_monthly_partition('2010-12-01', '2012-07-01');

-- Insert data into the partitioned table
INSERT INTO online_retail_partitioned
SELECT * FROM online_retail;

-- Rename tables
ALTER TABLE online_retail RENAME TO online_retail_unpartitioned;
ALTER TABLE online_retail_partitioned RENAME TO online_retail;


-- Create Indexes on the partitioned table

-- Index to speed up time-based queries
CREATE INDEX idx_invoicedate ON online_retail (InvoiceDate);

-- Index for faster country-based aggregations
CREATE INDEX idx_country ON online_retail (Country);

-- Index for product-level grouping
CREATE INDEX idx_description ON online_retail (Description);


EXPLAIN ANALYZE
SELECT * FROM online_retail
-- WHERE InvoiceDate >= '2011-01-01' AND InvoiceDate < '2011-02-01';


-- Add a Unique Constraint on (InvoiceNo, StockCode, InvoiceDate)
ALTER TABLE online_retail
ADD CONSTRAINT unique_invoice_stock_date
UNIQUE (InvoiceNo, StockCode, InvoiceDate);


select count(*) from online_retail

