## Query Optimization

1. Partitioning by InvoiceDate
    
    - Restricts scans to relevant partitions, speeding up time-based queries and improving performance on large datasets.

2. Indexing (CustomerID, Country, etc.)
    
    - Enables fast lookups and filtering, reducing full table scans and improving query response time.

3. Avoiding SELECT *
    
    - Reduces I/O by fetching only necessary columns, improving query execution speed and efficiency.

4. EXPLAIN ANALYZE Usage
    
    - Identifies bottlenecks and inefficient query plans, helping to fine-tune indexes and query structure.

5. Query Caching (Client-side)
    
    - Avoids repeated database hits for static or rarely updated data, reducing load and improving UX.

