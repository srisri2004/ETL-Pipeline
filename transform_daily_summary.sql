-- ELT transformation: dedupe latest version per order, clean types, aggregate by day.
DELETE FROM sales_daily_summary;
INSERT INTO sales_daily_summary (order_date, orders, units, revenue)
SELECT order_date, COUNT(*), SUM(qty), ROUND(SUM(qty * price), 2)
FROM (
  SELECT order_date, UPPER(status) AS status,
         CAST(quantity AS INTEGER) AS qty, CAST(unit_price AS REAL) AS price,
         ROW_NUMBER() OVER (PARTITION BY order_id
                            ORDER BY updated_at DESC, batch_id DESC) AS rn
  FROM stg_sales_raw
  WHERE TRIM(order_id) <> '' AND TRIM(customer_id) <> ''
    AND CAST(quantity AS INTEGER) > 0 AND CAST(unit_price AS REAL) >= 0
    AND order_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
    AND UPPER(status) IN ('PLACED','SHIPPED','DELIVERED','CANCELLED')
)
WHERE rn = 1 AND status <> 'CANCELLED'
GROUP BY order_date;
