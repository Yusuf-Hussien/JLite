# JLite Example Scripts

This file contains separate example scripts for testing DDL, DML, and DQL with realistic sample data.

The examples stay within the SQL surface currently supported by JLite: single-table `CREATE TABLE`, `INSERT`, `UPDATE`, `DELETE`, and `SELECT ... WHERE`.

## DDL Script

Use this to create a small customer and order dataset.

```sql
CREATE TABLE customers ( id INT, name TEXT, email TEXT, city TEXT, active BOOLEAN);

CREATE TABLE orders (id INT, customer_id INT, order_date TEXT, amount INT, status TEXT);
```

## DML Script

Use this to load real-looking rows, then update and delete a few records.

```sql
INSERT INTO customers (id, name, email, city, active) VALUES (1, 'Ava Johnson', 'ava.johnson@example.com', 'London', true), (2, 'Noah Patel', 'noah.patel@example.com', 'Manchester', true),(3, 'Mia Chen', 'mia.chen@example.com', 'Birmingham', true),(4, 'Liam Brown', 'liam.brown@example.com', 'Leeds', false),(5, 'Sophia Davis', 'sophia.davis@example.com', 'Bristol', true);

INSERT INTO orders (id, customer_id, order_date, amount, status) VALUES(1001, 1, '2026-04-01', 120, 'PAID'),(1002, 2, '2026-04-02', 75, 'PAID'),(1003, 3, '2026-04-03', 240, 'PENDING'),(1004, 1, '2026-04-04', 60, 'PAID'),(1005, 5, '2026-04-05', 310, 'PAID');

UPDATE customers SET city = 'Liverpool' WHERE id = 2;

UPDATE orders SET status = 'PAID' WHERE id = 1003;

DELETE FROM customers WHERE id = 4;

DELETE FROM orders WHERE amount < 70;
```

## DQL Script

Use this to verify the data after the inserts and updates.

```sql
SELECT * FROM customers;

SELECT id, name, city FROM customers WHERE active = true;

SELECT * FROM orders WHERE amount >= 100 AND status = 'PAID';

SELECT id, order_date, amount FROM orders WHERE customer_id = 1;
```

## Full End-to-End Run

If you want one clean run from empty tables, execute the scripts in this order:

1. DDL Script
2. DML Script
3. DQL Script

If you want to repeat the test, drop the tables first and start again:

```sql
DROP TABLE orders;
DROP TABLE customers;
```
