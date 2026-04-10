# STEP 0

import sqlite3
import pandas as pd

conn = sqlite3.connect('data.sqlite')


# STEP 1 - Employees in Boston office
df_boston = pd.read_sql("""
SELECT e.firstName, e.lastName, e.jobTitle
FROM employees e
JOIN offices o
ON e.officeCode = o.officeCode
WHERE o.city = 'Boston'
""", conn)


# STEP 2 - Offices with zero employees
df_zero_emp = pd.read_sql("""
SELECT o.*
FROM offices o
LEFT JOIN employees e
ON o.officeCode = e.officeCode
WHERE e.employeeNumber IS NULL
""", conn)


# STEP 3 - All employees with office city/state
df_employee = pd.read_sql("""
SELECT e.firstName, e.lastName, o.city, o.state
FROM employees e
LEFT JOIN offices o
ON e.officeCode = o.officeCode
ORDER BY e.firstName, e.lastName
""", conn)


# STEP 4 - Customers with no orders
df_contacts = pd.read_sql("""
SELECT c.contactFirstName, c.contactLastName, c.phone, c.salesRepEmployeeNumber
FROM customers c
LEFT JOIN orders o
ON c.customerNumber = o.customerNumber
WHERE o.customerNumber IS NULL
ORDER BY c.contactLastName
""", conn)


# STEP 5 - Customer payments sorted by amount (DESC)
df_payment = pd.read_sql("""
SELECT c.contactFirstName, c.contactLastName, p.paymentDate, p.amount
FROM customers c
JOIN payments p
ON c.customerNumber = p.customerNumber
ORDER BY CAST(p.amount AS REAL) DESC
""", conn)


# STEP 6 - Employees with avg credit limit > 90000
df_credit = pd.read_sql("""
SELECT e.employeeNumber, e.firstName, e.lastName, COUNT(c.customerNumber) AS num_customers
FROM employees e
JOIN customers c
ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY e.employeeNumber
HAVING AVG(c.creditLimit) > 90000
ORDER BY num_customers DESC
""", conn)


# STEP 7 - Product sales performance
df_product_sold = pd.read_sql("""
SELECT p.productName,
       COUNT(DISTINCT o.orderNumber) AS numorders,
       SUM(od.quantityOrdered) AS totalunits
FROM products p
JOIN orderdetails od
ON p.productCode = od.productCode
JOIN orders o
ON od.orderNumber = o.orderNumber
GROUP BY p.productCode
ORDER BY totalunits DESC
""", conn)


# STEP 8 - Number of customers per product (numpurchasers)
df_total_customers = pd.read_sql("""
SELECT p.productName,
       p.productCode,
       COUNT(DISTINCT o.customerNumber) AS numpurchasers
FROM products p
JOIN orderdetails od ON p.productCode = od.productCode
JOIN orders o ON od.orderNumber = o.orderNumber
GROUP BY p.productCode
ORDER BY numpurchasers DESC
""", conn)


# STEP 9 - Customers per office
df_customers = pd.read_sql("""
SELECT o.officeCode,
       o.city,
       COUNT(c.customerNumber) AS n_customers
FROM offices o
JOIN employees e ON o.officeCode = e.officeCode
JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
GROUP BY o.officeCode
""", conn)


# STEP 10 - Employees selling products ordered by < 20 customers
df_under_20 = pd.read_sql("""
WITH low_products AS (
    SELECT p.productCode
    FROM products p
    JOIN orderdetails od ON p.productCode = od.productCode
    JOIN orders o ON od.orderNumber = o.orderNumber
    GROUP BY p.productCode
    HAVING COUNT(DISTINCT o.customerNumber) < 20
)

SELECT DISTINCT e.employeeNumber,
       e.firstName,
       e.lastName,
       o.city,
       o.officeCode
FROM employees e
JOIN offices o ON e.officeCode = o.officeCode
JOIN customers c ON e.employeeNumber = c.salesRepEmployeeNumber
JOIN orders ord ON c.customerNumber = ord.customerNumber
JOIN orderdetails od ON ord.orderNumber = od.orderNumber
WHERE od.productCode IN (SELECT productCode FROM low_products)
""", conn)


conn.close()