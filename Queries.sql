-- Create a temporary table for the payment summary (months 1-3)
DROP TABLE IF EXISTS PaymentsSummary;
CREATE TEMP TABLE PaymentsSummary AS
SELECT 
    UserId, 
    SUM(Payment) AS TotalPayments
FROM Payments
WHERE Month IN (1, 2, 3)
GROUP BY UserId;

------------------------------
-- 4. Final Joined Table: 
-- Columns: UserId, FirstName, LastName, Subtotal, TotalPayments, Total 
------------------------------
SELECT 
    ub.UserId, 
    ub.FirstName, 
    ub.LastName, 
    ub.Subtotal, 
    COALESCE(ps.TotalPayments, 0) AS TotalPayments, 
    (ub.Subtotal - COALESCE(ps.TotalPayments, 0)) AS Total
FROM UserBalance ub
LEFT JOIN PaymentsSummary ps 
    ON ub.UserId = ps.UserId;

------------------------------
-- 1. Rows where payment total > Subtotal
------------------------------
SELECT *
FROM (
    SELECT 
        ub.UserId, 
        ub.FirstName, 
        ub.LastName, 
        ub.Subtotal, 
        COALESCE(ps.TotalPayments, 0) AS TotalPayments, 
        (ub.Subtotal - COALESCE(ps.TotalPayments, 0)) AS Total
    FROM UserBalance ub
    LEFT JOIN PaymentsSummary ps 
        ON ub.UserId = ps.UserId
) AS FinalTable
WHERE TotalPayments > Subtotal;

------------------------------
-- 2. Rows where payment total < Subtotal
------------------------------
SELECT *
FROM (
    SELECT 
        ub.UserId, 
        ub.FirstName, 
        ub.LastName, 
        ub.Subtotal, 
        COALESCE(ps.TotalPayments, 0) AS TotalPayments, 
        (ub.Subtotal - COALESCE(ps.TotalPayments, 0)) AS Total
    FROM UserBalance ub
    LEFT JOIN PaymentsSummary ps 
        ON ub.UserId = ps.UserId
) AS FinalTable
WHERE TotalPayments < Subtotal;

------------------------------
-- 3. Rows where payment total = Subtotal
------------------------------
SELECT *
FROM (
    SELECT 
        ub.UserId, 
        ub.FirstName, 
        ub.LastName, 
        ub.Subtotal, 
        COALESCE(ps.TotalPayments, 0) AS TotalPayments, 
        (ub.Subtotal - COALESCE(ps.TotalPayments, 0)) AS Total
    FROM UserBalance ub
    LEFT JOIN PaymentsSummary ps 
        ON ub.UserId = ps.UserId
) AS FinalTable
WHERE TotalPayments = Subtotal;

------------------------------
-- 5. Highest Payment for Each User (Payments table)
------------------------------
SELECT 
    UserId, 
    MAX(Payment) AS HighestPayment
FROM Payments
GROUP BY UserId;

------------------------------
-- 6. Lowest Payment for Each User (Payments table)
------------------------------
SELECT 
    UserId, 
    MIN(Payment) AS LowestPayment
FROM Payments
GROUP BY UserId;
