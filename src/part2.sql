CREATE OR REPLACE FUNCTION get_analysis_date() RETURNS VARCHAR AS $$
SELECT MAX(analysis_formation)
FROM date_of_analysis_formation;
$$ LANGUAGE SQL;








CREATE OR REPLACE FUNCTION get_primary_store() RETURNS TABLE (customer_id INT, prime_store INT) AS $$
WITH transaction_percentages AS (
    SELECT cards.customer_id, 
           transactions.transaction_store_id,
           COUNT(*)::NUMERIC / SUM(COUNT(*)) OVER (PARTITION BY cards.customer_id)::NUMERIC AS orders_percent,
           MAX(TO_TIMESTAMP(transactions.transaction_datetime, 'DD.MM.YYYY HH24:MI:SS')) AS last_time_order
    FROM personal_information
         JOIN cards ON personal_information.customer_id = cards.customer_id
         JOIN transactions ON cards.customer_card_id = transactions.customer_card_id
    GROUP BY cards.customer_id, 
             transactions.transaction_store_id
),

primary_store_by_last_3_transactions AS (
    SELECT customer_id,
           CASE WHEN COUNT(DISTINCT transaction_store_id) = 1 THEN MAX(transaction_store_id) END AS primary_store
    FROM (
        SELECT cards.customer_id, 
               transactions.transaction_store_id,
               ROW_NUMBER() OVER (PARTITION BY personal_information.customer_id ORDER BY TO_TIMESTAMP(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS') DESC) AS top
        FROM personal_information
             JOIN cards ON personal_information.customer_id = cards.customer_id
             JOIN transactions ON cards.customer_card_id = transactions.customer_card_id
    ) AS last_transactions
    WHERE top <= 3 
    GROUP BY customer_id
),

prime_store AS (
    SELECT
        tran_proc.customer_id, 
        tran_proc.transaction_store_id, 
        tran_proc.orders_percent, 
        tran_proc.last_time_order, 
        primary_store.primary_store,
        ROW_NUMBER() OVER (PARTITION BY tran_proc.customer_id ORDER BY tran_proc.orders_percent DESC, tran_proc.last_time_order DESC) AS top
    FROM transaction_percentages tran_proc
         LEFT JOIN primary_store_by_last_3_transactions primary_store ON tran_proc.customer_id = primary_store.customer_id
)

SELECT
    customer_id, 
    COALESCE(primary_store, transaction_store_id) AS prime_store
FROM prime_store
WHERE top = 1;
$$ LANGUAGE SQL;




CREATE OR REPLACE VIEW Customers AS
WITH customer_average_check_frequency AS (
SELECT personal_information.customer_id                                                             AS customer_id,
       AVG(transactions.transaction_summ)                                                           AS customer_average_check,

       EXTRACT(EPOCH FROM (MAX(TO_TIMESTAMP(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS')) - 
       MIN(TO_TIMESTAMP(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS')))) / 
       COUNT(transaction_datetime) / 60 / 60 / 24                                                   AS customer_frequency,

       MAX(TO_TIMESTAMP(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS')) AS last_transaction_date
FROM personal_information
    JOIN cards c ON personal_information.customer_id = c.customer_id
    JOIN transactions ON c.customer_card_id = transactions.customer_card_id
GROUP BY personal_information.customer_id), 

customer_average_check_frequency_segmentation_inactive AS (
SELECT customer_id, 
       customer_average_check,
       CASE
           WHEN PERCENT_RANK() OVER (ORDER BY customer_average_check DESC) <= 0.1 THEN 'High'
           WHEN PERCENT_RANK() OVER (ORDER BY customer_average_check DESC) <= 0.35 THEN 'Medium'
           ELSE 'Low'
       END                                                                                          AS customer_average_check_segment, 
       customer_frequency,
       CASE
           WHEN PERCENT_RANK() OVER (ORDER BY customer_frequency ASC) <= 0.1 THEN 'Often'
           WHEN PERCENT_RANK() OVER (ORDER BY customer_frequency ASC) <= 0.35 THEN 'Occasionally'
           ELSE 'Rarely'
       END                                                                                          AS customer_frequency_segment,
       EXTRACT(EPOCH FROM (to_timestamp(get_analysis_date(), 'DD.MM.YYYY HH24:MI:SS') 
       - last_transaction_date)) / 60 / 60 / 24                                                     AS customer_inactive_period
FROM customer_average_check_frequency), 

customer_churn_analysis AS (
SELECT *, 
       customer_inactive_period / NULLIF(customer_frequency, 0)                                     AS customer_churn_rate,
       CASE
           WHEN customer_inactive_period / NULLIF(customer_frequency, 0) >= 0 
           AND customer_inactive_period / NULLIF(customer_frequency, 0) < 2 THEN 'Low'
           WHEN customer_inactive_period / NULLIF(customer_frequency, 0) >= 2 
           AND customer_inactive_period / NULLIF(customer_frequency, 0) < 5 THEN 'Medium'
           ELSE 'High'
       END                                                                                          AS customer_churn_segment
FROM customer_average_check_frequency_segmentation_inactive)

SELECT customer_churn_analysis.customer_id,
       customer_average_check, 
       customer_average_check_segment, 
       customer_frequency, 
       customer_frequency_segment, 
       customer_inactive_period, 
       customer_churn_rate, 
       customer_churn_segment,
       CASE customer_average_check_segment
           WHEN 'Low' THEN 0
           WHEN 'Medium' THEN 9
           ELSE 18
       END + 
       CASE customer_frequency_segment
           WHEN 'Rarely' THEN 0
           WHEN 'Occasionally' THEN 3
           ELSE 6
       END + 
       CASE customer_churn_segment
           WHEN 'Low' THEN 1
           WHEN 'Medium' THEN 2
           ELSE 3
       END                                                                                          AS Customer_Segment, 
       prime_store                                                                                  AS Customer_Primary_Store
FROM customer_churn_analysis
    JOIN get_primary_store() ON get_primary_store.customer_id = customer_churn_analysis.customer_id
ORDER BY customer_churn_analysis.customer_id;








CREATE OR REPLACE VIEW Purchase_History AS
SELECT cards.customer_id                                                   AS customer_id,
       transactions.transaction_id                                         AS transaction_id,
       transactions.transaction_datetime                                   AS transaction_datetime,
       product_grid.group_id                                               AS group_id,
       SUM(stores.sku_purchase_price * checks.sku_amount)                  AS group_cost,
       SUM(checks.sku_summ)                                                AS group_summ,
       SUM(checks.sku_summ_paid)                                           AS group_summ_paid
FROM transactions
    JOIN cards ON cards.customer_card_id = transactions.customer_card_id
    JOIN checks ON transactions.transaction_id = checks.transaction_id
    JOIN product_grid ON product_grid.sku_id = checks.sku_id
    JOIN stores ON product_grid.sku_id = stores.sku_id
        AND transactions.transaction_store_id = stores.transaction_store_id
GROUP BY cards.customer_id,
         transactions.transaction_id,
         transactions.transaction_datetime,
         product_grid.group_id
ORDER BY cards.customer_id;








CREATE OR REPLACE VIEW Periods AS
SELECT purchase_history.customer_id                                                             AS customer_id,
       purchase_history.group_id                                                                AS group_id,
       MIN(TO_TIMESTAMP(purchase_history.transaction_datetime, 'DD.MM.YYYY HH24:MI:SS'))        AS first_group_purchase_date,
       MAX(TO_TIMESTAMP(purchase_history.transaction_datetime, 'DD.MM.YYYY HH24:MI:SS'))        AS last_group_purchase_date,
       COUNT(DISTINCT purchase_history.transaction_id)                                          AS group_purchase,

       (EXTRACT(EPOCH FROM (MAX(TO_TIMESTAMP(purchase_history.transaction_datetime, 'DD.MM.YYYY HH24:MI:SS')) - 
       MIN(TO_TIMESTAMP(purchase_history.transaction_datetime, 'DD.MM.YYYY HH24:MI:SS')))) / 60 / 60 / 24 + 1) / 

       COUNT(DISTINCT purchase_history.transaction_id)                                          AS group_frequency,
       COALESCE(MIN(CASE WHEN sku_discount / sku_summ > 0 THEN sku_discount / sku_summ END), 0) AS group_min_discount
FROM purchase_history
    JOIN product_grid product_grid ON purchase_history.group_id = product_grid.group_id
    JOIN checks ON checks.transaction_id = purchase_history.transaction_id 
        AND product_grid.sku_id = checks.sku_id
GROUP BY purchase_history.customer_id,
         purchase_history.group_id
ORDER BY purchase_history.customer_id;








CREATE OR REPLACE FUNCTION count_transactions(cust_id INT, first_date TIMESTAMPTZ, last_date TIMESTAMPTZ) 
RETURNS INT AS $$
SELECT COUNT(DISTINCT transaction_id)
FROM purchase_history
WHERE customer_id = cust_id
    AND TO_TIMESTAMP(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS') BETWEEN first_date AND last_date;
$$ LANGUAGE SQL;



CREATE OR REPLACE FUNCTION calc_stability_index() 
RETURNS TABLE ("Customer_id" INT, "Group_id" INT, calc_stability_index NUMERIC) AS $$
BEGIN
RETURN QUERY WITH transaction_data AS (
SELECT customer_id,
       group_id,
       transaction_datetime,
       LAG(TO_TIMESTAMP(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS'))
       OVER (PARTITION BY customer_id, group_id ORDER BY 
       TO_TIMESTAMP(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS') ASC) AS prev_transaction_datetime
FROM purchase_history
ORDER BY customer_id, 
         group_id, 
         TO_TIMESTAMP(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS') ASC)
         
SELECT transaction_data.customer_id,
       transaction_data.group_id,
       AVG(ABS(EXTRACT(EPOCH FROM(TO_TIMESTAMP(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS') - 
       prev_transaction_datetime))::DECIMAL / 60 / 60 / 24 - group_frequency::DECIMAL) 
       / group_frequency ::DECIMAL) AS group_stability_index
FROM transaction_data
    JOIN periods ON transaction_data.customer_id = periods.customer_id 
        AND transaction_data.group_id = periods.group_id
GROUP BY transaction_data.customer_id, 
         transaction_data.group_id;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION calc_margin(analysis_method VARCHAR, analysis_param INT) 
RETURNS TABLE ("Customer_id" INT, "Group_id" INT, "calc_margin" NUMERIC) AS $$
BEGIN
IF analysis_method = 'transactions' THEN

RETURN QUERY WITH relevant_transactions AS (
SELECT customer_id, 
       group_id, 
       transaction_datetime, 
       group_summ_paid - group_cost AS margin
FROM purchase_history
ORDER BY customer_id, 
            group_id, 
            TO_TIMESTAMP(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS') DESC
)
SELECT customer_id, 
       group_id, 
       SUM(margin) AS calc_margin
FROM (
SELECT *, 
       ROW_NUMBER() OVER (PARTITION BY customer_id, group_id) AS num
FROM relevant_transactions
) AS sub
WHERE num <= analysis_param
GROUP BY customer_id, 
         group_id;

ELSIF analysis_method = 'period' THEN

RETURN QUERY WITH relevant_transactions AS (
SELECT customer_id, 
       group_id, 
       transaction_datetime, 
       group_summ_paid - group_cost AS margin
FROM purchase_history
WHERE TO_TIMESTAMP(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS') >= get_analysis_date()::TIMESTAMP - INTERVAL '1 day' * analysis_param
AND TO_TIMESTAMP(transaction_datetime, 'DD.MM.YYYY HH24:MI:SS') <= get_analysis_date()::TIMESTAMP
)
SELECT customer_id, 
       group_id, 
       SUM(margin) AS calc_margin
FROM relevant_transactions
GROUP BY customer_id, 
         group_id;

END IF;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION calc_group_average_discount_and_share() 
RETURNS TABLE ("Customer_id" INT, "Group_id" INT, calc_group_discount_share NUMERIC, calc_group_average_discount NUMERIC) AS $$
BEGIN
RETURN QUERY WITH discount_amount_count AS (
SELECT customer_id,
       purchase_history.group_id,
       COUNT(transact.transaction_id) AS amount,
       SUM(group_summ_paid) / SUM(Group_Summ) AS group_average_discount
FROM purchase_history
    JOIN product_grid ON purchase_history.group_id = product_grid.group_id
    JOIN checks transact ON purchase_history.transaction_id = transact.transaction_id 
        AND product_grid.sku_id = transact.sku_id
WHERE sku_discount > 0
GROUP BY customer_id, 
         purchase_history.group_id
ORDER BY customer_id, 
         group_id)
SELECT periods.customer_id, 
       periods.group_id, 
       amount / group_purchase::NUMERIC AS group_discount_share, 
       group_average_discount
FROM periods
    LEFT JOIN discount_amount_count ON discount_amount_count.group_id = periods.group_id 
        AND discount_amount_count.customer_id = periods.customer_id;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION calc_group_min_discount() 
RETURNS TABLE ("Customer_id" INT, "Group_id" INT, calc_group_minimum_discount NUMERIC) AS $$
BEGIN
RETURN QUERY
SELECT customer_id,
       group_id,
       NULLIF(MIN(NULLIF(group_min_discount, 0)), 0) AS min_discount 
FROM periods
GROUP BY customer_id, 
         group_id;
END;
$$ LANGUAGE PLPGSQL;



CREATE OR REPLACE VIEW Groups AS
SELECT customer_id                                                                                  AS customer_id,
       group_id                                                                                     AS group_id,

       group_purchase::NUMERIC / count_transactions(customer_id, 
       first_group_purchase_date, last_group_purchase_date)                                         AS group_affinity_index,

       EXTRACT(EPOCH FROM (TO_TIMESTAMP(get_analysis_date(), 'DD.MM.YYYY HH24:MI:SS') - 
       last_group_purchase_date)) / 60 / 60 / 24 / periods.group_frequency                          AS group_churn_rate,

       calc_stability_index                                                                         AS group_stability_index,
       calc_margin                                                                                  AS group_margin,
       calc_group_discount_share                                                                    AS group_discount_share,
       calc_group_minimum_discount                                                                  AS group_minimum_discount,
       calc_group_average_discount                                                                  AS group_average_discount
FROM periods
    JOIN calc_stability_index() stability_index ON customer_id = stability_index."Customer_id"
        AND group_id = stability_index."Group_id"
    JOIN calc_margin('transactions', 60000) group_margin ON customer_id = group_margin."Customer_id"
        AND group_id = group_margin."Group_id"
    JOIN calc_group_average_discount_and_share() discount_share ON customer_id = discount_share."Customer_id"
        AND group_id = discount_share."Group_id"
    JOIN calc_group_min_discount() min_discount ON customer_id = min_discount."Customer_id"
        AND group_id = min_discount."Group_id";








SELECT * FROM Customers;

SELECT * FROM Purchase_History;

SELECT * FROM Periods;

SELECT * FROM Groups;