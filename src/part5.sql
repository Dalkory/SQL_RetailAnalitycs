DROP FUNCTION IF EXISTS calculate_growth_offer;


CREATE OR REPLACE FUNCTION calculate_growth_offer(
                                                  start_date DATE,
                                                  end_date DATE,
                                                  additional_transactions INT,
                                                  max_churn_rate NUMERIC,
                                                  max_discount_share NUMERIC,
                                                  max_margin_share NUMERIC
)
    RETURNS TABLE
            (
               "Customer_ID" INT,
               "Start_Date" timestamp,
               "End_Date" timestamp,
               "Required_Transactions_Count" NUMERIC,
               "Group_Name" VARCHAR,
               "Offer_Discount_Depth" NUMERIC
            )
AS
$$
BEGIN
    RETURN QUERY
        WITH
             eligible_groups
                 AS (SELECT groups.customer_id,
                            groups.group_id,
                            ceil(groups.group_minimum_discount / 0.05) * 5   AS Offer_Discount_Depth,
                            row_number()
                            OVER (PARTITION BY groups.customer_id, groups.group_id ORDER BY groups.group_affinity_index DESC) as row_num
                     FROM groups
                              LEFT JOIN purchase_history
                                        ON groups.customer_id = purchase_history.customer_id AND groups.group_id = purchase_history.group_id
                     WHERE groups.group_churn_rate <= max_churn_rate
                       AND groups.group_discount_share * 100 <= max_discount_share
                     GROUP BY groups.customer_id, groups.group_id, ceil(groups.group_minimum_discount / 0.05) * 5,
                              groups.group_affinity_index
                     HAVING ceil(groups.group_minimum_discount / 0.05) * 5 <= max_margin_share *
                            AVG((purchase_history.group_summ_paid - purchase_history.group_cost) / purchase_history.group_summ_paid)
                     ORDER BY groups.customer_id, groups.group_affinity_index DESC, row_num DESC),

             Required_Transactions
                 AS (SELECT customers.Customer_ID,
                            start_date::timestamp,
                            end_date::timestamp,
                            CAST(ROUND((end_date - start_date) / customers.customer_frequency + additional_transactions) AS NUMERIC) AS Required_Transactions_Count
                     FROM Customers)

        SELECT Required_Transactions.customer_id,
               Required_Transactions.start_date AS Start_Date,
               Required_Transactions.end_date AS End_Date,
               Required_Transactions.Required_Transactions_Count,
               sku_group.group_name,
               eligible_groups.Offer_Discount_Depth
        FROM Required_Transactions
                 LEFT JOIN eligible_groups  ON Required_Transactions.customer_id = eligible_groups.customer_id
                 LEFT JOIN sku_group ON eligible_groups.group_id = sku_group.group_id
        WHERE eligible_groups.row_num = 1;
END;
$$ LANGUAGE plpgsql;


SELECT * FROM calculate_growth_offer('2022-08-18 00:00:00', '2022-08-18 00:00:00', 1, 3, 70, 30);