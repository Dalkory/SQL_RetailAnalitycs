--1.1. calculation method by period
drop function if exists form_check_growth_offers(int, timestamp, timestamp, numeric, numeric, numeric, numeric) cascade;
create function form_check_growth_offers(
-- create or replace function form_check_growth_offers(--for debugging
    calculation_method int,--1: per period, 2: per quantity
    first_date timestamp,--for 1 method
    last_date timestamp,--for 1 method
    coefficient numeric,--of average check increase
    max_churn_index numeric,
    max_discount_share numeric,--maximum share of transactions with a discount
    allowable_margin_share numeric
) returns table (
    customer_id int,
    required_check_measure numeric,
    group_name varchar,
    offer_discount_depth numeric
) as
$$
begin
    if calculation_method = 2 then
        raise exception 'calculation_method % requires different parameters', calculation_method;
    elsif calculation_method != 1 and calculation_method != 2 then
        raise exception 'invalid calculation_method: %. choose 1 or 2', calculation_method;
    elsif last_date <= first_date then--also a condition is needed?
        raise exception 'the last date must be later than the first date';
    end if;

    return query (
        with
        --3. determination of the target value of the average check
            required_average_check as (
                select sorted_transactions.customer_id,
                       sum(transaction_summ) as customer_average_check,--2. determination of the average check
                       sum(transaction_summ) / count(transaction_id) * coefficient
                          as required_check_measure--3. determination of the target value of the average check
                from ( select transaction_id, cards.customer_id, transaction_summ from transactions
                       join cards on transactions.customer_card_id = cards.customer_card_id
                       where to_timestamp(transaction_datetime, 'dd.mm.yyyy hh24:mi:ss') between first_date and last_date
                     ) as sorted_transactions group by sorted_transactions.customer_id
            ),
        --4. determination of the group to form the reward
            eligible_groups as (
                select groups.customer_id,
                       groups.group_id,
                       sku_group.group_name,
                       ceil(groups.group_minimum_discount / 0.05) * 5   as offer_discount_depth,
                       groups.group_minimum_discount,
                       groups.group_margin,
                       row_number() over (partition by groups.customer_id, groups.group_id
                                              order by groups.group_affinity_index desc) as row_num
                from groups
                left join purchase_history
                  on groups.customer_id = purchase_history.customer_id
                 and groups.group_id = purchase_history.group_id
                join sku_group on groups.group_id = sku_group.group_id
                where groups.group_churn_rate <= max_churn_index
                  and groups.group_discount_share * 100 <= max_discount_share
                group by groups.customer_id,
                         groups.group_id,
                         sku_group.group_name,
                         ceil(groups.group_minimum_discount / 0.05) * 5,
                         groups.group_minimum_discount,
                         groups.group_margin,
                         groups.group_affinity_index--?
                having ceil(groups.group_minimum_discount / 0.05) * 5 <= allowable_margin_share
                     * avg((purchase_history.group_summ_paid - purchase_history.group_cost) / purchase_history.group_summ_paid)
                order by groups.customer_id, groups.group_affinity_index desc, row_num desc
            ),
        --5. determination of the maximum allowable size of a discount for the reward
            max_margin_table as (
                select purchase_history.customer_id,
                       purchase_history.group_id,
                       allowable_margin_share
                           / 100.0 * sum(group_summ - group_cost) / sum(group_summ) as discount
                from purchase_history
                group by purchase_history.customer_id,
                         purchase_history.group_id
            ),
        --6. determination of the discount size
            result_discount_table as (
                select eligible_groups.customer_id,
                       eligible_groups.group_id,
                       eligible_groups.group_name,
                       eligible_groups.row_num,
                       round(max_margin_table.discount / 0.05) * 5 as discount,
                       round(eligible_groups.group_minimum_discount / 0.05) * 5 as group_min_discount,
                       min(eligible_groups.row_num) over (partition by eligible_groups.customer_id) as min_row_num
                from eligible_groups
                join max_margin_table on eligible_groups.customer_id = max_margin_table.customer_id
                                     and eligible_groups.group_id = max_margin_table.group_id
                where max_margin_table.discount > eligible_groups.group_minimum_discount
            )

        select result_discount_table.customer_id,
               required_average_check.required_check_measure,
               result_discount_table.group_name,
               group_min_discount as offer_discount_depth
        from result_discount_table
        join customers on customers.customer_id = result_discount_table.customer_id
        join required_average_check on required_average_check.customer_id = result_discount_table.customer_id
        where result_discount_table.group_min_discount > 0
          and result_discount_table.row_num = result_discount_table.min_row_num
    );
end;
$$
language plpgsql;

select * from form_check_growth_offers(
    1,
    '2020-12-21 00:00:00',
    '2022-12-21 00:00:00',
    1.15,
    3,
    70,
    30
);
--error handling
/*
select * from form_check_growth_offers(
    2,
    '2020-12-21 00:00:00',--'21.12.2020 00:00:00'
    '2022-12-21 00:00:00',--'21.12.2022 00:00:00'
    1.15,
    3,
    70,
    30
);
select * from form_check_growth_offers(
    1,
    '2022-12-21 00:00:00',--'21.12.2022 00:00:00'
    '2020-12-21 00:00:00',--'21.12.2020 00:00:00'
    1.15,
    3,
    70,
    30
);
select * from form_check_growth_offers(1,100,1.15,3,70,30);
*/

--1.2. calculation method by the number of recent transactions
drop function if exists form_check_growth_offers(int, int, numeric, numeric, numeric, numeric) cascade;
create function form_check_growth_offers(
-- create or replace function form_check_growth_offers(--for debugging
    calculation_method int,--1: per period, 2: per quantity
    transactions_count int,--for 2 method
    coefficient numeric,--coefficient of average check increase
    max_churn_index numeric,
    max_discount_share numeric,--maximum share of transactions with a discount
    allowable_margin_share numeric
) returns table (
    customer_id int,
    required_check_measure numeric,
    group_name varchar,
    offer_discount_depth numeric
) as
$$
begin
    if calculation_method = 1 then
        raise exception 'calculation_method % requires different parameters', calculation_method;
    elsif calculation_method != 1 and calculation_method != 2 then
        raise exception 'invalid calculation_method: %. choose 1 or 2', calculation_method;
    end if;

    return query (
        with
            sorted_transactions as (
                select transaction_id,
                       transaction_summ,
                       cards.customer_id,
                       row_number() over (partition by cards.customer_id
                                              order by transaction_datetime desc) as transaction_rank
                from transactions
                left join cards on transactions.customer_card_id = cards.customer_card_id
            ),
            --3. determination of the target value of the average check
            required_average_check as (
                select sorted_transactions.customer_id,
                       sum(transaction_summ) as customer_average_check,--2. determination of the average check
                       sum(transaction_summ) / count(transaction_id) * coefficient
                          as required_check_measure--3. determination of the target value of the average check
                from sorted_transactions
                where transaction_rank <= transactions_count
                group by sorted_transactions.customer_id
            ),
            --4. determination of the group to form the reward
            eligible_groups as (
                select groups.customer_id,
                       groups.group_id,
                       sku_group.group_name,
                       ceil(groups.group_minimum_discount / 0.05) * 5   as offer_discount_depth,
                       groups.group_minimum_discount,
                       groups.group_margin,
                       row_number() over (partition by groups.customer_id, groups.group_id
                                              order by groups.group_affinity_index desc) as row_num
                from groups
                left join purchase_history
                       on groups.customer_id = purchase_history.customer_id
                      and groups.group_id = purchase_history.group_id
                join sku_group on groups.group_id = sku_group.group_id
                where groups.group_churn_rate <= max_churn_index
                  and groups.group_discount_share * 100 <= max_discount_share
                group by groups.customer_id,
                         groups.group_id,
                         sku_group.group_name,
                         ceil(groups.group_minimum_discount / 0.05) * 5,
                         groups.group_minimum_discount,
                         groups.group_margin,
                         groups.group_affinity_index
                having ceil(groups.group_minimum_discount / 0.05) * 5 <= allowable_margin_share
                     * avg((purchase_history.group_summ_paid - purchase_history.group_cost) / purchase_history.group_summ_paid)
                order by groups.customer_id, groups.group_affinity_index desc, row_num desc
            ),
            --5. determination of the maximum allowable size of a discount for the reward
            max_margin_table as (
                select purchase_history.customer_id,
                       purchase_history.group_id,
                       allowable_margin_share
                           / 100.0 * sum(group_summ - group_cost) / sum(group_summ) as discount
                from purchase_history
                group by purchase_history.customer_id,
                         purchase_history.group_id
            ),
            --6. determination of the discount size
            result_discount_table as (
                select eligible_groups.customer_id,
                       eligible_groups.group_id,
                       eligible_groups.group_name,
                       eligible_groups.row_num,
                       round(max_margin_table.discount / 0.05) * 5 as discount,
                       round(eligible_groups.group_minimum_discount / 0.05) * 5 as group_min_discount,
                       min(eligible_groups.row_num) over (partition by eligible_groups.customer_id) as min_row_num
                from eligible_groups
                join max_margin_table on eligible_groups.customer_id = max_margin_table.customer_id
                                     and eligible_groups.group_id = max_margin_table.group_id
                where max_margin_table.discount > eligible_groups.group_minimum_discount
            )

        select result_discount_table.customer_id,
               required_average_check.required_check_measure,
               result_discount_table.group_name,
               group_min_discount as offer_discount_depth
        from result_discount_table
        join customers on customers.customer_id = result_discount_table.customer_id
        join required_average_check
          on required_average_check.customer_id = result_discount_table.customer_id
        where result_discount_table.group_min_discount > 0
          and result_discount_table.row_num = result_discount_table.min_row_num
    );
end;
$$
language plpgsql;

select * from form_check_growth_offers(2,100,1.15,3,70,30);
--error handling
/*
select * from form_check_growth_offers(1,100,1.15,3,70,30);
select * from form_check_growth_offers(3,100,1.15,3,70,30);
select * from form_check_growth_offers(
    2,
    '2022-12-21 00:00:00',
    '2020-12-21 00:00:00',
    1.15,
    3,
    70,
    30
);
*/