create or replace function form_cross_selling_offers(
    num_of_groups int,
    max_churn_index numeric,
    max_cons_stability_index numeric,
    max_sku_share numeric,
    allowable_margin_share numeric
)
returns table (
    customer_id int,
    sku_name varchar,
    offer_discount_depth numeric
) as
$$
begin
return query (
    with
    group_selection as (
    --1.
        select * from (
            select *, row_number() over (partition by group_id
                                         order by group_affinity_index desc)
                                         as rank from groups
            where group_churn_rate <= max_churn_index
              and group_stability_index < max_cons_stability_index
            order by group_affinity_index desc
        ) ranked_groups where rank = 1
        limit num_of_groups
    ),
    sku_with_max_margin as (
    --2.
        select store_id, group_id, sku_id, max_margin, margin_share from (
            select transaction_store_id as store_id, group_id, stores.sku_id,
                sku_retail_price - sku_purchase_price as max_margin,
                (sku_retail_price - sku_purchase_price) * allowable_margin_share / 100 as margin_share,--4.
                row_number() over(partition by group_id
                                  order by sku_retail_price - sku_purchase_price--order by max_margin
                             desc) as row from stores
            join product_grid on stores.sku_id = product_grid.sku_id
        ) ranked_sku_with_max_margin where row = 1
    ),
    sku_group_share as (
    --3.
        select * from (
        select  sku_transactions.customer_id,
                sku_transactions.sku_id,
                sku_transactions.group_id,
                product_grid.sku_name,
                1.0 * count(distinct sku_transactions.transaction_id)
                    / count(distinct group_transactions.transaction_id) as sku_group_share
        from (select
                purchase_history.customer_id,
                purchase_history.transaction_id,
                purchase_history.group_id,
                checks.sku_id from purchase_history
                join checks on checks.transaction_id = purchase_history.transaction_id
             ) as sku_transactions--transactions containing this sku
        left join checks on checks.transaction_id = sku_transactions.transaction_id
        join product_grid on product_grid.sku_id = checks.sku_id and sku_transactions.group_id = product_grid.group_id
        left join (select distinct group_id, transaction_id from purchase_history)
            as group_transactions--transactions containing the group as a whole
            on sku_transactions.group_id = group_transactions.group_id
        group by sku_transactions.customer_id, sku_transactions.sku_id, sku_transactions.group_id, product_grid.sku_name
        ) as subquery where sku_group_share <= max_sku_share / 100
    )
    --5.
    select group_selection.customer_id, sku_group_share.sku_name,
            round(periods.group_min_discount / 5, 2) * 5 * 100
                                       as offer_discount_depth
        from group_selection
    join customers on customers.customer_id = group_selection.customer_id
    join sku_with_max_margin
        on customers.customer_primary_store = sku_with_max_margin.store_id
        and group_selection.group_id = sku_with_max_margin.group_id
    join sku_group_share
        on customers.customer_id = sku_group_share.customer_id
        and sku_with_max_margin.group_id = sku_group_share.group_id
        and sku_with_max_margin.sku_id = sku_group_share.sku_id
    left join periods
        on periods.customer_id = customers.customer_id
        and periods.group_id = sku_with_max_margin.group_id
    where margin_share >= periods.group_min_discount--not necessary to round?
);
end;
$$ language plpgsql;

select * from form_cross_selling_offers(5,3,0.5,100,30);
