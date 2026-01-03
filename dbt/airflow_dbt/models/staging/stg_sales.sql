with source as (

    select
        order_id,
        customer,
        amount,
        amount_with_tax,
        order_date
    from airflow_schema.sales

),

cleaned as (

    select
        order_id,
        trim(customer) as customer,
        amount,
        amount_with_tax,
        order_date
    from source
    where order_id is not null

)

select *
from cleaned