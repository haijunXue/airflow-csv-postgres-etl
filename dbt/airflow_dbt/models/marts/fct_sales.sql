select
    order_id,
    customer,
    order_date,
    amount,
    amount_with_tax,
    amount_with_tax - amount as tax_amount
from {{ ref('stg_sales') }}