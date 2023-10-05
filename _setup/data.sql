-- create resources
create warehouse transforming;
create database raw;
create database analytics;
create schema raw.jaffle_shop;
create schema raw.stripe;


-- create tables
create table raw.jaffle_shop.customers ( 
    id integer,
    first_name varchar,
    last_name varchar
);


create table raw.jaffle_shop.orders ( 
    id integer,
    user_id integer,
    order_date date,
    status varchar,
    _etl_loaded_at timestamp default current_timestamp
);


create table raw.stripe.payment ( 
    id integer,
    orderid integer,
    paymentmethod varchar,
    status varchar,
    amount integer,
    created date,
    _batched_at timestamp default current_timestamp
);


-- load data
copy into raw.jaffle_shop.customers (id, first_name, last_name)
from 's3://dbt-tutorial-public/jaffle_shop_customers.csv'
file_format = (
    type = 'CSV'
    field_delimiter = ','
    skip_header = 1
    ); 


copy into raw.jaffle_shop.orders (id, user_id, order_date, status)
from 's3://dbt-tutorial-public/jaffle_shop_orders.csv'
file_format = (
    type = 'CSV'
    field_delimiter = ','
    skip_header = 1
    );


copy into raw.stripe.payment (id, orderid, paymentmethod, status, amount, created)
from 's3://dbt-tutorial-public/stripe_payments.csv'
file_format = (
    type = 'CSV'
    field_delimiter = ','
    skip_header = 1
    );

----------------------------------------------------------------------------------------------------

-- snapshot demo
create or replace transient table analytics.dbt_nreyes.mock_orders (
    order_id integer,
    status varchar (100),
    created_at date,
    updated_at date
);


insert into analytics.dbt_nreyes.mock_orders (order_id, status, created_at, updated_at)
values
    (1, 'Delivered', '2020-01-01', '2020-01-04'),
    (2, 'Shipped', '2020-01-02', '2020-01-04'),
    (3, 'Shipped', '2020-01-03', '2020-01-04'),
    (4, 'Processed', '2020-01-04', '2020-01-04');
commit;


select * from analytics.dbt_nreyes.mock_orders;


-- recreate demo table and insert new records
create or replace transient table analytics.dbt_nreyes.mock_orders (
    order_id integer,
    status varchar (100),
    created_at date,
    updated_at date
);


insert into analytics.dbt_nreyes.mock_orders (order_id, status, created_at, updated_at)
values 
    (1, 'delivered', '2020-01-01', '2020-01-05'),
    (2, 'delivered', '2020-01-02', '2020-01-05'),
    (3, 'delivered', '2020-01-03', '2020-01-05'),
    (4, 'delivered', '2020-01-04', '2020-01-05');
commit;


select * from analytics.dbt_nreyes.mock_orders;


-- snapshot demo prod env
create or replace transient table analytics.dbt_prod.mock_orders (
    order_id integer,
    status varchar (100),
    created_at date,
    updated_at date
);


insert into analytics.dbt_prod.mock_orders (order_id, status, created_at, updated_at)
values
    (1, 'Delivered', '2020-01-01', '2020-01-04'),
    (2, 'Shipped', '2020-01-02', '2020-01-04'),
    (3, 'Shipped', '2020-01-03', '2020-01-04'),
    (4, 'Processed', '2020-01-04', '2020-01-04');
commit;


select * from analytics.dbt_prod.mock_orders;


-- recreate demo table and insert new records
create or replace transient table analytics.dbt_prod.mock_orders (
    order_id integer,
    status varchar (100),
    created_at date,
    updated_at date
);


insert into analytics.dbt_prod.mock_orders (order_id, status, created_at, updated_at)
values 
    (1, 'delivered', '2020-01-01', '2020-01-05'),
    (2, 'delivered', '2020-01-02', '2020-01-05'),
    (3, 'delivered', '2020-01-03', '2020-01-05'),
    (4, 'delivered', '2020-01-04', '2020-01-05');
commit;


select * from analytics.dbt_prod.mock_orders;


----------------------------------------------------------------------------------------------------
-- stale models
select
    last_altered,
    table_type,
    table_schema,
    table_name,
    case when table_type = upper('view') then table_type else upper('table') end as drop_type,
    upper('drop ') || drop_type || ' {{ database | upper }}.' || table_schema || '.' || table_name || ';' as drop_statement
from {{ database }}.information_schema.tables 
where table_schema = upper('{{ schema }}')
order by last_altered desc; 