{#-- The columns of each landed TPC-DS table, in dsdgen's own order, plus the two additions the
     white paper's section 4.5 makes: `cache_buster` on both fact tables and `d_date_sk_1` on
     date_dim. One list per table, used by all three dialect trees so the four engines store the
     same columns in the same order.

     THE LISTS ARE THE LANDED SCHEMA, NOT dsdgen's. The customisation happens at LAND time -- see
     download_tpcds.py -- so the parquet under parquet_raw/<table>/ already carries these columns and
     the models are pass-throughs. That is the same division of labour every other dataset here uses:
     landing is the irreversible half, so the downloader normalises and the models do not edit values.

     Sibling of macros/{cms_payment,bts_flight,nyc_trip,green_trip}_columns.sql. Unlike those there is
     no `_value` / `_type` helper: dsdgen writes one canonical schema at every scale factor, there is
     no drift to normalise around and no source pathology to guard, so the models CAST nothing.

     `.github/scripts/test_tpcds_columns.py` pins these lists against download_tpcds.py's COLUMNS
     dict and against the `mart_columns` entry in .github/scripts/datasets.py. Regenerate rather than
     retype: `INSTALL tpcds; LOAD tpcds; CALL dsdgen(sf=0); DESCRIBE <table>`. --#}
{% macro tpcds_columns(table) %}
  {%- set cols = {
    'store_sales': [
      'ss_sold_date_sk', 'ss_sold_time_sk', 'ss_item_sk', 'ss_customer_sk', 'ss_cdemo_sk',
      'ss_hdemo_sk', 'ss_addr_sk', 'ss_store_sk', 'ss_promo_sk', 'ss_ticket_number',
      'ss_quantity', 'ss_wholesale_cost', 'ss_list_price', 'ss_sales_price',
      'ss_ext_discount_amt', 'ss_ext_sales_price', 'ss_ext_wholesale_cost',
      'ss_ext_list_price', 'ss_ext_tax', 'ss_coupon_amt', 'ss_net_paid', 'ss_net_paid_inc_tax',
      'ss_net_profit', 'cache_buster'
    ],
    'catalog_sales': [
      'cs_sold_date_sk', 'cs_sold_time_sk', 'cs_ship_date_sk', 'cs_bill_customer_sk',
      'cs_bill_cdemo_sk', 'cs_bill_hdemo_sk', 'cs_bill_addr_sk', 'cs_ship_customer_sk',
      'cs_ship_cdemo_sk', 'cs_ship_hdemo_sk', 'cs_ship_addr_sk', 'cs_call_center_sk',
      'cs_catalog_page_sk', 'cs_ship_mode_sk', 'cs_warehouse_sk', 'cs_item_sk', 'cs_promo_sk',
      'cs_order_number', 'cs_quantity', 'cs_wholesale_cost', 'cs_list_price', 'cs_sales_price',
      'cs_ext_discount_amt', 'cs_ext_sales_price', 'cs_ext_wholesale_cost',
      'cs_ext_list_price', 'cs_ext_tax', 'cs_coupon_amt', 'cs_ext_ship_cost', 'cs_net_paid',
      'cs_net_paid_inc_tax', 'cs_net_paid_inc_ship', 'cs_net_paid_inc_ship_tax',
      'cs_net_profit', 'cache_buster'
    ],
    'date_dim': [
      'd_date_sk', 'd_date_id', 'd_date', 'd_month_seq', 'd_week_seq', 'd_quarter_seq',
      'd_year', 'd_dow', 'd_moy', 'd_dom', 'd_qoy', 'd_fy_year', 'd_fy_quarter_seq',
      'd_fy_week_seq', 'd_day_name', 'd_quarter_name', 'd_holiday', 'd_weekend',
      'd_following_holiday', 'd_first_dom', 'd_last_dom', 'd_same_day_ly', 'd_same_day_lq',
      'd_current_day', 'd_current_week', 'd_current_month', 'd_current_quarter',
      'd_current_year', 'd_date_sk_1'
    ],
    'item': [
      'i_item_sk', 'i_item_id', 'i_rec_start_date', 'i_rec_end_date', 'i_item_desc',
      'i_current_price', 'i_wholesale_cost', 'i_brand_id', 'i_brand', 'i_class_id', 'i_class',
      'i_category_id', 'i_category', 'i_manufact_id', 'i_manufact', 'i_size', 'i_formulation',
      'i_color', 'i_units', 'i_container', 'i_manager_id', 'i_product_name'
    ],
    'store': [
      's_store_sk', 's_store_id', 's_rec_start_date', 's_rec_end_date', 's_closed_date_sk',
      's_store_name', 's_number_employees', 's_floor_space', 's_hours', 's_manager',
      's_market_id', 's_geography_class', 's_market_desc', 's_market_manager', 's_division_id',
      's_division_name', 's_company_id', 's_company_name', 's_street_number', 's_street_name',
      's_street_type', 's_suite_number', 's_city', 's_county', 's_state', 's_zip', 's_country',
      's_gmt_offset', 's_tax_percentage'
    ],
    'promotion': [
      'p_promo_sk', 'p_promo_id', 'p_start_date_sk', 'p_end_date_sk', 'p_item_sk', 'p_cost',
      'p_response_target', 'p_promo_name', 'p_channel_dmail', 'p_channel_email',
      'p_channel_catalog', 'p_channel_tv', 'p_channel_radio', 'p_channel_press',
      'p_channel_event', 'p_channel_demo', 'p_channel_details', 'p_purpose',
      'p_discount_active'
    ],
    'ship_mode': [
      'sm_ship_mode_sk', 'sm_ship_mode_id', 'sm_type', 'sm_code', 'sm_carrier', 'sm_contract'
    ],
    'catalog_page': [
      'cp_catalog_page_sk', 'cp_catalog_page_id', 'cp_start_date_sk', 'cp_end_date_sk',
      'cp_department', 'cp_catalog_number', 'cp_catalog_page_number', 'cp_description',
      'cp_type'
    ],
    'customer_address': [
      'ca_address_sk', 'ca_address_id', 'ca_street_number', 'ca_street_name', 'ca_street_type',
      'ca_suite_number', 'ca_city', 'ca_county', 'ca_state', 'ca_zip', 'ca_country',
      'ca_gmt_offset', 'ca_location_type'
    ],
    'customer_demographics': [
      'cd_demo_sk', 'cd_gender', 'cd_marital_status', 'cd_education_status',
      'cd_purchase_estimate', 'cd_credit_rating', 'cd_dep_count', 'cd_dep_employed_count',
      'cd_dep_college_count'
    ]
  } -%}
  {%- if table not in cols -%}
    {{ exceptions.raise_compiler_error("tpcds_columns(): unknown table " ~ table ~ "; known: " ~ cols.keys() | list | join(", ")) }}
  {%- endif -%}
  {{ return(cols[table]) }}
{% endmacro %}
