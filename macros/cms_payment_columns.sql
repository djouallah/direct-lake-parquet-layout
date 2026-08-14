{#-- The columns fct_cms_payments reads from the landed CMS Open Payments parquet, in file order.

     ONE source of truth for all three dialects — the DuckDB read, the T-SQL SELECT and the Spark
     read all generate their column list from this, exactly as green_trip_columns() does.

     WHY ALL NINETY-ONE, where every other dataset takes a subset. The other four exist to vary
     SKEW at a roughly constant width (5, 17, 20, 22 columns). This one exists to vary the WIDTH,
     and to add a property none of them has: SPARSITY. Measured on a 100 MB / 187,750-row sample of
     PY2023, 54 of the 91 columns are more than half NULL, because CMS models a one-to-many product
     list as five repeated six-column groups plus a six-wide recipient-type/specialty group:

       group member    _1      _2      _3      _4      _5
       % NULL          ~7%    ~83%    ~95%    ~98%    ~99%

     and Covered_Recipient_Primary_Type_2..6 / Covered_Recipient_Specialty_2..6 are 100% NULL in the
     sample. Taking only the `_1` members — the obvious tidy-up — would delete exactly the surface
     this dataset was added to measure, so `test_cms_columns.py` asserts they are all still here.

     It carries BOTH skew regimes at once, which is what makes it a new point rather than a wider
     bts. From the same sample: Nature_of_Payment 92% one value, Form_of_Payment 86%,
     Physician_Ownership 87%, Related_Product 95%, Delay_in_Publication and Dispute_Status 100% —
     nyc's regime — beside Covered_Recipient_Specialty_1 at 302 values / 9.5% top and the
     manufacturer id at ~1,000 / 10.5% top, which is bts's competing regime.

     THE GUARD IS AT LAND TIME. download_cms_payments.py reads each year's CSV header and REFUSES
     to archive a year that does not carry all of these, so everything under parquet_raw/cms is
     readable by one statement per dialect and a schema surprise fails at download — free, on a
     runner — instead of mid-write with Fabric capacity already spent. That script holds the same
     list as CORE_COLUMNS and `.github/scripts/test_cms_columns.py` asserts the two never drift.

     DO NOT REBUILD THIS LIST FROM CMS's DATA DICTIONARY. The dictionary spells #78
     `Covered_or_Nonccovered_Indicator_4`, with a double c. The FILE spells it correctly, in every
     published year. The dictionary's spelling matches nothing.

     Not here on purpose: `file`. It is derived per dialect from the source path (parse_filename),
     not read from the parquet. --#}
{%- macro cms_payment_columns() -%}
  {{- return([
    'Change_Type',
    'Covered_Recipient_Type',
    'Teaching_Hospital_CCN',
    'Teaching_Hospital_ID',
    'Teaching_Hospital_Name',
    'Covered_Recipient_Profile_ID',
    'Covered_Recipient_NPI',
    'Covered_Recipient_First_Name',
    'Covered_Recipient_Middle_Name',
    'Covered_Recipient_Last_Name',
    'Covered_Recipient_Name_Suffix',
    'Recipient_Primary_Business_Street_Address_Line1',
    'Recipient_Primary_Business_Street_Address_Line2',
    'Recipient_City',
    'Recipient_State',
    'Recipient_Zip_Code',
    'Recipient_Country',
    'Recipient_Province',
    'Recipient_Postal_Code',
    'Covered_Recipient_Primary_Type_1',
    'Covered_Recipient_Primary_Type_2',
    'Covered_Recipient_Primary_Type_3',
    'Covered_Recipient_Primary_Type_4',
    'Covered_Recipient_Primary_Type_5',
    'Covered_Recipient_Primary_Type_6',
    'Covered_Recipient_Specialty_1',
    'Covered_Recipient_Specialty_2',
    'Covered_Recipient_Specialty_3',
    'Covered_Recipient_Specialty_4',
    'Covered_Recipient_Specialty_5',
    'Covered_Recipient_Specialty_6',
    'Covered_Recipient_License_State_code1',
    'Covered_Recipient_License_State_code2',
    'Covered_Recipient_License_State_code3',
    'Covered_Recipient_License_State_code4',
    'Covered_Recipient_License_State_code5',
    'Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name',
    'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_ID',
    'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name',
    'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State',
    'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Country',
    'Total_Amount_of_Payment_USDollars',
    'Date_of_Payment',
    'Number_of_Payments_Included_in_Total_Amount',
    'Form_of_Payment_or_Transfer_of_Value',
    'Nature_of_Payment_or_Transfer_of_Value',
    'City_of_Travel',
    'State_of_Travel',
    'Country_of_Travel',
    'Physician_Ownership_Indicator',
    'Third_Party_Payment_Recipient_Indicator',
    'Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value',
    'Charity_Indicator',
    'Third_Party_Equals_Covered_Recipient_Indicator',
    'Contextual_Information',
    'Delay_in_Publication_Indicator',
    'Record_ID',
    'Dispute_Status_for_Publication',
    'Related_Product_Indicator',
    'Covered_or_Noncovered_Indicator_1',
    'Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_1',
    'Product_Category_or_Therapeutic_Area_1',
    'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_1',
    'Associated_Drug_or_Biological_NDC_1',
    'Associated_Device_or_Medical_Supply_PDI_1',
    'Covered_or_Noncovered_Indicator_2',
    'Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_2',
    'Product_Category_or_Therapeutic_Area_2',
    'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_2',
    'Associated_Drug_or_Biological_NDC_2',
    'Associated_Device_or_Medical_Supply_PDI_2',
    'Covered_or_Noncovered_Indicator_3',
    'Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_3',
    'Product_Category_or_Therapeutic_Area_3',
    'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_3',
    'Associated_Drug_or_Biological_NDC_3',
    'Associated_Device_or_Medical_Supply_PDI_3',
    'Covered_or_Noncovered_Indicator_4',
    'Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_4',
    'Product_Category_or_Therapeutic_Area_4',
    'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_4',
    'Associated_Drug_or_Biological_NDC_4',
    'Associated_Device_or_Medical_Supply_PDI_4',
    'Covered_or_Noncovered_Indicator_5',
    'Indicate_Drug_or_Biological_or_Device_or_Medical_Supply_5',
    'Product_Category_or_Therapeutic_Area_5',
    'Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_5',
    'Associated_Drug_or_Biological_NDC_5',
    'Associated_Device_or_Medical_Supply_PDI_5',
    'Program_Year',
    'Payment_Publication_Date'
  ]) -}}
{%- endmacro -%}

{#-- The five columns that are NOT strings, kept as one list so the type macro and any future
     reader agree on which they are. Everything else is a string, which is what a source whose
     columns are 60% categorical looks like.

     BIGINT rather than INT on the three ids: an NPI is ten digits and overflows INT32.

     Record_ID is deliberately NOT here. Every observed value is ten numeric digits and a BIGINT
     merge key would be smaller and faster, but CMS documents it as a string and reinterpreting a
     documented identifier as a number is how leading zeros disappear — worse, a TRY_CAST that
     failed would produce a NULL merge key. See download_cms_payments.py's CANONICAL. --#}
{%- macro cms_payment_non_strings() -%}
  {{- return({
    'Date_of_Payment': 'date',
    'Payment_Publication_Date': 'date',
    'Total_Amount_of_Payment_USDollars': 'double',
    'Number_of_Payments_Included_in_Total_Amount': 'int',
    'Program_Year': 'int',
    'Covered_Recipient_NPI': 'bigint',
    'Covered_Recipient_Profile_ID': 'bigint',
    'Teaching_Hospital_ID': 'bigint'
  }) -}}
{%- endmacro -%}

{#-- The declared width of each string column ON FABRIC WAREHOUSE ONLY. DuckDB and Spark have an
     unlengthed string type and never see this.

     THESE ARE MEASURED, NOT GUESSED, and that matters because a length that is too short does not
     fail the same way everywhere: T-SQL truncates or raises on ONE engine while DuckDB and Spark
     store the full value, which is a parity difference produced by a declaration rather than by a
     writer — the exact class of bug the DUID whitespace incident was. The maxima below come from
     `length()` over every column of a 100 MB / 187,750-row sample of PY2023, rounded UP to the next
     tier because a 1.3% sample is not the maximum:

       observed max, by tier   -> declared
       Contextual_Information 428 -> 500   (CMS documents a 500-character cap)
       specialty / product / drug / nature / entity names, 70-146 -> 256
       addresses, primary type, form of payment, 38-55 -> 128
       ids, personal names, cities, countries, NDC/PDI codes, 10-50 -> 64
       state and 2-3 character indicator flags, 2-11 -> 32

     The declared total is ~9.5 KB per row, far under Fabric Warehouse's 1 MB row limit, and it is
     kept tiered rather than a flat VARCHAR(500) because this repo compares engines on cost and an
     inflated declaration would disadvantage dwh for a reason that is not the writer's. --#}
{%- macro cms_varchar_len(column) -%}
  {%- if column == 'Contextual_Information' -%}500
  {%- elif column.startswith('Covered_Recipient_Specialty_')
        or column.startswith('Product_Category_or_Therapeutic_Area_')
        or column.startswith('Name_of_Drug_or_Biological_or_Device_or_Medical_Supply_')
        or column in ['Teaching_Hospital_Name',
                      'Submitting_Applicable_Manufacturer_or_Applicable_GPO_Name',
                      'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_Name',
                      'Nature_of_Payment_or_Transfer_of_Value',
                      'Name_of_Third_Party_Entity_Receiving_Payment_or_Transfer_of_Value'] -%}256
  {%- elif column.startswith('Covered_Recipient_Primary_Type_')
        or column in ['Recipient_Primary_Business_Street_Address_Line1',
                      'Recipient_Primary_Business_Street_Address_Line2',
                      'Form_of_Payment_or_Transfer_of_Value'] -%}128
  {%- elif column in ['Recipient_State', 'State_of_Travel',
                      'Applicable_Manufacturer_or_Applicable_GPO_Making_Payment_State',
                      'Change_Type', 'Teaching_Hospital_CCN', 'Covered_Recipient_Name_Suffix',
                      'Physician_Ownership_Indicator', 'Charity_Indicator',
                      'Third_Party_Equals_Covered_Recipient_Indicator',
                      'Delay_in_Publication_Indicator', 'Dispute_Status_for_Publication',
                      'Related_Product_Indicator']
        or column.startswith('Covered_Recipient_License_State_code')
        or column.startswith('Covered_or_Noncovered_Indicator_') -%}32
  {%- else -%}64
  {%- endif -%}
{%- endmacro -%}

{#-- The target type of each column, per dialect. Every column is cast explicitly rather than
     inherited — an inherited type would make the stored schema depend on which years a dispatch
     happened to land, and `layout` compares encodings across engines BY COLUMN.

     Unlike nyc and green there is no drift to correct here: the CSV header is byte-identical
     across PY2019-2025 (checked by md5 of the literal header row on 2019, 2021, 2023 and 2025) and
     the landed parquet is written to this same schema by the downloader, so these CASTs are
     no-ops. They are kept as the explicit declaration that all four engines store the same
     types. --#}
{%- macro cms_payment_type(column, dialect) -%}
  {%- set kind = cms_payment_non_strings().get(column, 'string') -%}
  {%- if kind == 'date' -%}
    {{- 'DATE' -}}
  {%- elif kind == 'double' -%}
    {{- 'FLOAT' if dialect == 'fabric' else 'DOUBLE' -}}
  {%- elif kind == 'int' -%}
    {{- 'INT' -}}
  {%- elif kind == 'bigint' -%}
    {{- 'BIGINT' -}}
  {%- else -%}
    {#-- Spark has no unlengthed VARCHAR — `CAST(x AS VARCHAR)` is a parse error there, and STRING
         is the type Spark stores anyway. Fabric Warehouse needs an explicit length; see
         cms_varchar_len() for where those numbers come from. --#}
    {%- if dialect == 'fabric' -%}VARCHAR({{ cms_varchar_len(column) }})
    {%- elif dialect == 'fabricspark' -%}STRING
    {%- else -%}VARCHAR
    {%- endif -%}
  {%- endif -%}
{%- endmacro -%}
