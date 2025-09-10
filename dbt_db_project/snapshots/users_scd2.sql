{% snapshot users_scd2 %}

-- this snapshot tracks slowly changes on the users_silver table
-- the timestamp to check the changes is located on the column _airbyte_extracted_at

{{
  config(
    target_database='workspace',
    target_schema='default',
    unique_key='user_id',
    strategy='timestamp',
    updated_at='_airbyte_extracted_at'
  )
}}

select
  user_id, email, username, first_name, last_name,
  street, city, zipcode, country, phone, _airbyte_extracted_at
from {{ source('silver','users_silver') }}

{% endsnapshot %}
