{{ config(
    materialized='incremental'
) }}

with incr_model1 as (
	select
		id,
		value,
		is_deleted,
		current_timestamp as last_update
	from {{ ref('stg_is_deleted_0') }}
	{% if is_incremental() %}
		where updated_at >= (select coalesce(max(updated_at), '1900-01-01') 
								from {{ this }} )
	{% endif %}
)

select * from incr_model1