{{ config(
    materialized='incremental'
) }}

with incr_model1 as (
	select
		id,
		value,
		is_updated as is_deleted,
		current_timestamp as last_update
	from {{ ref('stg_is_deleted_1') }}
	{% if is_incremental() %}
		where is_deleted <> 'N'
		--from {{ this }}
	{% endif%}
)

select * from incr_model1