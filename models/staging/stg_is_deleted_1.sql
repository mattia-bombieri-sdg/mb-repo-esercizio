with source as (
    select
        id,
        value,
        is_deleted
    from {{ source('is_deleted', 'data_t1') }}
)
select * from source