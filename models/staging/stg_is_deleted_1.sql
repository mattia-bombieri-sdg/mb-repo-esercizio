with source as (
    select
        *
    from {{ source('is_deleted', 'data_t1') }}
)
select * from source
order by id