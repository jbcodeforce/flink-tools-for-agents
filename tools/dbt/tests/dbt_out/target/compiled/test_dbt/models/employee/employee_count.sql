

-- Migrated from dml.employee_count.sql
with deduplicated_employees as (
    select * from (
        select *,
        ROW_NUMBER() OVER (PARTITION BY emp_id ORDER BY emp_id DESC) as row_num
        from `env-yk3jm6`.`cc_flink`.`employees`
    ) where row_num = 1
)
select coalesce(dept_id, 0) as dept_id, count(*) as emp_count from deduplicated_employees group by dept_id