{{ dbt_utils.date_spine(
    datepart="day",
    start_date="to_date('06/24/2025', 'mm/dd/yyyy')",
    end_date="dateadd(week, 1, current_date)"
) }}