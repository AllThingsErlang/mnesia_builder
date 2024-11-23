
-record(subscribers, {subscriber_id, nag_id, customer_name, device_type, 
                      total_bad_gb_down, total_gb_down, total_bad_periods, total_periods, 
                      total_bad_days, total_days_active, bad_period_pctg=0, bad_period_pctg_bucket=0,
                      city, province, postal_code}).

