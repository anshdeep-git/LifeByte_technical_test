WITH date_range AS (
  SELECT generate_series('2020-06-01'::date, '2020-09-30'::date, '1 day'::interval) AS dt_report
),
trades_data AS (
  SELECT
    date_range.dt_report,
    users.login_hash,
    users.server_hash,
    trades.symbol,
    users.currency,
    COALESCE(SUM(tech_test.volume) FILTER (WHERE tech_test.open_time BETWEEN date_range.dt_report - '6 days'::interval AND date_range.dt_report), 0) AS sum_volume_prev_7d,
    COALESCE(SUM(tech_test.volume) FILTER (WHERE tech_test.open_time <= date_range.dt_report), 0) AS sum_volume_prev_all,
    RANK() OVER (PARTITION BY users.login_hash, trades.symbol ORDER BY SUM(tech_test.volume) FILTER (WHERE tech_test.open_time BETWEEN date_range.dt_report - '6 days'::interval AND date_range.dt_report) DESC) AS rank_volume_symbol_prev_7d,
    RANK() OVER (PARTITION BY users.login_hash ORDER BY COUNT(*) FILTER (WHERE tech_test.open_time BETWEEN date_range.dt_report - '6 days'::interval AND date_range.dt_report) DESC) AS rank_count_prev_7d,
    COALESCE(SUM(tech_test.volume) FILTER (WHERE tech_test.open_time >= '2020-08-01'::date AND tech_test.open_time <= date_range.dt_report), 0) AS sum_volume_2020_08,
    MIN(tech_test.open_time) FILTER (WHERE tech_test.open_time <= date_range.dt_report) AS date_first_trade,
    ROW_NUMBER() OVER (ORDER BY date_range.dt_report DESC, users.login_hash, users.server_hash, trades.symbol) AS row_number,
    tech_test.id
  FROM date_range
  CROSS JOIN users
  LEFT JOIN tech_test ON users.login_hash = tech_test.login_hash AND users.server_hash = tech_test.server_hash AND tech_test.open_time::date = date_range.dt_report
  LEFT JOIN trades ON users.login_hash = trades.login_hash AND users.server_hash = trades.server_hash AND trades.symbol = tech_test.symbol AND trades.open_time::date = date_range.dt_report
  WHERE users."enable" = 1
  GROUP BY date_range.dt_report, users.login_hash, users.server_hash, trades.symbol, users.currency, tech_test.id
)
SELECT
  trades_data.id,
  trades_data.dt_report,
  trades_data.login_hash,
  trades_data.server_hash,
  trades_data.symbol,
  trades_data.currency,
  trades_data.sum_volume_prev_7d,
  trades_data.sum_volume_prev_all,
  trades_data.rank_volume_symbol_prev_7d,
  trades_data.rank_count_prev_7d,
  trades_data.sum_volume_2020_08,
  trades_data.date_first_trade,
  trades_data.row_number
FROM trades_data
ORDER BY trades_data.row_number DESC;
