from pipelines.features_time_bar import build_feature_insert_sql, build_feature_quality_check_sql


def test_build_feature_insert_sql_uses_strictly_past_windows():
    sql = build_feature_insert_sql(interval_sec=60)

    assert "ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING" in sql
    assert "FOLLOWING" not in sql
    assert "INSERT INTO feature.features_time_bar" in sql


def test_build_feature_insert_sql_precomputes_prev_close_before_rolling_windows():
    sql = build_feature_insert_sql(interval_sec=60)

    assert "lagInFrame(close, 1) OVER base_w AS prev_close" in sql
    assert "stddevSamp(log_return) OVER w5" in sql
    assert "stddevSamp(log(close / nullIf(lagInFrame(close, 1) OVER w, 0))) OVER w5" not in sql


def test_build_feature_insert_sql_includes_phase1_alpha_factors():
    sql = build_feature_insert_sql(interval_sec=60)

    assert "AS vol_ratio_5_20" in sql
    assert "AS autocorrelation_5" in sql
    assert "AS autocorrelation_20" in sql
    assert "AS kyle_lambda_approx" in sql
    assert "AS amihud_illiquidity" in sql
    assert "AS oi_zscore_20" in sql
    assert "AS liq_volume_pct" in sql
    assert "AS price_acceleration" in sql


def test_build_feature_insert_sql_uses_signed_sqrt_dollar_volume_kyle_lambda():
    sql = build_feature_insert_sql(interval_sec=60)

    assert "signed_sqrt_dollar_volume" in sql
    assert "log_return * signed_sqrt_dollar_volume" in sql
    assert "signed_sqrt_dollar_volume * signed_sqrt_dollar_volume" in sql
    assert "corrStable(log_return, delta_volume) OVER w20" not in sql
    assert "stddevSamp(delta_volume) OVER w20" not in sql
    assert "abs(log_return) / nullIf(abs(delta_volume), 0) AS kyle_lambda_approx" not in sql


def test_build_feature_insert_sql_uses_bar_over_bar_open_interest_change():
    sql = build_feature_insert_sql(interval_sec=60)

    assert "lagInFrame(open_interest_close, 1) OVER base_w AS prev_oi_close" in sql
    assert "open_interest_close - prev_oi_close" in sql
    assert "(open_interest_close - prev_oi_close) / nullIf(prev_oi_close, 0)" in sql
    assert "open_interest_close - open_interest_open AS oi_change" not in sql


def test_build_feature_insert_sql_uses_explicit_utc_calendar_features():
    sql = build_feature_insert_sql(interval_sec=60)

    assert "toHour(bar_time, 'UTC') AS hour_of_day" in sql
    assert "toDayOfWeek(bar_time, 0, 'UTC') AS day_of_week" in sql


def test_build_feature_insert_sql_emits_nullable_large_trade_pct_until_source_is_implemented():
    sql = build_feature_insert_sql(interval_sec=60)

    assert "CAST(NULL, 'Nullable(Float64)') AS large_trade_pct" in sql


def test_build_feature_insert_sql_uses_lookback_buffer_for_time_range():
    sql = build_feature_insert_sql(
        interval_sec=60,
        start="2026-03-02T00:00:00Z",
        end="2026-03-02T01:00:00Z",
    )

    assert "bar_time >= toDateTime64('2026-03-01 22:59:00.000', 3, 'UTC')" in sql
    assert "bar_time < toDateTime64('2026-03-02 01:00:00.000', 3, 'UTC')" in sql
    assert "WHERE bar_time >= toDateTime64('2026-03-02 00:00:00.000', 3, 'UTC')" in sql
    assert "AND bar_time < toDateTime64('2026-03-02 01:00:00.000', 3, 'UTC')" in sql


def test_build_feature_quality_check_sql_targets_rolling_columns():
    sql = build_feature_quality_check_sql(
        interval_sec=60,
        start="2026-03-02T00:00:00Z",
        end="2026-03-02T01:00:00Z",
    )

    assert "FROM feature.features_time_bar" in sql
    assert "bar_interval_sec = 60" in sql
    assert "realized_vol_20" in sql
    assert "autocorrelation_5" in sql
    assert "kyle_lambda_bad" in sql
    assert "buy_ratio_out_of_range" in sql
    assert "large_trade_pct_distinct_values" in sql
    assert "2026-03-02 00:00:00.000" in sql
    assert "2026-03-02 01:00:00.000" in sql

def test_build_feature_insert_sql_includes_frac_diff_and_vpin():
    sql = build_feature_insert_sql(interval_sec=60)
    
    assert 'AS frac_diff_0_4' in sql
    assert 'AS frac_diff_0_6' in sql
    assert 'lagInFrame(close, 5)' in sql
    
    assert 'sum(abs(delta_volume)) OVER w5 / nullIf(sum(volume) OVER w5, 0) AS vpin_5' in sql
    assert 'AS vpin_10' in sql
    assert 'AS vpin_20' in sql
