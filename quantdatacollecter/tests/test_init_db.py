from init_db import build_schema_commands

def test_build_schema_commands_contains_layered_databases_and_core_tables():
    commands = "\n".join(build_schema_commands())

    assert "CREATE DATABASE IF NOT EXISTS raw" in commands
    assert "CREATE DATABASE IF NOT EXISTS sample" in commands
    assert "CREATE DATABASE IF NOT EXISTS feature" in commands
    assert "CREATE DATABASE IF NOT EXISTS label" in commands
    assert "CREATE DATABASE IF NOT EXISTS dataset" in commands
    assert "CREATE DATABASE IF NOT EXISTS meta" in commands

    assert "CREATE TABLE IF NOT EXISTS raw.trades" in commands
    assert "CREATE TABLE IF NOT EXISTS raw.book_topk_raw" in commands
    assert "CREATE TABLE IF NOT EXISTS raw.liquidations" in commands
    assert "CREATE TABLE IF NOT EXISTS raw.funding" in commands
    assert "CREATE TABLE IF NOT EXISTS raw.open_interest" in commands

    assert "CREATE TABLE IF NOT EXISTS sample.bars_time" in commands
    assert "CREATE TABLE IF NOT EXISTS feature.features_time_bar" in commands
    assert "CREATE TABLE IF NOT EXISTS label.labels_time_bar" in commands
    assert "CREATE TABLE IF NOT EXISTS meta.symbols" in commands
    assert "CREATE TABLE IF NOT EXISTS meta.pipeline_runs" in commands
    assert "signed_sqrt_dollar_volume" in commands
    assert "vol_ratio_5_20" in commands
    assert "autocorrelation_5" in commands
    assert "kyle_lambda_approx" in commands
    assert "amihud_illiquidity" in commands
    assert "oi_zscore_20" in commands
    assert "liq_volume_pct" in commands
    assert "price_acceleration" in commands

def test_schema_contains_new_quant_features():
    with open("init_db.py", "r", encoding="utf-8") as f:
        content = f.read()
    assert "frac_diff_0_4" in content
    assert "vpin_20" in content
    assert "triple_barrier_signal" in content
