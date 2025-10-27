-- ============================================================================
-- CALC-METRICS HEALTH CHECK QUERIES
-- Run these to verify real-time inserts are working across exchanges
-- ============================================================================

-- 1. CHECK LATEST DATA FRESHNESS (should be < 2 minutes old)
-- Shows most recent timestamp per exchange to detect stale data
SELECT 
  exchange,
  symbol,
  TO_TIMESTAMP(ts/1000) as last_update,
  EXTRACT(EPOCH FROM (NOW() - TO_TIMESTAMP(ts/1000)))/60 as minutes_ago,
  c, oi, pfr, lsr
FROM perp_metrics
WHERE ts > EXTRACT(EPOCH FROM NOW() - INTERVAL '5 minutes') * 1000
ORDER BY exchange, ts DESC
LIMIT 30;

-- 2. COUNT RECORDS BY EXCHANGE (last 10 minutes)
-- Should see similar volumes across exchanges if all are working
SELECT 
  exchange,
  COUNT(*) as record_count,
  COUNT(DISTINCT symbol) as unique_symbols,
  MIN(TO_TIMESTAMP(ts/1000)) as oldest_record,
  MAX(TO_TIMESTAMP(ts/1000)) as newest_record
FROM perp_metrics
WHERE ts > EXTRACT(EPOCH FROM NOW() - INTERVAL '10 minutes') * 1000
GROUP BY exchange
ORDER BY exchange;

-- 3. CHECK FIELD COVERAGE BY EXCHANGE (last 5 minutes)
-- Shows which exchanges have OI, PFR, LSR data
SELECT 
  exchange,
  COUNT(*) as total_rows,
  COUNT(oi) as oi_count,
  COUNT(pfr) as pfr_count,
  COUNT(lsr) as lsr_count,
  COUNT(rsi1) as rsi1_count,
  ROUND(100.0 * COUNT(oi) / NULLIF(COUNT(*), 0), 1) as oi_coverage_pct,
  ROUND(100.0 * COUNT(pfr) / NULLIF(COUNT(*), 0), 1) as pfr_coverage_pct,
  ROUND(100.0 * COUNT(lsr) / NULLIF(COUNT(*), 0), 1) as lsr_coverage_pct
FROM perp_metrics
WHERE ts > EXTRACT(EPOCH FROM NOW() - INTERVAL '5 minutes') * 1000
GROUP BY exchange
ORDER BY exchange;

-- 4. CHECK CHANGE CALCULATIONS (should have values, not all NULL)
-- If _chg columns are all NULL, calculations aren't running
SELECT 
  exchange,
  symbol,
  TO_TIMESTAMP(ts/1000) as time,
  c,
  c_chg_1m,
  c_chg_5m,
  oi_chg_1m,
  pfr_chg_1m,
  lsr_chg_1m
FROM perp_metrics
WHERE ts > EXTRACT(EPOCH FROM NOW() - INTERVAL '5 minutes') * 1000
  AND exchange = 'okx'  -- Change to bin/byb to check others
ORDER BY ts DESC
LIMIT 20;

-- 5. COMPARE PERP_DATA vs PERP_METRICS (should be near-identical counts)
-- If perp_data has records but perp_metrics doesn't, calc-metrics is failing
WITH data_counts AS (
  SELECT exchange, COUNT(*) as data_count
  FROM perp_data
  WHERE ts > EXTRACT(EPOCH FROM NOW() - INTERVAL '5 minutes') * 1000
  GROUP BY exchange
),
metrics_counts AS (
  SELECT exchange, COUNT(*) as metrics_count
  FROM perp_metrics
  WHERE ts > EXTRACT(EPOCH FROM NOW() - INTERVAL '5 minutes') * 1000
  GROUP BY exchange
)
SELECT 
  COALESCE(d.exchange, m.exchange) as exchange,
  COALESCE(d.data_count, 0) as perp_data_rows,
  COALESCE(m.metrics_count, 0) as perp_metrics_rows,
  COALESCE(d.data_count, 0) - COALESCE(m.metrics_count, 0) as missing_metrics
FROM data_counts d
FULL OUTER JOIN metrics_counts m ON d.exchange = m.exchange
ORDER BY exchange;

-- 6. CHECK OKX SPECIFICALLY (your concern)
-- Shows OI/PFR/LSR availability for OKX symbols
SELECT 
  symbol,
  COUNT(*) as row_count,
  COUNT(oi) as has_oi,
  COUNT(pfr) as has_pfr,
  COUNT(lsr) as has_lsr,
  MAX(TO_TIMESTAMP(ts/1000)) as latest_update
FROM perp_metrics
WHERE exchange = 'okx'
  AND ts > EXTRACT(EPOCH FROM NOW() - INTERVAL '10 minutes') * 1000
GROUP BY symbol
ORDER BY symbol
LIMIT 20;

-- 7. CHECK PERP_STATUS LOG (should show recent "running" status)
-- If last status is old, calc-metrics may have crashed
SELECT 
  script_name,
  status,
  message,
  ts
FROM perp_status
WHERE script_name = 'calc-metrics.js'
ORDER BY ts DESC
LIMIT 10;

-- 8. CHECK PERP_ERRORS (look for calc-metrics failures)
SELECT 
  error_type,
  error_code,
  error_message,
  details,
  ts
FROM perp_errors
WHERE script_name = 'calc-metrics.js'
  OR error_message ILIKE '%calc%'
ORDER BY ts DESC
LIMIT 10;