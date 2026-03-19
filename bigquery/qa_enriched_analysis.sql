-- =============================================================================
-- Project: precious-metals-pipeline | Dataset: commodity_dataset
-- =============================================================================

-- ── 1. LATEST RAG SESSION RESULTS ────────────────────────────────────────────
-- Optimized for the 5-Question Reasoning output.
SELECT
    run_id,
    run_at,
    question,
    answer,
    data_window_start,
    data_window_end
FROM `precious-metals-pipeline.commodity_dataset.enriched_analysis`
WHERE run_id = (
    SELECT run_id 
    FROM `precious-metals-pipeline.commodity_dataset.enriched_analysis` 
    ORDER BY run_at DESC LIMIT 1
)
ORDER BY run_at ASC;

-- ── 2. LLM TREND SYNTHESIS: MOMENTUM TRACKER ─────────────────────────────────
-- Specifically tracks the AI's perspective on momentum accelerating vs cooling.
SELECT
    run_at,
    answer AS momentum_analysis
FROM `precious-metals-pipeline.commodity_dataset.enriched_analysis`
WHERE question LIKE '%momentum accelerating or cooling%'
ORDER BY run_at DESC;

-- ── 3. RATIO ANALYSIS HISTORY ────────────────────────────────────────────────
-- Pulls only the LLM's reasoning on the Gold-to-Silver ratio trajectory.
SELECT
    run_at,
    answer AS ratio_analysis
FROM `precious-metals-pipeline.commodity_dataset.enriched_analysis`
WHERE question LIKE '%XAU/XAG%ratio%'
ORDER BY run_at DESC;

-- ── 4. AGGREGATED MARKET SNAPSHOT (WITH PRE-CALC) ────────────────────────────
-- Shows the current state using the same logic the LLM sees.
WITH latest_calc AS (
    SELECT 
        metal, 
        price_usd, 
        ingestion_timestamp,
        ROUND(((price_usd - FIRST_VALUE(price_usd) OVER(PARTITION BY metal ORDER BY ingestion_timestamp ASC)) / 
               NULLIF(FIRST_VALUE(price_usd) OVER(PARTITION BY metal ORDER BY ingestion_timestamp ASC), 0)) * 100, 3) as session_pct_change,
        ROW_NUMBER() OVER (PARTITION BY metal ORDER BY ingestion_timestamp DESC) as rn
    FROM `precious-metals-pipeline.commodity_dataset.raw_prices`
    WHERE ingestion_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
)
SELECT 
    metal, 
    price_usd, 
    session_pct_change,
    ingestion_timestamp,
    CASE 
        WHEN session_pct_change > 0 THEN '▲'
        WHEN session_pct_change < 0 THEN '▼'
        ELSE '▬'
    END AS trend_icon
FROM latest_calc 
WHERE rn = 1
ORDER BY session_pct_change DESC;

-- ── 5. RAG QUALITY MONITORING ──────────────────────────────────────
-- Now flags runs that didn't complete the NEW 5-question set.
SELECT
    run_id,
    MIN(run_at) AS run_at,
    COUNT(*) AS completed_questions,
    (5 - COUNT(*)) AS failed_questions
FROM `precious-metals-pipeline.commodity_dataset.enriched_analysis`
GROUP BY run_id
HAVING completed_questions < 5
ORDER BY run_at DESC;

-- ── 6. ANOMALY DETECTION: VOLATILITY SPIKES ─────────────────────────────────
-- This SQL identifies the spikes that Question 1 in your RAG is looking for.
SELECT
    metal,
    ingestion_timestamp,
    price_usd,
    ROUND(pct_change, 4) AS pct_change
FROM (
    SELECT
        metal,
        ingestion_timestamp,
        price_usd,
        ((price_usd - LAG(price_usd) OVER (PARTITION BY metal ORDER BY ingestion_timestamp)) 
        / NULLIF(LAG(price_usd) OVER (PARTITION BY metal ORDER BY ingestion_timestamp), 0)) * 100 AS pct_change
    FROM `precious-metals-pipeline.commodity_dataset.raw_prices`
)
WHERE ABS(pct_change) > 0.3
ORDER BY ingestion_timestamp DESC;