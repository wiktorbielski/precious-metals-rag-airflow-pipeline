-- 1. Delete the old tables entirely to clear metadata conflicts
DROP TABLE IF EXISTS `precious-metals-pipeline.commodity_dataset.raw_prices`;
DROP TABLE IF EXISTS `precious-metals-pipeline.commodity_dataset.enriched_analysis`;

-- 2. Create the Raw Prices table with the correct schema
CREATE TABLE `precious-metals-pipeline.commodity_dataset.raw_prices`
(
  event_id               STRING    OPTIONS(description="Unique UUID for deduplication"),
  metal                  STRING    OPTIONS(description="Metal symbol: XAU, XAG, XPT, XPD"),
  price_usd              FLOAT64   OPTIONS(description="Spot price in USD"),
  api_timestamp          TIMESTAMP OPTIONS(description="Market time from API response"),
  ingestion_timestamp    TIMESTAMP OPTIONS(description="Time Airflow pushed to Kafka"),
  processed_at           TIMESTAMP OPTIONS(description="Time Consumer wrote to BigQuery"),
  airflow_run_id         STRING    OPTIONS(description="Originating DAG Run ID")
)
PARTITION BY DATE(ingestion_timestamp)
CLUSTER BY metal
OPTIONS (
  description = "Raw precious metals prices partitioned by ingestion date.",
  labels = [("layer", "raw")]
);

-- 3. Create the Enriched Analysis table
CREATE TABLE `precious-metals-pipeline.commodity_dataset.enriched_analysis`
(
  run_id              STRING    OPTIONS(description="UUID grouping the RAG session"),
  run_at              TIMESTAMP OPTIONS(description="Execution time of the RAG task"),
  question            STRING    OPTIONS(description="The analytical prompt"),
  answer              STRING    OPTIONS(description="The response from local LLM"),
  data_window_start   TIMESTAMP OPTIONS(description="Context window start"),
  data_window_end     TIMESTAMP OPTIONS(description="Context window end")
)
PARTITION BY DATE(run_at)
CLUSTER BY run_id
OPTIONS (
  description = "Automated market analysis results from Ollama.",
  labels = [("layer", "enriched")]
);