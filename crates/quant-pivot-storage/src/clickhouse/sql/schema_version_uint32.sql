ALTER TABLE quant_crypto_price_report
    MODIFY COLUMN schema_version UInt32;

ALTER TABLE quant_weather_observation_report
    MODIFY COLUMN schema_version UInt32;

ALTER TABLE quant_weather_forecast_point
    MODIFY COLUMN schema_version UInt32;

ALTER TABLE quant_domain_event
    MODIFY COLUMN schema_version UInt32;

ALTER TABLE quant_entry_condition_evaluation_event
    MODIFY COLUMN schema_version UInt32;
