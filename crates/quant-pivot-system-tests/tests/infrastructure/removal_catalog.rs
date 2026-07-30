//! Canonical catalog probes for objects removed by clean-break migrations.

pub const CLICKHOUSE_REMOVED_TABLES_QUERY: &str = "\
    SELECT count() FROM system.tables \
    WHERE database = currentDatabase() \
      AND name IN ( \
        'quant_recommendation_attribution_event', \
        'quant_book_l2_event', \
        'quant_book_l2_checkpoint')";

pub const POSTGRES_REMOVED_OBJECTS_QUERY: &str = "\
    SELECT 'relation' AS object_kind, c.relname AS object_name \
    FROM pg_catalog.pg_class AS c \
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
    WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p', 'v', 'm') \
      AND c.relname IN ( \
        'quant_recommendation_attribution', \
        'quant_profile_allocation', \
        'quant_profile_allocation_artifact') \
    UNION ALL \
    SELECT 'column', c.relname || '.' || a.attname \
    FROM pg_catalog.pg_attribute AS a \
    JOIN pg_catalog.pg_class AS c ON c.oid = a.attrelid \
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
    WHERE n.nspname = 'public' AND c.relname = 'quant_model_version' \
      AND NOT a.attisdropped \
      AND a.attname IN ( \
        'source_backtest_report_id', \
        'score_multiplier_calibration_report') \
    UNION ALL \
    SELECT 'enum_type', t.typname \
    FROM pg_catalog.pg_type AS t \
    JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace \
    WHERE n.nspname = 'public' \
      AND t.typname IN ( \
        'qp_recommendation_attribution_outcome', \
        'qp_recommendation_outcome') \
    UNION ALL \
    SELECT 'enum_label', t.typname || '.' || e.enumlabel \
    FROM pg_catalog.pg_enum AS e \
    JOIN pg_catalog.pg_type AS t ON t.oid = e.enumtypid \
    JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace \
    WHERE n.nspname = 'public' \
      AND e.enumlabel IN ('attributed', 'score_multiplier_calibration') \
    UNION ALL \
    SELECT 'trigger', tg.tgname \
    FROM pg_catalog.pg_trigger AS tg \
    JOIN pg_catalog.pg_class AS c ON c.oid = tg.tgrelid \
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
    WHERE n.nspname = 'public' AND NOT tg.tgisinternal \
      AND tg.tgname IN ( \
        'trg_quant_recommendation_attribution_append_only', \
        'trg_quant_profile_allocation_artifact_append_only', \
        'trg_quant_factor_definition_status_guard') \
    UNION ALL \
    SELECT 'procedure', p.proname \
    FROM pg_catalog.pg_proc AS p \
    JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace \
    WHERE n.nspname = 'public' AND p.proname = 'guard_factor_definition' \
    UNION ALL \
    SELECT 'index', c.relname \
    FROM pg_catalog.pg_class AS c \
    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace \
    WHERE n.nspname = 'public' AND c.relkind = 'i' \
      AND c.relname IN ( \
        'uq_quant_factor_definition_published_name', \
        'idx_quant_factor_definition_family_status', \
        'idx_quant_model_version_source_backtest') \
    UNION ALL \
    SELECT 'constraint', con.conname \
    FROM pg_catalog.pg_constraint AS con \
    JOIN pg_catalog.pg_namespace AS n ON n.oid = con.connamespace \
    WHERE n.nspname = 'public' \
      AND con.conname = 'fk-quant_model_version-source_backtest_report_id' \
    ORDER BY object_kind, object_name";
