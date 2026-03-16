import pytest
from pyspark.sql.functions import col, count as spark_count, abs as spark_abs

from tests.conftest import read_table_or_fail


@pytest.mark.integration
class TestBronzeQuality:
    def test_required_bronze_tables_exist(self, spark, paths):
        required_tables = [
            paths.get_bronze_table("orders"),
            paths.get_bronze_table("order_items"),
            paths.get_bronze_table("reviews"),
        ]

        for table_name in required_tables:
            df = read_table_or_fail(spark, table_name)
            assert df is not None

    def test_orders_customer_id_not_null(self, spark, paths):
        df = read_table_or_fail(spark, paths.get_bronze_table("orders"))
        assert df.filter(col("customer_id").isNull()).count() == 0

    def test_orders_order_id_unique(self, spark, paths):
        df = read_table_or_fail(spark, paths.get_bronze_table("orders"))
        duplicates = (
            df.groupBy("order_id")
            .agg(spark_count("*").alias("row_count"))
            .filter(col("row_count") > 1)
            .count()
        )
        assert duplicates == 0

    def test_order_items_positive_price(self, spark, paths):
        df = read_table_or_fail(spark, paths.get_bronze_table("order_items"))
        assert df.filter(col("price") <= 0).count() == 0

    def test_order_items_positive_freight(self, spark, paths):
        df = read_table_or_fail(spark, paths.get_bronze_table("order_items"))
        assert df.filter(col("freight_value") < 0).count() == 0

    def test_order_items_order_item_key_unique(self, spark, paths):
        df = read_table_or_fail(spark, paths.get_bronze_table("order_items"))
        duplicates = (
            df.groupBy("order_id", "order_item_id")
            .agg(spark_count("*").alias("row_count"))
            .filter(col("row_count") > 1)
            .count()
        )
        assert duplicates == 0

    def test_reviews_valid_scores(self, spark, paths):
        df = read_table_or_fail(spark, paths.get_bronze_table("reviews"))
        assert df.filter(~col("review_score").between(1, 5)).count() == 0

    def test_reviews_order_id_not_null(self, spark, paths):
        df = read_table_or_fail(spark, paths.get_bronze_table("reviews"))
        assert df.filter(col("order_id").isNull()).count() == 0


@pytest.mark.integration
class TestSilverLayerQuality:
    def test_required_silver_tables_exist(self, spark, paths):
        required_tables = [
            paths.get_silver_table("orders_enriched"),
            paths.get_silver_table("products_enriched"),
            paths.get_silver_table("customers_enriched"),
            paths.get_silver_table("customer_level_enriched"),
        ]

        for table_name in required_tables:
            df = read_table_or_fail(spark, table_name)
            assert df is not None

    def test_orders_enriched_order_id_not_null(self, spark, paths):
        df = read_table_or_fail(spark, paths.get_silver_table("orders_enriched"))
        assert df.filter(col("order_id").isNull()).count() == 0

    def test_orders_enriched_total_value_consistency(self, spark, paths):
        df = read_table_or_fail(spark, paths.get_silver_table("orders_enriched"))

        comparable_rows = df.filter(
            col("price").isNotNull() & col("freight_value").isNotNull() & col("total_value").isNotNull()
        )

        invalid_rows = df.filter(
            col("price").isNotNull()
            & col("freight_value").isNotNull()
            & col("total_value").isNotNull()
            & (spark_abs(col("total_value") - (col("price") + col("freight_value"))) > 1e-6)
        ).count()

        assert comparable_rows.count() > 0
        assert invalid_rows == 0

    def test_orders_enriched_delivery_days_non_negative(self, spark, paths):
        df = read_table_or_fail(spark, paths.get_silver_table("orders_enriched"))
        invalid_rows = df.filter(
            col("order_delivered_customer_date").isNotNull()
            & col("order_purchase_timestamp").isNotNull()
            & (col("days_to_delivery") < 0)
        ).count()
        assert invalid_rows == 0

    def test_products_enriched_product_id_unique(self, spark, paths):
        df = read_table_or_fail(spark, paths.get_silver_table("products_enriched"))
        duplicates = (
            df.groupBy("product_id")
            .agg(spark_count("*").alias("row_count"))
            .filter(col("row_count") > 1)
            .count()
        )
        assert duplicates == 0

    def test_customer_level_enriched_unique_customer(self, spark, paths):
        df = read_table_or_fail(spark, paths.get_silver_table("customer_level_enriched"))
        duplicates = (
            df.groupBy("customer_unique_id", "zipcode", "city", "state")
            .agg(spark_count("*").alias("row_count"))
            .filter(col("row_count") > 1)
            .count()
        )
        assert df.filter(col("customer_unique_id").isNull()).count() == 0
        assert duplicates == 0


@pytest.mark.integration
class TestGoldAndFeatureQuality:
    def test_required_gold_tables_exist(self, spark, paths):
        required_tables = [
            paths.get_gold_table("product_metrics"),
            paths.get_gold_table("customer_features"),
            paths.get_gold_table("daily_metrics"),
            paths.get_gold_table("time_series_metrics"),
            paths.get_gold_table("customer_rfm_features"),
            paths.get_gold_table("customer_product_interactions"),
        ]

        for table_name in required_tables:
            df = read_table_or_fail(spark, table_name)
            assert df is not None

    def test_product_metrics_positive_orders_and_revenue(self, spark, paths):
        df = read_table_or_fail(spark, paths.get_gold_table("product_metrics"))
        product_rows = df.filter(col("product_id").isNotNull())

        invalid_rows = product_rows.filter(
            (col("total_orders") <= 0)
            | (col("total_revenue") < 0)
        ).count()

        assert product_rows.count() > 0
        assert invalid_rows == 0

    def test_daily_metrics_valid_values(self, spark, paths):
        df = read_table_or_fail(spark, paths.get_gold_table("daily_metrics"))
        invalid_rows = df.filter(
            col("order_date").isNull()
            | (col("num_orders") <= 0)
            | (col("total_revenue") < 0)
        ).count()
        assert invalid_rows == 0

    def test_time_series_metrics_valid_month_values(self, spark, paths):
        df = read_table_or_fail(spark, paths.get_gold_table("time_series_metrics"))
        invalid_rows = df.filter(
            col("year").isNull()
            | col("month").isNull()
            | (~col("month").between("01", "12"))
            | (col("num_orders") <= 0)
        ).count()
        assert invalid_rows == 0

    def test_customer_rfm_features_are_valid(self, spark, paths):
        df = read_table_or_fail(spark, paths.get_gold_table("customer_rfm_features"))
        invalid_rows = df.filter(
            col("customer_id").isNull()
            | (col("recency") < 0)
            | (col("frequency") <= 0)
            | (col("monetary") < 0)
        ).count()
        assert invalid_rows == 0

    def test_customer_product_interactions_are_valid(self, spark, paths):
        df = read_table_or_fail(spark, paths.get_gold_table("customer_product_interactions"))
        invalid_rows = df.filter(
            col("customer_id").isNull()
            | col("product_id").isNull()
            | (col("interaction_weight") < 0)
            | (~col("rating").between(1, 5))
        ).count()
        assert invalid_rows == 0


@pytest.mark.integration
class TestMlOutputQuality:
    def test_required_ml_output_tables_exist(self, spark, paths):
        required_tables = [
            paths.get_gold_table("customer_segments"),
            paths.get_gold_table("product_recommendations"),
        ]

        for table_name in required_tables:
            df = read_table_or_fail(spark, table_name)
            assert df is not None

    def test_customer_segments_valid_clusters(self, spark, paths):
        df = read_table_or_fail(spark, paths.get_gold_table("customer_segments"))
        invalid_rows = df.filter(
            col("customer_id").isNull()
            | col("cluster").isNull()
            | (~col("cluster").between(0, 4))
        ).count()
        assert invalid_rows == 0

    def test_product_recommendations_valid_rank(self, spark, paths):
        df = read_table_or_fail(spark, paths.get_gold_table("product_recommendations"))
        invalid_rows = df.filter(
            col("customer_idx").isNull()
            | col("product_idx").isNull()
            | col("predicted_rating").isNull()
            | (~col("rank").between(1, 10))
        ).count()
        assert invalid_rows == 0

    def test_product_recommendations_unique_rank_per_customer(self, spark, paths):
        df = read_table_or_fail(spark, paths.get_gold_table("product_recommendations"))
        duplicates = (
            df.groupBy("customer_idx", "rank")
            .agg(spark_count("*").alias("row_count"))
            .filter(col("row_count") > 1)
            .count()
        )
        assert duplicates == 0
