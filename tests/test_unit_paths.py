import pytest


@pytest.mark.unit
class TestPathConfiguration:
    def test_dev_paths_are_built_correctly(self, paths):
        assert paths.get_bronze_table("orders") == "Medallion.bronze_dev.orders"
        assert paths.get_silver_table("orders_enriched") == "Medallion.silver_dev.orders_enriched"
        assert paths.get_gold_table("daily_metrics") == "Medallion.gold_dev.daily_metrics"
        assert paths.get_model_name("kmeans_clustering") == "Medallion.models_dev.kmeans_clustering"

    def test_test_paths_are_built_correctly(self):
        from config.paths import DataPaths

        paths = DataPaths("test")
        assert paths.get_bronze_table("orders") == "Medallion.bronze_test.orders"
        assert paths.get_silver_table("orders_enriched") == "Medallion.silver_test.orders_enriched"
        assert paths.get_gold_table("daily_metrics") == "Medallion.gold_test.daily_metrics"
        assert paths.get_model_name("kmeans_clustering") == "Medallion.models_test.kmeans_clustering"

    def test_prod_model_registry_name(self):
        from config.paths import DataPaths

        paths = DataPaths("prod")
        assert paths.get_model_name("als_recommendations") == "Medallion.models_prod.als_recommendations"
