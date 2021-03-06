from nose.plugins.attrib import attr
from test.integration.base import DBTIntegrationTest

class TestSimpleReference(DBTIntegrationTest):

    def setUp(self):
        pass

    @property
    def schema(self):
        return "simple_reference_003"

    @property
    def models(self):
        return "test/integration/003_simple_reference_test/models"

    @attr(type='postgres')
    def test__postgres__simple_reference(self):
        self.use_profile('postgres')
        self.use_default_project()
        self.run_sql_file(
            "test/integration/003_simple_reference_test/seed.sql")

        results = self.run_dbt()
        # ephemeral_copy doesn't show up in results
        self.assertEqual(len(results),  7)

        # Copies should match
        self.assertTablesEqual("seed","incremental_copy")
        self.assertTablesEqual("seed","materialized_copy")
        self.assertTablesEqual("seed","view_copy")

        # Summaries should match
        self.assertTablesEqual("summary_expected","incremental_summary")
        self.assertTablesEqual("summary_expected","materialized_summary")
        self.assertTablesEqual("summary_expected","view_summary")
        self.assertTablesEqual("summary_expected","ephemeral_summary")

        self.run_sql_file("test/integration/003_simple_reference_test/update.sql")

        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        # Copies should match
        self.assertTablesEqual("seed","incremental_copy")
        self.assertTablesEqual("seed","materialized_copy")
        self.assertTablesEqual("seed","view_copy")

        # Summaries should match
        self.assertTablesEqual("summary_expected","incremental_summary")
        self.assertTablesEqual("summary_expected","materialized_summary")
        self.assertTablesEqual("summary_expected","view_summary")
        self.assertTablesEqual("summary_expected","ephemeral_summary")

    @attr(type='snowflake')
    def test__snowflake__simple_reference(self):
        self.use_profile('snowflake')
        self.use_default_project()
        self.run_sql_file("test/integration/003_simple_reference_test/seed.sql")

        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        # Copies should match
        self.assertTablesEqual("SEED", "incremental_copy")
        self.assertTablesEqual("SEED", "materialized_copy")
        self.assertTablesEqual("SEED", "view_copy")

        # Summaries should match
        self.assertTablesEqual("SUMMARY_EXPECTED", "incremental_summary")
        self.assertTablesEqual("SUMMARY_EXPECTED", "materialized_summary")
        self.assertTablesEqual("SUMMARY_EXPECTED", "view_summary")
        self.assertTablesEqual("SUMMARY_EXPECTED", "ephemeral_summary")

        self.run_sql_file(
            "test/integration/003_simple_reference_test/update.sql")

        results = self.run_dbt()
        self.assertEqual(len(results),  7)

        # Copies should match
        self.assertTablesEqual("SEED", "incremental_copy")
        self.assertTablesEqual("SEED", "materialized_copy")
        self.assertTablesEqual("SEED", "view_copy")

        # Summaries should match
        self.assertTablesEqual("SUMMARY_EXPECTED", "incremental_summary")
        self.assertTablesEqual("SUMMARY_EXPECTED", "materialized_summary")
        self.assertTablesEqual("SUMMARY_EXPECTED", "view_summary")
        self.assertTablesEqual("SUMMARY_EXPECTED", "ephemeral_summary")

    @attr(type='postgres')
    def test__postgres__simple_reference_with_models(self):
        self.use_profile('postgres')
        self.use_default_project()
        self.run_sql_file("test/integration/003_simple_reference_test/seed.sql")

        # Run materialized_copy, ephemeral_copy, and their dependents
        # ephemeral_copy should not actually be materialized b/c it is ephemeral
        results = self.run_dbt(
            ['run', '--models', 'materialized_copy', 'ephemeral_copy']
        )
        self.assertEqual(len(results),  1)

        # Copies should match
        self.assertTablesEqual("seed","materialized_copy")

        created_models = self.get_models_in_schema()
        self.assertTrue('materialized_copy' in created_models)

    @attr(type='postgres')
    def test__postgres__simple_reference_with_models_and_children(self):
        self.use_profile('postgres')
        self.use_default_project()
        self.run_sql_file("test/integration/003_simple_reference_test/seed.sql")

        # Run materialized_copy, ephemeral_copy, and their dependents
        # ephemeral_copy should not actually be materialized b/c it is ephemeral
        # the dependent ephemeral_summary, however, should be materialized as a table
        results = self.run_dbt(
            ['run', '--models', 'materialized_copy+', 'ephemeral_copy+']
        )
        self.assertEqual(len(results),  3)

        # Copies should match
        self.assertTablesEqual("seed","materialized_copy")

        # Summaries should match
        self.assertTablesEqual("summary_expected","materialized_summary")
        self.assertTablesEqual("summary_expected","ephemeral_summary")

        created_models = self.get_models_in_schema()

        self.assertFalse('incremental_copy' in created_models)
        self.assertFalse('incremental_summary' in created_models)
        self.assertFalse('view_copy' in created_models)
        self.assertFalse('view_summary' in created_models)

        # make sure this wasn't errantly materialized
        self.assertFalse('ephemeral_copy' in created_models)

        self.assertTrue('materialized_copy' in created_models)
        self.assertTrue('materialized_summary' in created_models)
        self.assertEqual(created_models['materialized_copy'], 'table')
        self.assertEqual(created_models['materialized_summary'], 'table')

        self.assertTrue('ephemeral_summary' in created_models)
        self.assertEqual(created_models['ephemeral_summary'], 'table')

    @attr(type='snowflake')
    def test__snowflake__simple_reference_with_models(self):
        self.use_profile('snowflake')
        self.use_default_project()
        self.run_sql_file("test/integration/003_simple_reference_test/seed.sql")

        # Run materialized_copy & ephemeral_copy
        # ephemeral_copy should not actually be materialized b/c it is ephemeral
        results = self.run_dbt(
            ['run', '--models', 'materialized_copy', 'ephemeral_copy']
        )
        self.assertEqual(len(results),  1)

        # Copies should match
        self.assertTablesEqual("SEED", "materialized_copy")

        created_models = self.get_models_in_schema()
        self.assertTrue('materialized_copy' in created_models)

    @attr(type='snowflake')
    def test__snowflake__simple_reference_with_models_and_children(self):
        self.use_profile('snowflake')
        self.use_default_project()
        self.run_sql_file("test/integration/003_simple_reference_test/seed.sql")

        # Run materialized_copy, ephemeral_copy, and their dependents
        # ephemeral_copy should not actually be materialized b/c it is ephemeral
        # the dependent ephemeral_summary, however, should be materialized as a table
        results = self.run_dbt(
            ['run', '--models', 'materialized_copy+', 'ephemeral_copy+']
        )
        self.assertEqual(len(results),  3)

        # Copies should match
        self.assertTablesEqual("SEED", "materialized_copy")

        # Summaries should match
        self.assertTablesEqual("SUMMARY_EXPECTED", "materialized_summary")
        self.assertTablesEqual("SUMMARY_EXPECTED", "ephemeral_summary")

        created_models = self.get_models_in_schema()

        self.assertFalse('incremental_copy' in created_models)
        self.assertFalse('incremental_summary' in created_models)
        self.assertFalse('view_copy' in created_models)
        self.assertFalse('view_summary' in created_models)

        # make sure this wasn't errantly materialized
        self.assertFalse('ephemeral_copy' in created_models)

        self.assertTrue('materialized_copy' in created_models)
        self.assertTrue('materialized_summary' in created_models)
        self.assertEqual(created_models['materialized_copy'], 'table')
        self.assertEqual(created_models['materialized_summary'], 'table')

        self.assertTrue('ephemeral_summary' in created_models)
        self.assertEqual(created_models['ephemeral_summary'], 'table')
