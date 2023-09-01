import os
import ibis

from dagster import ConfigurableIOManager


class ParquetIOManager(ConfigurableIOManager):
    """
    Manage tables as parquet files.
    """

    extension = "parquet"
    base_path = os.path.join("data", "system", "parquet")

    def handle_output(self, context, obj):
        dirname, filename = self._get_paths(context)
        os.makedirs(dirname, exist_ok=True)
        output_path = os.path.join(dirname, filename)
        obj.to_parquet(output_path)

    def load_input(self, context):
        dirname, filename = self._get_paths(context)
        input_path = os.path.join(dirname, filename)
        return ibis.read_parquet(input_path)

    def _get_paths(self, context):
        group_name = context.step_context.job_def.asset_layer.assets_def_for_asset(
            context.asset_key
        ).group_names_by_key[context.asset_key]
        dirname = os.path.join(self.base_path, group_name, *context.asset_key.path[:-1])
        filename = f"{context.asset_key.path[-1]}.{self.extension}"
        return dirname, filename


class DeltaIOManager(ConfigurableIOManager):
    """
    Manage tables as delta tables.
    """

    extension = "delta"
    base_path = os.path.join("data", "system", "delta")
    delta_write_mode = "overwrite"

    def handle_output(self, context, obj):
        dirname, filename = self._get_paths(context)
        os.makedirs(dirname, exist_ok=True)
        output_path = os.path.join(dirname, filename)
        obj.to_delta(output_path, mode=self.delta_write_mode)

    def load_input(self, context):
        dirname, filename = self._get_paths(context)
        input_path = os.path.join(dirname, filename)
        return ibis.read_delta(input_path)

    def _get_paths(self, context):
        group_name = context.step_context.job_def.asset_layer.assets_def_for_asset(
            context.asset_key
        ).group_names_by_key[context.asset_key]
        dirname = os.path.join(self.base_path, *context.asset_key.path)
        filename = f"{context.asset_key.path[-1]}.{self.extension}"
        return dirname, filename


class DuckDBIOManager(ConfigurableIOManager):
    """
    Manage tables as duckdb files.
    """

    extension = "ddb"
    base_path = os.path.join("data", "system", "duckdb")

    def handle_output(self, context, obj):
        dirname, filename = self._get_paths(context)
        os.makedirs(dirname, exist_ok=True)
        output_path = os.path.join(dirname, filename)
        con = ibis.duckdb.connect(output_path)
        con.create_table(context.asset_key.path[-1], obj.to_pyarrow(), overwrite=True)

    def load_input(self, context):
        dirname, filename = self._get_paths(context)
        input_path = os.path.join(dirname, filename)
        con = ibis.duckdb.connect(input_path)
        return con.table(context.asset_key.path[-1])

    def _get_paths(self, context):
        group_name = context.step_context.job_def.asset_layer.assets_def_for_asset(
            context.asset_key
        ).group_names_by_key[context.asset_key]
        dirname = os.path.join(self.base_path, *context.asset_key.path)
        filename = f"{context.asset_key.path[-1]}.{self.extension}"
        return dirname, filename
