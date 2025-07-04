import time
from argparse import ArgumentParser, Namespace
from typing import Any

import pyarrow as pa
from data_processing.transform import AbstractTableTransform, TransformConfiguration
from data_processing.utils import CLIArgumentProvider, TransformUtils


short_name = "my"
cli_prefix = f"{short_name}_"

column_key = "column"
column_cli_param = f"{cli_prefix}{column_key}"


class MyTransform(AbstractTableTransform):

    def __init__(self, config: dict[str, Any]):
        super().__init__(config)
        self.column = config.get(column_key, "text")

    def transform(self, table: pa.Table, file_name: str = None) -> tuple[list[pa.Table], dict[str, Any]]:
        TransformUtils.validate_columns(table=table, required=[self.column])

        result = pa.Table.from_pylist(
            [{"length": len(x)} for x in table[self.column].to_pylist()]
        )
        
        new_table = TransformUtils.add_column(table=table, name="length", content=result["length"])

        print(f"Transforming one table with {len(new_table)} rows")
        
        metadata = {"nfiles": 1, "nrows": len(new_table)}
        return [new_table], metadata


class MyTransformConfiguration(TransformConfiguration):

    def __init__(self):
        super().__init__(
            name=short_name,
            transform_class=MyTransform,
        )
        from data_processing.utils import get_logger

        self.logger = get_logger(__name__)

    def add_input_params(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            f"--{column_cli_param}",
            type=str,
            default="text",
            help="Specify the column name for analysis",
        )

    def apply_input_params(self, args: Namespace) -> bool:
        """
        Validate and apply the arguments that have been parsed
        :param args: user defined arguments.
        :return: True, if validate pass or False otherwise
        """
        captured = CLIArgumentProvider.capture_parameters(args, cli_prefix, False)
        self.params = self.params | captured
        self.logger.info(f"my parameters are : {self.params}")
        return True
