#
# MIT License
#
# Copyright (c) 2020 yangrenyong
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

"""
A small toolkit for parallelize the computing use both Spark & Pandas
"""

import traceback
import uuid
from functools import reduce
from typing import List, Tuple

import pandas as pd
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, lit
from pyspark.sql.functions import pandas_udf, PandasUDFType
from pyspark.sql.types import StringType, ByteType, LongType, DoubleType, FloatType, IntegerType, BooleanType
from pyspark.sql.types import StructType, StructField, DataType

import sparkypandas._logging_ as logging

_SPARK_TO_PANDAS_MAP = {
    StringType: 'str',
    ByteType: 'int',
    LongType: 'int64',
    DoubleType: 'float64',
    FloatType: 'float',
    IntegerType: 'int',
    BooleanType: 'bool'
}

_PANDAS_TO_SPARK_MAP = {
    'str': StringType(),
    'int': IntegerType(),
    'int64': LongType(),
    'bool': BooleanType(),
    'float': FloatType(),
    'float64': DoubleType()
}

PARTITIONS_KEY = 'partitions'

PARTITION_COLUMN_KEY = 'partition_column'

ERRORS_KEY = 'errors'

ERRORS_QUIET = 'quiet'

ERRORS_RAISE = 'raise'


class _DataFrameUtil:
    @staticmethod
    def add_constant_column_to_df(df, column_name: str, constant_val):
        col_set = set(df.columns)
        if column_name in col_set:
            raise ValueError(
                'column_name: {} already exists in column set: {} for data frame'.format(column_name, col_set))
        df = df.withColumn(column_name, lit(constant_val))
        return df

    @staticmethod
    def union_df(filter_column_name, union_index_lst):
        dfs_with_select_filter = [
            _DataFrameUtil.add_constant_column_to_df(union_index[0], filter_column_name, union_index[1]) for union_index
            in union_index_lst]
        all_column_set = reduce(lambda x, y: set(x.columns).union(set(y.columns)), dfs_with_select_filter)

        def fill_expr(cols, all_cols):
            lst = list()
            for c in all_cols:
                if c in cols:
                    lst.append(col(c))
                else:
                    lst.append(lit(None).alias(c))
            return lst

        unified_df_list = [df.select(*fill_expr(set(df.columns), all_column_set)) for df in dfs_with_select_filter]
        union_df = reduce(lambda x, y: x.unionAll(y), unified_df_list)
        return union_df


class SparkyPandasUtil:

    @staticmethod
    def compose_schema_from_column_with_type_list(column_with_type_list: List[Tuple[str, DataType]]):
        fld_lst = [StructField(f[0], f[1]) for f in column_with_type_list]
        return StructType(fld_lst)

    @staticmethod
    def _compose_pandas_input_type_conversion_map(df: DataFrame):
        result_map = {}
        for fld in df.schema.fields:
            name = fld.name
            data_type = type(fld.dataType)
            pandas_type_str = _SPARK_TO_PANDAS_MAP.get(data_type)
            if pandas_type_str is None:
                raise ValueError('unrecognized type: ' + str(data_type))
            result_map[name] = pandas_type_str
        return result_map

    @staticmethod
    def _compose_udf_output_schema(column_with_type_map: map):
        field_list = []
        for column_name, type_str in column_with_type_map.items():
            field_type = _PANDAS_TO_SPARK_MAP.get(type_str)
            if field_type is None:
                raise TypeError('unknown type: ' + type_str)
            field_list.append(StructField(column_name, field_type))
        return StructType(field_list)

    @staticmethod
    def _extract_kwarg_with_modify(kwarg: dict, key, default_value):
        value = kwarg.get(key)
        if value is None:
            return default_value
        del kwarg[key]
        return value

    @staticmethod
    def pandas_parallelize(solo_or_list_df, pandas_func, output_schema_map, *args, **kwargs):
        """
        Parallelize the computing of distributed spark rdd data frame with slices of pandas data frame computing
        :param solo_or_list_df: One or a list of rdd data frame, each will be sliced as partitions and each be converted
        to pandas data frame then passed as input for the pandas processing function. See the #pandas_func parameter
        below
        :param pandas_func: The pandas data processing function, which accepts one or more pandas data frames(plus
        other parameters), and returning the result which must be a pandas data frame as well
        :param output_schema_map: The schema of output pandas data frame passed as a map, e.g.{'column_a':'str',
        'column_b':'float64'}
        :param args: The extra positional arguments which will be passed to the #pandas_func function
        :param kwargs: The extra dict arguments which will be passed to the #pandas_func function.
        :keyword 'partition_column': The column name which will be used as the partition
        :keyword 'partitions': Number of partitions the spark data frame will be split, or the number of hotel ids will be used
        :keyword 'errors': `raise` or `quiet` can be set. If `raise`, any exception will be thrown during the pandas
        computing, otherwise the exception for the partition will be logged and ignored as normal.
        :return: The overall pandas processed results, as the distributed rdd data frame
        :rtype: pyspark.sql.dataframe.DataFrame
        :raises TypeError if pandas data type in `output_schema_map` not supported or wrong
        :raises ValueError if required arguments not provided
        :raises RuntimeError if column number of pandas result data frame not equal to column number specified by
        `output_schema_map`
        :raises Exception if errors != 'quiet' on pandas computing
        """
        partition_column = SparkyPandasUtil._extract_kwarg_with_modify(kwargs, PARTITION_COLUMN_KEY, None)
        if partition_column is None:
            raise ValueError("value for keyword argument '{}' not found!".format(PARTITION_COLUMN_KEY))
        errors = SparkyPandasUtil._extract_kwarg_with_modify(kwargs, ERRORS_KEY, ERRORS_RAISE)
        if isinstance(solo_or_list_df, list):
            return SparkyPandasUtil._pandas_parallelize_multiple(solo_or_list_df, pandas_func, output_schema_map,
                                                                 partition_column, errors, *args, **kwargs)
        else:
            return SparkyPandasUtil._pandas_parallelize_solo(solo_or_list_df, pandas_func, output_schema_map,
                                                             partition_column, errors, *args, **kwargs)

    @staticmethod
    def _call_pandas_func(solo_or_list_pandas_df, pandas_func, pandas_output_schema_map, partition_column, errors,
                          partition_ids, *args, **kwargs):
        expected_columns = len(pandas_output_schema_map.keys())
        df = pd.DataFrame(columns=list(pandas_output_schema_map.keys()))
        actual_columns_count = len(pandas_output_schema_map.keys())
        try:
            df = pandas_func(*solo_or_list_pandas_df, *args, **kwargs) if isinstance(solo_or_list_pandas_df,
                                                                                     list) else pandas_func(
                solo_or_list_pandas_df, *args, **kwargs)
            actual_columns_count = len(df.columns)
        except Exception as e:
            if errors == ERRORS_RAISE:
                raise RuntimeError('exception with partition column: {partition_column} happened, partition ids: '
                                   '{partition_ids}'.format(partition_column=partition_column,
                                                            partition_ids=partition_ids)) from e
            else:
                logger = logging.BasicLogger()
                logger.warning(
                    'exception happened: {}, partition ids: {}'.format(traceback.format_exc(), partition_ids))
        if actual_columns_count != expected_columns:
            # raise the exception before PySpark UDF caught it
            raise RuntimeError(
                'unexpected columns, expected: {}, actual: {}, partition ids: {}'.format(expected_columns,
                                                                                         actual_columns_count,
                                                                                         partition_ids))
        return df

    @staticmethod
    def _pandas_parallelize_solo(spark_df, pandas_func, pandas_output_schema_map, partition_column, errors, *args,
                                 **kwargs):
        udf_output_schema = SparkyPandasUtil._compose_udf_output_schema(pandas_output_schema_map)

        partition_num = SparkyPandasUtil._extract_kwarg_with_modify(kwargs, PARTITIONS_KEY, spark_df.select(
            partition_column).distinct().count())

        @pandas_udf(udf_output_schema, PandasUDFType.GROUPED_MAP)
        def udf_func_solo(pandas_df):
            partition_ids = set(pandas_df[partition_column])
            return SparkyPandasUtil._call_pandas_func(pandas_df, pandas_func, pandas_output_schema_map,
                                                      partition_column, errors, partition_ids, *args, **kwargs)

        return spark_df.repartition(partition_num).groupby(partition_column).apply(udf_func_solo)

    @staticmethod
    def _pandas_parallelize_multiple(spark_df_list: list, pandas_func, pandas_output_schema_map, partition_column,
                                     errors, *args, **kwargs):
        zipped_list = []
        index = 0
        union_index_lst = list()

        class _ZippedInfo(object):
            def __init__(self, idx, type_map):
                self.idx = idx
                self.type_cast_map = type_map

        for spark_df in spark_df_list:
            type_cast_map = SparkyPandasUtil._compose_pandas_input_type_conversion_map(spark_df)
            zipped_list.append(_ZippedInfo(index, type_cast_map))
            union_index_lst.append((spark_df, index))
            index += 1
        filter_column_name = str(uuid.uuid4().hex)
        union_df = _DataFrameUtil.union_df(filter_column_name, union_index_lst)
        udf_output_schema = SparkyPandasUtil._compose_udf_output_schema(pandas_output_schema_map)
        partition_num = SparkyPandasUtil._extract_kwarg_with_modify(kwargs, PARTITIONS_KEY, union_df.select(
            partition_column).distinct().count())

        @pandas_udf(udf_output_schema, PandasUDFType.GROUPED_MAP)
        def udf_func(un_df):
            pandas_dfs_list = []
            partition_ids = set(un_df[partition_column])
            for zipped in zipped_list:
                pandas_df = un_df[un_df[filter_column_name] == zipped.idx]
                cast_map = zipped.type_cast_map
                pandas_df = pandas_df.astype(cast_map)
                pandas_dfs_list.append(pandas_df[list(cast_map.keys())])
            return SparkyPandasUtil._call_pandas_func(pandas_dfs_list, pandas_func, pandas_output_schema_map,
                                                      partition_column, errors, partition_ids, *args, **kwargs)

        return union_df.repartition(partition_num).groupby(partition_column).apply(udf_func)
