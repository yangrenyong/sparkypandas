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
Demo job for illustrating the toolkit
"""

import pandas as pd

from job_prepare import start_spark
from job_work import mean_diff_pandas_func
from sparkypandas.utils import SparkyPandasUtil


def main():
    # begin with some Pandas sku sales data
    sales_pandas_df = pd.DataFrame({'sku_id': ['hotel_1', 'hotel_2', 'hotel_3', 'hotel_1'],
                                    'sku_date': ['2019-01-01', '2019-01-01', '2019-01-02',
                                                 '2019-01-01'],
                                    'price': [300, 200, 150, 320],
                                    'source': [1, 2, 1, 1]},
                                   )

    # some mean price data from sales history
    history_mean_price_pandas_df = pd.DataFrame({
        'sku_id': ['hotel_1', 'hotel_2', 'hotel_3'],
        'mean_price': [305, 190, 140]
    })

    # calculate the mean diff data on single-core Pandas (NOTE the `source` argument)
    mean_diff_pandas_df = mean_diff_pandas_func(sales_pandas_df, history_mean_price_pandas_df, source=1)

    '''
    output:
       hotel_id  source  mean_diff
    0  hotel_1       1          5
    1  hotel_3       1         10
    '''
    print(mean_diff_pandas_df.head())

    # start the Spark environment
    spark = start_spark()

    # create the Pandas' Spark counterparts
    sales_spark_df = spark.createDataFrame(sales_pandas_df)

    history_mean_price_spark_df = spark.createDataFrame(history_mean_price_pandas_df)

    # the schema of the output Pandas data frame on each single-core computing, described by a map
    mean_diff_output_schema_map = {'sku_id': 'str', 'source': 'int', 'mean_diff': 'float64'}

    # parallelize the Pandas computing using Spark, passing the required & customized arguments (NOTE the `source`
    # argument)
    mean_diff_spark_df = SparkyPandasUtil.pandas_parallelize([sales_spark_df, history_mean_price_spark_df],
                                                             mean_diff_pandas_func,
                                                             mean_diff_output_schema_map,
                                                             partition_column='sku_id',
                                                             errors='quiet',
                                                             partitions=2, source=1)

    '''
    output:
    +--------+------+---------+
    |hotel_id|source|mean_diff|
    +--------+------+---------+
    | hotel_3|     1|     10.0|
    | hotel_1|     1|      5.0|
    +--------+------+---------+
    '''
    mean_diff_spark_df.show()


if __name__ == '__main__':
    main()
