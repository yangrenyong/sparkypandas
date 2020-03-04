# SparkyPandas: A small toolkit for parallel pandas computing using Spark
## Introduction
As one of the most important Python data analysis library, [Pandas](https://pandas.pydata.org/) has proven its great 
success on single-core computing with its many analysis tools, along with many well-established & powerful machine
learning & data mining algorithm libraries(such as [NumPy](https://numpy.org/), [statsmodels](https://www.statsmodels.org/stable/index.html) etc.) for scientists using Python. On the other hand,
[Spark](https://spark.apache.org/) with its brother PySpark has been widely used for big data analysis, which can take advantage
of a distributed computing cluster with theoretically limitless number of CPU cores and memories for colossal data analysis.
However, tools are limited for Spark though there're libraries like [Spark MLib](https://spark.apache.org/mllib/), which contains
only a few algorithms in the Spark MLib. Things get a little hard.<br/>
So, can we make use the power of Pandas on one hand, and the power of distributed parallel computing of Spark/PySpark on the
other hand? Yes of course. The [Pyspark Pandas UDF](https://databricks.com/blog/2017/10/30/introducing-vectorized-udfs-for-pyspark.html)
is introduced in Spark 2.3 for algorithm and data scientists for Pandas-on-Spark. <br/>
Based on the PySpark Pandas UDF framework,
this small toolkit intends to fill up the gap between the PySpark side and the Pandas side, try to make the interaction between
the both seamlessly, and keeps both of the sides separately. It tries to illustrate a common structure which makes Pandas scientists focus on the pandas computing as long as
he/she follows some paradigm, and the big data engineer focus on the Spark side.
## How to use
Assume there're pandas function which accepts one or more pandas dataframe on the leading and other arguments on the tail:</br>
<pre>
<code>
def some_pandas_func(pands_dataframe_a, pandas_dataframe_b, .., param_1, param_2,..):
  # do some calculating
  ...
  # returns a result pandas dataframe
  return result_pandas_dataframe
</code>
</pre>
<em><strong>NOTE: All of the pandas dataframes must have an unified `'ID'` column whose values can be used as the criteria
for parallel computing.</strong></em> E.g. if the rows describes a person, the `ID` column could be the person's SSN
or the internal id. Now on the distributed computing, the counterpart of the single-core pandas dataframes could be a
distributed Spark RDD dataframe, i.e.
<pre>
<code>
spark_dataframe_a, spark_dataframe_b,..
</code>
</pre>
All you needed to do is make the call of:
<pre>
<code>
result_pands_df = some_pandas_func(pands_dataframe_a, pandas_dataframe_b, .., param_1, param_2,..)
</code>
</pre>
map to
<pre>
<code>
from sparkypandas.utils import SparkyPandasUtil
result_spark_df = SparkyPandasUtil.pandas_parallelize([spark_dataframe_a, spark_dataframe_b,..], some_pandas_func, output_pandas_schema_map, param_1, param_2, partition_column='id', partitions=100)
</code>
</pre>
And the `result_spark_df` Spark dataframe is the final result which utilized the power of Pandas and Spark both. <br/>
The [demo job](https://github.com/yangrenyong/sparkypandas/blob/master/demo_job/demo_job.py) demonstrates a usage of 
this small toolkit: two SKU(Stock Keeping Unit) data including [sku_id, sku_date, price, source] 
(`sku_id`: the id of the product, `sku_date`: the date of the sku such as the room-night 
of a hotel room, the `price` of the sku sold, and the `source` is where the sales came from like APP or Web) and 
[sku_id, mean_price](`sku_id` is the id of the product, and `mean_price` is the mean price sold calculated by some 
historical data). The job tries to illustrates some usage: calculating the deviation of current SKU sales for
some specific `source` from the historical mean price, so the fluctuations could be an important metric for some further
calculations <br/>
<em><strong>NOTE: The version of `pyarrow` must be <=1.4.1 if you are using `pyspark` 2.3.x and 2.4.x because  
newer versions are not compatible with spark 2.3.x and 2.4.x</strong></em> <br/>
<em><strong>NOTE: <br/>
<em>ARGUMENTS</em><br/>
`some_pandas_func`: The pandas function used for single-core computation <br/>
`output_pandas_schema_map`: Describes the schema of the result pandas dataframe when sliced to single-core pandas computing,
e.g. {'column_a': 'str', 'column_b': 'float64'} <br/>
<em>KEYWORD ARGUMENTS</em><br/>
`partitions`: This keyword argument is preserved and optional which provides you the opportunity to set 
how many partitions would be used, if not provided, the number of distinct values of the `partition_column`(see below)
would be used <br/>
`partition_column`: The column name of the input Spark data frames whose values will be used for partitioning<br/>
`errors`: Can be `quiet` or `raise`. If `quiet`, the exceptions will be logged and ignored during the Pandas computing,
otherwise will be thrown to fail the job
</strong></em>
## Behind the scene
The scene behind is demonstrated in the picture:<br/>
![avatar](https://github.com/yangrenyong/sparkypandas/blob/master/parallel_pandas.png)</br>
The overall principle is using the Spark `unionAll` operator to union all of the multiple input dataframes, repartition
and reshuffle all the data with the same partition key to the same computing node, and restore the shuffled partition
to multiple pandas dataframe, calling the pandas function to work, getting the results, and assemble them back to Spark
distributed RDD dataframe.
## Thanks
Some of the code of the logging module is copied and slightly modified from
 [stackoverflow](https://stackoverflow.com/questions/40806225/pyspark-logging-from-the-executor), thanks
 the original author [Oliver W.](https://stackoverflow.com/users/2476444/oliver-w) for the excellent answers.