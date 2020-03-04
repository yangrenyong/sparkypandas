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
Some simple business work
"""


def mean_diff_pandas_func(sales_pandas_df, history_mean_price_pandas_df, source=2):
    sales_pandas_df = sales_pandas_df[sales_pandas_df.source == source]
    mean_sales_df = sales_pandas_df.groupby('sku_id', as_index=False).mean()
    mean_diff_prepare_df = mean_sales_df.merge(history_mean_price_pandas_df, on='sku_id')
    mean_diff_prepare_df['mean_diff'] = mean_diff_prepare_df.price - mean_diff_prepare_df.mean_price
    return mean_diff_prepare_df[['sku_id', 'source', 'mean_diff']]
