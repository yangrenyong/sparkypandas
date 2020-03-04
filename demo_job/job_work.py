# Job business


def mean_diff_pandas_func(sales_pandas_df, history_mean_price_pandas_df, source=2):
    sales_pandas_df = sales_pandas_df[sales_pandas_df.source == source]
    mean_sales_df = sales_pandas_df.groupby('sku_id', as_index=False).mean()
    mean_diff_prepare_df = mean_sales_df.merge(history_mean_price_pandas_df, on='sku_id')
    mean_diff_prepare_df['mean_diff'] = mean_diff_prepare_df.price - mean_diff_prepare_df.mean_price
    return mean_diff_prepare_df[['sku_id', 'source', 'mean_diff']]
