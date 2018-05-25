import pandas as pd




def agg_dataset_from_dict(details):
    
    """
    Input dataset and parameters for aggregation in dict. Creates attribute 'agg_dataset'.
    
    """
    
    
    details['agg_dataset'] = details['dataset'].groupby(details['aggregation level'])[details['variable']].sum()
    
    label_info_cols = details['aggregation level'][1:] + [col.replace('ID', 'label') for col in details['aggregation level'][1:]]
    df = details['dataset'].reset_index()[label_info_cols].drop_duplicates()

    df['label'] = details['label prefix'] + df[[col for col in df.columns if 'label' in col]].apply(' - '.join, axis=1)
    df = details['agg_dataset'].reset_index().merge(df[details['aggregation level'][1:] + ['label']])
    
    details['label_ref'] = df[['label'] + details['aggregation level'][1:]].drop_duplicates()
    details['agg_dataset'] = df.set_index(details['aggregation level'])
    details['agg_dataset'].columns = ['volume_USD', 'label']
    
    return details


def drop_small_cats(df, level = .90):
    by_label = df.groupby('label').sum().sort_values(by = 'volume_USD', ascending = False).dropna()
    cumsum = by_label.div(by_label.sum()).cumsum()
    label_sel = cumsum.loc[cumsum.volume_USD < level].index.values
    return df.loc[df.label.isin(label_sel)]

#df is concat_data
def demean_df(df):
    df = df
    dm1 = df - df.mean()
    dm2 = (dm1.T - dm1.T.mean()).T
    
    demeaned_df = dm2
    
    return demeaned_df
