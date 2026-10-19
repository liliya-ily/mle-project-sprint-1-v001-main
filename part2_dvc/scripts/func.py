import pandas as pd


def make_ordinary_col(data, cat_ordinal_col):
    ordinal_categories = ["low", "med", "hig"]
    for col in cat_ordinal_col:
        data[col] = pd.Categorical(
            data[col], categories=ordinal_categories, ordered=True
        )


def bool_to_int(x):
    return x.astype(int)
