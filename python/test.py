import lance
import pandas as pd
import pyarrow as pa
import pyarrow.dataset

df = pd.DataFrame({"m": [-5], "n": [-10]})
new_table = pyarrow.Table.from_pandas(df)

dataset = lance.dataset("/tmp/test.lance")
merged = dataset.merge(new_table, left_on="id", right_on="id")
print(merged.to_pandas())

