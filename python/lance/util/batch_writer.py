import lance
from lance.data.convert.base import DatasetConverter
import pyarrow as pa
import math
import pandas as pd

class BatchWriter:
    def __init__(
        self,
        converter: DatasetConverter,
        write_to_loc: str = "/tmp/",
        append: bool = True,
        to_image: bool = True,
        batch_size: int = 500,
        verbose_mode: bool = False
    ):
        if converter is None:
            raise ValueError("The batch writer needs a converter, it cannot be None.")

        self.converter = converter
        self.write_to_loc = write_to_loc
        self.append = append
        self.to_image = to_image
        self.batch_size = batch_size
        self.verbose_mode = verbose_mode

    def write_in_batches(self):
        df = self.converter.read_metadata()
        max_rows = len(df.index)
        num_pages = math.ceil(max_rows / self.batch_size)
        for i in range(num_pages):
            page_floor = i * self.batch_size
            if i == num_pages - 1:
                page_ceil = max_rows - 1
            else:
                page_ceil = page_floor + (self.batch_size - 1)
            df_slice = df.loc[page_floor:page_ceil]
            table = self.converter.to_table(df=df_slice, to_image=self.to_image)
            lance.write_dataset(table, self.dataset_loc, mode="append" if (self.append or i > 0) else "create")
            if self.verbose_mode:
                print(f"just wrote a new batch of {self.batch_size} with page number of {i + 1} out of a total {num_pages}")