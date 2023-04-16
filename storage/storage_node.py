import os
from functools import lru_cache
from hashlib import sha256
import datetime
from pyarrow import parquet
import pandas as pd


def convert_bytes(size):
    """ Convert bytes to KB, or MB or GB"""  # https://pynative.com/python-get-file-size/
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return "%3.2f %s" % (size, x)
        size /= 1024.0


def unix_time_to_timestamp(unix_time):
    return datetime.datetime.fromtimestamp(unix_time)


class StorageNode:
    """ Interface for pandas dataframes / parquets to coordinate memory + disk + cloud
    """

    def __init__(self, key, df=None) -> None:
        self.key = key
        self.df = df
        self.disk_folder = os.path.join(os.path.split(__file__)[0], '.disk')
        self.disk_path = os.path.join(self.disk_folder, key)

    def put_to_disk(self, df):
        df.to_parquet(self.disk_path)

    def get_from_disk(self):
        if self.df is None:
            self.df = pd.read_parquet(self.disk_path)
        return self.df

    def put_to_cloud(self, df):
        ...

    def get_from_cloud(self):
        ...

    def to_csv(self):  # todo: rename?
        self.df.to_csv(self.key.replace('.parquet', '.csv'))

    @property
    def metadata(self):
        return parquet.read_metadata(self.disk_path)

    @property
    def schema(self):
        return parquet.read_schema(self.disk_path)

    @property
    def last_modified_disk(self):
        return unix_time_to_timestamp(
            os.path.getmtime(self.disk_path)
        )

    def block_size_bytes_disk(self):  # todo: normalize functions across disk and cloud
        return os.stat(self.disk_path).st_size

    def block_size_disk(self):
        size = self.block_size_bytes_disk()
        return convert_bytes(size)

    def mem_size_bytes_disk(self):
        return self.get_from_disk().size

    def mem_size(self):
        size = self.mem_size_bytes_disk()
        return convert_bytes(size)

    def hash(self, df):
        # by: https://stackoverflow.com/users/14908558/edmz?tab=profile
        # here: https://stackoverflow.com/questions/31567401/get-the-same-hash-value-for-a-pandas-dataframe-each-time/69890529#69890529
        s = str(df.columns) + str(df.index) + str(df.values)
        return sha256(s.encode()).hexdigest()

    def make_memory_hash(self):
        return self.hash(self.df)

    def make_disk_hash(self):
        return self.hash(self.get_from_disk())


def prepare():
    time = pd.date_range('1970-01-01', 'today', freq='MS', tz='UTC', name='calendar_day')
    epoch_time = time.astype(
        'int64') // 1e9  # https://stackoverflow.com/questions/35630098/convert-a-column-of-datetimes-to-epoch-in-python
    print(epoch_time)

    world_cities_ = pd.read_csv('https://datahub.io/core/world-cities/r/world-cities.csv')
    world_cities = world_cities_.set_index('geonameid', verify_integrity=True).rename(columns={"name": "city"})

    print(world_cities)

    dataframe = pd.DataFrame(
        index=pd.MultiIndex.from_product([time, world_cities.index]),
        data=0,
        columns=pd.Index(['dummy'], name='measure')
    )

    dataframe['dummy'] = dataframe['dummy'].astype('category')

    print(dataframe)

    storage_node = StorageNode('test.parquet')
    storage_node.put_to_disk(dataframe)


def explore():
    storage_node = StorageNode('test.parquet')
    print(storage_node.last_modified_disk)

    print(storage_node.mem_size())
    print(storage_node.block_size_disk())
    # storage_node.to_csv()

    print(storage_node.metadata)
    print(storage_node.schema)

    print("Disk Hash:")
    disk_hash = storage_node.make_memory_hash()
    print(disk_hash)
    print("Memory Hash:")
    memory_hash = storage_node.make_disk_hash()
    print(memory_hash)
    print("Hashes equal:")
    print(disk_hash == memory_hash)


if __name__ == "__main__":
    prepare()
    explore()
