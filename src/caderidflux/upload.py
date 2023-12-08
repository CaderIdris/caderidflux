""" Contains classes and methods that write data to InfluxDB v2.0
database

Communicates with InfluxDB 2.0 instance, location specified by config
file and writes data to it synchronously. It accepts data in varying
formats (TBD, currently only accepts list of jsons)
"""
import datetime as dt
from typing import Dict, List, TypeAlias, TypedDict, Union

import pandas as pd

from influxdb_client import InfluxDBClient, WriteOptions
from influxdb_client.client.write_api import SYNCHRONOUS


class Container(TypedDict):
    time: dt.datetime
    measurement: str
    field: Dict[str, Union[int, float]]
    tag: Dict[str, str]


ListOfContainers: TypeAlias = List[Container]


class InfluxWriter:
    def __init__(self, ip: str, port: str, token: str, organisation: str, bucket: str):
        """Initialises class

        """
        if port != "":
            url = f"{ip}:{port}"
        else:
            url = ip

        self._client = InfluxDBClient(
            url=url,
            token=token,
            org=organisation,
            timeout=15000000,
        )
        self.ip = ip
        self.port = port
        self.token = token
        self.organisation = organisation
        self.bucket = bucket
        self.write_client = self._client.write_api(write_options=SYNCHRONOUS)

    def write_container_list(self, list_of_containers: ListOfContainers):
        """
        Writes list of containers to an InfluxDB 2.0 database

        Parameters
        ----------
        list_of_containers : ListOfContainers
            Data to upload
        """
        self.write_client.write(
            self.bucket, self.organisation, list_of_containers
        )
    
    def write_dataframe(self, df: pd.DataFrame, measurement: str):
        """
        Writes dataframes to an InfluxDB 2.x database

        Parameters
        ----------
        df : pd.DataFrame
            DataFrame to upload
        measurement : str
            Measurement name
        """
        tag_cols = [col[0] for col in df.items() if col[1].dtype == 'object']
        self.write_client.write(
            self.bucket,
            self.organisation,
            record=df,
            data_frame_measurement_name=measurement,
            data_frame_tag_columns=tag_cols
        )
