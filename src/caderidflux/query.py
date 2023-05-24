""" Handles all communication to and from InfluxDB 2.x database when querying
measurements

Capable of generating both simple and complex queries in FluxQL format and
querying data from an InfluxDB 2.x database with them.

    Classes:
        InfluxQuery: Queries and formats data from InfluxDB 2.x database

        FluxQuery: Generates flux query for InfluxDB 2.x database

    Methods:
        dt_to_rfc3339: Converts datetime object to RFC3339 string used in
        queries to InfluxDB database

"""

__author__ = "Idris Hayward"
__copyright__ = "2021, Idris Hayward"
__credits__ = ["Idris Hayward"]
__license__ = "GNU General Public License v3.0"
__version__ = "0.4"
__maintainer__ = "Idris Hayward"
__email__ = "CaderIdrisGH@outlook.com"
__status__ = "Beta"

import dateutil.relativedelta as rd
import datetime as dt
from typing import Any, List, Literal, Optional, Union

from influxdb_client import InfluxDBClient
import numpy as np
import pandas as pd


class InfluxQuery:
    """Queries and formats data from InfluxDB 2.x database

    Attributes:
        _config (dict): The config file passed in via keyword argument during
        initialisation

        _client (InfluxDBClient): Object that handles connection to InfluxDB
        2.x database

        _query_api (InfluxDBClient.query_api): Handles queries to InfluxDB 2.x
        database

        _measurements (DataFrame): Measurements and timestamps from query

    Methods:
        data_query: Queries the InfluxDB database for the specified
        measurements and stores them in the measurements instance

        return_measurements: Returns measurements downloaded from InfluxDB
        database as a dictionary

        clear_measurements: Clears all measurements from the _measurements
        instance
    """

    def __init__(self, ip: str, port: str, token: str, organisation: str):
        """Initialises class

        Keyword Arguments:
            config (dict): Keys correspond to location and access info for
            InfluxDB 2.x database. Keys are:
                IP: IP/URL of database, localhost if on same machine
                Port: Port for database
                Token: Authorisation token to access database
                Organisation: Organisation of auth token
            corresponding organisation

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
        self._query_api = self._client.query_api()
        self._measurements: pd.DataFrame = pd.DataFrame()

    def custom_data_query(self, query):
        """Sends flux query, receives data and sorts it in to the measurement
        dict.

        Keyword Arguments:
            query (str): Flux query
        """
        query_return = self._query_api.query(
            query=query, org=self.organisation
        )
        # query_return should only have one table so this just selects the
        # first one
        if query_return:
            measurements = list()
            datetime = list()
            for record in query_return[0].records:
                values = record.values
                raw_measurement = values["_value"]
                if raw_measurement is None:
                    raw_measurement = np.nan
                measurements.append(raw_measurement)
                datetime.append(values["_time"])
            self._measurements = pd.DataFrame(
                data={"Datetime": datetime, "Values": measurements}
            )

    def data_query(
        self,
        bucket: str,
        start_date: dt.datetime,
        end_date: dt.datetime,
        measurement: str,
        fields: Union[list[str], str],
        groups: Union[list[str], str],
        win_range: str = "1h",
        win_func: str = "mean",
        bool_filters: dict[str, str] = dict(),
        range_filters: list[dict[str, Union[str, float, int, bool]]] = list(),
        hour_beginning: bool = False,
        scaling: list[dict[str, Union[str, int, float]]] = list(),
        multiindex: bool = False,
        aggregate: bool = False,
        time_split: Optional[
            Literal[
                'hour',
                'day',
                'week',
                'month',
                'year'
            ]
        ] = None
    ):
        """
        """
        time_dict: dict[
                Optional[str],
                Any
                ] = {
            'hour': {
                'Difference': (
                    (end_date - start_date).days * 24
                    ) + (
                        (end_date - start_date).seconds // 3600
                    ),
                'Timedelta': dt.timedelta(hours=1)
            },
            'day': {
                'Difference': (end_date - start_date).days,
                'Timedelta': dt.timedelta(days=1)
            },
            'week': {
                'Difference': int(np.ceil((end_date - start_date).days / 7)),
                'Timedelta': dt.timedelta(days=7)
            },
            'month': {
                'Difference': (
                    (end_date.year - start_date.year) * 12
                    ) + (
                        end_date.month - start_date.month
                        ),
                'Timedelta': rd.relativedelta(months=1)
            },
            'year': {
                'Difference': (end_date.year - start_date.year),
                'Timedelta': rd.relativedelta(years=1)
            },
            None: {
                'Difference': 1,
                'Timedelta': dt.timedelta(
                    seconds=(
                        (end_date - start_date).days * 86400
                        ) + (
                            end_date - start_date
                            ).seconds
                        )
            }
        }
        if isinstance(fields, str):
            fields = [fields]
        if isinstance(groups, str):
            groups = [groups]
        diff: int = time_dict[time_split]['Difference']
        for t_split in range(diff):
            start_t = start_date + (
                    time_dict[time_split]['Timedelta'] * t_split
                    )
            end_t = start_date + (
                    time_dict[time_split]['Timedelta'] * (t_split + 1)
                    )
            query = CustomFluxQuery(start_t, end_t, bucket, measurement)
            extra_fields = [
                    fr['Field'] for fr in range_filters
                    if fr['Field'] not in fields
                ]
            all_fields = fields.copy()
            all_fields.extend(extra_fields)
            query.add_field(all_fields)
            query.add_groups(groups + ["_field"])
            for key, value in bool_filters.items():
                if not isinstance(value, dict):
                    query.add_filter(key, value)
            for key, value in bool_filters.items():
                if isinstance(value, dict):
                    query.add_specific_filter(
                            key=key,
                            value=value.get('Value'),
                            col=value.get('Col')
                            )
            if range_filters:
                query.add_filter_range(range_filters, groups)
            if aggregate:
                query.add_window(
                        win_range,
                        win_func,
                        time_starting=hour_beginning
                        )
            if len(fields) > 1 or multiindex or len(groups) == 0:
                query.add_pivot(groups + ["_field"])
            else:
                query.add_pivot(groups)
            for scale_conf in scaling:
                query.add_scaling(scale_conf)
            print(query.return_query())
            query_return = self._query_api.query_data_frame(
                query=query.return_query(),
                org=self.organisation
            )
            if not query_return.empty:
                data: pd.DataFrame = query_return.drop(
                        ['result', 'table', '_start', '_stop'], axis=1
                        ).set_index('_time')
                data.index = pd.to_datetime(data.index)
                data = data.drop(extra_fields, axis=1)
                if len(fields) == 1 and "_field" in data.columns:
                    data = data.drop(["_field"], axis=1)
                if multiindex:
                    m_idx = data.columns.str.split('_', expand=True)
                    data.columns = m_idx
                self._measurements = pd.concat([self._measurements, data])

        self._measurements = self._measurements[
                ~self._measurements.index.duplicated(keep='first')
                ].sort_index()

    def return_measurements(self):
        """Returns the measurements downloaded from the database

        Returns:
            Copy of self._measurements (dict)
        """
        if self._measurements is not None:
            return self._measurements
        else:
            return None

    def clear_measurements(self):
        """Clears measurements downloaded from database

        Returns:
            None
        """
        self._measurements = pd.DataFrame()


class CustomFluxQuery:
    """Generates flux query for InfluxDB 2.x database

    InfluxDB 2.x uses the flux query language to query metadata and data. This
    class simplifies the query generation process.

    Attributes:
        _query_list (list): List of components of a Flux query

    Methods:
        add_field: Adds a field (measurand) to the query

        add_multiple_fields: Adds multiple fields to the query

        add_filter: Adds a key and value to the query, all other values that
        the key has are filtered out

        add_filter_range: Filters the measurements by tags within a specified
        range

        add_group: Adds a group to the query, all measurements are grouped by
        the specified key

        add_window: Adds a window aggregator to the query, measurements will be
        aggregated to specified time windows by the specified function (e.g
        hourly means)

        add_yield: Adds an output function, measurements are output with the
        specified name

        drop_start_stop: Drops the start and stops columns from the data
        returned from InfluxDB. Reduces data returned from database.

        scale_measurements: Scales measurements within a specified date range
        by a specified slope and offset

        return_query: Returns the query as a string, the query can't be
        accessed outside of the class


    """

    def __init__(self, start, end, bucket, measurement):
        """Initialises the class instance

        Keyword Arguments:
            start (datetime): Start time of data queried

            end (datetime): End time of data queried

            bucket (str): Bucket where data is stores

            measurement (str): Measurement tag where data is stored
        """
        self._query_list = [
            'import "internal/debug"',
            'import "experimental"',
            f'from(bucket: "{bucket}")',
            f"  |> range(start: {dt_to_rfc3339(start)}, "
            f"stop: {dt_to_rfc3339(end)})",
            f"  |> filter(fn: (r) => r._measurement == " f'"{measurement}")',
        ]
        self._start = start
        self._end = end

    def add_field(self, fields: Union[list[str], str]):
        """Adds a field to the query

        Parameters
        ----------
        fields : list, str
            The field(s) to query
        """
        join_str = '" or r["_field"] == "'
        if isinstance(fields, str):
            fields = [fields]
        self._query_list.append(
                f'  |> filter(fn: (r) => r["_field"] == '
                f'"{join_str.join(fields)}")'
                )

    def add_filter(self, key, value):
        """Adds a filter to the query

        Keyword Arguments:
            key (str): Key of the tag you want to isolate

            value (str): Tag you want to isolate
        """
        self._query_list.append(
                f'  |> filter(fn: (r) => r["{key}"] == "{value}")'
                )

    def add_specific_filter(self, key, value, col):
        """
        """
        self._query_list.append(
            f'  |> map(fn: (r) => ({{ r with "_value": if '
            f'r["{key}"] == "{value}" or r["_field"] != "{col}"'
            f' then r["_value"]'
            f' else debug.null(type: "float")}}))'
        )

    def add_filter_range(self, filter_fields, groups):
        """Adds filter range to the query

        Adds a filter to the query that only selects measurements when one
        measurement lies inside or outside a specified range

        Keyword arguments:
            field (str): The field that is being filtered

            filter_fields (list): Contains all fields used to filter field data
        """
        self.add_pivot(["_field"])
        for filter_field in filter_fields:
            name = filter_field["Field"]
            min = filter_field["Min"]
            max = filter_field["Max"]
            min_equals_sign = "=" if filter_field["Min Equal"] else ""
            max_equals_sign = "=" if filter_field["Max Equal"] else ""
            self._query_list.append(
                f'  |> filter(fn: (r) => r["{name}"] >{min_equals_sign}'
                f' {min} and r["{name}"] <{max_equals_sign} {max})'
            )
        self._query_list.append('  |> experimental.unpivot()')

    def add_groups(self, groups):
        """Adds group tag to query

        Keyword Arguments:
            group list[str]: Key to group measurements by
        """
        formatted_groups = '", "'.join(groups)
        self._query_list.append(f'  |> group(columns: ["{formatted_groups}"])')

    def add_pivot(self, groups):
        """
        """
        formatted_groups = '", "'.join(groups)
        self._query_list.append(
            f'  |> pivot(rowKey: ["_time"], '
            f'columnKey: ["{formatted_groups}"], valueColumn: "_value")'
            )

    def add_window(
        self,
        range,
        function,
        create_empty=True,
        time_starting=False,
        column="_value"
    ):
        """Adds aggregate window to data

        Keyword Arguments:
            range (str): Range of window, use InfluxDB specified ranges
            e.g 1h for 1 hour

            function (str): Aggregate function e.g. mean, median

            create_empty (bool): Add null values where measurements weren't
            made? (default: True)

            time_ending (bool): Timestamp corresponds to end of window?
            (default: True)

            column (str): Column to aggregate (default: "_value")
        """
        time_source = "_stop"
        if time_starting:
            time_source = "_start"
        self._query_list.append(
            f"  |> aggregateWindow(every: {range}, "
            f'fn: {function}, column: "{column}", timeSrc: '
            f'"{time_source}", timeDst: "_time", createEmpty: '
            f"{str(create_empty).lower()})"
        )

    def drop_columns(self, cols):
        """
        Drop unneeded columns
        """
        formatted_cols = '", "'.join(cols)
        self._query_list.append(f'  |> group(columns: ["{formatted_cols}"])')

    def keep_measurements(self):
        """Removes all columns except _time and _value, can help download
        time
        """
        self._query_list.append('  |> keep(columns: ["_time", "_value"])')

    def drop_start_stop(self):
        """Adds drop function which removes superfluous start and stop
        columns
        """
        self._query_list.append('  |> drop(columns: ["_start", "_stop"])')

    def add_scaling(self, scale_conf):
        """
        """
        name = scale_conf['Field']
        start = scale_conf.get('Start', self._start)
        if isinstance(start, str):
            start = dt.datetime.strptime(start, '%Y/%m/%d %H:%M:%S')
        if isinstance(start, dt.datetime):
            start = dt_to_rfc3339(start)
        end = scale_conf.get('End', self._end)
        if isinstance(end, str):
            end = dt.datetime.strptime(end, '%Y/%m/%d %H:%M:%S')
        if isinstance(end, dt.datetime):
            end = dt_to_rfc3339(end)
        slope = scale_conf.get('Slope', 1)
        offset = scale_conf.get('Offset', 0)
        self._query_list.append(
            f'  |> map(fn: (r) => ({{ r with "{name}": if '
            f'r["_time"] >= {start} and r["_time"] <= '
            f'{end} then (r["{name}"] * float(v: {slope})) + float(v: {offset})'
            f' else r["{name}"]}}))'
        )

    def add_yield(self, name):
        """Adds yield function, allows data to be output

        Keyword Arguments:
            name (str): Name for data, should be unique if multiple queries are
            made
        """
        self._query_list.append(f'  |> yield(name: "{name}")')

    def return_query(self):
        """Returns the query string

        Returns:
            String corresponding to a flux query
        """
        return "\n".join(self._query_list)


def dt_to_rfc3339(input, use_time=True):
    """Converts datetime to RFC3339 string

    Keyword Arguments:
        input (datetime): Datetime object to convert

        use_time (boolean): Include time? (default: True)

    Returns:
        RFC3339 string converted from input
    """
    if use_time:
        return input.strftime("%Y-%m-%dT%H:%M:%SZ")
    return input.strftime("%Y-%m-%d")
