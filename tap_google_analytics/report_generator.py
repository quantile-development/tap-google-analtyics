import os
import logging

from datetime import datetime, timedelta
from typing import Dict, Iterator, List

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest, RunReportResponse, MetricType
from google.analytics.data_v1beta.types.data import Row

from singer_sdk import typing as th

type_mapping = {
    MetricType.METRIC_TYPE_UNSPECIFIED: th.StringType,
    MetricType.TYPE_INTEGER: th.IntegerType,
    MetricType.TYPE_FLOAT: th.NumberType,
    MetricType.TYPE_SECONDS: th.IntegerType,
    MetricType.TYPE_MILLISECONDS: th.IntegerType,
    MetricType.TYPE_MINUTES: th.IntegerType,
    MetricType.TYPE_HOURS: th.IntegerType,
    MetricType.TYPE_STANDARD: th.IntegerType,  # Not sure about this type
    MetricType.TYPE_CURRENCY: th.StringType,
    MetricType.TYPE_FEET: th.NumberType,
    MetricType.TYPE_MILES: th.NumberType,
    MetricType.TYPE_METERS: th.NumberType,
    MetricType.TYPE_KILOMETERS: th.NumberType
}


class ReportGenerator:
    def __init__(self,
                 property_id: int,
                 dimensions: List[str],
                 metrics: List[str],
                 key_file_location: str) -> None:
        # Set the key location as an environment variable
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_file_location

        # Initially set the start date as the end date
        self._property = 'properties/' + str(property_id)
        self._dimension_names = dimensions
        self._metric_names = metrics
        self._client = BetaAnalyticsDataClient()

        # Request the report from Google Analytics
        self._report = self._fetch_report()

    @property
    def _end_date(self) -> str:
        today = datetime.now().date()
        yesterday = today - timedelta(days=1)

        return str(yesterday)

    @property
    def _dimensions(self):
        return [
            Dimension(name=dimension_name)
            for dimension_name
            in self._dimension_names
        ]

    @property
    def _metrics(self):
        return [
            Metric(name=metric_name)
            for metric_name
            in self._metric_names
        ]

    @property
    def dimension_names(self):
        return [
            header.name
            for header
            in self._report.dimension_headers
        ]

    @property
    def metric_names(self):
        return [
            header.name
            for header
            in self._report.metric_headers
        ]

    @property
    def header_names(self):
        return [*self.dimension_names, *self.metric_names]

    @staticmethod
    def _row_values(row: Row):
        return [
            item.value
            for item
            in [*row.dimension_values, *row.metric_values]
        ]

    @property
    def header_types(self):
        # All the dimensions are string typed
        dimension_types = [
            th.StringType
            for _
            in self._report.dimension_headers
        ]

        # Map the Google analytics typing to singer typing
        metric_types = [
            type_mapping.get(metric.type_, th.StringType)
            for metric
            in self._report.metric_headers
        ]

        return [*dimension_types, *metric_types]

    @staticmethod
    def _conform_types(types: list, values: List[str]):
        conformed_values = []

        for typing, value in zip(types, values):
            if typing == th.IntegerType:
                value = int(value)
            if typing == th.NumberType:
                value = float(value)

            conformed_values.append(value)

        return conformed_values

    def _fetch_report(self, start_date: str = None) -> RunReportResponse:
        report_request = RunReportRequest(
            property=self._property,
            dimensions=self._dimensions,
            metrics=self._metrics,
            date_ranges=[
                DateRange(
                    start_date=start_date if start_date else self._end_date,
                    end_date=self._end_date
                )
            ]
        )

        return self._client.run_report(request=report_request)

    def report_rows(self, start_date: str) -> Iterator[Dict[str, str]]:

        logging.info(
            f'Generating report with start date: {start_date} and end date: {self._end_date}'
        )

        # If the start date is the same as the end date there are no new rows
        if start_date == self._end_date:
            logging.info('No new rows to be fetched')
            return []

        # Fetch the report with the start_date from the state
        self._report = self._fetch_report(start_date=start_date)

        header_names = self.header_names
        header_types = self.header_types

        # Loop through the rows and parse their types
        for row in self._report.rows:
            row_values = self._row_values(row=row)
            row_values_conformed = self._conform_types(
                types=header_types,
                values=row_values
            )

            # Manually inject the end date to use as the singer replication key
            yield {
                **dict(zip(header_names, row_values_conformed)),
                '_end_date': self._end_date
            }
