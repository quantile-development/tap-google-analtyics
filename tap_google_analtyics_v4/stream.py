import os
import requests
from pathlib import Path
from typing import Any, Dict, Optional, Union, List, Iterable
from tap_google_analtyics_v4.report_generator import ReportGenerator

from singer_sdk import typing as th
from singer_sdk.streams import Stream


class TapGoogleAnalyticsV4Stream(Stream):
    """TapGoogleAnalyticsV4 stream class."""
    report_generator: ReportGenerator
    replication_key = '_end_date'

    @property
    def schema(self) -> Dict[str, Any]:
        """Overrides the static schema property, instead we take the header
        names and types from the Google Analytics report.

        Returns:
            Dict[str, Any]: Return all properties and their types
        """
        header_names = self.report_generator.header_names
        header_types = self.report_generator.header_types

        return th.PropertiesList(
            *[
                th.Property(property_name, property_type)
                for property_name, property_type
                in zip(header_names, header_types)
            ],
            th.Property('_end_date', th.DateTimeType)
        ).to_dict()

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Return a generator of row-type dictionary objects.

        The optional `context` argument is used to identify a specific slice of the
        stream if partitioning is required for the stream. Most implementations do not
        require partitioning and should ignore the `context` argument.
        """
        start_date = self.get_starting_timestamp(context=context).strftime('%Y-%m-%d')

        for row in self.report_generator.report_rows(start_date=start_date):
            yield row
