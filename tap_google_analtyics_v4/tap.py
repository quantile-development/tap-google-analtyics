"""TapGoogleAnalyticsV4 tap class."""

from tap_google_analtyics_v4.stream import TapGoogleAnalyticsV4Stream
from tap_google_analtyics_v4.report_generator import ReportGenerator
from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th  # JSON schema typing helpers


class TapTapGoogleAnalyticsV4(Tap):
    """TapGoogleAnalyticsV4 tap class."""
    name = "tap-google-analtyics-v4"

    config_jsonschema = th.PropertiesList(
        th.Property("key_file_location", th.StringType, required=False),
        th.Property("property_id", th.StringType, required=False),
        th.Property("dimensions", th.ArrayType(th.StringType), required=False),
        th.Property("metrics", th.ArrayType(th.StringType), required=False),
        th.Property("name", th.StringType, required=False),
        th.Property("start_date", th.DateTimeType)
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        # Instantiate the stream
        stream = TapGoogleAnalyticsV4Stream(tap=self, name=self.config['name'])

        # Inject the Google Analytics report generator
        stream.report_generator = ReportGenerator(
            property_id=self.config['property_id'],
            dimensions=self.config['dimensions'],
            metrics=self.config['metrics'],
            key_file_location=self.config['key_file_location'],
            logger=stream.logger
        )

        # The primary keys are the dimension names
        stream.primary_keys = stream.report_generator.dimension_names

        return [stream]
