"""TapGoogleAnalytics tap class."""

from tap_google_analytics.stream import TapGoogleAnalyticsStream
from tap_google_analytics.report_generator import ReportGenerator
from typing import List

from singer_sdk import Tap, Stream
from singer_sdk import typing as th


class TapTapGoogleAnalytics(Tap):
    """TapGoogleAnalytics tap class."""
    name = "tap-google-analytics"

    config_jsonschema = th.PropertiesList(
        th.Property("service_account_key", th.StringType, required=True),
        th.Property("property_id", th.StringType, required=True),
        th.Property("dimensions", th.ArrayType(th.StringType), required=True),
        th.Property("metrics", th.ArrayType(th.StringType), required=True),
        th.Property("name", th.StringType, required=True),
        th.Property("start_date", th.DateTimeType)
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""

        # Inject the Google Analytics report generator
        report_generator = ReportGenerator(
            property_id=self.config["property_id"],
            dimensions=self.config["dimensions"],
            metrics=self.config["metrics"],
            service_account_key=self.config["service_account_key"],
        )

        # Instantiate the stream
        stream = TapGoogleAnalyticsStream(
            tap=self, 
            name=self.config["name"],
            report_generator=report_generator,
        )

        # The primary keys are the dimension names
        stream.primary_keys = stream.report_generator.dimension_names

        return [stream]
