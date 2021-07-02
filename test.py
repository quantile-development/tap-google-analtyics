import os
from google.analytics.data_v1beta import BetaAnalyticsDataClient  # type: ignore
from google.analytics.data_v1beta.types import DateRange  # type: ignore
from google.analytics.data_v1beta.types import Dimension  # type: ignore
from google.analytics.data_v1beta.types import Metric  # type: ignore
from google.analytics.data_v1beta.types import Filter  # type: ignore
from google.analytics.data_v1beta.types import FilterExpression  # type: ignore
from google.analytics.data_v1beta.types import FilterExpressionList  # type: ignore
from google.analytics.data_v1beta.types import RunReportRequest  # type: ignore

PROPERTY_ID = '265077427'
START_DATE = '2021-01-01'
END_DATE = '2021-03-31'
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "client_secrets.json"

# {
#     "name": "traffic_sources",
#     "dimensions": [
#       "ga:date",
#       "ga:source",
#       "ga:medium",
#       "ga:socialNetwork"
#     ],
#     "metrics": [
#       "ga:users",
#       "ga:newUsers",
#       "ga:sessions",
#       "ga:sessionsPerUser",
#       "ga:avgSessionDuration",
#       "ga:pageviews",
#       "ga:pageviewsPerSession",
#       "ga:avgTimeOnPage",
#       "ga:bounceRate",
#       "ga:exitRate"
#     ]
#   },


def ga4(property_id, start_date, end_date):

    client = BetaAnalyticsDataClient()
    request = RunReportRequest(
        property='properties/' + property_id,
        dimensions=[
            Dimension(name='date'),
            Dimension(name='fullPageUrl'),
        ],
        metrics=[
            Metric(name='totalUsers'),
            Metric(name='userEngagementDuration')
        ],
        date_ranges=[
            DateRange(start_date=start_date, end_date=end_date)
        ]
    )

    print(request.dimensions)
    # request = RunReportRequest(
    #     property='properties/' + property_id,
    #     dimensions=[
    #         Dimension(name='date'),
    #         Dimension(name='source'),
    #         Dimension(name='medium'),
    #     ],
    #     metrics=[
    #         Metric(name='totalUsers'),
    #         Metric(name='userEngagementDuration'),
    #         Metric(name='eventCount'),
    #         Metric(name='screenPageViews'),
    #         Metric(name='newUsers'),
    #     ],
    #     date_ranges=[
    #         DateRange(start_date=start_date, end_date=end_date)
    #     ]
    # )
    # request = RunReportRequest(
    #     property='properties/' + property_id,
    #     dimensions=[
    #         Dimension(name='date')
    #     ],
    #     metrics=[
    #         Metric(name='totalUsers'),
    #         Metric(name='userEngagementDuration'),
    #         Metric(name='eventCount'),
    #         Metric(name='screenPageViews'),
    #         Metric(name='newUsers'),
    #         Metric(name='eventCountPerUser'),
    #         Metric(name='engagementRate'),
    #     ],
    #     date_ranges=[
    #         DateRange(start_date=start_date, end_date=end_date)
    #     ],
    #     keep_empty_rows=True
    # )

    response = client.run_report(request)
    print(response)
    # x, y = ([] for i in range(2))
    # for row in response.rows:
    #     x.append(row.dimension_values[0].value)
    #     y.append(row.metric_values[0].value)
    #     print(row.dimension_values[0].value, row.metric_values[0].value)


if __name__ == "__main__":
    ga4(PROPERTY_ID, START_DATE, END_DATE)
