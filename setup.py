import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="tap-google-analytics",
    version="0.0.1",
    author="Jules Huisman",
    author_email="jules.huisman@quantile.nl",
    description="A Singer.io tap for Google Analytics 4",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/quantile-development/tap-google-analytics",
    project_urls={
        "Bug Tracker": "https://github.com/quantile-development/tap-google-analytics/issues",
    },
    install_requires=[
        'singer-sdk==0.3.12',
        'google-analytics-data==0.5.1'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    packages=['tap_google_analytics'],
    entry_points="""
    [console_scripts]
    tap=tap_google_analtyics.tap:TapTapGoogleAnalytics.cli
    tap-google-analytics=tap_google_analtyics.tap:TapTapGoogleAnalytics.cli
    """,
    python_requires=">=3.8",
)