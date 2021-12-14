#! /usr/bin/env python3
# -*- coding: utf-8 -*-
# vim:fenc=utf-8

"""
Fetch county-level COVID-19 data for the state of Georgia.
"""

from __future__ import annotations
from meerschaum.utils.typing import Optional
from meerschaum.config._paths import PLUGINS_TEMP_RESOURCES_PATH
import datetime, pathlib
__version__ = '0.0.1'
TMP_PATH = PLUGINS_TEMP_RESOURCES_PATH / 'GA-covid_data'
ZIP_URL = 'https://ga-covid19.ondemand.sas.com/docs/ga_covid_data.zip'
ZIP_PATH = TMP_PATH / 'ga_covid_data.zip'
UNZIP_PATH = TMP_PATH / 'ga_covid_data'
CSV_PATH = UNZIP_PATH / 'epicurve_rpt_date.csv'
COUNTIES_PATH = pathlib.Path(__file__).parent / 'counties.csv'
required = ['requests', 'python-dateutil', 'pandas', 'duckdb',]

def register(pipe: meerschaum.Pipe, **kw):
    from meerschaum.utils.warnings import warn
    from meerschaum.utils.prompt import prompt, yes_no
    while True:
        fips_str = prompt("Please enter a list of FIPS codes separated by commas:")
        fips = fips_str.replace(' ', '').split(',')

        valid = True
        for f in fips:
            if not f.startswith("13"):
                warn("All FIPS codes must begin with 13 (prefix for the state of Texas).", stack=False)
                valid = False
                break
        if not valid:
            continue

        question = "Is this correct?"
        for f in fips:
            question += f"\n  - {f}"
        question += '\n'

        if not fips or not yes_no(question):
            continue
        break

    return {
        'columns': {
            'datetime': 'date',
            'id': 'fips',
            'value': 'cases'
        },
        'GA-covid': {
            'fips': fips,
        },
    }


def fetch(
        pipe: meerschaum.Pipe,
        begin: Optional[datetime.datetime] = None,
        end: Optional[datetime.datetime] = None,
        debug: bool = False,
        **kw
    ):
    import zipfile, textwrap
    from meerschaum.utils.misc import wget
    import duckdb
    import pandas as pd
    TMP_PATH.mkdir(exist_ok=True, parents=True)
    wget(ZIP_URL, ZIP_PATH)
    with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
        zip_ref.extractall(UNZIP_PATH)

    dtypes = {
        'date': 'datetime64[ms]',
        'county': str,
        'fips': str,
        'cases': int,
        'deaths': int,
    }
    fips = pipe.parameters['GA-covid']['fips']
    fips_where = "'" + "', '".join(fips) + "'"
    counties_df = pd.read_csv(COUNTIES_PATH, dtype={'fips': str, 'counties': str, 'state': str})

    query = textwrap.dedent(f"""
        SELECT
            CAST(d.report_date AS DATE) AS date, 
            c.fips,
            c.county,
            CAST(d.cases_cum AS INT) AS cases,
            CAST(d.death_cum AS INT) AS deaths
        FROM read_csv_auto('{str(CSV_PATH)}') AS d
        INNER JOIN counties_df AS c ON c.county = d.county
        WHERE c.fips IN ({fips_where})
            AND d.cases_cum IS NOT NULL
            AND d.death_cum IS NOT NULL
            AND d.report_date IS NOT NULL"""
    )
    begin = begin if begin is not None else pipe.get_sync_time(debug=debug)
    if begin is not None:
        query += f"\n    AND CAST(d.date AS DATE) >= CAST('{begin}' AS DATE)"
    if end is not None:
        query += f"\n    AND CAST(d.date AS DATE) <= CAST('{end}' AS DATE)"
    result = duckdb.query(query)
    df = result.df()[dtypes.keys()].astype(dtypes)
    return df

