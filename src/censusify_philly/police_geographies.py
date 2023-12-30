from enum import Enum
from pydantic import BaseModel
from pydantic import validator
import json
from pydantic import root_validator
import pandera as pa
from pathlib import Path
import pandas as pd
from census import Census
import os
import click

from censusify_philly.arcgis.census_geo_matcher import CensusBlockRelationship
from censusify_philly.arcgis.census_geo_matcher import CensusGeoMatcher
from censusify_philly.arcgis.models import (
    ArcgisQuery,
    ArcgisQuerySource,
)
from censusify_philly.census.models import CensusDataQuery, CensusDemographicsResult


class OpenDataPhillyGeographyName(str, Enum):
    police_division = "police_division"
    police_district = "police_district"
    police_service_area = "police_service_area"


CENSUS_BLOCK_GROUP_ARCGIS_QUERY_SOURCE = ArcgisQuerySource(
    url="https://tigerweb.geo.census.gov/arcgis/rest/services/Census2020/Tracts_Blocks/MapServer/1/query",
    unique_geo_column="GEOID",
    attribute_col_str=",".join(
        [
            "STATE",
            "COUNTY",
            "TRACT",
            "BLKGRP",
            "GEOID",
            "CENTLAT",
            "CENTLON",
        ],
    ),
)


OPEN_DATA_PHILLY_ARCGIS_QUERY_SOURCES = {
    OpenDataPhillyGeographyName.police_division: ArcgisQuerySource(
        url="https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/Boundaries_Division/FeatureServer/0/query?outFields=*&where=1%3D1",
        unique_geo_column="DIV_NAME",
    ),
    OpenDataPhillyGeographyName.police_district: ArcgisQuerySource(
        url="https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/Boundaries_District/FeatureServer/0/query?outFields=*&where=1%3D1",
        unique_geo_column="DIST_NUM",
    ),
    OpenDataPhillyGeographyName.police_service_area: ArcgisQuerySource(
        url="https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/Boundaries_PSA/FeatureServer/0/query",
        unique_geo_column="PSA_NUM",
    ),
}

STATE_FIPS = "42"  # Pennsylvania
COUNTY_FIPS = "101"  # Philadelphia County

import pandas as pd
from pandera.engines.pandas_engine import PydanticModel


class PoliceDataCensusDemographicsResult(CensusDemographicsResult):
    total: int
    hispanic_or_latino: int
    white: int
    black: int
    american_indian: int
    asian: int
    unknown: int

    @classmethod
    def from_raw_census_data(cls, results):
        """
        Converts the P# to sensible demographic groupings.
        The relevant P# are shown below:

        https://api.census.gov/data/2020/dec/pl/variables.json
        P1_001N  !!Total:
        P1_002N  !!Total:!!Population of one race:
        P1_003N  !!Total:!!Population of one race:!!White alone
        P1_004N  !!Total:!!Population of one race:!!Black or African American alone
        P1_005N  !!Total:!!Population of one race:!!American Indian and Alaska Native alone
        P1_006N  !!Total:!!Population of one race:!!Asian alone
        P1_007N  !!Total:!!Population of one race:!!Native Hawaiian and Other Pacific Islander alone
        P1_008N  !!Total:!!Population of one race:!!Some Other Race alone
        P1_009N  !!Total:!!Population of two or more races:

        P2_001N  !!Total:
        P2_002N  !!Total:!!Hispanic or Latino
        P2_003N  !!Total:!!Not Hispanic or Latino:
        P2_004N  !!Total:!!Not Hispanic or Latino:!!Population of one race:
        P2_005N  !!Total:!!Not Hispanic or Latino:!!Population of one race:!!White alone
        P2_006N  !!Total:!!Not Hispanic or Latino:!!Population of one race:!!Black or African American alone
        P2_007N  !!Total:!!Not Hispanic or Latino:!!Population of one race:!!American Indian and Alaska Native alone
        P2_008N  !!Total:!!Not Hispanic or Latino:!!Population of one race:!!Asian alone
        P2_009N  !!Total:!!Not Hispanic or Latino:!!Population of one race:!!Native Hawaiian and Other Pacific Islander alone
        P2_010N  !!Total:!!Not Hispanic or Latino:!!Population of one race:!!Some Other Race alone
        P2_011N  !!Total:!!Not Hispanic or Latino:!!Population of two or more races:

        """
        return cls(
            total=results["P1_001N"],
            hispanic_or_latino=results["P2_002N"],
            white=results["P2_005N"],
            black=results["P2_006N"],
            american_indian=results["P2_007N"],
            asian=results["P2_008N"] + results["P2_009N"],
            unknown=results["P2_010N"] + results["P2_011N"],
        )

    @staticmethod
    def as_df(results):
        return pd.DataFrame(
            [
                dict(
                    **PoliceDataCensusDemographicsResult.from_raw_census_data(
                        result
                    ).dict(),
                    geoid=f"{result['state']}{result['county']}{result['tract']}{result['block group']}",
                )
                for result in results
            ]
        ).set_index("geoid")


@click.group
def cli():
    pass


@cli.command
@click.option(
    "--census_api_key",
    default=os.environ.get("CENSUS_API_KEY"),
    help="API Key from census.gov",
)
def download_raw(census_api_key):
    census_data_query = CensusDataQuery(census=Census(census_api_key))
    census_demographics_results = census_data_query.get_demographic_data(
        state_fips=STATE_FIPS, county_fips=COUNTY_FIPS
    )
    Path("raw").mkdir(parents=True, exist_ok=True)
    with open("raw/census_demographics.json", "w") as f:
        json.dump(census_demographics_results, f)


@cli.command
def generate_csvs():
    # Download the demographic data
    print("Loading demographic data...")
    Path("raw").mkdir(parents=True, exist_ok=True)
    if not os.path.exists("raw/census_demographics.json"):
        raise ValueError(
            "You must first download census data using the `download-raw` command"
        )

    with open("raw/census_demographics.json", "r") as f:
        census_demographics_results = json.load(f)

    census_demo_data_df = PoliceDataCensusDemographicsResult.as_df(
        census_demographics_results
    )

    census_arcgis_query = ArcgisQuery(CENSUS_BLOCK_GROUP_ARCGIS_QUERY_SOURCE)
    for geography in OpenDataPhillyGeographyName:
        other_arcgis_query = ArcgisQuery(
            OPEN_DATA_PHILLY_ARCGIS_QUERY_SOURCES[geography.value]
        )
        matcher = CensusGeoMatcher(
            census_arcgis_query=census_arcgis_query,
            other_arcgis_query=other_arcgis_query,
        )

        # Download the geographic data
        print(f"Downloading geographic data for {geography}...")
        geo_results = matcher.generate_geo_matched_results(
            state_fips=STATE_FIPS,
            county_fips=COUNTY_FIPS,
            relationship=CensusBlockRelationship.centroid_is_within,
        )
        df = matcher.assign_demographic_data_to_custom_geographies(
            geo_results=geo_results, census_demographics_df=census_demo_data_df
        )
        Path("csvs").mkdir(parents=True, exist_ok=True)
        df.sort_index().to_csv(f"csvs/{geography.value}.csv")


if __name__ == "__main__":
    cli()
