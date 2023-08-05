from enum import Enum
from pathlib import Path
import pandas as pd
from census import Census
import os

from censusify_philly.arcgis.census_geo_matcher import CensusBlockRelationship
from censusify_philly.arcgis.census_geo_matcher import CensusGeoMatcher
from censusify_philly.arcgis.arcgis_query import (
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


class PoliceDataCensusDemographicsResult(CensusDemographicsResult):
    american_indian: int
    asian: int
    unknown: int
    white_latino: int
    white_nonlatino: int
    black_latino: int
    black_nonlatino: int
    total: int

    @classmethod
    def from_renamed_result(cls, renamed_results):
        american_indian = renamed_results[
            "p1_005n_american_indian_and_alaska_native_alone"
        ]
        asian = renamed_results["p1_006n_asian_alone"]
        pacific_islander = renamed_results[
            "p1_007n_native_hawaiian_and_other_pacific_islander_alone"
        ]
        other = renamed_results["p1_008n_some_other_race_alone"]
        multiracial = renamed_results["p1_009n_multiracial:"]

        white = renamed_results["p1_003n_white_alone"]
        white_nonlatino = renamed_results[
            "p2_005n_not_hispanic_or_latino:!!white_alone"
        ]
        white_latino = white - white_nonlatino

        black = renamed_results["p1_004n_black_or_african_american_alone"]
        black_nonlatino = renamed_results[
            "p2_006n_not_hispanic_or_latino:!!black_or_african_american_alone"
        ]
        black_latino = black - black_nonlatino
        total = renamed_results["p1_001n_!!total:"]

        return cls(
            american_indian=american_indian,
            asian=asian + pacific_islander,
            unknown=other + multiracial,
            black_latino=black_latino,
            black_nonlatino=black_nonlatino,
            white_latino=white_latino,
            white_nonlatino=white_nonlatino,
            total=total,
        )


def generate_demographics_df(
    *,
    census_data_query: CensusDataQuery,
    census_arcgis_query: ArcgisQuery,
    other_arcgis_query: ArcgisQuery,
    relationship: CensusBlockRelationship,
):

    census_demographics_df = census_data_query.get_all_demographic_data_for_county(
        state_fips=STATE_FIPS,
        county_fips=COUNTY_FIPS,
        CensusDemographicsResultClass=PoliceDataCensusDemographicsResult,
    )
    matcher = CensusGeoMatcher(
        census_arcgis_query=census_arcgis_query, other_arcgis_query=other_arcgis_query
    )
    geo_results = matcher.generate_geo_matched_results(
        state_fips=STATE_FIPS, county_fips=COUNTY_FIPS, relationship=relationship
    )
    return census_data_query.assign_demographic_data_to_custom_geographies(
        geo_results=geo_results, census_demographics_df=census_demographics_df
    )
