from censusify_philly import __version__
import pytest
import os
from census import Census
from censusify_philly.census.models import (
    CensusDataQuery,
)
from censusify_philly.arcgis.census_geo_matcher import CensusGeoMatcher
from censusify_philly.police_geographies import (
    CENSUS_BLOCK_GROUP_ARCGIS_QUERY_SOURCE,
    OPEN_DATA_PHILLY_ARCGIS_QUERY_SOURCES,
)
from censusify_philly.arcgis.models import ArcgisQuery, ArcgisResult


def test_version():
    assert __version__ == "0.1.0"


class PlaceFake:
    def state_county_blockgroup(
        self,
        fields: list[str],
        state_fips: str,
        county_fips: str,
        tract: str,
        blockgroup: str,
    ):
        tracts = [f"000{tract_num:03d}" for tract_num in range(100, 110)]
        return [
            {
                "NAME": "Block Group 1, Census Tract 1.02, Philadelphia County, Pennsylvania",
                "P2_004N": 1227.0,
                "P2_005N": 1046.0,
                "P2_006N": 57.0,
                "P2_007N": 3.0,
                "P2_008N": 108.0,
                "P2_009N": 1.0,
                "P2_010N": 12.0,
                "P2_011N": 50.0,
                "P1_003N": 1065.0,
                "P1_004N": 63.0,
                "P1_005N": 3.0,
                "P1_006N": 109.0,
                "P1_007N": 1.0,
                "P1_008N": 29.0,
                "P1_009N": 104.0,
                "P2_002N": 97.0,
                "P2_003N": 1277.0,
                "P1_001N": 1374.0,
                "state": "42",
                "county": "101",
                "tract": tract,
                "block group": f"{block_group}",
            }
            for tract in tracts
            for block_group in range(1, 9)
        ]


class CensusFake:
    def __init__(self, api_key: str):
        self.pl = PlaceFake()


class CensusArcgisQueryFake:
    source = CENSUS_BLOCK_GROUP_ARCGIS_QUERY_SOURCE

    def get_all_by_attribute(where_str: str, include_geometry: bool = True):
        return [
            ArcgisResult(
                attributes={
                    "STATE": "42",
                    "COUNTY": "101",
                    "TRACT": "000101",
                    "BLKGRP": "1",
                    "GEOID": "421010001011",
                    "CENTLON": "-75.5",
                    "CENTLAT": "+39.5",
                },
                geometry={
                    "rings": [[[]]],
                },
            )
        ]


class OtherArcgisQueryFakePSA:
    source = OPEN_DATA_PHILLY_ARCGIS_QUERY_SOURCES["police_service_area"]

    def get_all_by_attribute(self, where_str: str, include_geometry: bool = True):
        return [
            ArcgisResult(
                attributes=geo_attributes,
                geometry={
                    "rings": [
                        [
                            [-75.4, 39.4],
                            [-75.4, 39.6],
                            [-75.6, 39.6],
                            [-75.6, 39.4],
                            [-75.4, 39.4],
                        ]
                    ]
                },
            )
            for geo_attributes in [
                {"PSA_NUM": "077"},
                {"PSA_NUM": "078"},
            ]
        ]


@pytest.fixture
def census_data_query():
    return CensusDataQuery(census=CensusFake("FAKE_KEY"))


@pytest.fixture
def census_arcgis_query():
    return CensusArcgisQueryFake()


@pytest.fixture
def other_arcgis_query():
    return OtherArcgisQueryFakePSA()


def test_match(census_data_query):
    results = census_data_query.get_demographic_data(state_fips="42", county_fips="101")
