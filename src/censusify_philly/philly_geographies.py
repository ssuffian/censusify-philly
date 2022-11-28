from enum import Enum
from pathlib import Path
import pandas as pd
from census import Census
import os
from censusify_philly.arcgis.census_geo_matcher import CensusGeoMatcher
from censusify_philly.arcgis.census_geo_matcher import CensusBlockRelationship
from censusify_philly.arcgis.arcgis_query import (
    ArcgisQuery,
    ArcgisQuerySource,
)
from censusify_philly.census.models import (
    CensusDataQuery,
)


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


def generate_demographics_df(
    *,
    census_data_query: CensusDataQuery,
    census_arcgis_query: ArcgisQuery,
    other_arcgis_query: ArcgisQuery,
    relationship: CensusBlockRelationship,
):

    census_demographics_df = census_data_query.get_all_demographic_data_for_county(
        state_fips=STATE_FIPS, county_fips=COUNTY_FIPS
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


if __name__ == "__main__":
    census_api = Census(os.environ["CENSUS_API_KEY"])
    census_arcgis_query = ArcgisQuery(CENSUS_BLOCK_GROUP_ARCGIS_QUERY_SOURCE)
    local_geography_name = OpenDataPhillyGeographyName.police_service_area
    other_arcgis_query = ArcgisQuery(
        OPEN_DATA_PHILLY_ARCGIS_QUERY_SOURCES[local_geography_name]
    )
    for relationship in CensusBlockRelationship:
        relationship_name = relationship.value
        df = generate_demographics_df(
            census_arcgis_query=census_arcgis_query,
            other_arcgis_query=other_arcgis_query,
            census_data_query=CensusDataQuery(census=census_api),
            relationship=relationship,
        )

        Path("csvs").mkdir(parents=True, exist_ok=True)
        df.to_csv(f"csvs/{local_geography_name}__{relationship_name}.csv")
