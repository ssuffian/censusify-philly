from census import Census
from pathlib import Path
import os

from censusify_philly.arcgis.arcgis_query import ArcgisQuery
from censusify_philly.census.models import (
    CensusDataQuery,
)
from censusify_philly.arcgis.census_geo_matcher import CensusBlockRelationship
from censusify_philly.philly_geographies import (
    generate_demographics_df,
    OpenDataPhillyGeographyName,
    OPEN_DATA_PHILLY_ARCGIS_QUERY_SOURCES,
    CENSUS_BLOCK_GROUP_ARCGIS_QUERY_SOURCE,
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
        df.sort_values("PSA_NUM").to_csv(
            f"csvs/{local_geography_name}__{relationship_name}.csv"
        )
