import asyncio
from census import Census
import os
import json
from shapely.geometry import Polygon
import pandas as pd

from censusify_philly.services.census.models import (
    CensusDataQuery,
)
from censusify_philly.arcgis import ArcgisQuery

# map census demographics to ODP Demographics Columns
def census_to_odp_demographics_mapping(census_data):
    # Philly Police do not have a separate category for Pacific Islanders/Hawaiians
    return {
        "American Indian": census_data["p1_american_indian_and_alaska_native_alone"],
        "Asian": census_data["p1_asian_alone"]
        + census_data["p1_native_hawaiian_and_other_pacific_islander_alone"],
        "Unknown": census_data["p1_some_other_race_alone"]
        + census_data["p1_multiracial:"],
        "White - Non-Latino": census_data["p2_not_hispanic_or_latino:!!white_alone"],
        "White - Latino": census_data["p1_white_alone"]
        - census_data["p2_not_hispanic_or_latino:!!white_alone"],
        "Black - Non-Latino": census_data[
            "p2_not_hispanic_or_latino:!!black_or_african_american_alone"
        ],
        "Black - Latino": census_data["p1_black_or_african_american_alone"]
        - census_data["p2_not_hispanic_or_latino:!!black_or_african_american_alone"],
    }


class CensusGeoMatcher:
    def __init__(self, census_data_dict, census_geographies_dict):
        self.census_data_dict = census_data_dict
        self.census_geographies_dict = census_geographies_dict

    def _get_census_pct_area_of_geography(self, geo_polygon):
        census_block_groups = {
            census_name: census_x
            for census_name, census_x in self.census_data_dict.items()
            if census_x.intersects(geo_polygon)
            and census_x.intersection(geo_polygon).area > 0.000001
        }

        census_area = {
            census_name: census_x.intersection(geo_polygon).area
            for census_name, census_x in census_block_groups.items()
        }
        # % Of the Census Block groups area that is in a given police geography
        return {
            census_name: census_x.intersection(geo_polygon).area / census_x.area
            for census_name, census_x in census_block_groups.items()
        }

    def _get_demographics_results_and_pct_from_dict(self, geo_demographics):
        return {
            "demographics_count": geo_demographics,
            "demographics_percent": {
                key: val / sum(geo_demographics.values()) * 100
                for key, val in geo_demographics.items()
                if sum(geo_demographics.values())
            },
        }

    def get_demographics_based_on_overlap_with_census_blocks(self, geo_polygon):
        """
        Now we want to get a dictionary where for a given police geography,
        we know the demographics.
        We do this by adjusting each census block group by how much of it is
        in a police geography and then summing the result
        for every census block group in the police geography.
        We then round to make sure we get whole people numbers
        """
        census_pct = self._get_census_pct_area_of_geography(geo_polygon)
        census_series = pd.Series(census_pct)
        census_series = census_series[census_series > 0]

        # For all census block groups that are in the PSA, multiply the n_people by the % area of the PSA that
        # census block group takes up.
        geo_demographics = (
            pd.DataFrame(self.census_geographies_dict)
            .T.loc[census_series.index]
            .mul(census_series, axis=0)
            .sum()
            .round()
            .to_dict()
        )
        return self._get_demographics_results_and_pct_from_dict(geo_demographics)


async def main():
    arcgis_query = ArcgisQuery(
        base_url="https://tigerweb.geo.census.gov/arcgis/rest/services/Census2020/Tracts_Blocks/MapServer/1/query",
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
    census_data_query = CensusDataQuery(Census(os.environ["CENSUS_API_KEY"]))

    # Get all shapefiles for PSAs and Census Block Groups
    # This is used to build the pct mapping
    state = "42"  # PA
    county = "101"  # Philly

    ## DEMOGRAPHICS BY BLOCK GROUP?
    county_census_demographics = (
        await census_data_query.get_all_demographic_data_for_county(state, county)
    )
    all_census_data_dict = {
        result.census_geoid: census_to_odp_demographics_mapping(result.census_data)
        for result in county_census_demographics
    }

    # CENSUS GEOGRAPHIES BY BLOCK GROUP?
    philly_census_block_group_geographies = await arcgis_query.get_all_by_attribute(
        where_str=f"STATE='{state}' AND COUNTY='{county}'"
    )
    all_census_geographies_dict = {
        feat.attributes["GEOID"]: Polygon(feat.geometry_ring)
        for feat in philly_census_block_group_geographies
    }
    census_geo_matcher = CensusGeoMatcher(
        all_census_geographies_dict, all_census_data_dict
    )

    # Division Geographies
    psa_url = "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/Boundaries_PSA/FeatureServer/0/query"
    division_url = "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/Boundaries_Division/FeatureServer/0/query?outFields=*&where=1%3D1"
    all_division_data = await ArcgisQuery(division_url).get_all_by_attribute(
        where_str="1=1"
    )
    division_demographics = [
        {
            "division": feat.attributes["DIV_NAME"],
            **census_geo_matcher.get_demographics_based_on_overlap_with_census_blocks(
                Polygon(feat.geometry_ring)
            ),
        }
        for feat in all_division_data
    ]

    # District Geographies
    district_url = "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/Boundaries_District/FeatureServer/0/query?outFields=*&where=1%3D1"
    all_district_data = await ArcgisQuery(district_url).get_all_by_attribute(
        where_str="1=1"
    )
    district_demographics = [
        {
            "division": feat.attributes["DIV_CODE"],
            "district": f'{feat.attributes["DIST_NUM"]:02d}',
            **census_geo_matcher.get_demographics_based_on_overlap_with_census_blocks(
                Polygon(feat.geometry_ring)
            ),
        }
        for feat in all_district_data
    ]
    district_to_division_mapping = {
        dist["district"]: dist["division"] for dist in district_demographics
    }

    # PSA Geographies
    psa_url = "https://services.arcgis.com/fLeGjb7u4uXqeF9q/arcgis/rest/services/Boundaries_PSA/FeatureServer/0/query"
    all_psa_data = await ArcgisQuery(psa_url).get_all_by_attribute(where_str="1=1")
    psa_demographics = [
        {
            "division": district_to_division_mapping[feat.attributes["PSA_NUM"][:2]],
            "district": feat.attributes["PSA_NUM"][:2],
            "psa": feat.attributes["PSA_NUM"][2:],
            "full_psa_num": feat.attributes["PSA_NUM"],
            **census_geo_matcher.get_demographics_based_on_overlap_with_census_blocks(
                Polygon(feat.geometry_ring)
            ),
        }
        for feat in all_psa_data
    ]

    # Custom Geography
    citywide = pd.DataFrame(all_census_data_dict).sum(axis=1).to_dict()
    custom_geo_demographics = {
        "citywide": census_geo_matcher._get_demographics_results_and_pct_from_dict(
            citywide
        )
    }

    open("demographics.py", "w").write(
        "DEMOGRAPHICS = "
        + json.dumps(
            {
                "by_custom": custom_geo_demographics,
                "by_psa": {row["full_psa_num"]: row for row in psa_demographics},
                "by_district": {row["district"]: row for row in district_demographics},
                "by_division": {row["division"]: row for row in division_demographics},
            }
        )
    )


if __name__ == "__main__":
    asyncio.run(main())
