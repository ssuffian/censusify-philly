from censusify_philly.arcgis.arcgis_query import (
    ArcgisQuery,
)
from pydantic import BaseModel
from shapely.geometry import Polygon, Point
from functools import cached_property
from enum import Enum
from typing import Any
import pandas as pd


class CensusBlockRelationship(str, Enum):
    pct_overlap = "pct_overlap"
    centroid_is_within = "centroid_is_within"


class CensusGeoMatcher:
    def __init__(
        self,
        census_arcgis_query: ArcgisQuery,
        other_arcgis_query: ArcgisQuery,
    ):
        self.census_arcgis = census_arcgis_query
        self.other_arcgis = other_arcgis_query

    def get_arcgis_features(self, where_str="1=1") -> dict[str, Any]:
        return self.other_arcgis.get_all_by_attribute(where_str)

    def get_census_block_group_features(
        self, state_fips: str, county_fips: str
    ) -> dict[str, Any]:
        where_str = f"STATE='{state_fips}' AND COUNTY='{county_fips}'"
        return self.census_arcgis.get_all_by_attribute(where_str)

    def get_census_block_group_overlap_between_given_features(
        self,
        /,
        *,
        geo_features: dict[str, Any],
        census_features: dict[str, Any],
        relationship: CensusBlockRelationship,
    ):
        return {
            feat.attributes[
                self.other_arcgis.source.unique_geo_column
            ]: self.get_census_block_group_overlap_for_geometry(
                census_features=census_features,
                geo_feature=feat,
                relationship=relationship,
            )
            for feat in geo_features
        }

    def get_census_block_group_overlap_for_geometry(
        self,
        census_features: dict[str, Any],
        geo_feature: Any,
        relationship: CensusBlockRelationship,
    ):
        """
        This maps the given geographic polygon to a list of census block groups.

        If "pct_overlap" is used, then it returns a dictionary of census block
        groups with their % overlap of the geo polygon.

        If "centroid_is_within" is used, then it returns a dictionary fo census
        block groups and the value 1, implifying that each census block group
        will be weighted to be assumed to be 100% within the given geo polygon.

        """
        geo_polygon = Polygon(geo_feature.geometry_ring)

        if relationship == CensusBlockRelationship.pct_overlap:
            census_block_group_polygons = {
                feat.attributes["GEOID"]: Polygon(feat.geometry_ring)
                for feat in census_features
            }
            census_pct = self._get_census_blocks_in_geography_by_pct_area(
                census_block_group_polygons, geo_polygon
            )
        elif relationship == CensusBlockRelationship.centroid_is_within:
            census_block_group_centroids = {
                feat.attributes["GEOID"]: Point(
                    float(feat.attributes["CENTLON"]),
                    float(feat.attributes["CENTLAT"]),
                )
                for feat in census_features
            }
            census_pct = self._get_census_blocks_in_geography_by_centroid(
                census_block_group_centroids, geo_polygon
            )
        return census_pct

    def _get_census_blocks_in_geography_by_pct_area(
        self, census_block_group_polygons, geo_polygon
    ):
        census_block_groups = {
            census_name: census_x
            for census_name, census_x in census_block_group_polygons.items()
            if census_x.intersects(geo_polygon)
            and census_x.intersection(geo_polygon).area > 0.000001
        }
        return {
            census_name: census_x.intersection(geo_polygon).area / census_x.area
            for census_name, census_x in census_block_groups.items()
        }

    def _get_census_blocks_in_geography_by_centroid(
        self, census_block_group_centroids, geo_polygon
    ):
        return {
            census_name: 1
            for census_name, census_x in census_block_group_centroids.items()
            if census_x.within(geo_polygon)
        }

    def generate_geo_matched_results(
        self,
        /,
        *,
        state_fips: str,
        county_fips: str,
        relationship: CensusBlockRelationship,
    ):
        census_block_group_features = self.get_census_block_group_features(
            state_fips=state_fips, county_fips=county_fips
        )
        geo_features = self.get_arcgis_features()

        results = self.get_census_block_group_overlap_between_given_features(
            geo_features=geo_features,
            census_features=census_block_group_features,
            relationship=relationship,
        )
        return GeoMatchedResults(
            results=results,
            unique_geo_column=self.other_arcgis.source.unique_geo_column,
        )


class GeoMatchedResults(BaseModel):
    results: dict[str, Any]
    unique_geo_column: str
