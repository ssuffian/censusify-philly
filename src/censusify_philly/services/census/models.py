from censusify_philly.services.census.column_mappings import (
    variable_race_mapping,
    variable_hispanic_mapping,
)
from census import Census
from pydantic import BaseModel
from typing import Any


class CensusDataQuery:
    def __init__(self, census_obj: Census):
        self.c = census_obj

    async def get_all_demographic_data_for_county(self, state_fips, county_fips):
        census_demographic_results = await self._get_demographic_data(
            state_fips=state_fips, county_fips=county_fips, tract="*", blockgroup="*"
        )
        return [
            CensusBlockGroupDemographicsResult(result)
            for result in census_demographic_results
        ]

    async def get_demographic_data_from_census_dict(self, census_geo):
        census_demographic_results = await self._get_demographic_data(
            state_fips=census_geo["STATE"],
            county_fips=census_geo["COUNTY"],
            tract=census_geo["TRACT"],
            blockgroup=census_geo["BLKGRP"],
        )
        return CensusBlockGroupDemographicsResult(census_demographic_results[0])

    async def _get_demographic_data(self, state_fips, county_fips, tract, blockgroup):
        race_cols = [f"P1_00{i}N" for i in range(3, 10)]
        hispanic_cols = ["P2_002N", "P2_003N"]
        race_hispanic_cols = [f"P2_0{i:02}N" for i in range(4, 12)]
        results = self.c.pl.state_county_blockgroup(
            fields=["NAME"] + race_hispanic_cols + race_cols + hispanic_cols,
            state_fips=state_fips,
            county_fips=county_fips,
            tract=tract,
            blockgroup=blockgroup,
        )

        def _rename_key(key):
            if key in variable_race_mapping.keys():
                output = (
                    "p1_"
                    + variable_race_mapping[key]
                    .replace("!!Total:!!", "")
                    .replace("Population of one race:!!", "")
                    .replace("Population of two or more races", "Multiracial")
                    .strip()
                )
            elif key in variable_hispanic_mapping.keys():
                output = (
                    "p2_"
                    + variable_hispanic_mapping[key]
                    .replace("!!Total:!!", "")
                    .replace("Population of one race:!!", "")
                    .replace("Population of two or more races", "Multiracial")
                    .strip()
                )
            else:
                output = key
            return output.replace(" ", "_").lower()

        renamed_results = [
            {_rename_key(k): v for k, v in result.items()} for result in results
        ]
        return renamed_results


class CensusBlockGroupDemographicsResult:
    def __init__(self, census_data):
        self.state_fips = census_data["state"]
        self.county_fips = census_data["county"]
        self.census_tract = census_data["tract"]
        self.census_block_group = census_data["block_group"]
        self.census_geoid = f"{self.state_fips}{self.county_fips}{self.census_tract}{self.census_block_group}"
        self.census_data = census_data

    def __str__(self):
        return f"{census_geoid}, {self.odp_demographics_data_pct}"
