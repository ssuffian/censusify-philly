from censusify_philly.census.column_mappings import (
    variable_race_mapping,
    variable_hispanic_mapping,
)
from census import Census
import pandas as pd
from pydantic import BaseModel
from pydantic import root_validator
from typing import Any


class CensusDemographicsResult(BaseModel):
    @root_validator
    def check_numbers_equal_total(cls, values: dict[str, Any]) -> dict[str, Any]:
        total_check = sum([values[key] for key in values.keys() if key != "total"])
        total = values["total"]
        if total_check != total:
            raise ValueError(
                f"There should be {total} people but demographics add up to {total_check}"
            )
        return values

    @classmethod
    def from_raw_census_data(cls, results):
        raise NotImplementedError

    @staticmethod
    def as_df(results):
        raise NotImplementedError


class CensusDataQuery:
    def __init__(self, census: Census):
        self.census = census

    def get_demographic_data(
        self, state_fips: str, county_fips: str, tract: str = "*", blockgroup: str = "*"
    ):
        race_cols = [f"P1_00{i}N" for i in range(1, 10)]
        race_hispanic_cols = [f"P2_0{i:02}N" for i in range(1, 12)]
        return self.census.pl.state_county_blockgroup(
            fields=["NAME"] + race_hispanic_cols + race_cols,
            state_fips=state_fips,
            county_fips=county_fips,
            tract=tract,
            blockgroup=blockgroup,
        )


class CensusBlockGroupDemographics(BaseModel):
    result: CensusDemographicsResult
    name: str
    state_fips: str
    county_fips: str
    tract: str
    block_group: str

    @property
    def geoid(self):
        return f"{self.state_fips}{self.county_fips}{self.tract}{self.block_group}"

    @staticmethod
    def rename_census_demographics_columns(
        single_geography_demographics: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Renames census results from fields:
        i.e. P2_008N becomes p2_008n_not_hispanic_or_latino:!!asian_alone'
        """

        def _rename_key(key):
            if key in variable_race_mapping.keys():
                output = (
                    key
                    + "_"
                    + variable_race_mapping[key]
                    .replace("!!Total:!!", "")
                    .replace("Population of one race:!!", "")
                    .replace("Population of two or more races", "Multiracial")
                    .strip()
                    .rstrip(":")
                )
            elif key in variable_hispanic_mapping.keys():
                output = (
                    key
                    + "_"
                    + variable_hispanic_mapping[key]
                    .replace("!!Total:!!", "")
                    .replace("Population of one race:!!", "")
                    .replace("Population of two or more races", "Multiracial")
                    .strip()
                    .rstrip(":")
                )
            else:
                output = key
            return output.replace(" ", "_").lower()

        return {_rename_key(k): v for k, v in single_geography_demographics.items()}

    def as_flat_dict(self):
        return {"geoid": self.geoid, **self.result.dict()}

    @classmethod
    def from_census_data(
        cls,
        CensusDemographicsResultClass: CensusDemographicsResult,
        result: dict[str, Any],
    ):
        # Converts the columns to names with underscores and that include description
        renamed_result = cls.rename_census_demographics_columns(result)
        # Further simplifies the names
        simplified_result = CensusDemographicsResultClass.from_renamed_result(
            renamed_result
        )
        return cls(
            name=result["NAME"],
            state_fips=result["state"],
            county_fips=result["county"],
            tract=result["tract"],
            block_group=result["block group"],
            result=simplified_result,
        )


class CensusBlockGroupDemographicsCollection(BaseModel):
    demographics: list[CensusBlockGroupDemographics]

    def to_df(self):
        return pd.DataFrame(
            [demo.as_flat_dict() for demo in self.demographics]
        ).set_index("geoid")
