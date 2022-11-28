from censusify_philly.census.column_mappings import (
    variable_race_mapping,
    variable_hispanic_mapping,
)
from census import Census
import pandas as pd
from pydantic import BaseModel
from pydantic import root_validator
from typing import Any


class CensusDataQuery:
    def __init__(self, census: Census):
        self.census = census

    def assign_demographic_data_to_custom_geographies(
        self, geo_results: dict[str, Any], census_demographics_df: pd.DataFrame
    ):

        results = []
        for (
            geography_name,
            census_block_group_overlap_weights,
        ) in geo_results.results.items():
            census_weighting_series = pd.Series(census_block_group_overlap_weights)
            result = (
                census_demographics_df.loc[census_weighting_series.index]
                .mul(census_weighting_series, axis=0)
                .sum()
                .round()
                .to_dict()
            )
            geography_col = geo_results.unique_geo_column
            result[geography_col] = geography_name
            results.append(result)
        return pd.DataFrame(results).set_index(geography_col)

    def get_all_demographic_data_for_county(self, state_fips, county_fips):
        census_demographic_results = self._get_demographic_data(
            state_fips=state_fips, county_fips=county_fips, tract="*", blockgroup="*"
        )
        return CensusBlockGroupDemographicsCollection(
            demographics=[
                CensusBlockGroupDemographics.from_census_data(result)
                for result in census_demographic_results
            ]
        ).to_df()

    def _get_demographic_data(self, state_fips, county_fips, tract, blockgroup):
        totals_cols = ["P1_001N"]
        race_cols = [f"P1_00{i}N" for i in range(3, 10)]
        hispanic_cols = ["P2_002N", "P2_003N"]
        race_hispanic_cols = [f"P2_0{i:02}N" for i in range(4, 12)]
        return self.census.pl.state_county_blockgroup(
            fields=["NAME"]
            + race_hispanic_cols
            + race_cols
            + hispanic_cols
            + totals_cols,
            state_fips=state_fips,
            county_fips=county_fips,
            tract=tract,
            blockgroup=blockgroup,
        )


class CensusDemographicsResult(BaseModel):
    american_indian: int
    asian: int
    unknown: int
    white: int
    black_or_african_american: int
    hispanic_or_latino: int
    total: int

    @root_validator
    def check_numbers_equal_total(cls, values: dict[str, Any]) -> dict[str, Any]:
        total_check = sum([values[key] for key in values.keys() if key != "total"])
        total = values["total"]
        if total_check != total:
            raise ValueError(
                f"There should be {total} people but demographics add up to {total_check}"
            )
        return values


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
                )
            else:
                output = key
            return output.replace(" ", "_").lower()

        return {_rename_key(k): v for k, v in single_geography_demographics.items()}

    @staticmethod
    def simplify_census_demographics_columns(renamed_results: dict[str, Any]):
        return CensusDemographicsResult(
            american_indian=renamed_results[
                "p2_007n_not_hispanic_or_latino:!!american_indian_and_alaska_native_alone"
            ],
            asian=renamed_results["p2_008n_not_hispanic_or_latino:!!asian_alone"]
            + renamed_results[
                "p2_009n_not_hispanic_or_latino:!!native_hawaiian_and_other_pacific_islander_alone"
            ],
            unknown=renamed_results[
                "p2_010n_not_hispanic_or_latino:!!some_other_race_alone"
            ]
            + renamed_results["p2_011n_not_hispanic_or_latino:!!multiracial:"],
            white=renamed_results["p2_005n_not_hispanic_or_latino:!!white_alone"],
            black_or_african_american=renamed_results[
                "p2_006n_not_hispanic_or_latino:!!black_or_african_american_alone"
            ],
            hispanic_or_latino=renamed_results["p2_002n_hispanic_or_latino"],
            total=renamed_results["p1_001n_!!total:"],
        )

    def as_flat_dict(self):
        return {"geoid": self.geoid, **self.result.dict()}

    @classmethod
    def from_census_data(cls, result):
        renamed_result = cls.rename_census_demographics_columns(result)
        simplified_result = cls.simplify_census_demographics_columns(renamed_result)
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
