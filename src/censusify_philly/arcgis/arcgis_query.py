import httpx
from pydantic import BaseModel


class ArcgisQuerySource(BaseModel):
    url: str
    unique_geo_column: str
    attribute_col_str: str = "*"


class ArcgisQuery:
    def __init__(self, arcgis_query_source: ArcgisQuerySource):
        self.source = arcgis_query_source
        self.base_url = self.source.url
        self.initial_params = {
            "inSr": 4326,  # required to pass in lat/lngs
            "outSr": 4326,  # required to return data in lat/lng format
            "spatialRel": "esriSpatialRelWithin",
            "outFields": self.source.attribute_col_str,
            "f": "json",
        }

    def get_by_lat_lng(self, /, *, lat, lng, include_geometry=False):
        params = self.initial_params.copy()
        params.update(
            {
                "geometry": f"{lng},{lat}",
                "geometryType": "esriGeometryPoint",
                "returnGeometry": include_geometry,
            }
        )
        return self._get(params)

    def get_raw_geojson(self):
        params = self.initial_params.copy()
        response = self._request(params=params)
        return response.json()

    def get_all_by_attribute(self, where_str, /, *, include_geometry=True):
        params = self.initial_params.copy()
        params.update(
            {
                "where": where_str,
                "geometryType": "esriGeometryPoint",
                "returnGeometry": include_geometry,
            }
        )
        return self._list(params)

    def _request(self, params):
        with httpx.Client(timeout=30) as client:
            return client.get(self.base_url, params=params)

    def _get(self, params):
        response = self._request(params=params)
        result = response.json()["features"][0]
        return ArcgisResult(
            attributes=result["attributes"], geometry=result.get("geometry")
        )

    def _list(self, params):
        response = self._request(params=params)
        results = response.json()["features"]
        return [
            ArcgisResult(
                attributes=result["attributes"], geometry=result.get("geometry")
            )
            for result in results
        ]


class ArcgisResult:
    def __init__(self, /, *, attributes, geometry):
        self.attributes = attributes
        self.geometry_ring = geometry["rings"][0] if geometry else None
