import httpx


class ArcgisQuery:
    def __init__(self, base_url, attribute_col_str="*"):
        self.base_url = base_url
        self.initial_params = {
            "inSr": 4326,  # required to pass in lat/lngs
            "outSr": 4326,  # required to return data in lat/lng format
            "spatialRel": "esriSpatialRelWithin",
            "outFields": attribute_col_str,
            "f": "json",
        }

    async def get_by_lat_lng(self, /, *, lat, lng, include_geometry=False):
        params = self.initial_params.copy()
        params.update(
            {
                "geometry": f"{lng},{lat}",
                "geometryType": "esriGeometryPoint",
                "returnGeometry": include_geometry,
            }
        )
        return await self._get(params)

    async def get_all_by_attribute(self, /, *, where_str, include_geometry=True):
        params = self.initial_params.copy()
        params.update(
            {
                "where": where_str,
                "geometryType": "esriGeometryPoint",
                "returnGeometry": include_geometry,
            }
        )
        return await self._list(params)

    async def _request(self, params):
        async with httpx.AsyncClient(timeout=30) as client:
            return await client.get(self.base_url, params=params)

    async def _get(self, params):
        response = await self._request(params=params)
        result = response.json()["features"][0]
        return ArcgisResult(
            attributes=result["attributes"], geometry=result.get("geometry")
        )

    async def _list(self, params):
        response = await self._request(params=params)
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
