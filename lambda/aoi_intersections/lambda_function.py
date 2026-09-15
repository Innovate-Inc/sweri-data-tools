"""Lambda implementation of AOI intersection processing without arcpy.

Inputs (event/body JSON):
- aoi: AOI geometry as Esri FeatureSet JSON, GeoJSON FeatureCollection, or list of features.
- intersection_features_url: ArcGIS FeatureServer layer URL containing at least unique_id, feat_source, geometry.
- intersections_source_list: either
    * list[dict] rows with id_source/use_as_target/source_type/name
    * ArcGIS table URL returning rows with those fields

Output:
- ArcGIS FeatureSet-like JSON with features[].attributes and features[].geometry.
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import boto3
import requests
from pyproj import Geod, Transformer
from shapely.geometry import GeometryCollection, LineString, MultiLineString, MultiPolygon, Point, Polygon, shape
from shapely.ops import unary_union

GEOD = Geod(ellps="WGS84")
M2_TO_ACRES = 0.00024710538146717
SOURCE_FIELDS = ["source", "id_source", "uid_fields", "use_as_target", "source_type", "name"]


@dataclass
class FeatureRow:
    geometry: Any
    unique_id: str
    feat_source: str


@dataclass
class AoiRow:
    geometry: Any
    unique_id: str
    feat_source: str


def _parse_event_payload(event: Dict[str, Any]) -> Dict[str, Any]:
    if event.get("body"):
        body = event["body"]
        if event.get("isBase64Encoded"):
            raise ValueError("Base64 encoded payloads are not supported")
        return json.loads(body) if isinstance(body, str) else body
    return event


def _to_feature_collection(value: Any) -> Dict[str, Any]:
    if isinstance(value, str):
        value = json.loads(value)

    if isinstance(value, dict) and "features" in value:
        # GeoJSON feature collection
        if value.get("type") == "FeatureCollection":
            return value
        # Esri FeatureSet style
        if isinstance(value.get("features"), list):
            return _esri_featureset_to_geojson(value)

    if isinstance(value, list):
        return {"type": "FeatureCollection", "features": value}

    if isinstance(value, dict) and value.get("type") == "Feature":
        return {"type": "FeatureCollection", "features": [value]}

    raise ValueError("Unsupported AOI payload format")


def _esri_geom_to_geojson(geom: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not geom:
        return None

    if "rings" in geom:
        rings = geom["rings"]
        if not rings:
            return None
        if len(rings) == 1:
            return {"type": "Polygon", "coordinates": [rings[0]]}
        return {"type": "Polygon", "coordinates": rings}

    if "paths" in geom:
        paths = geom["paths"]
        if len(paths) == 1:
            return {"type": "LineString", "coordinates": paths[0]}
        return {"type": "MultiLineString", "coordinates": paths}

    if "x" in geom and "y" in geom:
        return {"type": "Point", "coordinates": [geom["x"], geom["y"]]}

    return None


def _esri_featureset_to_geojson(value: Dict[str, Any]) -> Dict[str, Any]:
    features: List[Dict[str, Any]] = []
    for feat in value.get("features", []):
        esri_geom = feat.get("geometry")
        geojson_geom = _esri_geom_to_geojson(esri_geom) if esri_geom else None
        features.append(
            {
                "type": "Feature",
                "geometry": geojson_geom,
                "properties": feat.get("attributes", {}),
            }
        )
    return {"type": "FeatureCollection", "features": features}


def _load_intersection_source_rows(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        return value

    if isinstance(value, str):
        # Treat string as ArcGIS table URL
        return _query_arcgis_table_rows(value, SOURCE_FIELDS)

    if isinstance(value, dict) and isinstance(value.get("features"), list):
        rows: List[Dict[str, Any]] = []
        for feat in value["features"]:
            if isinstance(feat, dict):
                rows.append(feat.get("attributes", {}))
        return rows

    raise ValueError("Unsupported intersections_source_list format")


def _query_arcgis_table_rows(table_url: str, fields: List[str]) -> List[Dict[str, Any]]:
    params = {
        "f": "json",
        "where": "1=1",
        "outFields": ",".join(fields),
        "returnGeometry": "false",
    }

    resp = requests.post(f"{table_url}/query", data=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()
    if "error" in data:
        raise RuntimeError(f"Failed to query intersections source list: {data['error']}")

    rows: List[Dict[str, Any]] = []
    for feat in data.get("features", []):
        rows.append(feat.get("attributes", {}))

    # Preserve source_type order like original script.
    rows.sort(key=lambda r: (str(r.get("source_type", "")), str(r.get("id_source", ""))))
    return rows


def configure_intersection_sources(rows: List[Dict[str, Any]]) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Dict[str, Any]]]:
    intersect_sources: Dict[str, Dict[str, Any]] = {}
    intersect_targets: Dict[str, Dict[str, Any]] = {}

    for r in rows:
        source_key = str(r.get("id_source", ""))
        source = {
            "source": r.get("source"),
            "id": r.get("uid_fields"),
            "source_type": r.get("source_type"),
            "name": r.get("name") or source_key,
        }
        intersect_sources[source_key] = source

        use_as_target = r.get("use_as_target")
        if use_as_target in (1, True, "1", "true", "True"):
            intersect_targets[source_key] = source

    return intersect_sources, intersect_targets


def _parse_aoi_rows(aoi_payload: Any) -> List[AoiRow]:
    fc = _to_feature_collection(aoi_payload)
    rows: List[AoiRow] = []
    for feat in fc.get("features", []):
        geom = feat.get("geometry")
        if not geom:
            continue
        shp = shape(geom)
        props = feat.get("properties", {})
        rows.append(
            AoiRow(
                geometry=shp,
                unique_id=str(props.get("unique_id", "aoi-feature")),
                feat_source=str(props.get("feat_source", "aoi")),
            )
        )
    if not rows:
        raise ValueError("AOI did not contain any valid geometry")
    return rows


def _normalize_to_wgs84(geom_json: Dict[str, Any], wkid: Optional[int]) -> Any:
    shp = shape(geom_json)
    if wkid in (None, 4326):
        return shp

    if wkid == 3857:
        transformer = Transformer.from_crs("EPSG:3857", "EPSG:4326", always_xy=True)

        def _tx(x: float, y: float, z: Optional[float] = None) -> Tuple[float, float]:
            return transformer.transform(x, y)

        from shapely.ops import transform

        return transform(_tx, shp)

    return shp


def _get_layer_default_wkid(layer_url: str) -> Optional[int]:
    response = requests.get(layer_url, params={"f": "json"}, timeout=30)
    response.raise_for_status()
    data = response.json()
    if "extent" in data and isinstance(data["extent"], dict):
        sr = data["extent"].get("spatialReference") or {}
        if isinstance(sr.get("wkid"), int):
            return sr["wkid"]
    return None


def _query_target_features(layer_url: str, target_key: str, default_wkid: Optional[int]) -> List[FeatureRow]:
    params = {
        "f": "json",
        "where": f"feat_source = '{target_key}'",
        "outFields": "unique_id,feat_source",
        "returnGeometry": "true",
        "outSR": 4326,
    }

    response = requests.post(f"{layer_url}/query", data=params, timeout=120)
    response.raise_for_status()
    data = response.json()

    if "error" in data:
        raise RuntimeError(f"Failed to query target features for {target_key}: {data['error']}")

    rows: List[FeatureRow] = []
    for feat in data.get("features", []):
        attrs = feat.get("attributes", {})
        esri_geom = feat.get("geometry")
        if not esri_geom:
            continue

        geom_geojson = _esri_geom_to_geojson(esri_geom)
        if not geom_geojson:
            continue

        sr = esri_geom.get("spatialReference") or {}
        wkid = sr.get("wkid") if isinstance(sr, dict) else None
        wkid = wkid if isinstance(wkid, int) else default_wkid

        rows.append(
            FeatureRow(
                geometry=_normalize_to_wgs84(geom_geojson, wkid),
                unique_id=str(attrs.get("unique_id", "")),
                feat_source=str(attrs.get("feat_source", target_key)),
            )
        )

    return rows


def _extract_preferred_geometry(geom: Any) -> Optional[Any]:
    if geom is None or geom.is_empty:
        return None

    if isinstance(geom, GeometryCollection):
        polygons = [g for g in geom.geoms if isinstance(g, (Polygon, MultiPolygon)) and not g.is_empty]
        if polygons:
            return unary_union(polygons)
        lines = [g for g in geom.geoms if isinstance(g, (LineString, MultiLineString)) and not g.is_empty]
        if lines:
            return unary_union(lines)
        points = [g for g in geom.geoms if isinstance(g, Point) and not g.is_empty]
        if points:
            return points[0]
        return None

    return geom


def _geodesic_acres(geom: Any, aoi_is_polygon: bool) -> float:
    if not aoi_is_polygon or geom is None or geom.is_empty:
        return 0.0

    if not isinstance(geom, (Polygon, MultiPolygon)):
        return 0.0

    area_m2, _ = GEOD.geometry_area_perimeter(geom)
    return abs(area_m2) * M2_TO_ACRES


def _shape_to_esri_geometry(geom: Any) -> Optional[Dict[str, Any]]:
    if geom is None or geom.is_empty:
        return None

    if isinstance(geom, Polygon):
        rings = [list(geom.exterior.coords)] + [list(r.coords) for r in geom.interiors]
        return {"rings": rings, "spatialReference": {"wkid": 4326}}

    if isinstance(geom, MultiPolygon):
        rings: List[List[List[float]]] = []
        for poly in geom.geoms:
            rings.append(list(poly.exterior.coords))
            rings.extend(list(r.coords) for r in poly.interiors)
        return {"rings": rings, "spatialReference": {"wkid": 4326}}

    if isinstance(geom, LineString):
        return {"paths": [list(geom.coords)], "spatialReference": {"wkid": 4326}}

    if isinstance(geom, MultiLineString):
        return {"paths": [list(g.coords) for g in geom.geoms], "spatialReference": {"wkid": 4326}}

    if isinstance(geom, Point):
        return {"x": geom.x, "y": geom.y, "spatialReference": {"wkid": 4326}}

    return None


def _build_result_feature_set(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    fields = [
        {"name": "OBJECTID", "type": "esriFieldTypeOID", "alias": "OBJECTID"},
        {"name": "id_1", "type": "esriFieldTypeString", "alias": "id_1", "length": 255},
        {"name": "id_1_source", "type": "esriFieldTypeString", "alias": "id_1_source", "length": 255},
        {"name": "id_2", "type": "esriFieldTypeString", "alias": "id_2", "length": 255},
        {"name": "id_2_source", "type": "esriFieldTypeString", "alias": "id_2_source", "length": 255},
        {"name": "acre_overlap", "type": "esriFieldTypeDouble", "alias": "acre_overlap"},
    ]

    features = []
    for i, rec in enumerate(records, start=1):
        attrs = dict(rec["attributes"])
        attrs["OBJECTID"] = i
        features.append({"attributes": attrs, "geometry": rec.get("geometry")})

    geometry_type = "esriGeometryPolygon"
    for feat in features:
        geom = feat.get("geometry") or {}
        if "paths" in geom:
            geometry_type = "esriGeometryPolyline"
            break
        if "x" in geom and "y" in geom:
            geometry_type = "esriGeometryPoint"
            break

    return {
        "displayFieldName": "",
        "geometryType": geometry_type,
        "spatialReference": {"wkid": 4326},
        "fields": fields,
        "features": features,
    }


def _upload_result_if_configured(feature_set: Dict[str, Any], name: str) -> Optional[Dict[str, str]]:
    bucket = os.getenv("AOI_INTERSECTIONS_BUCKET")
    if not bucket:
        return None

    prefix = os.getenv("AOI_INTERSECTIONS_PREFIX", "aoi-intersections").rstrip("/")
    key = f"{prefix}/{name}.json"

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False, encoding="utf-8") as tmp:
        json.dump(feature_set, tmp, separators=(",", ":"))
        tmp_path = tmp.name

    s3 = boto3.client("s3")
    s3.upload_file(tmp_path, bucket, key, ExtraArgs={"ContentType": "application/json"})
    os.remove(tmp_path)

    url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=3600,
    )
    return {"bucket": bucket, "key": key, "downloadUrl": url}


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    try:
        payload = _parse_event_payload(event)
        aoi_payload = payload.get("aoi")
        intersection_features_url = payload.get("intersection_features_url")
        source_list_payload = payload.get("intersections_source_list")

        if (
            not aoi_payload
            or not isinstance(intersection_features_url, str)
            or not intersection_features_url
            or source_list_payload is None
        ):
            return {
                "statusCode": 400,
                "body": json.dumps(
                    {
                        "message": "aoi, intersection_features_url, and intersections_source_list are required"
                    }
                ),
            }

        aoi_rows = _parse_aoi_rows(aoi_payload)
        aoi_is_polygon = all(isinstance(r.geometry, (Polygon, MultiPolygon)) for r in aoi_rows)

        source_rows = _load_intersection_source_rows(source_list_payload)
        _, intersect_targets = configure_intersection_sources(source_rows)

        default_wkid = _get_layer_default_wkid(intersection_features_url)
        records: List[Dict[str, Any]] = []

        for target_key, target_value in intersect_targets.items():
            target_rows = _query_target_features(intersection_features_url, target_key, default_wkid)

            # Raw intersections
            dissolve_geoms: List[Any] = []
            for aoi_row in aoi_rows:
                for target_row in target_rows:
                    if not aoi_row.geometry.intersects(target_row.geometry):
                        continue

                    inter = _extract_preferred_geometry(aoi_row.geometry.intersection(target_row.geometry))
                    if inter is None:
                        continue

                    dissolve_geoms.append(inter)
                    records.append(
                        {
                            "attributes": {
                                "id_1": aoi_row.unique_id,
                                "id_1_source": "aoi",
                                "id_2": target_row.unique_id,
                                "id_2_source": target_key,
                                "acre_overlap": _geodesic_acres(inter, aoi_is_polygon),
                            },
                            "geometry": _shape_to_esri_geometry(inter),
                        }
                    )

            # Dissolved total overlap per target source
            if dissolve_geoms:
                dissolved = _extract_preferred_geometry(unary_union(dissolve_geoms))
                if dissolved is not None:
                    records.append(
                        {
                            "attributes": {
                                "id_1": "aoi-feature",
                                "id_1_source": "aoi",
                                "id_2": "dissolve",
                                "id_2_source": target_key,
                                "acre_overlap": _geodesic_acres(dissolved, aoi_is_polygon),
                            },
                            "geometry": _shape_to_esri_geometry(dissolved),
                        }
                    )

        feature_set = _build_result_feature_set(records)
        result_name = payload.get("result_name") or "aoi_intersections"
        upload = _upload_result_if_configured(feature_set, result_name)

        response_body: Dict[str, Any] = {
            "message": "AOI intersections generated",
            "count": len(feature_set["features"]),
            "result": feature_set,
        }
        if upload:
            response_body.update(upload)

        return {"statusCode": 200, "body": json.dumps(response_body)}
    except Exception as exc:  # pylint: disable=broad-except
        return {"statusCode": 500, "body": json.dumps({"message": str(exc)})}


