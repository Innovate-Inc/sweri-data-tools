"""AWS Lambda download export service.

This Lambda mirrors the GP download behavior without arcpy:
- Queries an ArcGIS Feature Service layer
- Applies the NASF/NGO exclusion for reshape services when supported
- Exports to GeoJSON or CSV
- Adds the CMS disclaimer file
- Uploads a zip to S3 and returns a presigned download URL
"""

import csv
import datetime as dt
import json
import os
import re
import tempfile
import zipfile
from typing import Any, Dict, Iterable, List, Optional

import boto3
import requests

try:
    from osgeo.gdal import VectorTranslate, VectorTranslateOptions

    HAS_GDAL = True
except ModuleNotFoundError:
    VectorTranslate = None
    VectorTranslateOptions = None
    HAS_GDAL = False

RESHAPE_HOST_PATTERN = re.compile(r"reshapewildfire\.org", re.IGNORECASE)
DEFAULT_API_BASE_URL = "https://cms.reshapewildfire.org/api/v2/"
MAX_FEATURES = 10000
CHUNK_SIZE = 1000
SUPPORTED_FILETYPES = {"geojson", "csv", "filegdb", "shapefile"}


def _sanitize_name(value: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in value)


def _get_request_payload(event: Dict[str, Any]) -> Dict[str, Any]:
    if "body" in event and event["body"]:
        body = event["body"]
        if event.get("isBase64Encoded"):
            raise ValueError("Base64 encoded payloads are not supported")
        if isinstance(body, str):
            return json.loads(body)
        if isinstance(body, dict):
            return body
    return event


def _is_reshape_url(url: str) -> bool:
    return RESHAPE_HOST_PATTERN.search(url) is not None


def _get_layer_fields(layer_url: str) -> List[str]:
    response = requests.get(layer_url, params={"f": "json"}, timeout=30)
    response.raise_for_status()
    data = response.json()
    names: List[str] = []
    for field in data.get("fields", []):
        if isinstance(field, dict) and isinstance(field.get("name"), str):
            names.append(field["name"])
    return names


def _build_where_clause(layer_url: str, where: str) -> str:
    where_clause = where or "1=1"
    if _is_reshape_url(layer_url):
        fields = _get_layer_fields(layer_url)
        if "identifier_database" in fields:
            where_clause += " AND identifier_database NOT IN ('NASF','NGO')"
    return where_clause


def _get_ids(layer_url: str, where: str, geometry: Optional[Dict[str, Any]], geometry_type: Optional[str]) -> List[int]:
    geom_map = {
        "polygon": "esriGeometryPolygon",
        "point": "esriGeometryPoint",
        "extent": "esriGeometryEnvelope",
        "multipoint": "esriGeometryMultipoint",
        "polyline": "esriGeometryPolyline",
    }

    params: Dict[str, Any] = {
        "where": where,
        "returnIdsOnly": "true",
        "f": "json",
    }

    geometry_key = geom_map.get(geometry_type) if geometry_type else None
    if geometry and geometry_key:
        params.update(
            {
                "geometry": json.dumps(geometry),
                "geometryType": geometry_key,
                "spatialRel": "esriSpatialRelIntersects",
            }
        )

    response = requests.post(f"{layer_url}/query", data=params, timeout=60)
    response.raise_for_status()
    data = response.json()

    if "error" in data:
        raise RuntimeError(f"Failed to fetch object IDs: {data['error']}")

    ids = data.get("objectIds") or []
    if not isinstance(ids, list):
        raise RuntimeError("Invalid objectIds response")
    return ids


def _chunked(items: List[int], size: int) -> Iterable[List[int]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def _fetch_geojson_features(layer_url: str, ids: List[int]) -> List[Dict[str, Any]]:
    features: List[Dict[str, Any]] = []
    for id_chunk in _chunked(ids, CHUNK_SIZE):
        params = {
            "f": "geojson",
            "objectIds": ",".join(str(i) for i in id_chunk),
            "outFields": "*",
            "returnGeometry": "true",
            "outSR": 4326,
        }
        response = requests.post(f"{layer_url}/query", data=params, timeout=120)
        response.raise_for_status()
        data = response.json()
        chunk_features = data.get("features", [])
        if not isinstance(chunk_features, list):
            raise RuntimeError("Unexpected feature response while downloading")
        features.extend(chunk_features)
    return features


def _write_geojson(path: str, features: List[Dict[str, Any]]) -> None:
    collection = {
        "type": "FeatureCollection",
        "features": features,
    }
    with open(path, "w", encoding="utf-8") as f:
        json.dump(collection, f, separators=(",", ":"))


def _write_csv(path: str, features: List[Dict[str, Any]]) -> None:
    property_keys = sorted(
        {
            key
            for feature in features
            for key in (feature.get("properties") or {}).keys()
        }
    )
    fieldnames = property_keys + ["geometry"]

    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for feature in features:
            row = dict(feature.get("properties") or {})
            row["geometry"] = json.dumps(feature.get("geometry"))
            writer.writerow(row)


def _export_with_gdal(out_dir: str, out_name: str, features: List[Dict[str, Any]], filetype: str) -> None:
    if not HAS_GDAL:
        raise RuntimeError(
            "GDAL is required for filegdb and shapefile exports. "
            "Attach a Lambda layer/image with osgeo support."
        )

    source_geojson = os.path.join(out_dir, f"{out_name}.geojson")
    _write_geojson(source_geojson, features)

    if filetype == "filegdb":
        dst = os.path.join(out_dir, f"{out_name}.gdb")
        options = VectorTranslateOptions(
            format="OpenFileGDB",
            layerName=out_name,
            geometryType=["PROMOTE_TO_MULTI", "MULTIPOLYGON"],
            makeValid=True,
        )
    elif filetype == "shapefile":
        dst = os.path.join(out_dir, f"{out_name}.shp")
        options = VectorTranslateOptions(
            format="ESRI Shapefile",
            layerName=out_name,
            geometryType=["PROMOTE_TO_MULTI", "MULTIPOLYGON"],
            makeValid=True,
        )
    else:
        raise RuntimeError(f"Unsupported GDAL export type: {filetype}")

    out_ds = VectorTranslate(destNameOrDestDS=dst, srcDS=source_geojson, options=options)
    if out_ds is None:
        raise RuntimeError(f"Failed to convert GeoJSON to {filetype}")
    out_ds = None

    os.remove(source_geojson)


def _get_disclaimer_html(api_url: str) -> Optional[str]:
    try:
        response = requests.get(api_url, timeout=30)
        response.raise_for_status()
        body = response.json()
        content = body.get("content")
        return content if isinstance(content, str) else None
    except requests.RequestException:
        return None


def _zip_output(out_dir: str, zip_path: str) -> None:
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, _dirs, files in os.walk(out_dir):
            for file_name in files:
                abs_name = os.path.join(root, file_name)
                arc_name = os.path.relpath(abs_name, out_dir)
                zf.write(abs_name, arc_name)


def _upload_zip(zip_path: str, key: str, expires_in: int = 3600) -> Dict[str, str]:
    bucket = os.environ["DOWNLOAD_EXPORT_BUCKET"]
    s3 = boto3.client("s3")

    s3.upload_file(
        zip_path,
        bucket,
        key,
        ExtraArgs={
            "ContentType": "application/zip",
            "ServerSideEncryption": "AES256",
        },
    )

    download_url = s3.generate_presigned_url(
        "get_object",
        Params={"Bucket": bucket, "Key": key},
        ExpiresIn=expires_in,
    )

    return {"bucket": bucket, "key": key, "downloadUrl": download_url}


def lambda_handler(event: Dict[str, Any], _context: Any) -> Dict[str, Any]:
    payload = _get_request_payload(event)

    layer_url = payload.get("url")
    fc_in = payload.get("fc")
    filetype = (payload.get("type") or payload.get("filetype") or "").lower()
    where = payload.get("where") or "1=1"
    geometry = payload.get("geometry")
    geometry_type = payload.get("geometry_type")
    api_url = payload.get("api_url")

    if not isinstance(layer_url, str) or not isinstance(fc_in, str) or not filetype:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": "url, fc, and type are required"}),
        }

    if not isinstance(where, str):
        where = "1=1"

    if geometry_type is not None and not isinstance(geometry_type, str):
        geometry_type = None

    if filetype == "gdb":
        # Keep compatibility with the old GP input label.
        filetype = "filegdb"

    if filetype not in SUPPORTED_FILETYPES:
        return {
            "statusCode": 400,
            "body": json.dumps({"message": f"Unsupported export type: {filetype}"}),
        }

    api_base_url = os.getenv("API_URL", DEFAULT_API_BASE_URL)
    resolved_api_url = api_url or f"{api_base_url.rstrip('/')}/snippets/download_disclaimer/"

    try:
        final_where = _build_where_clause(layer_url, where)
        ids = _get_ids(layer_url, final_where, geometry, geometry_type)

        if len(ids) > MAX_FEATURES:
            raise RuntimeError(f"Feature count {len(ids)} exceeds 10,000, please refine your query")

        features = _fetch_geojson_features(layer_url, ids)
        if not features:
            raise RuntimeError(f"No features fetched for ids: {ids}")

        safe_fc = _sanitize_name(fc_in)
        ts = dt.datetime.now(dt.UTC).strftime("%m-%d-%Y_%H-%M-%S")
        out_name = f"{safe_fc}_{ts}"

        with tempfile.TemporaryDirectory(prefix="download_export_") as tmp_dir:
            output_dir = os.path.join(tmp_dir, out_name)
            os.makedirs(output_dir, exist_ok=True)

            data_file = os.path.join(output_dir, f"{out_name}.{filetype}")
            if filetype == "geojson":
                _write_geojson(data_file, features)
            elif filetype == "csv":
                _write_csv(data_file, features)
            elif filetype in {"filegdb", "shapefile"}:
                _export_with_gdal(output_dir, out_name, features, filetype)

            disclaimer_html = _get_disclaimer_html(resolved_api_url)
            if disclaimer_html:
                with open(os.path.join(output_dir, "disclaimer.html"), "w", encoding="utf-8") as f:
                    f.write(disclaimer_html)

            zip_path = os.path.join(tmp_dir, f"{out_name}.zip")
            _zip_output(output_dir, zip_path)

            prefix = os.getenv("DOWNLOAD_EXPORT_PREFIX", "gp-download")
            key = f"{prefix.rstrip('/')}/{out_name}.zip"
            upload_result = _upload_zip(zip_path, key)

        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": "Export created",
                    "featureCount": len(features),
                    "where": final_where,
                    **upload_result,
                }
            ),
        }
    except requests.RequestException as exc:
        return {
            "statusCode": 502,
            "body": json.dumps({"message": f"Request failed: {str(exc)}"}),
        }
    except Exception as exc:  # pylint: disable=broad-except
        return {
            "statusCode": 500,
            "body": json.dumps({"message": str(exc)}),
        }



