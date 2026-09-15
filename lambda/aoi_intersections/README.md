# AOI Intersections Lambda (No arcpy)

This Lambda reproduces the core behavior of `scripts/gp_tool/aoi_intersections.py` using HTTP + spatial Python libraries (`shapely`, `pyproj`) instead of `arcpy`.

## What it does

- Reads AOI geometry from request payload (Esri FeatureSet JSON or GeoJSON)
- Reads intersection target config from `intersections_source_list`
- Queries target features from ArcGIS REST (`intersection_features_url`) by `feat_source`
- Computes pairwise intersections and dissolved overlap by target source
- Calculates geodesic acres for polygon AOIs
- Returns ArcGIS FeatureSet JSON (`result.features`) compatible with existing client-side handling
- Optionally uploads result JSON to S3 and returns a presigned URL

## Request payload

```json
{
  "aoi": {
    "features": [
      {
        "attributes": {"unique_id": "aoi-feature", "feat_source": "aoi"},
        "geometry": {
          "rings": [[[-120, 45], [-120, 46], [-119, 46], [-119, 45], [-120, 45]]],
          "spatialReference": {"wkid": 4326}
        }
      }
    ]
  },
  "intersection_features_url": "https://.../FeatureServer/0",
  "intersections_source_list": [
    {"id_source": "treatments", "use_as_target": 1, "source_type": "polygon", "name": "Treatments"}
  ],
  "result_name": "aoi_intersections"
}
```

`intersections_source_list` can also be an ArcGIS table URL.

## Response shape

```json
{
  "message": "AOI intersections generated",
  "count": 12,
  "result": {
    "fields": [...],
    "features": [...]
  },
  "bucket": "...",
  "key": "...",
  "downloadUrl": "..."
}
```

## Environment variables

- `AOI_INTERSECTIONS_BUCKET` (required for upload)
- `AOI_INTERSECTIONS_PREFIX` (optional, default `aoi-intersections`)

## Deploy

```bash
sam build
sam deploy --guided
```

## Local test event

Use `event.example.json` and run:

```bash
sam local invoke AoiIntersectionsFunction --event event.example.json
```

