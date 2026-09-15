# GP Download Export Lambda (No arcpy)

This Lambda replaces the ArcGIS Pro `gp_download.py` flow for serverless execution where `arcpy` is not available.

## What it does

- Accepts export request payloads similar to the GP service (`url`, `fc`, `where`, `type`, `geometry`, `geometry_type`, `api_url`)
- Applies the `identifier_database NOT IN ('NASF','NGO')` exclusion when:
  - the URL contains `reshapewildfire.org`, and
  - the layer includes the `identifier_database` field
- Queries ArcGIS REST `/query` for IDs and features
- Exports data as:
  - `geojson`
  - `csv`
  - `filegdb` (GDAL `OpenFileGDB`)
  - `shapefile` (GDAL `ESRI Shapefile`)
- Writes `disclaimer.html` (from CMS endpoint) into the same zip
- Uploads the zip to S3 and returns a presigned download URL

## GDAL requirement for `filegdb` and `shapefile`

`filegdb` and `shapefile` exports require GDAL (`osgeo`) in the Lambda runtime.
Use one of these deployment approaches:

- Lambda container image that installs GDAL
- Lambda Layer that includes `osgeo` and matching native libraries

If GDAL is missing, the function returns an error stating GDAL support is required.

## Request payload

```json
{
  "url": "https://.../FeatureServer/0",
  "fc": "treatment_index",
  "type": "geojson",
  "where": "1=1",
  "geometry": null,
  "geometry_type": null,
  "api_url": "https://cms.reshapewildfire.org/api/v2/snippets/download_disclaimer/"
}
```

## Response payload

```json
{
  "message": "Export created",
  "featureCount": 123,
  "where": "1=1 AND identifier_database NOT IN ('NASF','NGO')",
  "bucket": "my-private-bucket",
  "key": "gp-download/treatment_index_05-28-2026_16-20-00.zip",
  "downloadUrl": "https://...presigned..."
}
```

## Environment variables

- `DOWNLOAD_EXPORT_BUCKET` (required)
- `DOWNLOAD_EXPORT_PREFIX` (optional, default `gp-download`)
- `API_URL` (optional, default `https://cms.reshapewildfire.org/api/v2/`)

## Deploy with SAM

```bash
sam build
sam deploy --guided
```

## Local invocation example

```bash
sam local invoke GpDownloadExportFunction --event event.json
```


