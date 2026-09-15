import json
import os
import tempfile
import unittest
from unittest.mock import Mock, patch

from lambda_function import _build_where_clause, _write_csv, lambda_handler


class TestGpDownloadLambda(unittest.TestCase):
    @patch("lambda_function._get_layer_fields")
    def test_build_where_clause_adds_exclusion_for_reshape(self, mock_get_fields):
        mock_get_fields.return_value = ["objectid", "identifier_database"]
        result = _build_where_clause(
            "https://services.reshapewildfire.org/FeatureServer/0",
            "1=1",
        )
        self.assertIn("identifier_database NOT IN ('NASF','NGO')", result)

    def test_write_csv_includes_geometry_column(self):
        features = [
            {
                "type": "Feature",
                "properties": {"name": "A", "acres": 12},
                "geometry": {"type": "Point", "coordinates": [-120, 45]},
            }
        ]
        with tempfile.TemporaryDirectory() as d:
            output = os.path.join(d, "out.csv")
            _write_csv(output, features)
            with open(output, "r", encoding="utf-8") as f:
                text = f.read()
                self.assertIn("geometry", text)
                self.assertIn("name", text)

    @patch("lambda_function._upload_zip")
    @patch("lambda_function._zip_output")
    @patch("lambda_function._get_disclaimer_html")
    @patch("lambda_function._fetch_geojson_features")
    @patch("lambda_function._get_ids")
    @patch("lambda_function._build_where_clause")
    def test_lambda_handler_returns_200(
        self,
        mock_where,
        mock_ids,
        mock_features,
        mock_disclaimer,
        _mock_zip,
        mock_upload,
    ):
        mock_where.return_value = "1=1"
        mock_ids.return_value = [1]
        mock_features.return_value = [
            {
                "type": "Feature",
                "properties": {"name": "A"},
                "geometry": {"type": "Point", "coordinates": [0, 0]},
            }
        ]
        mock_disclaimer.return_value = "ok"
        mock_upload.return_value = {
            "bucket": "b",
            "key": "k",
            "downloadUrl": "https://example.com",
        }

        event = {
            "body": json.dumps(
                {
                    "url": "https://example.com/FeatureServer/0",
                    "fc": "treatment_index",
                    "type": "geojson",
                    "where": "1=1",
                }
            )
        }

        result = lambda_handler(event, None)
        self.assertEqual(result["statusCode"], 200)

    @patch("lambda_function.HAS_GDAL", False)
    @patch("lambda_function._upload_zip")
    @patch("lambda_function._zip_output")
    @patch("lambda_function._get_disclaimer_html")
    @patch("lambda_function._fetch_geojson_features")
    @patch("lambda_function._get_ids")
    @patch("lambda_function._build_where_clause")
    def test_lambda_handler_filegdb_without_gdal_returns_500(
        self,
        mock_where,
        mock_ids,
        mock_features,
        mock_disclaimer,
        _mock_zip,
        _mock_upload,
    ):
        mock_where.return_value = "1=1"
        mock_ids.return_value = [1]
        mock_features.return_value = [
            {
                "type": "Feature",
                "properties": {"name": "A"},
                "geometry": {"type": "Point", "coordinates": [0, 0]},
            }
        ]
        mock_disclaimer.return_value = None

        event = {
            "body": json.dumps(
                {
                    "url": "https://example.com/FeatureServer/0",
                    "fc": "treatment_index",
                    "type": "filegdb",
                    "where": "1=1",
                }
            )
        }

        result = lambda_handler(event, None)
        self.assertEqual(result["statusCode"], 500)
        self.assertIn("GDAL is required", result["body"])


if __name__ == "__main__":
    unittest.main()


