import json
import pathlib
import sys
import unittest
from unittest.mock import patch

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from lambda_function import (
    _build_result_feature_set,
    _esri_featureset_to_geojson,
    configure_intersection_sources,
    lambda_handler,
)


class TestAoiIntersectionsLambda(unittest.TestCase):
    def test_configure_intersection_sources(self):
        rows = [
            {"id_source": "foo", "use_as_target": 0, "source_type": "a", "name": "Foo"},
            {"id_source": "bar", "use_as_target": 1, "source_type": "b", "name": "Bar"},
        ]
        _sources, targets = configure_intersection_sources(rows)
        self.assertIn("bar", targets)
        self.assertNotIn("foo", targets)

    def test_esri_featureset_to_geojson(self):
        fs = {
            "features": [
                {
                    "attributes": {"unique_id": "aoi-feature"},
                    "geometry": {
                        "rings": [[[-120, 45], [-120, 46], [-119, 46], [-119, 45], [-120, 45]]]
                    },
                }
            ]
        }
        fc = _esri_featureset_to_geojson(fs)
        self.assertEqual(fc["type"], "FeatureCollection")
        self.assertEqual(len(fc["features"]), 1)

    def test_build_result_feature_set_adds_objectid(self):
        fs = _build_result_feature_set(
            [
                {
                    "attributes": {
                        "id_1": "a",
                        "id_1_source": "aoi",
                        "id_2": "b",
                        "id_2_source": "target",
                        "acre_overlap": 10.5,
                    },
                    "geometry": {"x": -120, "y": 45, "spatialReference": {"wkid": 4326}},
                }
            ]
        )
        self.assertEqual(fs["features"][0]["attributes"]["OBJECTID"], 1)

    @patch("lambda_function._upload_result_if_configured")
    @patch("lambda_function._query_target_features")
    @patch("lambda_function._get_layer_default_wkid")
    @patch("lambda_function._load_intersection_source_rows")
    @patch("lambda_function._parse_aoi_rows")
    def test_lambda_handler_success(
        self,
        mock_parse_aoi,
        mock_source_rows,
        mock_layer_wkid,
        mock_query_target,
        mock_upload,
    ):
        from shapely.geometry import Polygon

        mock_parse_aoi.return_value = [
            type("Aoi", (), {"geometry": Polygon([(0, 0), (0, 1), (1, 1), (1, 0)]), "unique_id": "aoi-feature", "feat_source": "aoi"})
        ]
        mock_source_rows.return_value = [
            {"id_source": "treatments", "use_as_target": 1, "source_type": "polygon", "name": "Treatments"}
        ]
        mock_layer_wkid.return_value = 4326
        mock_query_target.return_value = [
            type("T", (), {"geometry": Polygon([(0.5, 0.5), (0.5, 1.5), (1.5, 1.5), (1.5, 0.5)]), "unique_id": "t1", "feat_source": "treatments"})
        ]
        mock_upload.return_value = None

        event = {
            "aoi": {"features": [{"attributes": {"unique_id": "aoi-feature"}, "geometry": {"rings": [[[0,0],[0,1],[1,1],[1,0],[0,0]]]}}]},
            "intersection_features_url": "https://example.com/FeatureServer/0",
            "intersections_source_list": [
                {"id_source": "treatments", "use_as_target": 1, "source_type": "polygon", "name": "Treatments"}
            ],
        }
        res = lambda_handler(event, None)
        self.assertEqual(res["statusCode"], 200)
        body = json.loads(res["body"])
        self.assertIn("result", body)


if __name__ == "__main__":
    unittest.main()


