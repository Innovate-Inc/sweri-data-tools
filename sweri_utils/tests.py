from typing import cast
from unittest import TestCase, main
from unittest.mock import patch, Mock, call

from .analysis import layer_intersections
from .download import *
from .files import *
from .conversion import *


class DownloadTests(TestCase):
    def setUp(self):
        self.retry_count = 0

    @patch('requests.get')
    def test_get_fields_no_fields(self, mock_get):
        mockresponse = Mock()
        mockresponse.json = lambda: {}
        mock_get.return_value = mockresponse

        try:
            get_fields('http://test.url')
        except KeyError:
            self.assertTrue(True)

    @patch('requests.get')
    def test_get_fields_with_fields(self, mock_get):
        mockresponse = Mock()
        fields = "lots of great field details"
        mockresponse.json = lambda: {"fields": fields}
        mock_get.return_value = mockresponse
        r = get_fields('http://test.url')
        self.assertEqual(r, fields)

    @patch('requests.post')
    def test_get_ids_without_geom(self, mock_post):
        mockresponse = Mock()
        mockresponse.json = lambda: {"objectIds": ["peach", "orange"]}
        mockresponse.status_code = 200
        mock_post.return_value = mockresponse
        expected_args = ['http://test.url/query', {'where': '46=2', 'returnIdsOnly': 'true', 'f': 'json'}]
        r = get_ids('http://test.url', '46=2')
        self.assertTrue(mock_post.called_once_with(expected_args))
        self.assertEqual(r, ["peach", "orange"])

    @patch('requests.post')
    def test_get_ids_with_geom(self, mock_post):
        mockresponse = Mock()
        mockresponse.json = lambda: {"objectIds": ["peach", "orange"]}
        mockresponse.status_code = 200
        mock_post.return_value = mockresponse
        expected_args = ['http://test.url/query', {'where': '46=2', 'returnIdsOnly': 'true', 'f': 'json',
                                                   'geometry': {'some': 'shape'},
                                                   'geometryType': 'esriGeometryEnvelope',
                                                   'spatialRel': 'esriSpatialRelIntersects'
                                                   }]
        r = get_ids('http://test.url', '46=2', {'some': 'shape'}, 'extent')
        self.assertTrue(mock_post.called_once_with(expected_args))
        self.assertEqual(r, ["peach", "orange"])

    @patch('requests.post')
    def test_get_ids_fails_no_ids(self, mock_post):
        mockresponse = Mock()
        mockresponse.json = lambda: {}
        mockresponse.status_code = 200
        mock_post.return_value = mockresponse
        expected_args = ['http://test.url/query', {'where': '46=2', 'returnIdsOnly': 'true', 'f': 'json',
                                                   'geometry': {'some': 'shape'},
                                                   'geometryType': 'esriGeometryEnvelope',
                                                   'spatialRel': 'esriSpatialRelIntersects'
                                                   }]

        try:
            get_ids('http://test.url', '46=2', {'some': 'shape'}, 'extent')
        except Exception:
            self.assertTrue(mock_post.called_once_with(expected_args))
            self.assertTrue(True)

    @patch('requests.post')
    def test_get_ids_fails_bad_response(self, mock_post):
        mockresponse = Mock()
        mockresponse.json = lambda: {"objectIds": ["a", "b"]}
        mockresponse.status_code = 400
        mock_post.return_value = mockresponse
        expected_args = ['http://test.url/query', {'where': '46=2', 'returnIdsOnly': 'true', 'f': 'json',
                                                   'geometry': {'some': 'shape'},
                                                   'geometryType': 'esriGeometryEnvelope',
                                                   'spatialRel': 'esriSpatialRelIntersects'
                                                   }]

        try:
            get_ids('http://test.url', '46=2', {'some': 'shape'}, 'extent')
        except Exception:
            self.assertTrue(mock_post.called_once_with(expected_args))
            self.assertTrue(True)

    @patch('arcpy.Exists')
    @patch('arcpy.conversion.JSONToFeatures')
    @patch('arcpy.management.DefineProjection')
    def test_fetch_create_new_fc(self, mock_project, mock_json_to_fc, mock_exists):
        with patch('sweri_utils.download.get_ids') as get_ids_mock, patch(
                'sweri_utils.download.get_all_features') as get_feat_mock, patch(
            'sweri_utils.download.get_fields') as get_fields_mock:
            url = 'http://test.url'
            where = '1=1'
            geom = {'rings': []}
            out_fc = 'some_path_to_fc'
            get_ids_mock.return_value = [1, 2, 3]
            get_feat_mock.return_value = [{'attributes': {'hello': 'there'}}, {'attributes': {'another': 'feature'}}]

            get_fields_mock.return_value = []

            mock_exists.return_value = False

            mock_json_to_fc.return_value = out_fc

            r = fetch_and_create_featureclass(url, where, 'out.gdb', 'fires',
                                              geom, 'polygon', 102100)
            mock_project.assert_called()
            self.assertEqual(r, out_fc)
            self.assertTrue(get_feat_mock.called_once_with(url, where, 102100, None))
            self.assertTrue(get_ids_mock.called_once_with(url, where, geom, 'polygon', ))
            self.assertTrue(get_fields_mock.called_once_with(url))
            self.assertTrue(mock_exists.called_once_with(out_fc))

    def test_retry_calls_num_times_calls_failure_callback(self):
        i_failed = Mock(side_effect=Exception('retries exceeded'))

        @retry(2, i_failed)
        def decorate_me():
            self.retry_count += 1
            raise Exception

        try:
            decorate_me()
        except Exception:
            # should raise an exception after 2 tries, which calls the on_failure callback, which raises an exception
            self.assertEqual(self.retry_count, 2)
            self.assertEqual(i_failed.call_count, 1)

    def test_fetch_failure(self):
        func = Mock()
        try:
            fetch_failure(func, 'url', {'objectIds': '1,2'})
        except:
            self.assertEqual(func.call_args_list, [
                call('url', {'objectIds': '1,2', 'limit': 1, 'offset': 0}),
                call('url', {'objectIds': '1,2', 'limit': 1, 'offset': 1})
            ])

    @patch('arcpy.AddMessage')
    @patch('arcpy.AddError')
    def test_get_all_features(self, mock_add_error, mock_add_message):
        with patch('sweri_utils.download.fetch_features') as ff_mock:
            ff_mock.return_value = [{'attributes': {'hello': 'there'}}]
        r = yield get_all_features('http://some.url', ['a', 1, 'c', 'd'], 102100, ['id', 'color'], 2)

        self.assertEqual(ff_mock.call_args_list, [
            call('http://some.url/query',
                 {'f': 'json', 'outSR': 102100, 'outFields': 'id,color', 'returnGeometry': 'true',
                  'objectIds': ['a', '1']}),
            call('http://some.url/query',
                 {'f': 'json', 'outSR': 102100, 'outFields': 'id,color', 'returnGeometry': 'true',
                  'objectIds': ['c', 'd']})
        ])
        self.assertEqual(len(r), 4)

    @patch('arcpy.AddWarning')
    @patch('arcpy.AddError')
    @patch('requests.post')
    def test_fetch_features(self, mock_post, mock_add_error, mock_add_warning):
        mockresponse = Mock()
        f = [{"attributes": {'something': 'hello'}}, {"attributes": {'something': 'new'}}]
        mockresponse.json = lambda: {"features": f}
        mock_post.return_value = mockresponse
        r = fetch_features('http://test.url/query', {'where': '1=1'})
        self.assertEqual(r, f)


class FilesTests(TestCase):
    @patch('zipfile.ZipFile')
    def test_export_zip(self, zip_mock):
        z = create_zip('zip_dir', 'test')
        zip_mock.assert_called()
        self.assertEqual(z, 'test.zip')

    @patch('arcpy.management.CreateFileGDB')
    def test_create_gdb(self, mock_gdb):
        p = create_gdb('test', 'dir')
        self.assertTrue(mock_gdb.called_once_with('out_dir', 'test.gdb'))
        self.assertEqual(p, os.path.join('out_dir', 'test.gdb'))

    def test_export_gdb(self):
        g = export_file_by_type('test', 'gdb', 'out_dir', 'test', 'any')
        self.assertEqual(g, os.path.join('out_dir', 'test.gdb'))

    @patch('arcpy.conversion.ExportTable')
    def test_export_csv(self, table_mock):
        table_mock.return_value = 'new_table'
        s = export_file_by_type('test', 'csv', 'out_dir', 'test', 'any')
        table_mock.assert_called()
        self.assertEqual(s, 'new_table')

    @patch('arcpy.conversion.FeatureClassToShapefile')
    def test_export_shapefile(self, shp_mock):
        export_file_by_type('test', 'shapefile', 'out_dir', 'test', 'any')
        shp_mock.assert_called()

    @patch('arcpy.conversion.FeaturesToJSON')
    def test_export_geojson(self, ftj_mock):
        export_file_by_type('test', 'geojson', 'out_dir', 'test', 'any')
        ftj_mock.assert_called()

    def test_export_throws_error(self):
        try:
            export_file_by_type('test', 'other', 'out_dir', 'test', 'any')
        except ValueError:
            self.assertTrue(True)


class ConversionTests(TestCase):
    @patch('arcpy.Describe')
    @patch('arcpy.management.Project')
    @patch('logging.warning')
    def test_reproject(self, mock_log, mock_project, mock_describe):
        fake_sr = cast(arcpy.SpatialReference, {'factoryCode': 1234})
        project = reproject('fc', fake_sr, os.path.join('new', 'out', 'path'))
        new_fc = os.path.join('new', 'out', 'path', 'fc_reprojected')
        self.assertTrue(mock_project.called_once_with('fc', new_fc, fake_sr))
        self.assertEqual(project, new_fc)

    def test_array_to_dict(self):
        fields = ['text_field', 'number_field', 'none_field', 'nested_dict', 'nested_arr']
        row = ['yellow', 123, None, {'hello': 'world'}, ['apples', 'bananas']]
        actual = array_to_dict(fields, row)
        expected = {
            'text_field': 'yellow',
            'number_field': 123,
            'none_field': None,
            'nested_dict': {'hello': 'world'},
            'nested_arr': ['apples', 'bananas']
        }
        self.assertEqual(actual, expected)

    @patch('arcpy.AddMessage')
    def test_insert_from_db_with_globalid(self, message_mock):
        mock_conn = Mock()
        mock_conn.execute.return_value = True
        insert_from_db(mock_conn, 'dev', 'insert_here', ['field1', 'field2'],
                       'from_here', ['from1', 'from2'])
        expected = '''insert into dev.insert_here (field1,field2) select sde.next_rowid('dev','insert_here'),sde.next_globalid(),from1,from2 from dev.from_here;'''
        self.assertEqual(
            mock_conn.execute.call_args_list,
            [
                call('BEGIN;'),
                call(expected),
                call('COMMIT;')
            ]
        )

    @patch('arcpy.AddMessage')
    def test_insert_from_db_without_globalid(self, message_mock):
        mock_conn = Mock()
        mock_conn.execute.return_value = True
        insert_from_db(mock_conn, 'dev', 'insert_here', ['field1', 'field2'],
                       'from_here', ['from1', 'from2'], False)
        expected = '''insert into dev.insert_here (field1,field2) select sde.next_rowid('dev','insert_here'),from1,from2 from dev.from_here;'''

        self.assertEqual(
            mock_conn.execute.call_args_list,
            [
                call('BEGIN;'),
                call(expected),
                call('COMMIT;')
            ]
        )


class AnalysisTests(TestCase):
    @patch('arcpy.management.MakeFeatureLayer')
    @patch('arcpy.analysis.PairwiseIntersect')
    def test_layer_intersect(self, intersect_mock, make_layer_mock):
        layer_intersections('intersection_features', 'source', 'target', 'out_name', 'gdb', 'something')
        make_layer_mock.side_effect = ['source_fl', 'target_fl']
        make_layer_mock.assert_has_calls(
            [
                call('intersection_features', where_clause="something = 'source'"),
                call('intersection_features', where_clause="something = 'target'")
            ]
        )
        intersect_mock.assert_called()
