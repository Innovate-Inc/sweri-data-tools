from typing import cast
from unittest import TestCase
from unittest.mock import patch, Mock, call, mock_open, MagicMock
from .download import *
from .files import *
from .conversion import *
from .s3 import import_s3_csv_to_postgres_table
from .sql import rename_postgres_table, insert_from_db, run_vacuum_analyze, delete_from_table, rotate_tables, \
    refresh_spatial_index, connect_to_pg_db, copy_table_across_servers, calculate_index_for_fields

from .swizzle import get_layer_definition, get_new_definition, get_view_admin_url, clear_current_definition, \
    add_to_definition, swizzle_service


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
        expected_args = ['http://test.url/query',
                         {'where': '46=2', 'returnIdsOnly': 'true', 'f': 'json'}]
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
            get_feat_mock.return_value = [{'attributes': {'hello': 'there'}}, {
                'attributes': {'another': 'feature'}}]

            get_fields_mock.return_value = []

            mock_exists.return_value = False

            mock_json_to_fc.return_value = out_fc

            r = fetch_and_create_featureclass(url, where, 'out.gdb', 'fires',
                                              geom, 'polygon', 102100)
            mock_project.assert_called()
            self.assertEqual(r, out_fc)
            self.assertTrue(get_feat_mock.called_once_with(
                url, where, 102100, None))
            self.assertTrue(get_ids_mock.called_once_with(
                url, where, geom, 'polygon', ))
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
        f = [{"attributes": {'something': 'hello'}},
             {"attributes": {'something': 'new'}}]
        mockresponse.json = lambda: {"features": f}
        mock_post.return_value = mockresponse
        r = fetch_features('http://test.url/query', {'where': '1=1'})
        self.assertEqual(r, f)

    @patch('arcgis.features.FeatureLayer')
    def test_service_to_postgres(self, fl_mock):
        with patch('sweri_utils.download.get_ids') as get_ids_mock, patch(
                'sweri_utils.download.query_by_id_and_save_to_fc') as query_and_save_mock:
            get_ids_mock.return_value = [1, 2, 3]
            query_and_save_mock.return_value = (10, 'out_filepath')
            url = 'http://some.url/'
            where_clause = '1=1'
            wkid = 3857
            database = 'test_db'
            schema = 'test_schema'
            destination_table = 'test_table'
            cursor = Mock()
            sde_file = 'test_sde'
            insert_function = Mock()
            chunk_size = 2
            expected_pg_path = os.path.join('test_sde', 'test_db.test_schema.test_table_additions')

            service_to_postgres(url, where_clause, wkid, database, schema, destination_table, cursor, sde_file,
                                insert_function, chunk_size)

            cursor.execute.assert_called_once_with(f'TRUNCATE {schema}.{destination_table}')
            get_ids_mock.assert_called_once_with(url, where=where_clause)
            query_and_save_mock.assert_has_calls(
                [call('1,2', fl_mock(), expected_pg_path, wkid, sde_file, f'{destination_table}_additions'),
                 call('3', fl_mock(), expected_pg_path, wkid, sde_file, f'{destination_table}_additions')]
            )
            insert_function.assert_has_calls(
                [call(cursor, schema),
                 call(cursor, schema)]
            )

    @patch('arcpy.management.GetCount')
    @patch('arcpy.management.Delete')
    @patch('arcpy.Exists')
    def test_query_by_id_and_save_to_fc(self, mock_exists, mock_delete, mock_get_count):
        id_list = '1,2,3,4'
        fl_mock = Mock()
        sde_fc_path = 'test_path'
        wkid = 3857
        sde_file = 'test_sde_file'
        out_fc_name = 'test_out_fc_name'

        mock_query = Mock()
        mock_query.save.return_value = 'saved_fc_path'
        fl_mock.query.return_value = mock_query

        mock_exists.return_value = True
        mock_get_count.return_value = ['5']

        count, saved_fc = query_by_id_and_save_to_fc(id_list, fl_mock, sde_fc_path, wkid, sde_file, out_fc_name)

        self.assertEqual(count, 5)
        self.assertEqual(saved_fc, 'saved_fc_path')
        fl_mock.query.assert_called_once_with(object_ids=id_list, out_fields="*", return_geometry=True, out_sr=wkid)
        mock_exists.assert_called_once_with(sde_fc_path)
        mock_delete.assert_called_once_with(sde_fc_path)
        mock_query.save.assert_called_once_with(sde_file, out_fc_name)
        mock_get_count.assert_called_once_with('saved_fc_path')


class FilesTests(TestCase):
    @patch('zipfile.ZipFile')
    def test_export_zip(self, zip_mock):
        z = create_zip('zip_dir', 'test')
        zip_mock.assert_called()
        self.assertEqual(z, 'test.zip')

    @patch('arcpy.management.CreateFileGDB')
    def test_create_gdb(self, mock_gdb):
        p = create_gdb('test', 'out_dir')
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

    @patch('requests.get')
    @patch('builtins.open', new_callable=mock_open)
    def test_download_file_from_url(self, mock_open, mock_get):
        # Arrange
        url = 'http://example.com/file'
        destination_path = 'path/to/destination/file'
        mock_response = mock_get.return_value
        mock_response.content = b'Test content'
        mock_response.status_code = 200

        # Act
        download_file_from_url(url, destination_path)

        # Assert
        mock_get.assert_called_once_with(url)
        mock_open.assert_called_once_with(destination_path, 'wb')
        mock_open().write.assert_called_once_with(b'Test content')

    @patch('arcpy.conversion.FeatureClassToGeodatabase')
    @patch('arcpy.management.Delete')
    @patch('arcpy.Exists')
    @patch('arcpy.management.Project')
    def test_gdb_to_postgres(self, mock_project, mock_exist, mock_delete, mock_fc_gdb):
        with patch('sweri_utils.files.download_file_from_url') as download_file_mock, patch(
                'sweri_utils.files.extract_and_remove_zip_file') as extract_zip_mock:
            mock_exist.return_value = True
            sde_file = 'fake_sde_connection_file'
            gdb_name = 'test_gdb'
            gdb_path = os.path.join(os.getcwd(), gdb_name)
            projection = arcpy.SpatialReference(3857)
            postgres_table_name = 'test_table'
            schema = 'test_schema'
            postgres_table_location = os.path.join(sde_file, f'sweri.{schema}.{postgres_table_name}')
            fc_name = 'Activity_HazFuelTrt_PL'

            feature_class = os.path.join(gdb_path, fc_name)
            reprojected_fc = os.path.join(gdb_path, f'{postgres_table_name}')
            url = 'http://test.url'
            zip_file = f'{postgres_table_name}.zip'

            gdb_to_postgres(url, gdb_name, projection, fc_name, postgres_table_name, sde_file, schema)

            download_file_mock.assert_called_once_with(url, zip_file)
            extract_zip_mock.assert_called_once_with(zip_file)
            mock_project.assert_called_once_with(feature_class, reprojected_fc, projection)
            mock_exist.assert_called_once_with(postgres_table_location)
            mock_delete.assert_has_calls(
                [
                    call(postgres_table_location),
                    call(gdb_path)
                ]
            )
            mock_fc_gdb.assert_called_once_with(reprojected_fc, sde_file)


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
        fields = ['text_field', 'number_field',
                  'none_field', 'nested_dict', 'nested_arr']
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

    @patch('sweri_utils.conversion.pg_copy_to_csv')
    @patch('sweri_utils.conversion.upload_to_s3')
    def test_create_csv_and_upload_to_s3(self, mock_upload_to_s3, mock_pg_copy_to_csv):
        # Arrange
        cursor = MagicMock()
        schema = 'public'
        table = 'test_table'
        columns = ['col1', 'col2']
        filename = 'test.csv'
        bucket = 'test-bucket'

        mock_file = MagicMock()
        mock_file.name = filename
        mock_pg_copy_to_csv.return_value = mock_file
        mock_upload_to_s3.return_value = 'upload_success'

        # Act
        result = create_csv_and_upload_to_s3(cursor, schema, table, columns, filename, bucket)

        # Assert
        mock_pg_copy_to_csv.assert_called_once_with(cursor, schema, table, filename, columns)
        mock_upload_to_s3.assert_called_once_with(bucket, filename, filename)
        self.assertEqual(result, 'upload_success')

    @patch('arcpy.AddMessage')
    def test_insert_from_db(self, message_mock):
        mock_conn = Mock()
        mock_conn.execute.return_value = True
        insert_from_db(mock_conn, 'dev', 'insert_here', ['field1', 'field2'],
                       'from_here', ['from1', 'from2'])

        expected = f'''INSERT INTO dev.insert_here (shape, field1,field2) SELECT ST_MakeValid(ST_TRANSFORM(shape, 3857)), from1,from2 FROM dev.from_here;'''
        self.assertEqual(
            mock_conn.execute.call_args_list,
            [
                call('BEGIN;'),
                call(expected),
                call('COMMIT;')
            ]
        )

    @patch('requests.get')
    def test_create_coded_val_dict(self, mock_get):
        # Mock response data
        mock_response = {
            'domains': [
                {
                    'codedValues': [
                        {'code': 1, 'name': 'Value1'},
                        {'code': 2, 'name': 'Value2'}
                    ]
                }
            ]
        }
        mock_get.return_value.json.return_value = mock_response

        url = 'http://example.com/arcgis/rest/services'
        layer = '0'
        expected_result = {1: 'Value1', 2: 'Value2'}

        result = create_coded_val_dict(url, layer)
        self.assertEqual(result, expected_result)

    @patch('requests.get')
    def test_create_coded_val_dict_missing_domains(self, mock_get):
        mock_get.return_value.json.return_value = {}

        url = 'http://example.com/arcgis/rest/services'
        layer = '0'

        with self.assertRaises(ValueError) as context:
            create_coded_val_dict(url, layer)
        self.assertEqual(str(context.exception), 'missing domains')

    @patch('requests.get')
    def test_create_coded_val_dict_missing_coded_values(self, mock_get):
        mock_response = {
            'domains': [{}]
        }
        mock_get.return_value.json.return_value = mock_response

        url = 'http://example.com/arcgis/rest/services'
        layer = '0'

        with self.assertRaises(ValueError) as context:
            create_coded_val_dict(url, layer)
        self.assertEqual(str(context.exception), 'missing coded values or incorrect domain type')


class SqlTests(TestCase):
    @patch('sweri_utils.sql.psycopg.Connection')
    def test_rename_postgres_table(self, mock_connection):
        # Mock the connection and cursor
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_transaction = MagicMock()
        mock_connection.transaction.return_value.__enter__.return_value = mock_transaction

        # Call the function with test data
        rename_postgres_table(
            mock_connection, "public", "old_table", "new_table")

        # Assert the execute method was called with the correct SQL
        mock_cursor.execute.assert_has_calls(
            [
                call('ALTER TABLE public.old_table RENAME TO new_table;'),
            ]
        )
        mock_connection.transaction.assert_called_once()

    def test_run_vacuum_analyze(self):
        # Arrange
        mock_connection = Mock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor

        schema = 'public'
        table = 'test_table'

        # Act
        run_vacuum_analyze(mock_connection, schema, table)

        # Assert
        mock_cursor.execute.assert_has_calls([
            call(f'VACUUM ANALYZE {schema}.{table};')
        ])

    def test_delete_from_table(self):
        # Arrange
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_transaction = MagicMock()
        mock_connection.transaction.return_value.__enter__.return_value = mock_transaction

        schema = 'public'
        table = 'test_table'
        where = 'id = 1'

        # Act
        delete_from_table(mock_connection, schema, table, where)

        # Assert
        expected_calls = [
            call(f'DELETE FROM {schema}.{table} WHERE {where};'),
        ]
        mock_cursor.execute.assert_has_calls(expected_calls)
        mock_connection.transaction.assert_called_once()

    def test_rotate_tables_drop(self):
        with patch('sweri_utils.sql.rename_postgres_table') as mock_rename, patch(
                'sweri_utils.sql.drop_temp_table') as mock_drop:
            conn = MagicMock()
            schema = 'public'
            main_table_name = 'main_table'
            backup_table_name = 'backup_table'
            new_table_name = 'new_table'

            rotate_tables(conn, schema, main_table_name, backup_table_name, new_table_name)

            mock_rename.assert_has_calls(
                [
                    call(conn, schema, backup_table_name, f'{backup_table_name}_temp'),
                    call(conn, schema, main_table_name, backup_table_name),
                    call(conn, schema, new_table_name, main_table_name)
                ]
            )
            mock_drop.assert_has_calls(
                [
                    call(conn, schema, backup_table_name),
                    call(conn, schema, backup_table_name)
                ]
            )

    def test_rotate_tables_keep(self):
        with patch('sweri_utils.sql.rename_postgres_table') as mock_rename, patch(
                'sweri_utils.sql.drop_temp_table') as mock_drop:
            conn = MagicMock()
            schema = 'public'
            main_table_name = 'main_table'
            backup_table_name = 'backup_table'
            new_table_name = 'new_table'
            rotate_tables(conn, schema, main_table_name, backup_table_name, new_table_name, False)
            mock_rename.assert_has_calls(
                [
                    call(conn, schema, backup_table_name, f'{backup_table_name}_temp'),
                    call(conn, schema, main_table_name, backup_table_name),
                    call(conn, schema, new_table_name, main_table_name),
                    call(conn, schema, f'{backup_table_name}_temp', new_table_name)
                ]
            )
            mock_drop.assert_called_once_with(conn, schema, backup_table_name)

    def test_refresh_spatial_index(self):
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_transaction = MagicMock()
        mock_connection.transaction.return_value.__enter__.return_value = mock_transaction

        schema = 'public'
        table = 'test_table'

        refresh_spatial_index(mock_connection, schema, table)

        mock_cursor.execute.assert_called_once_with(f'CREATE INDEX ON {schema}.{table} USING GIST (shape);')
        mock_connection.transaction.assert_called_once()

    @patch('psycopg.connect')
    def test_connect_to_pg_db(self, mock_connect):
        # Arrange
        mock_connection = MagicMock()
        mock_connect.return_value = mock_connection

        db_host = 'localhost'
        db_port = 5432
        db_name = 'test_db'
        db_user = 'test_user'
        db_password = 'test_password'

        # Act
        connection = connect_to_pg_db(db_host, db_port, db_name, db_user, db_password)

        # Assert
        mock_connect.assert_called_once_with(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password,
            autocommit=True
        )

        self.assertEqual(connection, mock_connection)

    def test_insert_from_db(self):
        # Mock the connection and transaction context
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_transaction = MagicMock()
        mock_connection.transaction.return_value.__enter__.return_value = mock_transaction

        # Define the parameters
        schema = 'public'
        insert_table = 'target_table'
        insert_fields = ['field1', 'field2']
        from_table = 'source_table'
        from_fields = ['field1', 'field2']
        expected_q = f'''INSERT INTO {schema}.{insert_table} (shape, {','.join(insert_fields)}) SELECT ST_MakeValid(ST_TRANSFORM(shape, 3857)), {','.join(from_fields)} FROM {schema}.{from_table};'''
        # Call the function
        insert_from_db(mock_connection, schema, insert_table, insert_fields, from_table, from_fields)

        # Check if the correct SQL commands were executed
        mock_cursor.execute.assert_called_once_with(expected_q)
        mock_connection.transaction.assert_called_once()

    def test_copy_table_across_servers(self):
        from_mock_connection = MagicMock()
        from_mock_cursor = MagicMock()
        from_mock_connection.cursor.return_value = from_mock_cursor
        from_mock_transaction = MagicMock()
        from_mock_connection.transaction.return_value.__enter__.return_value = from_mock_transaction

        to_mock_connection = MagicMock()
        to_mock_cursor = MagicMock()
        to_mock_connection.cursor.return_value = to_mock_cursor
        to_mock_transaction = MagicMock()
        to_mock_connection.transaction.return_value.__enter__.return_value = to_mock_transaction

        from_schema = 'public'
        from_table = 'source_table'
        to_schema = 'public'
        to_table = 'destination_table'
        from_columns = ['field1', 'field2']
        to_columns = ['field1', 'field2']

        copy_table_across_servers(from_mock_connection, from_schema, from_table, to_mock_connection, to_schema, to_table, from_columns,
                                  to_columns)

        from_mock_cursor.copy.assert_called_once_with(
            f"COPY (SELECT field1,field2 FROM {from_schema}.{from_table}) TO STDOUT (FORMAT BINARY)")
        assert call(f"DELETE FROM {to_schema}.{to_table};") not in to_mock_cursor.execute.call_args_list
        to_mock_cursor.copy.assert_called_once_with(
            f"COPY {to_schema}.{to_table} (field1,field2) FROM STDIN (FORMAT BINARY)")
        from_mock_connection.transaction.assert_called_once()
        to_mock_connection.transaction.assert_called_once()

    def test_copy_table_across_servers_with_delete(self):
        from_mock_connection = MagicMock()
        from_mock_cursor = MagicMock()
        from_mock_connection.cursor.return_value = from_mock_cursor
        from_mock_transaction = MagicMock()
        from_mock_connection.transaction.return_value.__enter__.return_value = from_mock_transaction

        to_mock_connection = MagicMock()
        to_mock_cursor = MagicMock()
        to_mock_connection.cursor.return_value = to_mock_cursor
        to_mock_transaction = MagicMock()
        to_mock_connection.transaction.return_value.__enter__.return_value = to_mock_transaction

        from_schema = 'public'
        from_table = 'source_table'
        to_schema = 'public'
        to_table = 'destination_table'
        from_columns = ['field1', 'field2']
        to_columns = ['field1', 'field2']

        copy_table_across_servers(from_mock_connection, from_schema, from_table, to_mock_connection, to_schema, to_table, from_columns,
                                  to_columns, True)

        from_mock_cursor.copy.assert_called_once_with(
            f"COPY (SELECT field1,field2 FROM {from_schema}.{from_table}) TO STDOUT (FORMAT BINARY)")
        to_mock_cursor.execute.assert_any_call(f"DELETE FROM {to_schema}.{to_table}")
        to_mock_cursor.copy.assert_called_once_with(
            f"COPY {to_schema}.{to_table} (field1,field2) FROM STDIN (FORMAT BINARY)")
        to_mock_connection.transaction.assert_called_once()
        from_mock_connection.transaction.assert_called_once()

    def test_copy_table_across_servers_with_delete_with_where(self):
        from_mock_connection = MagicMock()
        from_mock_cursor = MagicMock()
        from_mock_connection.cursor.return_value = from_mock_cursor
        from_mock_transaction = MagicMock()
        from_mock_connection.transaction.return_value.__enter__.return_value = from_mock_transaction

        to_mock_connection = MagicMock()
        to_mock_cursor = MagicMock()
        to_mock_connection.cursor.return_value = to_mock_cursor
        to_mock_transaction = MagicMock()
        to_mock_connection.transaction.return_value.__enter__.return_value = to_mock_transaction

        from_schema = 'public'
        from_table = 'source_table'
        to_schema = 'public'
        to_table = 'destination_table'
        from_columns = ['field1', 'field2']
        to_columns = ['field1', 'field2']

        copy_table_across_servers(from_mock_connection, from_schema, from_table, to_mock_connection, to_schema, to_table, from_columns,
                                  to_columns, True, 'field1 = 1')

        from_mock_cursor.copy.assert_called_once_with(
            f"COPY (SELECT field1,field2 FROM {from_schema}.{from_table} WHERE field1 = 1) TO STDOUT (FORMAT BINARY)")
        to_mock_cursor.execute.assert_any_call(f"DELETE FROM {to_schema}.{to_table} WHERE field1 = 1")
        to_mock_cursor.copy.assert_called_once_with(
            f"COPY {to_schema}.{to_table} (field1,field2) FROM STDIN (FORMAT BINARY)")

        to_mock_connection.transaction.assert_called_once()
        from_mock_connection.transaction.assert_called_once()

    def test_calculate_index_for_fields(self):
        with patch('sweri_utils.sql.refresh_spatial_index') as mock_refresh, patch('sweri_utils.sql.postgres_create_index') as mock_index:
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            schema = 'public'
            table = 'test_table'
            fields = ['field1', 'field2']
            spatial = True

            calculate_index_for_fields(mock_conn, schema, table, fields, spatial)

            mock_refresh.assert_called_once_with(mock_conn, schema, table)
            mock_index.assert_has_calls(
                [
                    call(mock_conn, schema, table, 'field1'),
                    call(mock_conn, schema, table, 'field2')
                ]
            )
        # Arrange
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        schema = "public"
        table = "test_table"
        fields = ["field1", "field2"]
        spatial = True

class S3Tests(TestCase):
    def test_import_s3_csv_to_postgres_table(self):
        # Mock cursor
        mock_cursor = MagicMock()

        # Define test parameters
        db_schema = 'public'
        fields = ['id', 'name', 'value']
        destination_table = 'test_table'
        s3_bucket = 'test_bucket'
        csv_file = 'test_file.csv'
        aws_region = 'us-west-2'

        # Call the function
        import_s3_csv_to_postgres_table(mock_cursor, db_schema, fields, destination_table, s3_bucket, csv_file,
                                        aws_region)

        mock_cursor.execute.assert_has_calls(
            [
                call('BEGIN;'),
                call(f'DELETE FROM {db_schema}.{destination_table};'),
                call(
                    f"""SELECT aws_s3.table_import_from_s3('{db_schema}.{destination_table}', '{",".join(fields)}', '(format csv, HEADER)', aws_commons.create_s3_uri('{s3_bucket}', '{csv_file}', '{aws_region}'));"""),
                call('COMMIT;')
            ]
        )

    def test_upload_to_s3(self):
        # Arrange
        bucket = 'test_bucket'
        file_name = 'test_file.txt'
        obj_name = 'test_file.txt'

        # Mock the boto3 client and its upload_file method
        with patch('boto3.client') as mock_boto_client:
            mock_s3 = mock_boto_client.return_value
            mock_s3.upload_file.return_value = None

            # Act
            upload_to_s3(bucket, file_name, obj_name)

            # Assert
            mock_boto_client.assert_called_once_with('s3')
            mock_s3.upload_file.assert_called_once_with(file_name, bucket, obj_name)


class SwizzleTests(TestCase):
    @patch('requests.get')
    def test_retrieves_layer_definition_when_all_parameters_are_valid(self, mock_get):
        mock_get.return_value.json.return_value = {"some_key": "some_value"}
        result = get_layer_definition("any_url", 1, "any_service", "any_token")
        assert "some_key" in result

    @patch('requests.get')
    def test_returns_empty_sets_when_service_has_no_layers_or_tables(self, mock_get):
        mock_get.return_value.json.return_value = {'layers': [], 'tables': []}
        result = get_new_definition("any_url", "any_service", "any_token")
        assert result["layers"] == []
        assert result["tables"] == []

    def test_constructs_administrative_url_correctly(self):
        result = get_view_admin_url("any_url", "any_service")
        assert "arcgis/rest/admin/services/Hosted/any_service/FeatureServer" in result

    @patch('requests.get')
    @patch('requests.post')
    def test_clears_definition_when_layers_and_tables_exist(self, mock_post, mock_get):
        mock_get.return_value.json.return_value = {"layers": [{"id": 1}], "tables": [{"id": 2}]}
        resp = clear_current_definition("any_view_url", "any_token")
        assert mock_post.called

    @patch('requests.post')
    def test_adds_provided_definition(self, mock_post):
        resp = add_to_definition("any_view_url", {"layers": [], "tables": []}, "any_token")
        assert mock_post.called

    @patch('requests.get')
    @patch('requests.post')
    def test_swizzle_service_success(self, mock_post, mock_get):
        mock_get.return_value.json.side_effect = [
            {'layers': [{'id': 1}], 'tables': [{'id': 2}]},  # get_new_definition
            {},  # get_layer_definition
            {},  # get_layer_definition
            {'layers': [{'id': 1}], 'tables': [{'id': 2}]}  # clear_current_definition
        ]
        mock_post.return_value = MagicMock()

        swizzle_service('http://example.com', 'view_name', 'new_service_name', 'token')

        self.assertTrue(mock_get.called)
        self.assertTrue(mock_post.called)

    @patch('requests.get')
    @patch('requests.post')
    def test_swizzle_service_no_layers_or_tables(self, mock_post, mock_get):
        mock_get.return_value.json.side_effect = [
            {'layers': [], 'tables': []},  # get_new_definition
            {'layers': [], 'tables': []}  # clear_current_definition
        ]
        mock_post.return_value = MagicMock()

        swizzle_service('http://example.com', 'view_name', 'new_service_name', 'token')

        self.assertTrue(mock_get.called)
        self.assertTrue(mock_post.called)

    @patch('requests.get')
    @patch('requests.post')
    def test_swizzle_service_invalid_token(self, mock_post, mock_get):
        mock_get.return_value.json.side_effect = [
            {'error': 'Invalid token'},  # get_new_definition
            {'error': 'Invalid token'}  # clear_current_definition
        ]
        mock_post.return_value = MagicMock()

        with self.assertRaises(TypeError):
            swizzle_service('http://example.com', 'view_name', 'new_service_name', 'invalid_token')
