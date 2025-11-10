import os
from unittest import TestCase
from unittest.mock import patch, Mock, call, mock_open, MagicMock
from . import download, files, conversion, s3, sql
from .hosted import verify_feature_count
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
            download.get_fields('http://test.url')
        except KeyError:
            self.assertTrue(True)

    @patch('requests.get')
    def test_get_fields_with_fields(self, mock_get):
        mockresponse = Mock()
        fields = "lots of great field details"
        mockresponse.json = lambda: {"fields": fields}
        mock_get.return_value = mockresponse
        r = download.get_fields('http://test.url')
        self.assertEqual(r, fields)

    @patch('requests.post')
    def test_get_ids_without_geom(self, mock_post):
        mockresponse = Mock()
        mockresponse.json = lambda: {"objectIds": ["peach", "orange"]}
        mockresponse.status_code = 200
        mock_post.return_value = mockresponse
        r = conversion.get_ids('http://test.url', '46=2')
        mock_post.assert_called_once_with('http://test.url/query',
                                          data={'where': '46=2', 'returnIdsOnly': 'true', 'f': 'json'})
        self.assertEqual(r, ["peach", "orange"])

    @patch('requests.post')
    def test_get_ids_with_geom(self, mock_post):
        mockresponse = Mock()
        mockresponse.json = lambda: {"objectIds": ["peach", "orange"]}
        mockresponse.status_code = 200
        mock_post.return_value = mockresponse

        r = conversion.get_ids('http://test.url', '46=2', {'some': 'shape'}, 'extent')
        mock_post.assert_called_once_with('http://test.url/query',
                                          data={'where': '46=2', 'returnIdsOnly': 'true', 'f': 'json',
                                                'geometry': {'some': 'shape'},
                                                'geometryType': 'esriGeometryEnvelope',
                                                'spatialRel': 'esriSpatialRelIntersects'
                                                })
        self.assertEqual(r, ["peach", "orange"])

    @patch('requests.post')
    def test_get_ids_fails_no_ids(self, mock_post):
        mockresponse = Mock()
        mockresponse.json = lambda: {}
        mockresponse.status_code = 200
        mock_post.return_value = mockresponse

        try:
            conversion.get_ids('http://test.url', '46=2', {'some': 'shape'}, 'extent')
        except Exception:
            mock_post.assert_called_with('http://test.url/query',
                                         data={'where': '46=2', 'returnIdsOnly': 'true', 'f': 'json',
                                               'geometry': {'some': 'shape'},
                                               'geometryType': 'esriGeometryEnvelope',
                                               'spatialRel': 'esriSpatialRelIntersects'
                                               })
            self.assertTrue(True)

    @patch('requests.post')
    def test_get_ids_fails_bad_response(self, mock_post):
        mockresponse = Mock()
        mockresponse.json = lambda: {"objectIds": ["a", "b"]}
        mockresponse.status_code = 400
        mock_post.return_value = mockresponse

        try:
            conversion.get_ids('http://test.url', '46=2', {'some': 'shape'}, 'extent')
        except Exception:
            mock_post.assert_called_with('http://test.url/query',
                                         data={'where': '46=2', 'returnIdsOnly': 'true', 'f': 'json',
                                               'geometry': {'some': 'shape'},
                                               'geometryType': 'esriGeometryEnvelope',
                                               'spatialRel': 'esriSpatialRelIntersects'
                                               })
            self.assertTrue(True)

    @patch('osgeo.gdal.VectorTranslate')
    @patch.object(download, 'get_fields')
    @patch.object(download, 'get_all_features')
    @patch.object(download, 'get_ids')
    def test_service_to_postgres(self, get_ids_mock, get_feat_mock, get_fields_mock, mock_VectorTranslate):
        url = 'http://test.url'
        where = '1=1'
        geom = {'rings': []}
        get_ids_mock.return_value = [1, 2, 3]
        get_feat_mock.return_value = yield {'features': [{'attributes': {'hello': 'there'}}, {
            'attributes': {'another': 'feature'}}],
                                            'fields': []}
        get_fields_mock.return_value = []
        mock_VectorTranslate.return_value = None

        conn = MagicMock()
        download.service_to_postgres(url, where, 4326, 'PG:', 'test', 'dest_tabl',
                                     conn, 1)

        get_feat_mock.assert_called_once_with(url, where, 102100, None)
        get_ids_mock.assert_called_once_with(url, where, geom, 'polygon', )
        get_fields_mock.assert_called_once_with(url)

    def test_retry_calls_num_times_calls_failure_callback(self):
        i_failed = Mock(side_effect=Exception('retries exceeded'))

        @download.retry(2, i_failed)
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
            download.fetch_failure(func, 'url', {'objectIds': '1,2'})
        except:
            self.assertEqual(func.call_args_list, [
                call('url', {'objectIds': '1,2', 'limit': 1, 'offset': 0}),
                call('url', {'objectIds': '1,2', 'limit': 1, 'offset': 1})
            ])

    def test_get_all_features(self):
        with patch.object(download, 'fetch_features') as ff_mock:
            ff_mock.return_value = [{'attributes': {'hello': 'there'}}]
        r = yield conversion.get_all_features('http://some.url', ['a', 1, 'c', 'd'], 102100, ['id', 'color'], 2)

        self.assertEqual(ff_mock.call_args_list, [
            call('http://some.url/query',
                 {'f': 'json', 'outSR': 102100, 'outFields': 'id,color', 'returnGeometry': 'true',
                  'objectIds': ['a', '1']}),
            call('http://some.url/query',
                 {'f': 'json', 'outSR': 102100, 'outFields': 'id,color', 'returnGeometry': 'true',
                  'objectIds': ['c', 'd']})
        ])
        self.assertEqual(len(r), 4)

    @patch('requests.post')
    def test_fetch_features(self, mock_post):
        mockresponse = Mock()
        f = [{"attributes": {'something': 'hello'}},
             {"attributes": {'something': 'new'}}]
        mockresponse.json = lambda: {"features": f}
        mock_post.return_value = mockresponse
        r = download.fetch_features('http://test.url/query', {'where': '1=1'})
        self.assertEqual(r, f)

    # def test_service_to_postgres(self):
    #     with patch('sweri_utils.download.get_ids') as get_ids_mock:
    #         get_ids_mock.return_value = [1, 2, 3]
    #         query_and_save_mock.return_value = (10, 'out_filepath')
    #         url = 'http://some.url/'
    #         where_clause = '1=1'
    #         wkid = 3857
    #         database = 'test_db'
    #         schema = 'test_schema'
    #         destination_table = 'test_table'
    #         cursor = Mock()
    #         sde_file = 'test_sde'
    #         insert_function = Mock()
    #         chunk_size = 2
    #         expected_pg_path = os.path.join('test_sde', 'test_db.test_schema.test_table_additions')
    #
    #         service_to_postgres(url, where_clause, wkid, database, schema, destination_table, cursor, sde_file,
    #                             insert_function, chunk_size)
    #
    #         cursor.execute.assert_called_once_with(f'TRUNCATE {schema}.{destination_table}')
    #         get_ids_mock.assert_called_once_with(url, where=where_clause)
    #         query_and_save_mock.assert_has_calls(
    #             [call('1,2', fl_mock(), expected_pg_path, wkid, sde_file, f'{destination_table}_additions'),
    #              call('3', fl_mock(), expected_pg_path, wkid, sde_file, f'{destination_table}_additions')]
    #         )
    #         insert_function.assert_has_calls(
    #             [call(cursor, schema),
    #              call(cursor, schema)]
    #         )

    # todo: create test for replacement method
    # def test_query_by_id_and_save_to_fc(self):
    #     id_list = '1,2,3,4'
    #     fl_mock = Mock()
    #     sde_fc_path = 'test_path'
    #     wkid = 3857
    #     sde_file = 'test_sde_file'
    #     out_fc_name = 'test_out_fc_name'
    #
    #     mock_query = Mock()
    #     mock_query.save.return_value = 'saved_fc_path'
    #     fl_mock.query.return_value = mock_query
    #
    #
    #
    #     count, saved_fc = query_by_id_and_save_to_fc(id_list, fl_mock, sde_fc_path, wkid, sde_file, out_fc_name)
    #
    #     self.assertEqual(count, 5)
    #     self.assertEqual(saved_fc, 'saved_fc_path')
    #     fl_mock.query.assert_called_once_with(object_ids=id_list, out_fields="*", return_geometry=True, out_sr=wkid)
    #     mock_exists.assert_called_once_with(sde_fc_path)
    #     mock_delete.assert_called_once_with(sde_fc_path)
    #     mock_query.save.assert_called_once_with(sde_file, out_fc_name)
    #     mock_get_count.assert_called_once_with('saved_fc_path')


class FilesTests(TestCase):
    @patch('zipfile.ZipFile')
    def test_export_zip(self, zip_mock):
        z = files.create_zip('zip_dir', 'test')
        zip_mock.assert_called()
        self.assertEqual(z, 'test.zip')

    @patch('arcpy.conversion.FeatureClassToGeodatabase')
    def test_export_gdb(self, fgdb_mock):
        fgdb_mock.return_value = 'new_gdb'
        g = files.export_file_by_type('test', 'gdb', 'out_dir', 'test', 'any')
        fgdb_mock.assert_called()
        self.assertEqual(g, 'new_gdb')

    @patch('arcpy.conversion.ExportTable')
    def test_export_csv(self, table_mock):
        table_mock.return_value = 'new_table'
        s = files.export_file_by_type('test', 'csv', 'out_dir', 'test', 'any')
        table_mock.assert_called()
        self.assertEqual(s, 'new_table')

    @patch('arcpy.conversion.FeatureClassToShapefile')
    def test_export_shapefile(self, shp_mock):
        files.export_file_by_type('test', 'shapefile', 'out_dir', 'test', 'any')
        shp_mock.assert_called()

    @patch('arcpy.conversion.FeaturesToJSON')
    def test_export_geojson(self, ftj_mock):
        files.export_file_by_type('test', 'geojson', 'out_dir', 'test', 'any')
        ftj_mock.assert_called()

    def test_export_throws_error(self):
        try:
            files.export_file_by_type('test', 'other', 'out_dir', 'test', 'any')
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
        files.download_file_from_url(url, destination_path)

        # Assert
        mock_get.assert_called_once_with(url)
        mock_open.assert_called_once_with(destination_path, 'wb')
        mock_open().write.assert_called_once_with(b'Test content')

    # @patch('arcpy.conversion.FeatureClassToGeodatabase')
    # @patch('arcpy.management.Delete')
    # @patch('arcpy.Exists')
    # @patch('arcpy.management.Project')
    # def test_gdb_to_postgres(self, mock_project, mock_exist, mock_delete, mock_fc_gdb):
    #     with patch('sweri_utils.files.download_file_from_url') as download_file_mock, patch(
    #             'sweri_utils.files.extract_and_remove_zip_file') as extract_zip_mock:
    #         mock_exist.return_value = True
    #         sde_file = 'fake_sde_connection_file'
    #         gdb_name = 'test_gdb'
    #         gdb_path = os.path.join(os.getcwd(), gdb_name)
    #         projection = arcpy.SpatialReference(3857)
    #         postgres_table_name = 'test_table'
    #         schema = 'test_schema'
    #         postgres_table_location = os.path.join(sde_file, f'sweri.{schema}.{postgres_table_name}')
    #         fc_name = 'Activity_HazFuelTrt_PL'
    #
    #         feature_class = os.path.join(gdb_path, fc_name)
    #         reprojected_fc = os.path.join(gdb_path, f'{postgres_table_name}')
    #         url = 'http://test.url'
    #         zip_file = f'{postgres_table_name}.zip'
    #
    #         gdb_to_postgres(url, gdb_name, projection, fc_name, postgres_table_name, sde_file, schema)
    #
    #         download_file_mock.assert_called_once_with(url, zip_file)
    #         extract_zip_mock.assert_called_once_with(zip_file)
    #         mock_project.assert_called_once_with(feature_class, reprojected_fc, projection)
    #         mock_exist.assert_called_once_with(postgres_table_location)
    #         mock_delete.assert_has_calls(
    #             [
    #                 call(postgres_table_location),
    #                 call(gdb_path)
    #             ]
    #         )
    #         mock_fc_gdb.assert_called_once_with(reprojected_fc, sde_file)


class ConversionTests(TestCase):
    def test_array_to_dict(self):
        fields = ['text_field', 'number_field',
                  'none_field', 'nested_dict', 'nested_arr']
        row = ['yellow', 123, None, {'hello': 'world'}, ['apples', 'bananas']]
        actual = conversion.array_to_dict(fields, row)
        expected = {
            'text_field': 'yellow',
            'number_field': 123,
            'none_field': None,
            'nested_dict': {'hello': 'world'},
            'nested_arr': ['apples', 'bananas']
        }
        self.assertEqual(actual, expected)

    @patch.object(conversion, 'pg_copy_to_csv')
    @patch.object(conversion, 'upload_to_s3')
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
        result = conversion.create_csv_and_upload_to_s3(cursor, schema, table, columns, filename, bucket)

        # Assert
        mock_pg_copy_to_csv.assert_called_once_with(cursor, schema, table, filename, columns)
        mock_upload_to_s3.assert_called_once_with(bucket, filename, filename)
        self.assertEqual(result, 'upload_success')

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

        result = conversion.create_coded_val_dict(url, layer)
        self.assertEqual(result, expected_result)

    @patch('requests.get')
    def test_create_coded_val_dict_missing_domains(self, mock_get):
        mock_get.return_value.json.return_value = {}

        url = 'http://example.com/arcgis/rest/services'
        layer = '0'

        with self.assertRaises(ValueError) as context:
            conversion.create_coded_val_dict(url, layer)
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
            conversion.create_coded_val_dict(url, layer)
        self.assertEqual(str(context.exception), 'missing coded values or incorrect domain type')


class SqlTests(TestCase):

    def setUp(self):
        self.conn = MagicMock()
        self.cursor = MagicMock()
        self.conn.cursor.return_value = self.cursor
        self.transaction = MagicMock()
        self.conn.transaction.return_value.__enter__.return_value = self.transaction

    @patch('psycopg.connection')
    def test_rename_postgres_table(self, mock_connection):
        # Mock the connection and cursor
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_transaction = MagicMock()
        mock_connection.transaction.return_value.__enter__.return_value = mock_transaction

        # Call the function with test data
        sql.rename_postgres_table(
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
        sql.run_vacuum_analyze(mock_connection, schema, table)

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
        sql.delete_from_table(mock_connection, schema, table, where)

        # Assert
        expected_calls = [
            call(f'DELETE FROM {schema}.{table} WHERE {where};'),
        ]
        mock_cursor.execute.assert_has_calls(expected_calls)
        mock_connection.transaction.assert_called_once()

    def test_rotate_tables_drop(self):
        with (patch.object(sql, 'rename_postgres_table') as mock_rename,
              patch.object(sql, 'drop_temp_table') as mock_drop):
            conn = MagicMock()
            schema = 'public'
            main_table_name = 'main_table'
            backup_table_name = 'backup_table'
            new_table_name = 'new_table'

            sql.rotate_tables(conn, schema, main_table_name, backup_table_name, new_table_name)

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
        with (patch.object(sql, 'rename_postgres_table') as mock_rename,
              patch.object(sql, 'drop_temp_table') as mock_drop):
            conn = MagicMock()
            schema = 'public'
            main_table_name = 'main_table'
            backup_table_name = 'backup_table'
            new_table_name = 'new_table'
            sql.rotate_tables(conn, schema, main_table_name, backup_table_name, new_table_name, False)
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

        sql.refresh_spatial_index(mock_connection, schema, table)

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
        connection = sql.connect_to_pg_db(db_host, db_port, db_name, db_user, db_password)

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
        expected_q = f'''INSERT INTO {schema}.{insert_table} (objectid, shape, {','.join(insert_fields)}) SELECT sde.next_rowid('{schema}', '{insert_table}'),ST_MakeValid(ST_TRANSFORM(shape, 4326)), {','.join(from_fields)} FROM {schema}.{from_table};'''
        # Call the function
        sql.insert_from_db(mock_connection, schema, insert_table, insert_fields, from_table, from_fields)

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

        sql.copy_table_across_servers(from_mock_connection, from_schema, from_table, to_mock_connection, to_schema,
                                      to_table, from_columns,
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

        sql.copy_table_across_servers(from_mock_connection, from_schema, from_table, to_mock_connection, to_schema,
                                      to_table, from_columns,
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

        sql.copy_table_across_servers(from_mock_connection, from_schema, from_table, to_mock_connection, to_schema,
                                      to_table, from_columns,
                                      to_columns, True, 'field1 = 1')

        from_mock_cursor.copy.assert_called_once_with(
            f"COPY (SELECT field1,field2 FROM {from_schema}.{from_table} WHERE field1 = 1) TO STDOUT (FORMAT BINARY)")
        to_mock_cursor.execute.assert_any_call(f"DELETE FROM {to_schema}.{to_table} WHERE field1 = 1")
        to_mock_cursor.copy.assert_called_once_with(
            f"COPY {to_schema}.{to_table} (field1,field2) FROM STDIN (FORMAT BINARY)")

        to_mock_connection.transaction.assert_called_once()
        from_mock_connection.transaction.assert_called_once()

    def test_calculate_index_for_fields(self):
        with (patch.object(sql, 'refresh_spatial_index') as mock_refresh,
              patch.object(sql, 'postgres_create_index') as mock_index):
            mock_conn = MagicMock()
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            schema = 'public'
            table = 'test_table'
            fields = ['field1', 'field2']
            spatial = True

            sql.calculate_index_for_fields(mock_conn, schema, table, fields, spatial)

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

    def test_truncate_and_insert(self):
        sql.truncate_and_insert('myschema', 'src', 'dst', self.conn, ['field1', 'field2'])
        self.cursor.execute.assert_has_calls([
            call("TRUNCATE TABLE myschema.dst;"),
            call(
                "INSERT INTO myschema.dst (objectid,field1, field2) SELECT sde.next_rowid('myschema', 'dst') AS objectid, field1, field2 FROM myschema.src;")
        ])

    def test_switch_autovacuum_and_triggers_enable(self):
        sql.switch_autovacuum_and_triggers(True, self.conn, 'myschema', ['t1', 't2'])
        self.cursor.execute.assert_has_calls([
            call("ALTER TABLE myschema.t1 SET (autovacuum_enabled = true);"),
            call("ALTER TABLE myschema.t1 ENABLE TRIGGER ALL;"),
            call("ALTER TABLE myschema.t2 SET (autovacuum_enabled = true);"),
            call("ALTER TABLE myschema.t2 ENABLE TRIGGER ALL;"),
        ])

    def test_switch_autovacuum_and_triggers_disable(self):
        sql.switch_autovacuum_and_triggers(False, self.conn, 'myschema', ['t1'])
        self.cursor.execute.assert_has_calls([
            call("ALTER TABLE myschema.t1 SET (autovacuum_enabled = false);"),
            call("ALTER TABLE myschema.t1 DISABLE TRIGGER ALL;"),
        ])

    def test_delete_duplicate_records(self):
        sql.delete_duplicate_records('myschema', 'mytable', self.conn, ['f1', 'f2'], 'oid')
        expected_query = '''
                         delete
                         from myschema.mytable
                         where ctid in (select ctid
                                        from (select ctid,
                                                     row_number() over (
                               partition by f1, f2
                               order by oid
                           ) as rn
                                              from myschema.mytable) t
                                        where t.rn > 1); \
                         '''

        # Remove whitespace, newlines, and special characters for comparison
        def normalize(s):
            import re
            return re.sub(r'[\s\\;]+', '', s.lower())

        actual_query = self.cursor.execute.call_args[0][0]
        self.assertEqual(normalize(actual_query), normalize(expected_query))


    def test_remove_blank_strings(self):
        # Arrange
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_transaction = MagicMock()
        mock_connection.transaction.return_value.__enter__.return_value = mock_transaction

        schema = 'public'
        table = 'test_table'
        fields = ['test1', 'test2']

        # Act
        sql.remove_blank_strings(mock_connection, schema, table, fields)

        # Assert
        expected_calls = [
            call(f'''

                UPDATE {schema}.{table}
                SET test1 = NULLIF(test1, '');

            '''),
            call(f'''

                UPDATE {schema}.{table}
                SET test2 = NULLIF(test2, '');

            ''')
        ]

        mock_cursor.execute.assert_has_calls(expected_calls)
        mock_connection.transaction.assert_called_once()

    def test_simplify_large_polygons(self):
        # Arrange
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_transaction = MagicMock()
        mock_connection.transaction.return_value.__enter__.return_value = mock_transaction

        schema = 'public'
        table = 'test_table'
        resolution = 0.000000001
        points_cutoff = 10000
        tolerance = .0000009

        expected_sql = f"""
        
            UPDATE {schema}.{table}
			set shape = 
                    ST_UnaryUnion(      -- combines overlapping or touching geometries into single shapes
                      ST_MakeValid(     -- ensures shape validity for successful union
                        ST_SnapToGrid(  -- snaps to grid to emulate ESRI resolutoin
                          ST_SimplifyPreserveTopology(shape, {tolerance}), -- simplify, but preserve topology (holes, boundaries)
                          {resolution}  -- set to resolution of feature class
                        ), 'method=structure' -- stucture makevalid prevents overlaps from being interpreted as holes
                      )
                    ),
              error = CASE
                        WHEN error IS NULL THEN 'MODIFIED_SHAPE'
                        ELSE error || ';MODIFIED_SHAPE'
                      END
            WHERE ST_NPoints(shape) > {points_cutoff}; -- all shapes with more than points_cutoff points will be simplified
        """

        # Act
        sql.simplify_large_polygons(mock_connection, schema, table)

        # Assert
        mock_connection.transaction.assert_called_once()
        mock_cursor.execute.assert_called_once_with(expected_sql)

    def test_simplify_large_polygons(self):
        # Arrange
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_transaction = MagicMock()
        mock_connection.transaction.return_value.__enter__.return_value = mock_transaction

        schema = 'public'
        table = 'test_table'

        expected_sql = f"""
            WITH polygons AS (
              SELECT
                objectid,
                (ST_Dump(shape)).geom::geometry(Polygon,4326) AS geom
              FROM {schema}.{table}
              WHERE ST_NumGeometries(shape) = 1
                AND ST_GeometryType(shape) = 'ST_MultiPolygon'
            )
            UPDATE {schema}.{table} AS t
            SET shape = p.geom
            FROM polygons AS p
            WHERE t.objectid = p.objectid;
        """

        # Act
        sql.revert_multi_to_poly(mock_connection, schema, table)

        # Assert
        mock_connection.transaction.assert_called_once()
        mock_cursor.execute.assert_called_once_with(expected_sql)

    def test_makevalid_shapes(self):
        # Arrange
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_transaction = MagicMock()
        mock_conn.transaction.return_value.__enter__.return_value = mock_transaction

        schema = "public"
        table = "test_table"
        shape_field = "shape"
        resolution = 0.000000001

        sql_call_one = f"""
        
            UPDATE {schema}.{table}    -- PostGIS repair, method structure ensures overlaps are not interpreted as holes
            SET {shape_field} = ST_MakeValid({shape_field}, 'method=structure') 
            WHERE NOT ST_IsValid({shape_field});                   
            
        """

        sql_call_two = f"""

            UPDATE {schema}.{table}
            SET {shape_field} =
                            ST_MakeValid(                                   -- Repair geometries after snapping to grid
                                ST_SnapToGrid({shape_field}, {resolution})  -- Snap to ESRI feature class grid
                            , 'method=structure'                            -- Ensures overlaps are not interpreted as holes
                            )
            WHERE NOT ST_IsValid(ST_SnapToGrid({shape_field}, {resolution}));   -- Check validity using ESRI-like resolution

        """

        # Act
        sql.makevalid_shapes(mock_conn, schema, table, shape_field, resolution)

        # Assert
        mock_conn.transaction.assert_called_once_with()
        assert mock_cursor.execute.call_count == 2
        mock_cursor.execute.assert_any_call(sql_call_one)
        mock_cursor.execute.assert_any_call(sql_call_two)

    def test_extract_geometry_collections(self):
        # Arrange
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_transaction = MagicMock()
        mock_conn.transaction.return_value.__enter__.return_value = mock_transaction

        schema = "public"
        table = "test_table"
        resolution = 0.000000001

        sql_call = f"""
            UPDATE {schema}.{table}
            SET shape =
                ST_MakeValid(            -- Repair geometries after snapping to grid and union
                    ST_UnaryUnion(       -- Unions geoms to ensure makvalid does not revert back to geometry collection
                        ST_SnapToGrid(                    -- Snap to ESRI feature class grid  
                          ST_CollectionExtract(shape, 3), -- Extracts geometry collections to polygon
                          {resolution}
                        )                                    
                  ),
                  'method=structure'
                )
            WHERE ST_GeometryType(shape) = 'ST_GeometryCollection';

        """


        sql.extract_geometry_collections(mock_conn, schema, table)

        # Assert
        mock_conn.transaction.assert_called_once()
        mock_cursor.execute.assert_called_once_with(sql_call)

class S3Tests(TestCase):
    def test_import_s3_csv_to_postgres_table(self):
        # Mock connection, cursor, and trasaction context
        mock_connection = MagicMock()
        mock_cursor = MagicMock()
        mock_connection.cursor.return_value = mock_cursor
        mock_transaction = MagicMock()
        mock_connection.transaction.return_value.__enter__.return_value = mock_transaction

        # Define test parameters
        db_schema = 'public'
        fields = ['id', 'name', 'value']
        destination_table = 'test_table'
        s3_bucket = 'test_bucket'
        csv_file = 'test_file.csv'
        aws_region = 'us-west-2'

        # Call the function
        s3.import_s3_csv_to_postgres_table(mock_connection, db_schema, fields, destination_table, s3_bucket, csv_file,
                                           aws_region)

        mock_cursor.execute.assert_has_calls(
            [
                call(f'DELETE FROM {db_schema}.{destination_table};'),
                call(
                    f"""SELECT aws_s3.table_import_from_s3('{db_schema}.{destination_table}', '{",".join(fields)}', '(format csv, HEADER)', aws_commons.create_s3_uri('{s3_bucket}', '{csv_file}', '{aws_region}'));"""),
            ]
        )

        mock_connection.transaction.assert_called_once()

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
            conversion.upload_to_s3(bucket, file_name, obj_name)

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

class HostedTests(TestCase):
    @patch('sweri_utils.hosted.get_count')
    def test_verify_feature_same_count(self, mock_get_count):
        mock_conn = MagicMock()
        mock_fl = MagicMock()
        mock_get_count.return_value = 1400000
        mock_fl.query.return_value = 1400000

        verify_feature_count(mock_conn, 'schema', 'table', mock_fl)

    @patch('sweri_utils.hosted.get_count')
    def test_verify_feature_mismatched_count(self, mock_get_count):
        mock_conn = MagicMock()
        mock_fl = MagicMock()
        mock_get_count.return_value = 1400000
        mock_fl.query.return_value = 1000000

        with self.assertRaises(ValueError):
            verify_feature_count(mock_conn, 'schema', 'table', mock_fl)
