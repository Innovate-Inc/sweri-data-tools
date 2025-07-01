from sweri_utils.sweri_logging import log_this
from sweri_utils.sql import  connect_to_pg_db, postgres_create_index
import os
import logging
from dotenv import load_dotenv
import watchtower


@log_this
def flag_duplicate_ids(conn, schema, table_name):
    # Sets DUPLICATE-ID Flag for all entries where the id appears in the table more than once

    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''

            UPDATE {schema}.{table_name} tid
             SET error = 
                    CASE
                        WHEN tid.error IS NULL THEN 'DUPLICATE-ID'
                        ELSE tid.error || ';DUPLICATE-ID'
                    END
            WHERE unique_id IN (
                SELECT unique_id
                FROM {schema}.{table_name}
                GROUP BY unique_id
                HAVING COUNT(*) > 1
            );

        ''')


@log_this
def create_duplicate_table(conn, schema, table_name):
    cursor = conn.cursor()
    with conn.transaction():
        # Makes space for duplicate table
        cursor.execute(f'''
    
           DROP TABLE IF EXISTS
           {schema}.treatment_index_duplicates;
    
        ''')

        # Creates a table of duplicates
        cursor.execute(f'''
    
            CREATE TABLE {schema}.treatment_index_duplicates AS 
                SELECT * FROM {schema}.{table_name} as s
                WHERE shape IS NOT NULL
                AND EXISTS (SELECT 1
                    FROM sweri.treatment_index as s2
                    WHERE shape IS NOT NULL
                    AND s.actual_completion_date = s2.actual_completion_date
                    AND s.activity = s2.activity
                    AND s.shape::text = s2.shape::text
                    GROUP BY actual_completion_date, activity, shape::text
                    HAVING COUNT(*) > 1);
        ''')
        # logging.info(f'Duplicates table created at {schema}.treatment_index_duplicates')

        cursor.execute(f'''
    
            CREATE INDEX
            ON {schema}.treatment_index_duplicates
            USING GIST (shape);
    
        ''')

    postgres_create_index(conn, schema, 'treatment_index_duplicates', 'activity')
    postgres_create_index(conn, schema, 'treatment_index_duplicates', 'actual_completion_date')



@log_this
def flag_duplicate_table(conn, schema, table_name):
    # Creates partitions of each group of duplicates and ranks them
    # Changes rank 1 to DUPLICATE-KEEP, and all others to DUPLICATE-DROP
    # Ensures 1 record kept and all others dropped for each group of duplicates
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
            WITH ranking_treatment_duplicates AS (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY actual_completion_date, activity, shape::text
                    ) AS row_num
                FROM {schema}.treatment_index_duplicates
            )
                
            UPDATE {schema}.treatment_index_duplicates tid
                SET error =
                    CASE
                        WHEN ranking_treatment_duplicates.row_num = 1 THEN 
                            CASE
                                WHEN tid.error IS NULL THEN 'DUPLICATE-KEEP'
                                ELSE tid.error || ';DUPLICATE-KEEP'
                            END
                        ELSE 
                            CASE
                                WHEN tid.error IS NULL THEN 'DUPLICATE-DROP'
                                ELSE tid.error || ';DUPLICATE-DROP'
                            END
                    END	
                FROM ranking_treatment_duplicates
                WHERE tid.unique_id = ranking_treatment_duplicates.unique_id;
    
        ''')
    # logging.info(f'Duplicates flagged in {schema}.treatment_index_duplicates')

@log_this
def update_treatment_index_duplicates(conn, schema, table_name):
    # Updates treatment index table to match duplicate flags in duplicate table
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''

            UPDATE {schema}.{table_name} ti
            SET error = dup.error
            FROM {schema}.treatment_index_duplicates dup
            WHERE ti.unique_id = dup.unique_id;
    
        ''')
    # logging.info(f'Duplicates updated in {schema}.{table_name}')


@log_this
def flag_duplicates(cursor, schema, table_name):
    # Create a table of all duplicates
    # Flag table with DUPLICATE-KEEP or DUPLICATE-DROP for each record
    # Update treatment index with duplicate flags from duplicate table

    create_duplicate_table(cursor, schema, table_name)
    flag_duplicate_table(cursor, schema, table_name)
    update_treatment_index_duplicates(cursor, schema, table_name)

@log_this
def flag_high_cost(conn, schema, table_name):
    # Flags treatments with more than $10,000 spent per acre of treatment
    # Different functions are needed based on the uom or Unit of Measure
    # Current uom possibilites are acres, each, and miles
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
            UPDATE {schema}.{table_name}
            SET error =            
                CASE
                    WHEN error IS NULL THEN 'HIGH_COST'
                    ELSE error || ';HIGH_COST'
                END
             WHERE (
                (uom = 'EACH' AND acres IS NOT NULL AND cost_per_uom / acres > 10000)
                OR
                (uom = 'ACRES' AND acres IS NOT NULL AND cost_per_uom > 10000)
                );
    
        ''')

@log_this
def flag_uom_outliers(conn, schema, table_name):
    cursor = conn.cursor()
    with conn.transaction():
        cursor.execute(f'''
            UPDATE {schema}.{table_name}
            SET error = 
                CASE
                    WHEN error IS NULL THEN 'CHECK_UOM'
                    ELSE error || ';CHECK_UOM'
                END
            WHERE
            uom = 'MILES' 
            OR
            uom = 'EACH';
        ''')

if __name__ == "__main__":
    load_dotenv()
    target_table = 'treatment_index_facts_nfpors_temp'
    target_schema = os.getenv('SCHEMA')
    logger = logging.getLogger(__name__)
    logging.basicConfig( format='%(asctime)s %(levelname)-8s %(message)s',filename='./error_flagging.log', encoding='utf-8', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
    logger.addHandler(watchtower.CloudWatchLogHandler())


    conn = connect_to_pg_db(os.getenv('RDS_DB_HOST'), os.getenv('RDS_DB_PORT'), os.getenv('RDS_DB_NAME'), os.getenv('RDS_DB_USER'), os.getenv('RDS_DB_PASSWORD'))
    flag_high_cost(conn, target_schema, target_table)
    flag_duplicates(conn, target_schema, target_table)
    flag_uom_outliers(conn, target_schema, target_table)
    conn.close()
