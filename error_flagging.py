from sweri_utils.sql import  connect_to_pg_db
import os
import logging
from dotenv import load_dotenv
# import watchtower

def create_duplicate_table(cursor, schema, table_name):
    
    # Makes space for duplicate table
    cursor.execute('BEGIN;')
    cursor.execute(f'''

       DROP TABLE IF EXISTS
       {schema}.treatment_index_duplicates;

    ''')
    cursor.execute('COMMIT;')

    # Creates a table of duplicates 
    cursor.execute('BEGIN;')
    cursor.execute(f'''

        CREATE TABLE {schema}.treatment_index_duplicates AS 
            SELECT * FROM {schema}.{table_name}
            WHERE shape IS NOT NULL
            AND (actual_completion_date, activity, shape::text) IN (
                SELECT 
                    actual_completion_date, 
                    activity, 
                    shape::text AS shape_text
                FROM {schema}.{table_name}
                WHERE shape IS NOT NULL
                GROUP BY actual_completion_date, activity, shape::text
                HAVING COUNT(*) > 1);

    ''')
    cursor.execute('COMMIT;')
    logging.info(f'Duplicates table created at {schema}.treatment_index_duplicates')

def flag_duplicate_table(cursor, schema, table_name):
    # Creates partitions of each group of duplicates and ranks them
    # Changes rank 1 to DUPLICATE-KEEP, and all others to DUPLICATE-DROP
    # Ensures 1 record kept and all others dropped for each group of duplicates
    cursor.execute('BEGIN;')
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
    cursor.execute('COMMIT;')
    logging.info(f'Duplicates flagged in {schema}.treatment_index_duplicates')

def update_treatment_index_duplicates(cursor, schema, table_name):
    # Updates treatment index table to match duplicate flags in duplicate table
    cursor.execute('BEGIN;')
    cursor.execute(f'''

        UPDATE {schema}.{table_name} ti
        SET error = dup.error
        FROM {schema}.treatment_index_duplicates dup
        WHERE ti.unique_id = dup.unique_id;

    ''')
    cursor.execute('COMMIT;')
    logging.info(f'Duplicates updated in {schema}.{table_name}')


def flag_duplicates(cursor, schema, table_name):
    # Create a table of all duplicates
    # Flag table with DUPLICATE-KEEP or DUPLICATE-DROP for each record
    # Update treatment index with duplicate flags from duplicate table

    create_duplicate_table(cursor, schema, table_name)
    flag_duplicate_table(cursor, schema, table_name)
    update_treatment_index_duplicates(cursor, schema, table_name)

def flag_high_cost_acres(cursor, schema, table_name):
    cursor.execute('BEGIN;')
    cursor.execute(f'''
        UPDATE {schema}.{table_name}
        SET error = 
            CASE
                WHEN error IS NULL THEN 'HIGH_COST'
                ELSE error || ';HIGH_COST'
            END
        WHERE
        uom = 'ACRES' 
        AND
        acres is not null
        AND 
        cost_per_uom > 10000;
    ''')
    cursor.execute('COMMIT;')

def flag_high_cost_each(cursor, schema, table_name):
    cursor.execute('BEGIN;')
    cursor.execute(f'''
        UPDATE {schema}.{table_name}
        SET error =            
            CASE
                WHEN error IS NULL THEN 'HIGH_COST'
                ELSE error || ';HIGH_COST'
            END
        WHERE
        uom = 'EACH' 
        AND
        acres is not null
        AND
        cost_per_uom/acres > 10000;
    ''')
    cursor.execute('COMMIT;')

def flag_high_cost_miles(cursor, schema, table_name):
    cursor.execute('BEGIN;')
    cursor.execute(f'''
        UPDATE {schema}.{table_name}
        SET error = 
            CASE
                WHEN error IS NULL THEN 'HIGH_COST'
                ELSE error || ';HIGH_COST'
            END
        WHERE
        uom = 'MILES' 
        AND
        acres is not null
        AND
        cost_per_uom/(acres*640) > 10000;
    ''')
    cursor.execute('COMMIT;')
    logging.info(f'High cost flagged in error field for {schema}.{table_name}')

def flag_high_cost(cursor, schema, table_name):
    # Flags treatments with more than $10,000 spent per acre of treatment 
    # Different functions are needed based on the uom or Unit of Measure
    # Current uom possibilites are acres, each, and miles

    flag_high_cost_acres(cursor, schema, table_name)
    flag_high_cost_each(cursor, schema, table_name)
    flag_high_cost_miles(cursor, schema, table_name)

if __name__ == "__main__":
    load_dotenv()
    target_table = 'treatment_index_facts_nfpors_temp'
    target_schema = os.getenv('SCHEMA')
    logger = logging.getLogger(__name__)
    logging.basicConfig( format='%(asctime)s %(levelname)-8s %(message)s',filename='./error_flagging.log', encoding='utf-8', level=logging.INFO, datefmt='%Y-%m-%d %H:%M:%S')
    # logger.addHandler(watchtower.CloudWatchLogHandler())


    cur = connect_to_pg_db(os.getenv('DB_HOST'), os.getenv('DB_PORT'), os.getenv('DB_NAME'), os.getenv('DB_USER'), os.getenv('DB_PASSWORD'))
    flag_high_cost(cur, target_schema, target_table)
    flag_duplicates(cur, target_schema, target_table)
