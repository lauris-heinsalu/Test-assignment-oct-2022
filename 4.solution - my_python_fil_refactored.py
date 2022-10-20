import json
from pathlib import Path

SPECIFICATION_BASE_PATH = r"C:/Users/lauris/Downloads/Python_Task"
COMMON_SPECIFICATION_BASE_PATH = r"C:/Users/lauris/Downloads/Python_Task/COMMON"
COMMON_SPECIFICATION_FILE_PATH = R"postgres_extraction_spec.json"
DB_FOR_AUTO_DDL = 'my_db'
MONESE_TERMS_CONNECTION = 'monese_terms_connection'
MY_TARGET_DB =  'MY_TARGET_DB'

# def helper_function1
def read_json_file_to_dict(filepath: str) -> dict:
    try:
        with Path(filepath).open() as file:
            return json.loads(file.read()) 
    except FileNotFoundError:
        print('File not found')
    except ValueError:
        print('Failed to load Json')
    exit()

# def helper_function2
def add_dev_suffix(value, separator, suffix) -> str:
    return f"{value}{separator}{suffix}"

# def helper_function3
def get_db_object_full_path_in_uppercase(db: str, schema:str, object_name:str) -> str:
    return f'"{db.upper()}"."{schema.upper()}"."{object_name.upper()}"'

# was part of def main_function
def get_full_extraction_config(target_db, specfication_base_path, common_specification_base_path, common_specification_file_path, db_for_auto_ddl):
    # reads Python_Task/my_target_db/my_db/my_db.json file to dict.
    full_extraction_config = read_json_file_to_dict(str(Path(specfication_base_path, target_db, db_for_auto_ddl, db_for_auto_ddl + '.json')))
    
    # adds 'snowflake_staging_database': 'my_db', 'bucket': 'some-bucket'.
    full_extraction_config.update(read_json_file_to_dict(str(Path(common_specification_base_path, common_specification_file_path)))[target_db.lower()])

    full_extraction_config['bucket'] = add_dev_suffix(full_extraction_config['bucket'], "-", 'dev')
    return full_extraction_config


# def main_function
def print_alter_table_set_masking_policy_statements(connection_name: str, target_db: str, specfication_base_path: str, common_specification_base_path: str, common_specification_file_path: str, db_for_auto_ddl: str) -> None:

    full_extraction_config = get_full_extraction_config(target_db, specfication_base_path, common_specification_base_path, common_specification_file_path, db_for_auto_ddl)
    all_tables_info = full_extraction_config['tables']
    sql_list = []

    snowflake_staging_database = full_extraction_config['snowflake_staging_database']

    #Nested loops that generate alter statements for each column that has a masking policy set in the tables in the configuration file. 
    for table in all_tables_info:
        table_fully_qualified_path = get_db_object_full_path_in_uppercase(snowflake_staging_database, db_for_auto_ddl, table['name'])
        masking_policies_list = table.get('masking_policies_list', False)
        if masking_policies_list:
            for masking_policy_spec in masking_policies_list:
                try:
                    policy_name = list(masking_policy_spec.keys())[0]
                except:
                    print("No policy name in masking_policy_spec list[0]")
                    exit()

                target_columns = masking_policy_spec[policy_name]
                for column in target_columns:
                    sql_list.append(
                        f"""ALTER TABLE {table_fully_qualified_path} MODIFY COLUMN {column} SET MASKING POLICY {snowflake_staging_database}.METADATA.{policy_name};""")
    
    # instead of print, in the end the function probably would like to connect to a target_db and execute the Alter statements in sql_list. 
    print(sql_list) 

if __name__ == "__main__":
    print_alter_table_set_masking_policy_statements(MONESE_TERMS_CONNECTION, MY_TARGET_DB, SPECIFICATION_BASE_PATH, COMMON_SPECIFICATION_BASE_PATH, COMMON_SPECIFICATION_FILE_PATH, DB_FOR_AUTO_DDL)

    # I assumed it's allowed to change the function names so the code would comment itselt as they say. Also refactored a bit. 
     
    # How would "sql_list" look like, roughly?
            #sql_list would be alter statements setting masking policy for table column separated by comma. For example alter table database.schema.table modify column set masking policy masking_policy_name; 

    # Return the code with any changes required for it to work in your computer
        # minimally needed to change file location constants and change them to r"string", also add import statements. Moved ALTER statement to one line to remove line breaks \n.

