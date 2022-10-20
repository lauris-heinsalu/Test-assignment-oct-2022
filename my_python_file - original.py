SPECIFICATION_BASE_PATH = "/some/path/in/system"
COMMON_SPECIFICATION_BASE_PATH = "/some/path/in/system/COMMON"


def helper_function1(filepath: str) -> dict:
    path = Path(filepath)
    with path.open() as f:
        contents_str = f.read()

    contents_dict = json.loads(contents_str)
    return contents_dict


def helper_function2(value, separator):
    suffix = 'dev'
    return f"{value}{separator}{suffix}"


def helper_function3(db: str, schema:str, object_name:str) -> str:
    return f'"{db.upper()}"."{schema.upper()}"."{object_name.upper()}"'


def main_function(connection_name: str, target_db: str) -> None:
    db_for_auto_ddl = 'my_db'

    full_extraction_config = {}
    db_spec_file_fullpath = Path(SPECIFICATION_BASE_PATH, target_db, db_for_auto_ddl, db_for_auto_ddl + '.json')
    db_specific_config = helper_function1(str(db_spec_file_fullpath))
    full_extraction_config.update(db_specific_config)
    common_spec_file_fullpath = Path(COMMON_SPECIFICATION_BASE_PATH,'postgres_extraction_spec.json')
    monese_stage_conn = helper_function1(str(common_spec_file_fullpath))[target_db.lower()]
    full_extraction_config.update(monese_stage_conn)
    full_extraction_config['bucket'] = helper_function2(full_extraction_config['bucket'], "-")
    all_tables_info = full_extraction_config['tables']
    sql_list = []
    for table in all_tables_info:
        masking_policies_list = table.get('masking_policies_list', None)
        if masking_policies_list:
            for masking_policy_spec in masking_policies_list:
                policy_name = list(masking_policy_spec.keys())[0]
                target_columns = masking_policy_spec[policy_name]
                for column in target_columns:
                    sql_list.append(
                        f"""ALTER TABLE {helper_function3(db=full_extraction_config['snowflake_staging_database'], schema=db_for_auto_ddl, object_name=table['name'])}
                            MODIFY COLUMN {column} SET MASKING POLICY {full_extraction_config['snowflake_staging_database']}.METADATA.{policy_name};""")
                print(sql_list)

if __name__ == "__main__":
    main_function(connection_name='monese_terms_connection', target_db='MY_TARGET_DB')

    # Comment/reorganise the main_function as you see fit
    # Describe what each function does
    # How would "sql_list" look like, roughly?
    # Return the code with any changes required for it to work in your computer
