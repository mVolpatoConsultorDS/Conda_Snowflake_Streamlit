CREATE OR REPLACE STREAMLIT DATA_CRAWLER
ROOT_LOCATION = '@data_catalog.table_catalog.src_files'
MAIN_FILE = '/manage.py'
QUERY_WAREHOUSE = COMPUTE_WH
COMMENT = '{"origin": "sf_sit",
            "name": "data_catalog",
            "version": {"major": 1, "minor": 1}}';