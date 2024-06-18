from snowflake.snowpark.session import Session
from snowflake.snowpark.context import get_active_session
import toml

def create_session_object():
    config_path = '/Library/Application Support/snowflake/connections.toml'
    configs = toml.load(config_path)['default']  # assuming 'default' is your profile
    try:
        session = get_active_session()
    except:
        session = Session.builder.configs(configs).create()
    #with session:
    #    print(f"Minha ses: {session}")
    #    print(f"Current Database and schema: {session.get_fully_qualified_current_schema()}")
    #    print(f"Current Warehouse: {session.get_current_warehouse()}")
    return session 
    

if __name__ == "__main__":   
    # Function call    
    try:
        session = create_session_object()
        print(session)
    finally:
        if not session._conn.is_closed():
            session.close();