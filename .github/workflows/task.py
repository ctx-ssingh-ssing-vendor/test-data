import snowflake.connector
import os
import re

def execute_sql_scripts():
    try:
        ctx = snowflake.connector.connect(
            account=os.environ['SNOWFLAKE_ACCOUNT'],
            user=os.environ['SNOWFLAKE_USER'],
            private_key=os.environ['SNOWFLAKE_PRIVATE_KEY'],
            warehouse=os.environ['SNOWFLAKE_WAREHOUSE']
        )
        cs = ctx.cursor()

        sql_directory = 'click_integrations_snowflake/bi_us/click_adhoc/sql_deployements/'
        for filename in os.listdir(sql_directory):
            if filename.endswith('.sql'):
                filepath = os.path.join(sql_directory, filename)
                print(f"Executing script: {filename}")
                with open(filepath, 'r') as f:
                    sql_content = f.read()
                    # Split SQL content into individual statements, handling semicolons
                    sql_statements = [stmt.strip() for stmt in re.split(';', sql_content) if stmt.strip()]
                    for sql in sql_statements:
                        try:
                            cs.execute(sql)
                            if cs.rowcount > 0:
                                print(f"  {cs.rowcount} rows affected.")
                            elif cs.fetchone():
                                for row in cs:
                                    print(f"  {row}")
                            else:
                                print("  No result.")
                        except snowflake.connector.errors.ProgrammingError as e:
                            print(f"  Error executing statement: {e}")
                print("-" * 20)

        cs.close()
    except snowflake.connector.errors.Error as e:
        print(f"Snowflake connection error: {e}")
    finally:
        if 'ctx' in locals() and ctx:
            ctx.close()

if __name__ == "__main__":
    execute_sql_scripts()
