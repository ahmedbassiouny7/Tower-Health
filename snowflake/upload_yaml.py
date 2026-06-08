import snowflake.connector

con = snowflake.connector.connect(
    user='<USER>',
    password='<PASSWORD>',
    account='<ACCOUNT>',     # from your Snowflake URL, e.g. abcd-xy12345
    role='<ROLE>',           # ACCOUNTADMIN or whichever owns SEMANTIC_MODELS
    warehouse='<WAREHOUSE>',
    database='TOWER_HEALTH_DB',
    schema='PUBLIC',
)

con.cursor().execute(
    "PUT 'file://C:/Users/A-bsy/Downloads/tower_health_semantic_model.yaml' "
    "@SEMANTIC_MODELS AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
)
con.cursor().execute("ALTER STAGE SEMANTIC_MODELS REFRESH")
print(con.cursor().execute("LS @SEMANTIC_MODELS").fetchall())
