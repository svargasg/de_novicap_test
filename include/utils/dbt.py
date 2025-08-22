from cosmos import ExecutionConfig, ProfileConfig, ProjectConfig
from cosmos.profiles import SnowflakeUserPasswordProfileMapping
from include.common.params import GlobalParams

params = GlobalParams()
sf_db = ProfileConfig(
    profile_name="sf_profile",
    target_name=params.ENV,
    profile_mapping=SnowflakeUserPasswordProfileMapping(
        conn_id=params.DBT_CONN_ID,
        profile_args={"schema": params.SCH_DBT},
    ),
)

dbt_project_config = ProjectConfig(
    dbt_project_path=f"{params.DBT_DIR}{params.DBT_PROJECT}",
    # manifest_path=f"{params.DBT_DIR}{params.DBT_PROJECT}/target/manifest.json",
    # project_name=params.DBT_PROJECT,
    env_vars={"dbt_env": params.ENV},
)

dbt_execution_config = ExecutionConfig(
    dbt_executable_path=params.DBT_VENV,
)
