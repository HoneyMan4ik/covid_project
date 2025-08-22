import os
from dotenv import load_dotenv
from snowflake.snowpark import Session

load_dotenv()

def _normalize_account(account: str | None) -> str | None:
    if not account:
        return None
    account = account.replace("https://", "").replace("http://", "")
    if ".snowflakecomputing.com" in account:
        account = account.split(".snowflakecomputing.com")[0]
    return account


def get_sf_session() -> Session:
    host = os.getenv("SNOWFLAKE_HOST")
    account = _normalize_account(os.getenv("SNOWFLAKE_ACCOUNT"))
    authenticator = (os.getenv("SNOWFLAKE_AUTHENTICATOR") or "").lower().strip()

    cfg = {
        "user": os.getenv("SNOWFLAKE_USER"),
        "role": os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database": os.getenv("SNOWFLAKE_DATABASE", "SNOWFLAKE_LEARNING_DB"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
    }

    if host:
        cfg["host"] = host

        derived = host.replace("https://", "").replace("http://", "")
        if derived.endswith(".snowflakecomputing.com"):
            derived = derived[: -len(".snowflakecomputing.com")]

        if account:
            cfg["account"] = account
        else:
            cfg["account"] = derived
    elif account:
        cfg["account"] = account
    else:
        raise ValueError("Provide SNOWFLAKE_HOST or SNOWFLAKE_ACCOUNT in .env")

    insecure_flag = os.getenv("SNOWFLAKE_INSECURE_MODE", "false").lower() in {"1", "true", "yes"}
    if insecure_flag:
        cfg["insecure_mode"] = True

    if authenticator in {"externalbrowser", "oauth"}:
        cfg["authenticator"] = authenticator
        if authenticator == "oauth":
            token = os.getenv("SNOWFLAKE_OAUTH_TOKEN")
            if not token:
                raise ValueError("SNOWFLAKE_OAUTH_TOKEN is required for authenticator=oauth")
            cfg["token"] = token

        if not cfg.get("user"):
            raise ValueError("SNOWFLAKE_USER is required")
    else:
        # default: password auth
        cfg["password"] = os.getenv("SNOWFLAKE_PASSWORD")
        for key in ("user", "password"):
            if not cfg.get(key):
                raise ValueError(f"Missing env var for Snowflake: {key.upper()}")

    return Session.builder.configs(cfg).create()


def get_sf_config_summary() -> dict:
    host = os.getenv("SNOWFLAKE_HOST")
    account = _normalize_account(os.getenv("SNOWFLAKE_ACCOUNT"))
    authenticator = (os.getenv("SNOWFLAKE_AUTHENTICATOR") or "").lower().strip()
    return {
        "host": host or None,
        "account": account or None,
        "role": os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        "database": os.getenv("SNOWFLAKE_DATABASE", "SNOWFLAKE_LEARNING_DB"),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
        "authenticator": authenticator or "password",
        "note": "Set SNOWFLAKE_HOST to the exact login domain (no https), or set SNOWFLAKE_ACCOUNT as account_locator.region",
    }