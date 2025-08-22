from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import os
import pandas as pd
import numpy as np

load_dotenv()
app = FastAPI(title="covid_de_platform")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

COVID_TABLE = os.getenv("COVID_TABLE", "CALIFORNIA_COVID19_DATASETS.PUBLIC.CASE_RATES_BY_ZIP")
DATE_COL_DEFAULT = "DATE"

def json_safe_records(df: pd.DataFrame) -> list[dict]:
    if df is None or df.empty:
        return []
    if "MONTH" in df.columns:
        df["MONTH"] = pd.to_datetime(df["MONTH"], errors="coerce").dt.strftime("%Y-%m-%d")
    df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
    return df.to_dict("records")

@app.get("/health")
def health():
    return {"status": "ok"}

@app.get("/sf/ping")
def sf_ping():
    try:
        from app.deps import get_sf_session
        s = get_sf_session()
        ver = s.sql("select current_version()").collect()[0][0]
        return {"snowflake_version": ver}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/sf/config")
def sf_config():
    try:
        from app.deps import get_sf_config_summary
        return get_sf_config_summary()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/covid/summary")
def covid_summary(limit: int = 5):
    try:
        from app.deps import get_sf_session
        from app.eda import sample
        s = get_sf_session()
        return sample(s, limit=limit)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/covid/columns")
def covid_columns():
    try:
        from app.deps import get_sf_session
        from app.eda import list_columns
        s = get_sf_session()
        return list_columns(s)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/covid/aggregate")
def covid_aggregate(
    date_col: str,
    value_col: str,
    geo_col: str | None = None,
    agg: str = "sum",
    limit: int = 1000,
):
    try:
        from app.deps import get_sf_session
        from app.eda import aggregate_timeseries
        from app.cache import get_if_fresh, set_with_ttl
        s = get_sf_session()
        cache_key = f"agg:{date_col}:{value_col}:{geo_col}:{agg}:{limit}"
        cached = get_if_fresh(cache_key)
        if cached is not None:
            return {"cached": True, "rows": cached}
        rows = aggregate_timeseries(
            s,
            date_col=date_col,
            value_col=value_col,
            geo_col=geo_col,
            agg=agg,
            limit=limit,
        )
        set_with_ttl(cache_key, rows, ttl_seconds=300)
        return {"cached": False, "rows": rows}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/analytics/summary")
def analytics_summary():
    try:
        from app.analytics import get_analytics_summary
        return get_analytics_summary()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/analytics/forecast")
def analytics_forecast(periods: int = 7):
    try:
        from app.analytics import get_covid_data_for_analysis, simple_forecast
        df = get_covid_data_for_analysis()
        if df.empty:
            raise HTTPException(status_code=400, detail="No data available")
        
        forecast_df = simple_forecast(df, periods)
        return {"forecast": forecast_df.to_dict('records')}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/annotations")
def add_annotation(geo: str, text: str, author: str | None = None):
    try:
        from app.nosql import add_annotation as add_note
        return add_note(geo=geo, text=text, author=author)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/annotations")
def list_annotation(geo: str | None = None):
    try:
        from app.nosql import list_annotations
        return list_annotations(geo=geo)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/eda/mobility")
def get_mobility_data():
    try:
        from app.deps import get_sf_session
        from app.cache import get_if_fresh, set_with_ttl
        from snowflake.snowpark.functions import col, date_trunc, sum as ssum, avg as aavg
        
        # Check cache first
        cache_key = "mobility:joined_data"
        cached = get_if_fresh(cache_key)
        if cached is not None:
            return {"cached": True, "rows": cached}
        
        s = get_sf_session()

        cases = (
            s.table("CALIFORNIA_COVID19_DATASETS.COVID.CASES")
            .where((col("AREA_TYPE") == "County") & (col("AREA") != "Unknown"))
            .with_column("MONTH", date_trunc("month", col("DATE")))
            .group_by(col("MONTH"))
            .agg(ssum(col("CASES")).alias("MONTHLY_CASES"))
        )

        mobility = (
            s.table("COVID19_EPIDEMIOLOGICAL_DATA.PUBLIC.GOOG_GLOBAL_MOBILITY_REPORT")
            .where(
                (col("COUNTRY_REGION") == "United States")
                & (col("PROVINCE_STATE") == "California")
                & col("SUB_REGION_2").is_null()
            )
            .with_column("MONTH", date_trunc("month", col("DATE")))
            .group_by(col("MONTH"))
            .agg(
                aavg(col("RETAIL_AND_RECREATION_CHANGE_PERC")).alias("RETAIL"),
                aavg(col("WORKPLACES_CHANGE_PERC")).alias("WORKPLACES"),
                aavg(col("RESIDENTIAL_CHANGE_PERC")).alias("RESIDENTIAL"),
            )
        )

        # LEFT JOIN and order
        joined = (
            cases.join(mobility, cases["MONTH"] == mobility["MONTH"], how="left")
            .select(
                cases["MONTH"],
                cases["MONTHLY_CASES"],
                mobility["RETAIL"],
                mobility["WORKPLACES"],
                mobility["RESIDENTIAL"],
            )
            .order_by(cases["MONTH"])
        )

        df = joined.to_pandas()
        month_like = [c for c in df.columns if str(c).upper().endswith("MONTH")] if not df.empty else []
        if month_like and "MONTH" not in df.columns:
            df = df.rename(columns={month_like[0]: "MONTH"})

        records = json_safe_records(df)
        
        # Cache the result for 10 minutes
        set_with_ttl(cache_key, records, ttl_seconds=600)
        
        return {"cached": False, "rows": records}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
