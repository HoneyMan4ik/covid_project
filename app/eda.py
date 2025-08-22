import os
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, date_trunc, sum as ssum, avg as aavg
from .deps import get_sf_session

def _covid_table_name() -> str:
    return os.getenv(
        "COVID_TABLE",
        "CALIFORNIA_COVID19_DATASETS.PUBLIC.CASE_RATES_BY_ZIP",
    )


def list_columns(s: Session) -> list[str]:
    df = s.table(_covid_table_name())
    return list(df.columns)


def sample(s: Session, limit: int = 5) -> list[dict]:
    df = s.table(_covid_table_name()).limit(int(limit))
    return [dict(r.asDict()) for r in df.collect()]


def aggregate_timeseries(
    s: Session,
    date_col: str,
    value_col: str,
    geo_col: str | None = None,
    agg: str = "sum",
    limit: int = 1000,
) -> list[dict]:
    df = s.table(_covid_table_name())
    df = df.with_column("__date__", col(date_col))

    if agg.lower() == "avg":
        from snowflake.snowpark.functions import avg as a_avg
        agg_expr = a_avg(col(value_col)).alias("value")
    elif agg.lower() == "max":
        from snowflake.snowpark.functions import max as smax
        agg_expr = smax(col(value_col)).alias("value")
    elif agg.lower() == "min":
        from snowflake.snowpark.functions import min as smin
        agg_expr = smin(col(value_col)).alias("value")
    else:
        agg_expr = ssum(col(value_col)).alias("value")

    if geo_col:
        grouped = (
            df.group_by(col("__date__"), col(geo_col))
            .agg(agg_expr)
            .select(col("__date__").alias("date"), col(geo_col).alias("geo"), col("value"))
            .order_by(col("date"))
            .limit(int(limit))
        )
    else:
        grouped = (
            df.group_by(col("__date__"))
            .agg(agg_expr)
            .select(col("__date__").alias("date"), col("value"))
            .order_by(col("date"))
            .limit(int(limit))
        )

    rows = []
    for r in grouped.collect():
        d = dict(r.asDict())
        d = {str(k).lower(): v for k, v in d.items()}
        rows.append(d)
    return rows


def run_eda():
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
    print(df.head())
    if not df.empty:
        print(df.describe(include="all"))


if __name__ == "__main__":
    run_eda()