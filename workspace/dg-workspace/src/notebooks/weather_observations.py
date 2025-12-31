import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    import os
    from pathlib import Path
    import duckdb
    return Path, duckdb, mo, os


@app.cell(hide_code=True)
def _(Path, duckdb, os):
    db_path = os.getenv("DUCKDB_DATABASE")
    if db_path:
        DB_PATH = Path(db_path)
    else:
        DB_PATH = (
            Path(__file__).parent.parent.parent.parent.parent / "data" / "weather.db"
        )

    conn = duckdb.connect(str(DB_PATH), read_only=True)
    return (conn,)


@app.cell
def _(mo):
    mo.md(r"""
    ### Question 1: What is the current temperature and wind speed for each location?
    """)
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM default__dev.weather_current
        """,
        engine=conn
    )
    return


@app.cell
def _(mo):
    mo.md(r"""
    ### Question 2: What is the hourly forecast for each location for the next 5 days?
    """)
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM default__dev.weather_timeseries WHERE location_id = 1
        """,
        engine=conn
    )
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
