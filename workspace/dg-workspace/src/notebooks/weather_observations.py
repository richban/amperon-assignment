import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell(hide_code=True)
def _():
    import marimo as mo
    import os
    from pathlib import Path
    import duckdb
    import altair as alt
    return Path, alt, duckdb, mo, os


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
    weather_current_df = mo.sql(
        f"""
        SELECT * FROM default__dev.weather_current
        """,
        engine=conn
    )
    return (weather_current_df,)


@app.cell(hide_code=True)
def _(alt, weather_current_df):
    # replace _df with your data source
    _chart = (
        alt.Chart(weather_current_df)
        .mark_point()
        .encode(
            x=alt.X(field='location_id', type='nominal', title='Location', sort='ascending'),
            y=alt.Y(field='current_temperature_c', type='quantitative', title='Temperature °C', stack=True, sort='ascending'),
            color=alt.Color(field='current_temperature_c', type='quantitative'),
            tooltip=[
                alt.Tooltip(field='location_id', format=',.0f', title='Location'),
                alt.Tooltip(field='current_temperature_c', format=',.2f', title='Temperature °C'),
                alt.Tooltip(field='current_temperature_c', format=',.2f')
            ]
        )
        .properties(
            title='Current Temperature °C (Nowcast) ',
            height=334,
            width=512,
            config={
                'axis': {
                    'grid': True
                }
            }
        )
    )
    _chart
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
