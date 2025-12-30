import marimo

__generated_with = "0.18.4"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    import duckdb

    DATABASE_URL = "/Users/melchior/Developer/amperon/data/weather.db"
    engine = duckdb.connect(DATABASE_URL, read_only=False)
    return (engine,)


@app.cell
def _(engine, mo):
    _df = mo.sql(
        f"""
        SELECT * FROM weather_data.weather_observations
        """,
        engine=engine
    )
    return


if __name__ == "__main__":
    app.run()
