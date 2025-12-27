"""Interactive Weather Visualization"""

import marimo

__generated_with = "0.18.4"
app = marimo.App(width="full", app_title="Weather Forecast Visualization")


@app.cell
def _():
    import marimo as mo
    import duckdb
    from pathlib import Path
    from lonboard import Map, H3HexagonLayer
    import numpy as np
    import matplotlib.pyplot as plt
    from lonboard.colormap import apply_continuous_cmap
    return (
        H3HexagonLayer,
        Map,
        Path,
        apply_continuous_cmap,
        duckdb,
        mo,
        np,
        plt,
    )


@app.cell
def _(Path, duckdb):
    DB_PATH = Path(__file__).parent.parent.parent.parent.parent / "data" / "weather.db"
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute("INSTALL h3 FROM community; LOAD h3;")
    return (conn,)


@app.cell
def _(conn, mo):
    forecast_hours_df = mo.sql(
        f"""
        SELECT DISTINCT
            forecast_hour
        FROM
            default__dev.weather_timeseries
        WHERE
            data_type = 'Forecast'
        ORDER BY
            forecast_hour
        """,
        engine=conn
    )
    return (forecast_hours_df,)


@app.cell
def _(forecast_hours_df, mo):
    max_index = len(forecast_hours_df) - 1

    time_slider = mo.ui.slider(
        start=0,
        stop=max_index,
        value=0,
        step=1,
        label="Hour",
        debounce=True,
    )

    time_slider
    return (time_slider,)


@app.cell
def _(forecast_hours_df, time_slider):
    selected_hour = forecast_hours_df[time_slider.value]["forecast_hour"].item()
    return (selected_hour,)


@app.cell
def _(conn, selected_hour):
    query = f"""
        WITH location_coords AS (
            SELECT id, lat, lon 
            FROM weather_data.locations
        )
        SELECT 
            h3_latlng_to_cell(l.lat, l.lon, 9) AS hex_id,
            AVG(w.temperature_celsius) AS temperature,
            AVG(l.lat) AS lat,
            AVG(l.lon) AS lon
        FROM default__dev.weather_timeseries w
        JOIN location_coords l ON w.location_id = l.id
        WHERE w.forecast_hour = TIMESTAMP '{selected_hour}'
        GROUP BY hex_id
    """

    weather_arrow = conn.execute(query).fetch_arrow_table()
    weather_arrow
    return (weather_arrow,)


@app.cell
def _(apply_continuous_cmap, np, plt):
    def generate_colors(table):
        # Extract temperature column as numpy array
        temp_values = table["temperature"].to_numpy()
    
        # Normalize to 0-1 range based on expected climate bounds
        norm_values = (temp_values - 20) / (35 - 20)
        norm_values = np.clip(norm_values, 0, 1)
    
        # Apply colormap (e.g., 'inferno' for heat)
        cmap = plt.get_cmap('inferno')
        return apply_continuous_cmap(norm_values, cmap)
    return (generate_colors,)


@app.cell
def _(H3HexagonLayer, Map, generate_colors, weather_arrow):
    colors = generate_colors(weather_arrow)

    layer = H3HexagonLayer(
        table=weather_arrow,
        get_hexagon=weather_arrow["hex_id"],
        get_fill_color=colors,
        extruded=True,
        # get_elevation=weather_arrow["temperature"].to_numpy() * 100,
        opacity=0.8,
        pickable=True,
    )

    map_widget = Map(
        layers=[layer],
        view_state={
            "latitude": weather_arrow["lat"].to_numpy().mean(),
            "longitude": weather_arrow["lon"].to_numpy().mean(),
            "zoom": 11,
            "pitch": 45,
        },
    )
    return (map_widget,)


@app.cell
def _(map_widget):
    map_widget
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
