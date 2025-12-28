"""Interactive Weather Visualization - Operational View"""

import marimo

__generated_with = "0.18.4"
app = marimo.App(width="full", app_title="Weather Forecast Visualization")


@app.cell
def __():
    import marimo as mo
    import duckdb
    from pathlib import Path
    from lonboard import Map, H3HexagonLayer
    import matplotlib.pyplot as plt
    from matplotlib.colors import Normalize
    from lonboard.colormap import apply_continuous_cmap

    return H3HexagonLayer, Map, Normalize, Path, apply_continuous_cmap, duckdb, mo, plt


@app.cell
def __(Path, duckdb):
    DB_PATH = Path(__file__).parent.parent.parent.parent.parent / "data" / "weather.db"
    conn = duckdb.connect(str(DB_PATH), read_only=True)
    conn.execute("INSTALL h3 FROM community; LOAD h3;")
    return (conn,)


@app.cell
def __(conn):
    runs_df = conn.execute("""
        SELECT DISTINCT observation_timestamp_utc
        FROM default__dev.bronze_weather
        ORDER BY observation_timestamp_utc DESC
    """).df()
    available_runs = runs_df["observation_timestamp_utc"].tolist()
    return available_runs, runs_df


@app.cell
def __(mo, available_runs):
    run_selector = mo.ui.dropdown(
        options={str(run): run for run in available_runs},
        value=str(available_runs[0]),
        label="Observation Time (When was forecast made):",
    )
    return (run_selector,)


@app.cell
def __(mo, run_selector):
    selected_run = run_selector.value

    mo.md(f"""
    # 🌤️ Weather Forecast - Operational View
    ## Port of Brownsville, TX

    {run_selector}

    **Viewing forecast made at**: {selected_run}
    """)
    return (selected_run,)


@app.cell
def __(conn, selected_run):
    forecast_times_df = conn.execute(f"""
        SELECT DISTINCT forecast_timestamp_utc
        FROM default__dev.bronze_weather
        WHERE observation_timestamp_utc = '{selected_run}'
        ORDER BY forecast_timestamp_utc
    """).df()
    forecast_times = forecast_times_df["forecast_timestamp_utc"].tolist()
    return forecast_times, forecast_times_df


@app.cell
def __(mo, forecast_times):
    time_slider = mo.ui.slider(
        start=0,
        stop=len(forecast_times) - 1,
        value=0,
        step=1,
        label=f"Forecast Hour (0 = Now, {len(forecast_times) - 1} = +5 Days):",
        debounce=True,
        show_value=True,
    )
    return (time_slider,)


@app.cell
def __(mo, time_slider, forecast_times):
    selected_time = forecast_times[time_slider.value]

    mo.md(f"""
    ### 🕐 Time Slider

    {time_slider}

    **Currently viewing**: {selected_time}
    """)
    return (selected_time,)


@app.cell
def __(conn, selected_run, selected_time):
    weather_arrow = conn.execute(f"""
        SELECT
            h3_latlng_to_cell(l.lat, l.lon, 9) AS hex_id,
            AVG(bw.temperature_celsius) AS temperature,
            AVG(bw.wind_speed_mps) AS wind_speed,
            AVG(l.lat) AS lat,
            AVG(l.lon) AS lon
        FROM default__dev.bronze_weather bw
        JOIN weather_data.locations l ON l.id = bw.location_id
        WHERE bw.observation_timestamp_utc = '{selected_run}'
          AND bw.forecast_timestamp_utc = '{selected_time}'
        GROUP BY hex_id
    """).fetch_arrow_table()
    return (weather_arrow,)


@app.cell
def __(Normalize, apply_continuous_cmap, plt):
    def generate_colors(table):
        temp_values = table["temperature"].to_numpy()
        min_temp, max_temp = -20, 50
        normalizer = Normalize(vmin=min_temp, vmax=max_temp, clip=True)
        normalized = normalizer(temp_values)
        cmap = plt.get_cmap("RdYlBu_r")
        return apply_continuous_cmap(normalized, cmap)

    return (generate_colors,)


@app.cell
def __(H3HexagonLayer, Map, generate_colors, weather_arrow):
    colors = generate_colors(weather_arrow)

    layer = H3HexagonLayer(
        table=weather_arrow,
        get_hexagon=weather_arrow["hex_id"],
        get_fill_color=colors,
        extruded=True,
        get_elevation=weather_arrow["temperature"].to_numpy() * 100,
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
    return layer, map_widget


@app.cell
def __(map_widget):
    map_widget


@app.cell
def __(mo, weather_arrow):
    temp_values = weather_arrow["temperature"].to_numpy()
    wind_values = weather_arrow["wind_speed"].to_numpy()

    mo.md(f"""
    ### 📊 Statistics

    - **Locations**: {len(weather_arrow)} hexagons
    - **Temperature**: {temp_values.min():.1f}°C - {temp_values.max():.1f}°C (Avg: {temp_values.mean():.1f}°C)
    - **Wind Speed**: {wind_values.min():.1f} - {wind_values.max():.1f} m/s (Avg: {wind_values.mean():.1f} m/s)

    **Color Legend**: 🔵 Blue = Cold → 🔴 Red = Hot
    """)


if __name__ == "__main__":
    app.run()
