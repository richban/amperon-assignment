"""Interactive Weather Visualization - Operational View"""

import marimo

__generated_with = "0.18.4"
app = marimo.App(width="full", app_title="Weather Forecast Visualization")


@app.cell
def _():
    import marimo as mo
    import duckdb
    from pathlib import Path
    from lonboard import Map, H3HexagonLayer
    import matplotlib.pyplot as plt
    from matplotlib.colors import Normalize
    from lonboard.colormap import apply_continuous_cmap
    import pandas as pd
    import altair as alt
    import os
    return (
        H3HexagonLayer,
        Map,
        Normalize,
        Path,
        alt,
        apply_continuous_cmap,
        duckdb,
        mo,
        os,
        pd,
        plt,
    )


@app.cell
def _(Path, duckdb, os):
    db_path = os.getenv("DUCKDB_DATABASE")
    if db_path:
        DB_PATH = Path(db_path)
    else:
        DB_PATH = (
            Path(__file__).parent.parent.parent.parent.parent / "data" / "weather.db"
        )

    conn = duckdb.connect(str(DB_PATH), read_only=True)
    conn.execute("INSTALL h3 FROM community; LOAD h3;")
    return (conn,)


@app.cell
def _(conn):
    runs_df = conn.execute("""
        SELECT DISTINCT observation_timestamp_utc
        FROM default__dev.silver_weather
        ORDER BY observation_timestamp_utc DESC
    """).df()
    available_runs = runs_df["observation_timestamp_utc"].tolist()
    return (available_runs,)


@app.cell
def _(available_runs, mo):
    run_selector = mo.ui.dropdown(
        options={str(run): run for run in available_runs},
        value=str(available_runs[0]),
        label="Observation Time (When was forecast made):",
    )
    return (run_selector,)


@app.cell
def _(mo, run_selector):
    selected_run = run_selector.value

    mo.md(f"""
    # 🌤️ Weather Forecast - Operational View
    ## Port of Brownsville, TX

    {run_selector}

    **Viewing forecast made at**: {selected_run}
    """)
    return (selected_run,)


@app.cell(hide_code=True)
def _(conn, selected_run):
    forecast_times_df = conn.execute(f"""
        SELECT DISTINCT forecast_timestamp_utc
        FROM default__dev.silver_weather
        WHERE observation_timestamp_utc = '{selected_run}'
        ORDER BY forecast_timestamp_utc
    """).df()
    forecast_times = forecast_times_df["forecast_timestamp_utc"].tolist()
    return (forecast_times,)


@app.cell(hide_code=True)
def _(forecast_times, mo):
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


@app.cell(hide_code=True)
def _(forecast_times, mo, time_slider):
    selected_time = forecast_times[time_slider.value]

    mo.md(f"""
    ### 🕐 Time Slider

    {time_slider}

    **Currently viewing**: {selected_time}
    """)
    return (selected_time,)


@app.cell(hide_code=True)
def _(conn, selected_run, selected_time):
    weather_arrow = conn.execute(f"""
        SELECT
            h3_latlng_to_cell(l.lat, l.lon, 9) AS hex_id,
            bw.temperature_celsius AS temperature,
            bw.wind_speed_mps AS wind_speed,
            l.lat,
            l.lon
        FROM default__dev.silver_weather bw
        JOIN weather_data.locations l ON l.id = bw.location_id
        WHERE bw.observation_timestamp_utc = '{selected_run}'
          AND bw.forecast_timestamp_utc = '{selected_time}'
    """).fetch_arrow_table()
    return (weather_arrow,)


@app.cell(hide_code=True)
def _(Normalize, apply_continuous_cmap, plt):
    def generate_colors(table):
        temp_values = table["temperature"].to_numpy()
        min_temp, max_temp = -20, 50
        normalizer = Normalize(vmin=min_temp, vmax=max_temp, clip=True)
        normalized = normalizer(temp_values)
        cmap = plt.get_cmap("RdYlBu_r")
        return apply_continuous_cmap(normalized, cmap)
    return (generate_colors,)


@app.cell(hide_code=True)
def _(H3HexagonLayer, Map, generate_colors, mo, weather_arrow):
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

    # State to track clicked hex_id - initialize with first hex
    first_hex_id = weather_arrow["hex_id"][0].as_py()

    get_clicked_hex, set_clicked_hex = mo.state(first_hex_id)

    map_widget = Map(
        layers=[layer],
        view_state={
            "latitude": weather_arrow["lat"].to_numpy().mean(),
            "longitude": weather_arrow["lon"].to_numpy().mean(),
            "zoom": 11,
            "pitch": 45,
        },
        height=600,
    )

    # Handle map clicks - update state when user clicks on a hexagon
    def on_hex_click(coordinate):
        # Find the hex_id at this coordinate
        lon, lat = coordinate

        # Calculate distances and find nearest hex
        lats = weather_arrow["lat"].to_numpy()
        lons = weather_arrow["lon"].to_numpy()
        distances = ((lats - lat) ** 2 + (lons - lon) ** 2) ** 0.5
        nearest_idx = distances.argmin()
        new_hex = weather_arrow["hex_id"][nearest_idx].as_py()

        set_clicked_hex(new_hex)

    # Register click handler
    map_widget.on_click(on_hex_click)
    return get_clicked_hex, map_widget


@app.cell
def _(map_widget):
    # Display map
    map_widget
    return


@app.cell(hide_code=True)
def _(mo, weather_arrow):
    temp_values = weather_arrow["temperature"].to_numpy()
    wind_values = weather_arrow["wind_speed"].to_numpy()

    mo.md(f"""
    ### 📊 Map Statistics

    - **Locations**: {len(weather_arrow)} hexagons
    - **Temperature**: {temp_values.min():.1f}°C - {temp_values.max():.1f}°C (Avg: {temp_values.mean():.1f}°C)
    - **Wind Speed**: {wind_values.min():.1f} - {wind_values.max():.1f} m/s (Avg: {wind_values.mean():.1f} m/s)

    **Color Legend**: 🔵 Blue = Cold → 🔴 Red = Hot

    **💡 Tip**: Click on a hexagon to see detailed time series below!
    """)
    return


@app.cell(hide_code=True)
def _(conn, get_clicked_hex, mo, selected_run):
    # Get clicked location hex_id from state
    selected_hex = get_clicked_hex()

    mo.md(f"""
    ### 📈 Drill-Down Time Series
    **Selected Hexagon**: `{selected_hex}`

    Showing hourly forecast from -24h to +5 days for this location

    💡 **Click on any hexagon on the map above to update this view**
    """)

    # Fetch time series data for clicked location
    timeseries_df = conn.execute(f"""
        SELECT
            bw.forecast_timestamp_utc,
            bw.observation_timestamp_utc,
            bw.temperature_celsius,
            bw.feels_like_celsius,
            bw.wind_speed_mps,
            bw.wind_direction_degrees,
            bw.humidity_percent,
            bw.cloud_cover_percent,
            l.name as location_name,
            l.lat,
            l.lon,
            CASE
                WHEN bw.forecast_timestamp_utc <= '{selected_run}' THEN 'Historical'
                ELSE 'Forecast'
            END as data_type
        FROM default__dev.silver_weather bw
        JOIN weather_data.locations l ON l.id = bw.location_id
        WHERE bw.observation_timestamp_utc = '{selected_run}'
          AND h3_latlng_to_cell(l.lat, l.lon, 9) = '{selected_hex}'
        ORDER BY bw.forecast_timestamp_utc
    """).df()
    return (timeseries_df,)


@app.cell(hide_code=True)
def _(alt, mo, pd, timeseries_df):
    # Create temperature line chart
    if len(timeseries_df) == 0:
        mo.md("**No data available for selected location**")
    else:
        # Prepare data
        df = timeseries_df.copy()
        df["forecast_timestamp_utc"] = pd.to_datetime(df["forecast_timestamp_utc"])

        # Temperature Line Chart
        temp_chart = (
            alt.Chart(df)
            .mark_line(point=True, strokeWidth=2)
            .encode(
                x=alt.X(
                    "forecast_timestamp_utc:T",
                    title="Forecast Time",
                    axis=alt.Axis(format="%m/%d %H:%M", labelAngle=-45),
                ),
                y=alt.Y(
                    "temperature_celsius:Q",
                    title="Temperature (°C)",
                    scale=alt.Scale(
                        domain=[
                            df["temperature_celsius"].min() - 2,
                            df["temperature_celsius"].max() + 2,
                        ]
                    ),
                ),
                color=alt.Color(
                    "data_type:N",
                    scale=alt.Scale(
                        domain=["Historical", "Forecast"], range=["#1f77b4", "#ff7f0e"]
                    ),
                    legend=alt.Legend(title="Data Type"),
                ),
                tooltip=[
                    alt.Tooltip(
                        "forecast_timestamp_utc:T",
                        title="Time",
                        format="%Y-%m-%d %H:%M",
                    ),
                    alt.Tooltip(
                        "temperature_celsius:Q", title="Temperature", format=".1f"
                    ),
                    alt.Tooltip(
                        "feels_like_celsius:Q", title="Feels Like", format=".1f"
                    ),
                    alt.Tooltip("data_type:N", title="Type"),
                    alt.Tooltip("location_name:N", title="Location"),
                ],
            )
            .properties(
                width=800, height=300, title="Temperature Forecast (-24h to +5 Days)"
            )
        )

        # Add "Now" line
        now_line = (
            alt.Chart(
                pd.DataFrame(
                    {"time": [pd.to_datetime(df["observation_timestamp_utc"].iloc[0])]}
                )
            )
            .mark_rule(strokeDash=[5, 5], color="red", strokeWidth=2)
            .encode(x="time:T")
        )

        temp_chart_final = (
            (temp_chart + now_line)
            .configure_axis(labelFontSize=12, titleFontSize=14)
            .configure_title(fontSize=16)
        )
    return df, temp_chart_final


@app.cell(hide_code=True)
def _(alt, df):
    # Wind Speed Line Chart
    wind_chart = (
        alt.Chart(df)
        .mark_area(
            line={"color": "#2ca02c", "strokeWidth": 2},
            color=alt.Gradient(
                gradient="linear",
                stops=[
                    alt.GradientStop(color="#d4f1d4", offset=0),
                    alt.GradientStop(color="#2ca02c", offset=1),
                ],
                x1=0,
                x2=0,
                y1=1,
                y2=0,
            ),
        )
        .encode(
            x=alt.X(
                "forecast_timestamp_utc:T",
                title="Forecast Time",
                axis=alt.Axis(format="%m/%d %H:%M", labelAngle=-45),
            ),
            y=alt.Y("wind_speed_mps:Q", title="Wind Speed (m/s)"),
            tooltip=[
                alt.Tooltip(
                    "forecast_timestamp_utc:T", title="Time", format="%Y-%m-%d %H:%M"
                ),
                alt.Tooltip("wind_speed_mps:Q", title="Wind Speed (m/s)", format=".1f"),
                alt.Tooltip(
                    "wind_direction_degrees:Q", title="Direction (°)", format=".0f"
                ),
            ],
        )
        .properties(width=380, height=250, title="Wind Speed Forecast")
    )

    combined_chart = (
        alt.hconcat(wind_chart)
        .configure_axis(labelFontSize=11, titleFontSize=13)
        .configure_title(fontSize=14)
    )
    return (combined_chart,)


@app.cell
def _(mo, temp_chart_final):
    mo.md("#### 🌡️ Temperature Time Series")
    temp_chart_final
    return


@app.cell
def _(combined_chart, mo):
    mo.md("#### 💨 Wind")
    combined_chart
    return


@app.cell
def _(df, mo):
    # Additional statistics table
    location_name = df["location_name"].iloc[0] if len(df) > 0 else "Unknown"
    lat = df["lat"].iloc[0] if len(df) > 0 else 0
    lon = df["lon"].iloc[0] if len(df) > 0 else 0

    historical_df = df[df["data_type"] == "Historical"]
    forecast_df = df[df["data_type"] == "Forecast"]

    mo.md(f"""
    ### 📍 Location Details

    - **Name**: {location_name}
    - **Coordinates**: ({lat:.4f}, {lon:.4f})
    - **Data Points**: {len(historical_df)} historical, {len(forecast_df)} forecast
    - **Temperature Range**: {df["temperature_celsius"].min():.1f}°C to {df["temperature_celsius"].max():.1f}°C
    - **Max Wind Speed**: {df["wind_speed_mps"].max():.1f} m/s
    """)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
