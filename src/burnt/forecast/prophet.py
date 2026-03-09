"""Prophet-based cost forecasting.

This module provides time-series forecasting for DBU costs using Facebook Prophet.

Requires the 'prophet' extra to be installed:
    uv sync --extra prophet
    # or
    pip install burnt[prophet]

Note: This is a post-MVP feature. The forecast accuracy depends on having
sufficient historical data (minimum 30 days recommended).
"""

from burnt._compat import require


def forecast_costs(usage_df, periods: int = 30, freq: str = "D"):
    require("prophet")
    from prophet import Prophet

    df = usage_df.rename(columns={"usage_date": "ds", "total_cost": "y"})

    model = Prophet(
        daily_seasonality=True,
        weekly_seasonality=True,
        yearly_seasonality=True,
    )
    model.fit(df)

    future = model.make_future_dataframe(periods=periods, freq=freq)
    forecast = model.predict(future)

    return forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]]
