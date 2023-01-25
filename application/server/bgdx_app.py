import scripts.Admin as Admin
import scripts.SnowflakeAPI as snwflk
import app_data_io as app_data
from components.AppLayout import build_app_layout
import pandas as pd
import dash
from dash import html, dcc
from dash.dependencies import Input, Output
from dash import dash_table
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px
from flask import Flask
import datetime as dt
import pytz


SNWFLK_API = snwflk.SnowflakeAPI(schema="dbt_output", wh="APPLICATION")
APP_REFRESH_RATE = 10  # in seconds

server = Flask(__name__)
app = dash.Dash(name=__name__, server=server, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.layout = build_app_layout(APP_REFRESH_RATE)
app.title = "Big Dorks Only"

GRAPH_PPR_COLOR = 'rgba(255,255,255,0.8)'


@app.callback(Output(component_id='bar_plot', component_property='figure'),
              [Input(component_id='dropdown', component_property='value')])
def graph_update(dropdown_value):
    df = app_data.get_btc_price_data(SNWFLK_API)
    fig = px.line(df, x="TIME", y='{}'.format(dropdown_value), color="ASSET", log_y=True
                  # hover_data=['SYMBOL', 'MARKET_CAP_RANK']
                  )
    fig.update_layout(title='<b>BTC Price</b>',
                      paper_bgcolor=GRAPH_PPR_COLOR,
                      xaxis_title='<b>Date</b>',
                      yaxis_title='<b>Price</b>'
                      )
    return fig


@app.callback(Output(component_id='cg_trending_plot', component_property='figure'),
              Input('interval-component', 'n_intervals'))
def graph_trending_update(n):
    df = app_data.get_trending_data(SNWFLK_API)
    fig = px.line(df, x="TIME", y="TRENDING_SCORE", color="NAME",
                  hover_data=['SYMBOL', 'MARKET_CAP_RANK'])

    update_ts = dt.datetime.now(pytz.utc).strftime("%m/%d/%Y, %H:%M:%S")
    fig.update_layout(title=f'<b>Trending Coins on CoinGecko</b> (updated {update_ts})',
                      paper_bgcolor=GRAPH_PPR_COLOR,
                      xaxis_title='<b>Date</b>',
                      yaxis_title='<b>Trending Score</b>'
                      )
    return fig


@app.callback(Output('live-price-feed', 'children'),
              Input('interval-component', 'n_intervals'))
def update_spot_prices(n):
    price_df = app_data.get_spot_prices(SNWFLK_API)
    price_df["ASSET"] = price_df["ASSET"].str.upper()

    base_style = {'padding': '7px', 'fontSize': '16px'}

    span_list = [html.Span("Live Crypto Prices: ", style=base_style | {'fontWeight': 'bold'})]

    for i, row in price_df.iterrows():
        spot_val_str = Admin.format_num_to_sig_figs(row["PRICE"])
        span_str = f"{row['ASSET']}: {spot_val_str}"
        span_list.append(html.Span(span_str, style=base_style | {'fontWeight': 'bold', 'color': '#018708'}))

    update_ts = dt.datetime.now(pytz.utc).strftime("%m/%d/%Y, %H:%M:%S")
    span_list.append(html.Span(f" (refresh approx. every minute, last updated {update_ts}, source: Cryptowatch)",
                               style=base_style | {'fontStyle': 'italic'}))

    return span_list


if __name__ == '__main__':
    app.run()





