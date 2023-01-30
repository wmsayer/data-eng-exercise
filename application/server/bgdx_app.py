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
from plotly.subplots import make_subplots
import plotly.express as px
from flask import Flask
from sys import platform
import math
import datetime as dt


if platform == "linux":
    DBT_ENV = "dbt_output_prod"
else:
    DBT_ENV = "dbt_output_dev"

SNWFLK_API = snwflk.SnowflakeAPI(schema=DBT_ENV, wh="APPLICATION")
APP_IO = app_data.AppDataIO(SNWFLK_API, DBT_ENV)
APP_IO.get_trending_data()

APP_REFRESH_RATE = 60*5  # in seconds

server = Flask(__name__)
app = dash.Dash(name=__name__, server=server, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.layout = build_app_layout(APP_REFRESH_RATE)
app.title = "Big Dorks Only"

GRAPH_PPR_COLOR = 'rgba(255,255,255,0.8)'

APP_METRICS = [
    'Trending Score',
    'Price',
    # 'Market Cap',
    'Volume',
    'Market Cap Rank',
    # 'Circulating Supply'
    ]
APP_COLORS = px.colors.sequential.Plasma


@app.callback(Output(component_id='bar_plot', component_property='figure'),
              [Input(component_id='dropdown', component_property='value')])
def graph_update(dropdown_value):
    df = APP_IO.trending_df
    plt_metric = dropdown_value

    min_trnd_ts = df.dropna(subset=[dropdown_value])["Time"].min()
    temp_df = df.loc[df["Time"] >= min_trnd_ts, :].sort_values(by="Time")

    fig = px.line(temp_df, x="Time", y=plt_metric, color="Name", log_y=True)


    update_ts = temp_df["Time"].max().strftime("%m/%d/%Y, %H:%M:%S")
    fig.update_layout(title=f'<b>Trending Coins on CoinGecko Search Engine</b> <i>(updated {update_ts}, source: CoinGecko)</i>',
                      paper_bgcolor=GRAPH_PPR_COLOR,
                      xaxis_title='<b>Date</b>',
                      yaxis_title=f'<b>{plt_metric}</b>',
                      yaxis_tickprefix="",
                      yaxis_dtick=1
                      )
    return fig


# @app.callback(Output(component_id='cg_trending_plot', component_property='figure'),
#               [Input(component_id='dropdown', component_property='value')])
# def graph_trending_update(plt_metric):
#     df = APP_IO.trending_df
#
#     # fig = make_subplots(rows=1, cols=2)
#     cg_ids = list(set(list(df.index)))
#     colors = px.colors.sequential.Plasma
#     plt_id = cg_ids[0]
#
#     prim_axis_label = "Trending Score"
#
#     temp_df = df.loc[plt_id, :]
#     min_trnd_ts = temp_df.dropna(subset=[prim_axis_label])["Time"].min()
#     temp_df = temp_df.loc[temp_df["Time"] >= min_trnd_ts, :].sort_values(by="Time")
#
#     # fig = px.line(temp_df, x="Time", y=plt_metric, color="Name", log_y=True)
#     fig = make_subplots(specs=[[{"secondary_y": True}]])
#     fig.add_trace(
#         go.Scatter(x=temp_df["Time"], y=temp_df[prim_axis_label], name=prim_axis_label),
#         secondary_y=False,
#     )
#     fig.add_trace(
#         go.Scatter(x=temp_df["Time"], y=temp_df[plt_metric], name=plt_metric),
#         secondary_y=True,
#     )
#
#     update_ts = df["Time"].max().strftime("%m/%d/%Y, %H:%M:%S")
#     # Add figure title
#     fig.update_layout(
#         title_text=f'<b>{prim_axis_label} vs {plt_metric} for {df.loc[plt_id, "Name"].values[0]}</b> <i>(updated {update_ts}, source: CoinGecko)</i>'
#     )
#
#     # Set x-axis title
#     fig.update_xaxes(title_text="<b>Date</b>")
#
#     # Set y-axes titles
#     fig.update_yaxes(title_text=f"<b>{prim_axis_label}</b>", secondary_y=False)
#     fig.update_yaxes(title_text=f'<b>{plt_metric}</b>', secondary_y=True)
#     fig.update_layout(paper_bgcolor=GRAPH_PPR_COLOR,
#                       xaxis_tickformat="%H:%M\n%b %d")
#
#     return fig


@app.callback(Output(component_id='cg_trending_grid', component_property='figure'),
              [Input(component_id='dropdown-assets', component_property='value'),
               Input(component_id='metric-lead', component_property='value')])
def graph_trending_asset_grid(plt_id, mtx_lead):
    df = APP_IO.trending_df
    num_metrics = len(APP_METRICS)
    fig = make_subplots(rows=2, cols=2)

    if plt_id != "None":
        temp_df = df.loc[plt_id, :]
        min_trnd_ts = temp_df.dropna(subset=["Trending Score"])["Time"].min() - dt.timedelta(hours=mtx_lead)
        temp_df = temp_df.loc[temp_df["Time"] >= min_trnd_ts, :].sort_values(by="Time")

        for i in range(0, num_metrics):
            temp_metric = APP_METRICS[i]
            fig.add_trace(
                go.Scatter(x=temp_df["Time"], y=temp_df[temp_metric], name=temp_metric),
                row=math.ceil((i+1)/2), col=i%2+1
            )
            fig['layout'][f'yaxis{i+1}'].update(title_text=f'<b>{temp_metric}</b>')

            if temp_metric == "Market Cap Rank":
                fig['layout'][f'yaxis{i + 1}'].update(autorange='reversed')

        fig.update_layout(
            height=600,
            title_text=f"Side By Side Subplots for Project: {plt_id}",
            paper_bgcolor=GRAPH_PPR_COLOR,
            xaxis_tickformat="%H:%M\n%b %d"
        )

    return fig


@app.callback([Output('live-price-feed', 'children'), Output('dropdown-assets', 'options')],
              Input('interval-component', 'n_intervals'))
def update_spot_prices(n):
    price_df = APP_IO.get_spot_prices()
    price_df["ASSET"] = price_df["ASSET"].str.upper()

    base_style = {'padding': '7px', 'fontSize': '16px'}

    span_list = [html.Span("Live Crypto Prices: ", style=base_style | {'fontWeight': 'bold'})]

    for i, row in price_df.iterrows():
        spot_val_str = Admin.format_num_to_sig_figs(row["PRICE"])
        span_str = f"{row['ASSET']}: {spot_val_str}"
        span_list.append(html.Span(span_str, style=base_style | {'fontWeight': 'bold', 'color': '#018708'}))

    update_ts = price_df["TIME"].max().strftime("%m/%d/%Y, %H:%M:%S")
    span_list.append(html.Span(f" (refresh approx. every minute, last updated {update_ts}, source: Cryptowatch)",
                               style=base_style | {'fontStyle': 'italic'}))

    APP_IO.get_trending_data()

    return span_list, APP_IO.get_available_asset_opts()


if __name__ == '__main__':
    app.run()





