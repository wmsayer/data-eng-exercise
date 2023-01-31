import numpy as np
import scripts.Admin as Admin
import scripts.SnowflakeAPI as snwflk
import app_data_io as app_data
from components.AppLayout import build_app_layout
import pandas as pd
import dash
from dash.dash_table.Format import Format, Scheme, Sign, Symbol
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
import pathlib
import time


if platform == "linux":
    DBT_ENV = "dbt_output_prod"
else:
    DBT_ENV = "dbt_output_dev"

PROJECT_ROOT = "%s" % pathlib.Path(__file__).parent.parent.parent.absolute()
SNWFLK_API = snwflk.SnowflakeAPI(schema=DBT_ENV, wh="APPLICATION")
APP_IO = app_data.AppDataIO(SNWFLK_API, DBT_ENV)
# APP_IO.get_trending_data()
# APP_IO.get_curr_trend_summ()
while APP_IO.trending_df.empty or APP_IO.curr_trend_summ_df.empty:
    print("sleepy")
    time.sleep(1)

APP_REFRESH_RATE = 60*5  # in seconds

server = Flask(__name__)
app = dash.Dash(name=__name__, server=server, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.layout = build_app_layout(APP_REFRESH_RATE)
app.title = "Big Dorks Only"

GRAPH_PPR_COLOR = 'rgba(255,255,255,0.8)'

APP_PLOT_CFG = pd.read_csv("/".join([PROJECT_ROOT, "seeds/plot_config.csv"]))
APP_PLOT_CFG["prefix"] = APP_PLOT_CFG["prefix"].fillna("")
APP_COLORS = px.colors.sequential.Plasma
APP_FORMATS = {
    "dollar_millions": dict(type='numeric',
                            format=Format(precision=3, scheme=Scheme.decimal).symbol(Symbol.yes).symbol_prefix('$').symbol_suffix('M')),
    "dollar_billions": dict(type='numeric',
                            format=Format(precision=3, scheme=Scheme.decimal).symbol(Symbol.yes).symbol_prefix('$').symbol_suffix('B')),
    "dollar": dict(type='numeric', format=Format(precision=4, scheme=Scheme.decimal, symbol=Symbol.yes)),
    "number_SI": dict(type='numeric', format=Format(precision=3, scheme=Scheme.decimal, symbol=Symbol.no)),
    "integer": dict(type='numeric', format=Format(scheme=Scheme.decimal_integer)),
    "percent_w_sign": dict(type='numeric', format=Format(precision=2, sign=Sign.positive, scheme=Scheme.percentage))
}


@app.callback(Output(component_id='cg_trnd_scatter_price_vs_vol', component_property='figure'),
              Input('table-interval', 'value'))
def update_scatter_grid_price_vs_vol(time_int):
    trend_col = f"Trending Score {time_int}"
    price_col = f"Price Pct Chg {time_int}"
    vol_col = f"Volume Pct Chg {time_int}"

    df = APP_IO.curr_trend_summ_df.copy().dropna(subset=trend_col)
    # df["Market Cap"] = np.log(df["Market Cap"].values)**4
    fig = px.scatter(df, x=vol_col, y=price_col, color="Name", text="Symbol",
                     size=trend_col)

    fig.update_xaxes(zeroline=True, zerolinewidth=2, zerolinecolor='black')
    fig.update_yaxes(zeroline=True, zerolinewidth=2, zerolinecolor='black')

    fig.update_layout(title=f'<b>Price % Chg vs 24hr Volume % Chg --- {time_int}</b> <br><sup>(size >> Trend Score)</sup>',
                      paper_bgcolor=GRAPH_PPR_COLOR,
                      xaxis_title=f'<b>{vol_col}</b>',
                      xaxis_ticksuffix="%",
                      yaxis_title=f'<b>{price_col}</b>',
                      yaxis_ticksuffix="%",
                      showlegend=False
                      )
    return fig


@app.callback(Output(component_id='cg_trnd_scatter_volume', component_property='figure'),
              Input('table-interval', 'value'))
def update_scatter_grid_volume(time_int):
    trend_col = f"Trending Score {time_int}"
    vol_col = f"Volume Pct Chg {time_int}"

    df = APP_IO.curr_trend_summ_df.copy()
    df["Market Cap"] = np.log(df["Market Cap"].values)**4
    fig = px.scatter(df, x=trend_col, y=vol_col, color="Name", text="Symbol",
                     size='Market Cap')

    fig.update_xaxes(zeroline=True, zerolinewidth=2, zerolinecolor='black')
    fig.update_yaxes(zeroline=True, zerolinewidth=2, zerolinecolor='black')

    fig.update_layout(title=f'<b>Trending Score vs 24hr Volume % Chg --- {time_int}</b> <br><sup>(size >> Market Cap)</sup>',
                      paper_bgcolor=GRAPH_PPR_COLOR,
                      xaxis_title=f'<b>{trend_col}</b>',
                      yaxis_title=f'<b>{vol_col}</b>',
                      yaxis_ticksuffix="%",
                      showlegend=False
                      )
    return fig


@app.callback(Output(component_id='cg_trnd_scatter_price', component_property='figure'),
              Input('table-interval', 'value'))
def update_scatter_grid_price(time_int):
    trend_col = f"Trending Score {time_int}"
    price_col = f"Price Pct Chg {time_int}"

    df = APP_IO.curr_trend_summ_df.copy()
    df["Market Cap"] = np.log(df["Market Cap"].values)**4
    fig = px.scatter(df, x=trend_col, y=price_col, color="Name", text="Symbol",
                     size='Market Cap')

    fig.update_xaxes(zeroline=True, zerolinewidth=2, zerolinecolor='black')
    fig.update_yaxes(zeroline=True, zerolinewidth=2, zerolinecolor='black')

    fig.update_layout(title=f'<b>Trending Score vs Price % Chg --- {time_int}</b> <br><sup>(size >> Market Cap)</sup>',
                      paper_bgcolor=GRAPH_PPR_COLOR,
                      xaxis_title=f'<b>{trend_col}</b>',
                      yaxis_title=f'<b>{price_col}</b>',
                      yaxis_ticksuffix="%",
                      showlegend=False
                      )
    return fig


@app.callback(Output(component_id='data_table', component_property='children'),
              Input('table-interval', 'value'))
def update_table(time_int):
    plot_cfg_df = APP_PLOT_CFG.loc[APP_PLOT_CFG["inc_table"], :].copy()
    inc_metrics_inst = plot_cfg_df.loc[np.logical_not(APP_PLOT_CFG["time_based"]), "metric"].tolist()
    inc_metrics_time = plot_cfg_df.loc[APP_PLOT_CFG["time_based"], "metric"].tolist()
    inc_metrics_time_ext = [m + " " + time_int for m in inc_metrics_time]
    id_cols = ["Name", "Symbol"]
    end_cols = ["Last Seen Trending"]
    og_cols = id_cols + inc_metrics_inst + inc_metrics_time + end_cols
    tbl_cols = id_cols + inc_metrics_inst + inc_metrics_time_ext + end_cols
    df = APP_IO.curr_trend_summ_df[tbl_cols].copy().sort_values(by=[f"Trending Score {time_int}"], ascending=False)

    col_fmts = []
    og_col_dict = dict(zip(tbl_cols, og_cols))
    plot_cfg_df.set_index(keys="metric", inplace=True)
    for c in tbl_cols:
        og_col = og_col_dict[c]
        temp_fmt = {"name": c, "id": c}

        if c not in id_cols + end_cols:
            fmt_type = plot_cfg_df.loc[og_col, "format_type"]
            temp_fmt.update(APP_FORMATS[fmt_type])
            if fmt_type in ["percent_w_sign"]:
                df[c] = df[c]/100
            if fmt_type in ["dollar_millions"]:
                df[c] = df[c]/10**6
            if fmt_type in ["dollar_billions"]:
                df[c] = df[c]/10**9
        col_fmts.append(temp_fmt)

    price_chg_col = "Price Pct Chg %s" % time_int
    vol_chg_col = "Volume Pct Chg %s" % time_int

    tbl = dash_table.DataTable(data=df.to_dict('records'),
                               columns=col_fmts,
                               # sort_mode="multi",
                               column_selectable="single",
                               # row_selectable="multi",
                               style_cell={'textAlign': 'center'},
                               style_header={
                                   'backgroundColor': 'black',
                                   'color': 'lightgrey',
                                   'fontWeight': 'bold'
                               },
                               style_data_conditional=[{'if': {'row_index': 'odd'},
                                                        'backgroundColor': 'rgb(220, 220, 220)'},
                                                       {'if': {'filter_query': '{%s} < 0' % price_chg_col,
                                                               'column_id': price_chg_col}, 'color': 'tomato'},
                                                       {'if': {'filter_query': '{%s} > 0' % price_chg_col,
                                                               'column_id': price_chg_col},'color': 'green'},
                                                       {'if': {'filter_query': '{%s} < 0' % vol_chg_col,
                                                               'column_id': vol_chg_col}, 'color': 'tomato'},
                                                       {'if': {'filter_query': '{%s} > 0' % vol_chg_col,
                                                               'column_id': vol_chg_col}, 'color': 'green'},
                                                       ],
                               sort_action='native')
    return tbl


@app.callback(Output(component_id='bar_plot', component_property='figure'),
              [Input(component_id='plt_metric_dropdown', component_property='value'),
               Input(component_id='num_assets_dropdown', component_property='value')])
def update_top_x_graph(dropdown_value, num_assets):
    df = APP_IO.trending_df.copy()
    top_x_assets = APP_IO.assets_by_trending_24hr[:num_assets]
    df = df.loc[top_x_assets, :]
    plt_metric = dropdown_value

    min_trnd_ts = df.dropna(subset=[dropdown_value])["Time"].min()
    temp_df = df.loc[df["Time"] >= min_trnd_ts, :].sort_values(by="Time")
    temp_df["Project"] = temp_df["Name"] + " (" + temp_df["Symbol"] + ")"
    # normalized to 100 for THIS window
    temp_df["Trending Score"] = 100 * temp_df["Trending Score"]/temp_df["Trending Score"].max()

    plot_opts = APP_PLOT_CFG.set_index(keys="metric").loc[dropdown_value, :]

    fig = px.line(temp_df, x="Time", y=plt_metric, color="Project", log_y=plot_opts["log_y"])

    update_ts = temp_df["Time"].max().strftime("%m/%d/%Y, %H:%M:%S")
    fig.update_layout(title=f'<b>Top {num_assets} Trending Coins on CoinGecko Search Engine</b> <br><sub><i>(updated {update_ts}, source: CoinGecko)</i></sub>',
                      paper_bgcolor=GRAPH_PPR_COLOR,
                      xaxis_title='<b>Date</b>',
                      yaxis_title=f'<b>{plt_metric}</b>',
                      yaxis_tickprefix=plot_opts["prefix"],
                      height=600
                      )

    if plot_opts["log_y"]:
        fig.update_layout(yaxis_dtick=1)

    if plot_opts["reversed"]:
        fig.update_layout(yaxis_autorange='reversed')

    return fig


@app.callback(Output(component_id='cg_trending_grid', component_property='figure'),
              [Input(component_id='dropdown-assets', component_property='value'),
               Input(component_id='metric-lead', component_property='value')])
def graph_trending_asset_grid(plt_id, mtx_lead):
    df = APP_IO.trending_df
    asset_lab = APP_IO.trending_projects_df.loc[plt_id, "Name"]
    plot_cfg_df = APP_PLOT_CFG.loc[APP_PLOT_CFG["inc_asset_grid"], :].reset_index(drop=True)
    num_cols = 2
    fig = make_subplots(rows=math.ceil(plot_cfg_df.shape[0]/num_cols), cols=num_cols)

    if plt_id != "None":
        temp_df = df.loc[plt_id, :]
        min_trnd_ts = temp_df.dropna(subset=["Trending Score"])["Time"].min() - dt.timedelta(hours=mtx_lead)
        temp_df = temp_df.loc[temp_df["Time"] >= min_trnd_ts, :].sort_values(by="Time")

        for i, row in plot_cfg_df.iterrows():
            temp_metric = row["metric"]
            fig.add_trace(
                go.Scatter(x=temp_df["Time"], y=temp_df[temp_metric], name=temp_metric),
                row=math.ceil((i+1)/2), col=i%2+1
            )
            fig['layout'][f'yaxis{i+1}'].update(title_text=f'<b>{temp_metric}</b>')

            # if temp_metric == "Market Cap Rank":
            if row["reversed"]:
                fig['layout'][f'yaxis{i + 1}'].update(autorange='reversed')

        fig.update_layout(
            height=600,
            title_text=f"<b>Metric Grid Plot for Project: {asset_lab}</b>",
            paper_bgcolor=GRAPH_PPR_COLOR,
            xaxis_tickformat="%H:%M\n%b %d"
        )

    return fig


@app.callback([Output('live-price-feed', 'children'), Output('dropdown-assets', 'options'), Output('dropdown-assets', 'value')],
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
    span_list.append(html.Span(f" (last updated {update_ts}, source: Cryptowatch)",
                               style=base_style | {'fontStyle': 'italic'}))

    available_assets = APP_IO.get_available_asset_opts()
    def_val = available_assets[0]['value'] if available_assets else ""

    return span_list, available_assets, def_val


if __name__ == '__main__':
    app.run(debug=True)





