import scripts.SnowflakeAPI as snwflk
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


SNWFLK_SCHEMA = "dbt_wsayer2"
SNWFLK_API = snwflk.SnowflakeAPI(schema=SNWFLK_SCHEMA)

server = Flask(__name__)
app = dash.Dash(name=__name__, server=server, external_stylesheets=[dbc.themes.BOOTSTRAP])


def get_btc_price_data():
    query = """
    SELECT *
    FROM flipside.coingecko.historical_prices
    """
    result_df = SNWFLK_API.run_get_query(query)
    result_df['TIME'] = pd.to_datetime(result_df['TIME'])
    return result_df


LOGO_IMG = "/assets/bgdx_logo.png"
BKGRND_IMG = "/assets/BGDXBathroom.png"
app.layout = build_app_layout(LOGO_IMG, BKGRND_IMG)


@app.callback(Output(component_id='bar_plot', component_property='figure'),
              [Input(component_id='dropdown', component_property='value')])
def graph_update(dropdown_value):
    df = get_btc_price_data()
    fig = go.Figure([go.Scatter(x=df['TIME'], y=df['{}'.format(dropdown_value)],
                                line=dict(color='firebrick', width=4))
                     ])

    fig.update_layout(title='Stock prices over time',
                      paper_bgcolor='rgba(0,0,0,0)',
                      xaxis_title='Dates',
                      yaxis_title='Prices'
                      )
    return fig




if __name__ == '__main__':
    app.run()





