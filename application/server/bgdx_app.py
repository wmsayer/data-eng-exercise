import scripts.SnowflakeAPI as snwflk
import pandas as pd
import dash
from dash import html, dcc
from dash.dependencies import Input, Output
from dash import dash_table
import dash_bootstrap_components as dbc
import plotly.graph_objects as go
import plotly.express as px
from flask import Flask


if __name__ == '__main__':
    SNWFLK_PROF = "chip"
else:
    SNWFLK_PROF = "server"


SNWFLK_DB = "flipside"
SNWFLK_SCHEMA = "dbt_wsayer2"
SNWFLK_API = snwflk.SnowflakeAPI(db=SNWFLK_DB, schema=SNWFLK_SCHEMA, profile=SNWFLK_PROF)

server = Flask(__name__)
app = dash.Dash(name=__name__, server=server)


def get_btc_price_data():
    query = """
    SELECT *
    FROM flipside.coingecko.historical_prices
    """
    result_df = SNWFLK_API.run_get_query(query)
    result_df['TIME'] = pd.to_datetime(result_df['TIME'])
    return result_df


# df = px.data.stocks()
df = get_btc_price_data()

app.layout = html.Div(id='parent', children=[
   html.H1(id='H1', children='Styling using good components YAY', style={'textAlign': 'center', \
                                                                     'marginTop': 40, 'marginBottom': 40}),

   dcc.Dropdown(id='dropdown',
                options=[
                   {'label': 'Price', 'value': 'PRICES'},
                   {'label': 'Market Cap', 'value': 'MARKET_CAPS'},
                   {'label': 'Volume', 'value': 'TOTAL_VOLUMES'},
                ],
                value='PRICES'),
   dcc.Graph(id='bar_plot')
])


@app.callback(Output(component_id='bar_plot', component_property='figure'),
              [Input(component_id='dropdown', component_property='value')])
def graph_update(dropdown_value):
   print(dropdown_value)
   fig = go.Figure([go.Scatter(x=df['TIME'], y=df['{}'.format(dropdown_value)], \
                               line=dict(color='firebrick', width=4))
                    ])

   fig.update_layout(title='Stock prices over time',
                     xaxis_title='Dates',
                     yaxis_title='Prices'
                     )
   return fig

# app.layout = dbc.Container([
#     dbc.Label('Click a cell in the table:'),
#     dash_table.DataTable(df.to_dict('records'),[{"name": i, "id": i} for i in df.columns], id='tbl'),
#     dbc.Alert(id='tbl_out'),
# ])
#
# @app.callback(Output('tbl_out', 'children'), Input('tbl', 'active_cell'))
# def update_graphs(active_cell):
#     return str(active_cell) if active_cell else "Click the table"


if __name__ == '__main__':
   app.run()





