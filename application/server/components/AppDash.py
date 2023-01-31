from dash import html, dcc, dash_table
import dash_bootstrap_components as dbc


def build_top_assets_metrics_graph():
    graph_layout = html.Div(children=[html.Center(html.Div(children=dbc.Row([
                                        dbc.Col(html.Div(["Plot Metric:",
                                                          dcc.Dropdown(id='plt_metric_dropdown',
                                                                       options=[
                                                                           {'label': 'Price', 'value': 'Price'},
                                                                           {'label': 'Market Cap', 'value': 'Market Cap'},
                                                                           {'label': 'Volume', 'value': 'Volume'},
                                                                           {'label': 'Market Cap Rank', 'value': 'Market Cap Rank'},
                                                                           {'label': 'Trending Score', 'value': 'Trending Score'},
                                                                           {'label': 'Circulating Supply', 'value': 'Circulating Supply'},
                                                                       ],
                                                                       value='Trending Score', style={'width': '100%'})],
                                                         style={'textAlign': 'center',
                                                                'backgroundColor': 'rgba(255,255,255,0.8)'}
                                                         )),
                                        dbc.Col(html.Div(["Top X Assets:",
                                                          dcc.Dropdown(id='num_assets_dropdown',
                                                                       options=[
                                                                           {'label': '1', 'value': 1},
                                                                           {'label': '3', 'value': 3},
                                                                           {'label': '5', 'value': 5},
                                                                           {'label': '10', 'value': 10}
                                                                       ],
                                                                       value=5, style={'width': '100%'})],
                                                         style={'textAlign': 'center',
                                                                'backgroundColor': 'rgba(255,255,255,0.8)'}
                                                         )),
                                    ]), style={'width': '95%'})),
        html.Center(dcc.Graph(id='bar_plot', style={'width': '95%'}))])

    return graph_layout


def build_asset_grid_plot():
    grid_layout = html.Div(children=[html.Center(html.Div(children=dbc.Row([
                                        dbc.Col(html.Div(["Plot Asset:",
                                                          dcc.Dropdown(id='dropdown-assets',
                                                                       options=[
                                                                           {'label': 'CANTO', 'value': 'CANTO'}],
                                                                       value='CANTO', style={'width': '100%'})],
                                                         style={'textAlign': 'center',
                                                                'backgroundColor': 'rgba(255,255,255,0.8)'}
                                                         )),
                                        dbc.Col(html.Div(["Metric Lead:",
                                                          dcc.Dropdown(id='metric-lead',
                                                                       options=[{'label': '1hr', 'value': 1},
                                                                                {'label': '3hr', 'value': 3},
                                                                                {'label': '6hr', 'value': 6},
                                                                                {'label': '12hr', 'value': 12}],
                                                                       value=1, style={'width': '100%'})],
                                                         style={'textAlign': 'center',
                                                                'backgroundColor': 'rgba(255,255,255,0.8)'}
                                                         )),
                                    ]), style={'width': '95%'})),
        html.Center(dcc.Graph(id='cg_trending_grid', style={'width': '95%'}))])

    return grid_layout


def build_app_dash(bkgrnd_img):
    dashboard_layout = html.Div(id='graph-parent',
                                style={'background-image': f'url("{bkgrnd_img}")',
                                       # 'verticalAlign': 'middle',
                                       # 'background-size': '100%',
                                       # 'background-repeat': 'no-repeat',
                                       },
                                children=[
                                    # html.Div(id='live-price-feed'),
                                    html.Div(children=html.H1(id='H1', children='CoinGecko Trending Coins',
                                                              style={'textAlign': 'center',
                                                                     'backgroundColor': 'rgba(255,255,255,0.8)',
                                                                     'marginTop': 0, 'marginBottom': 20})
                                             ),
                                    html.Center(html.Div(style={'width': '90%'}, children=dbc.Row([
                                        dbc.Col(html.Div(build_top_assets_metrics_graph())),
                                        dbc.Col(html.Div(build_asset_grid_plot()))
                                        ]))),
                                    html.Center(html.Div(style={'width': '90%', 'marginTop': 20}, children=dbc.Row([
                                        html.Center(html.Div(html.H2(children='CoinGecko Trending Coins --- Aggregate Stats',
                                                             style={'textAlign': 'center',
                                                                    'backgroundColor': 'rgba(255,255,255,0.8)',
                                                                    'marginTop': 0, 'marginBottom': 10}
                                                             ))),
                                        html.Center(html.Div([
                                            html.Div("Time Interval:",
                                                     style={'textAlign': 'center'}),
                                            html.Center(dcc.Dropdown(id='table-interval',
                                                           options=[{'label': '1hr', 'value': '1Hr'},
                                                                    {'label': '6hr', 'value': '6Hr'},
                                                                    {'label': '24hr', 'value': '24Hr'},
                                                                    {'label': '3d', 'value': '3D'},
                                                                    {'label': '7d', 'value': '7D'},
                                                                    {'label': '14d', 'value': '14D'},
                                                                    {'label': '28d', 'value': '28D'},
                                                                    ], value='24Hr',
                                                           style={'width': '50%'}))],
                                            style={'textAlign': 'center',
                                                   'backgroundColor': 'rgba(255,255,255,0.8)',
                                                   'marginBottom': 10,
                                                   'width': '30%'}
                                            )),
                                        html.Center(html.Div(dbc.Row([
                                            dbc.Col(html.Div(style={'width': '95%'},
                                                             children=dcc.Graph(id='cg_trnd_scatter_price'))),
                                            dbc.Col(html.Div(style={'width': '95%'},
                                                             children=dcc.Graph(id='cg_trnd_scatter_volume'))),
                                            dbc.Col(html.Div(style={'width': '95%'},
                                                             children=dcc.Graph(id='cg_trnd_scatter_price_vs_vol')))
                                        ]), style={'width': '100%'}))
                                    ]))),
                                    html.Center(html.Div(id='data_table',
                                                         style={'width': '90%', 'marginTop': 10},
                                                         children=dash_table.DataTable(data=[], columns=[])))
    ])

    return dashboard_layout
