from dash import html, dcc
import dash_bootstrap_components as dbc


def build_app_dash(refresh_rate, bkgrnd_img):
    dashboard_layout = html.Div(id='graph-parent',
                                style={'background-image': f'url("{bkgrnd_img}")',
                                       # 'verticalAlign': 'middle',
                                       # 'background-size': '100%',
                                       # 'background-repeat': 'no-repeat',
                                       },
                                children=[
                                    # html.Div(id='live-price-feed'),
                                    html.H1(id='H1', children='Crypto Data Dashboard',
                                            style={'textAlign': 'center', 'marginTop': 0, 'marginBottom': 40}),

                                    html.Center(dcc.Dropdown(id='dropdown',
                                                options=[
                                                   {'label': 'Price', 'value': 'Price'},
                                                   {'label': 'Market Cap', 'value': 'Market Cap'},
                                                   {'label': 'Volume', 'value': 'Volume'},
                                                   {'label': 'Market Cap Rank', 'value': 'Market Cap Rank'},
                                                   {'label': 'Trending Score', 'value': 'Trending Score'},
                                                   {'label': 'Circulating Supply', 'value': 'Circulating Supply'},
                                                ],
                                                value='Trending Score', style={'width': '50%'})),
                                    html.Center(dcc.Graph(id='bar_plot', style={'width': '50%'})),
                                    # html.Center(dcc.Graph(id='cg_trending_plot', style={'width': '50%'})),
                                    html.Center(html.Div(dbc.Row([
                                        dbc.Col(dcc.Dropdown(id='dropdown-assets',
                                                             options=[{'label': 'Choose Asset', 'value': 'None'}],
                                                             value='None')),
                                        dbc.Col(dcc.Dropdown(id='metric-lead',
                                                             options=[{'label': '1hr', 'value': 1},
                                                                      {'label': '3hr', 'value': 3},
                                                                      {'label': '6hr', 'value': 6},
                                                                      {'label': '12hr', 'value': 12}],
                                                             value=1))
                                        ]), style={'width': '50%'})),
                                    html.Center(dcc.Graph(id='cg_trending_grid', style={'width': '50%'}))

    ])

    return dashboard_layout
