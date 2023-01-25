from dash import html, dcc


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
                                                ],
                                                value='Price', style={'width': '50%'})),
                                    html.Center(dcc.Graph(id='bar_plot', style={'width': '50%'})),
                                    html.Center(dcc.Graph(id='cg_trending_plot', style={'width': '50%'}))
    ])

    return dashboard_layout
