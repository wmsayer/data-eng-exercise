from dash import html, dcc


def build_app_dash(bkgrnd_img):
    dashboard_layout = html.Div(id='graph-parent',
                                # style={'background-image': f'url("{bkgrnd_img}")',
                                #        'verticalAlign': 'middle',
                                #        'background-size': '100%',
                                #        'background-repeat': 'no-repeat',
                                #        },
                          children=[
                            html.Img(src=bkgrnd_img, width="100%"),
                            html.H1(id='H1', children='Bitcoin Price data',
                                    style={'textAlign': 'center', 'marginTop': 40, 'marginBottom': 40}),

                            dcc.Dropdown(id='dropdown',
                                         options=[
                                           {'label': 'Price', 'value': 'PRICES'},
                                           {'label': 'Market Cap', 'value': 'MARKET_CAPS'},
                                           {'label': 'Volume', 'value': 'TOTAL_VOLUMES'},
                                         ],
                                         value='PRICES'),
                            dcc.Graph(id='bar_plot')
    ])
    return dashboard_layout