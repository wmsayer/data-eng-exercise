from components.AppNavbar import build_app_navbar
from components.AppDash import build_app_dash
import dash_bootstrap_components as dbc
from dash import html, dcc

LOGO_IMG = "/assets/bgdx_logo.png"
BKGRND_IMG = "/assets/circuit_background.jpg"


def build_app_layout(refresh_rate):

    app_layout = html.Div(id='app-parent',
                          children=[
                              build_app_navbar(LOGO_IMG),
                              html.Div('The official home for crypto’s biggest data nerds.',
                                                  style={"fontStyle": "italic", 'textAlign': 'center'}),
                                         # dark=True, color="dark"),
                              html.Div(id='live-price-feed'),
                              build_app_dash(BKGRND_IMG),
                              dcc.Interval(id='interval-component', interval=refresh_rate * 1000)  # in milliseconds
                          ])

    return app_layout
