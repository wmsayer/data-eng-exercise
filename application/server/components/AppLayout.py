from components.AppNavbar import build_app_navbar
from components.AppDash import build_app_dash
from dash import html, dcc

LOGO_IMG = "/assets/bgdx_logo.png"
BKGRND_IMG = "/assets/circuit_background.jpg"


def build_app_layout(refresh_rate):

    app_layout = html.Div(id='app-parent',
                          children=[
                              build_app_navbar(LOGO_IMG),
                              html.Div(id='live-price-feed'),
                              build_app_dash(BKGRND_IMG),
                              dcc.Interval(id='interval-component', interval=refresh_rate * 1000)  # in milliseconds
                          ])

    return app_layout
