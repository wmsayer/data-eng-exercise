from dash import html
from components.AppNavbar import build_app_navbar
from components.AppDash import build_app_dash


def build_app_layout(logo_img, bkgrnd_img):
    layout_children = [build_app_navbar(logo_img), build_app_dash(bkgrnd_img)]
    return html.Div(id='parent', children=layout_children)
