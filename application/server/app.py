from flask import Flask

# Create app
app = Flask(__name__)


# ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
# page routes
@app.route("/")
def home():
    return '<h1>Hello, World!</h1>'


# @app.route("/yall")
# def hey_yall():
#     return '<h1>Hello, Y\'all!</h1>'


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
