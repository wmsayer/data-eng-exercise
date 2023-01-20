# from urllib import parse
from flask import Flask, request, render_template, Response
# from flask_cors import CORS
# from flask_sqlalchemy import SQLAlchemy
# import json
# from flask import jsonify

# if __name__ == "__main__":
#     from MovieTraitNetwork import *
#     from SQLAdmin import *
#     from handle import *
# else:
#     from application.server.MovieTraitNetwork import *
#     from application.server.SQLAdmin import *
#     from application.server.handle import *

app = Flask(__name__, static_folder="../static/dist", template_folder="../static")
# CORS(app)
# app.config['SQLALCHEMY_DATABASE_URI'] = 'mysql://cs411ccsquad_admin:password;uiuc@localhost/cs411ccsquad_FlicksNDrinks'
# db = SQLAlchemy(app)
# eng = db.engine


# ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
# page routes
@app.route("/")
def home():
    return '<h1>Hello, World!</h1>'


@app.route("/yall")
def hey_yall():
    return '<h1>Hello, Y\'all!</h1>'

# @app.route("/")
# def home():
#     return render_template("./pages/home.html")
#
#
# @app.route("/flicks_n_drinks")
# def flicks_n_drinks():
#     return render_template("./pages/loginPage.html")
#
#
# # ///////////////////////////////////////////////////////////////////////////////////////////////////////////////
# # Utility API routes
#
# @app.route('/MTNN/<json_uri>', methods=['GET'])
# def movie_trait_network(json_uri):
#     """
#     if 'tConst' empty, returns compatibilities for top 5 most compatible genres
#     if 'tConst' non-empty, calculates personalized ratings for movies in 'tConst'
#     :param json_uri: {'userId':[int, ...], 'tConst': [int, int, ...]}
#     :return:
#     """
#     if request.method == 'GET':
#         with eng.connect() as conn:
#             json_dict = json.loads(parse.unquote(json_uri))
#             result_df = handle_mtnn_api(json_dict, mt_model, conn)
#         return Response(result_df.to_json(orient="records"), mimetype='application/json')
#
#
# @app.route('/delete/<table>/<item_id>', methods=['GET'])
# def delete(table, item_id):
#     if request.method == 'GET':
#         conn = eng.connect()
#
#         if table == 'Movie':
#             sel_query = 'SELECT * FROM Movie WHERE tConst = %s' % item_id
#             result = query_data(sel_query, conn, 'json')
#             del_query = 'DELETE FROM Movie WHERE tConst = %s' % item_id
#         else:
#             sel_query = 'SELECT * FROM CocktailRecipe WHERE recipeId = %s' % item_id
#             result = query_data(sel_query, conn, 'json')
#             del_query = 'DELETE FROM CocktailRecipe WHERE recipeId = %s' % item_id
#
#         conn.execute(del_query)
#         return result
#
#
# @app.route('/add/<table>/<new_input>', methods=['GET'])
# def insert(table, new_input):
#     conn = eng.connect()
#     json_dict = json.loads(parse.unquote(new_input))
#
#     if table == "Cocktail":
#         if "userId" in json_dict.keys():
#             json_dict.pop("userId")
#         if "recipeId" in json_dict.keys():
#             json_dict.pop("recipeId")
#         handle_recipe_action(json_dict, conn, "insert")
#
#     if table == "Movie":
#         handle_add_movie(json_dict, conn, "insert")
#
#     else:
#         query = build_insert_query(table, json_dict)
#         conn.execute(query)
#
#     response = {'status': 'success', 'message': 'Record added successfully'}
#     return jsonify(response)
#
#
# @app.route('/edit/<table>/<item_id>/<title>', methods=['GET'])
# def edit_user(table, item_id, title):
#     conn = eng.connect()
#
#     if table == "User":
#         trs = title.split(':')
#         query = "UPDATE User SET trOpen = '%s',trCon = '%s',trex = '%s',trAg = '%s',trNe = '%s' WHERE (userId = %s)" % (trs[0],trs[1],trs[2],trs[3],trs[4], item_id)
#         conn.execute(query)
#
#     response = {'status': 'success', 'message': 'Product edit successfully'}
#     return response


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80)
