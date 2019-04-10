#!/bin/python

from flask_api import FlaskAPI
from flask import request, jsonify, abort
from create_json import create_json
import json


app = FlaskAPI(__name__, instance_relative_config=True)
app.config["DEBUG"]=True

@app.route('/get_lineage', methods=['GET','POST'])
def get_lineage():
	if request.method == "POST":
		data = request.get_json(silent=True)
		feed = data['body']['data']['feedname']
		feed=feed.encode("utf-8")
		response = create_json(feed)
	return response

app.run(host='0.0.0.0',port=8092)
