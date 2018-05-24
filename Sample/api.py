"""Restful API."""

from __future__ import absolute_import

from Sample.core import OpenWeatherMap
from flask import Flask, request, jsonify


app = Flask(__name__)
client = OpenWeatherMap()


@app.route('/weather/<string:city_name>', methods=['GET'])
def get(city_name):
    response = client.get(city_name,
                          country_code=request.args.get('country'),
                          unit=client.meta.get(request.args.get('unit')))
    return jsonify(response)


if __name__ == '__main__':
    app.run(threaded=True)
