# =================================================================
#
# Authors: Tom Kralidis <tomkralidis@gmail.com>
#          Norman Barker <norman.barker@gmail.com>
#
# Copyright (c) 2022 Tom Kralidis
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation
# files (the "Software"), to deal in the Software without
# restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following
# conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
# OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
# WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# =================================================================

""" Flask module providing the route paths to the api"""

import os

import click

from flask import Flask, Blueprint, make_response, request, send_from_directory

from pygeoapi.api import API
from pygeoapi.util import get_mimetype, yaml_load

CONFIG = None

if 'PYGEOAPI_CONFIG' not in os.environ:
    raise RuntimeError('PYGEOAPI_CONFIG environment variable not set')

with open(os.environ.get('PYGEOAPI_CONFIG'), encoding='utf8') as fh:
    CONFIG = yaml_load(fh)

STATIC_FOLDER = 'static'
if 'templates' in CONFIG['server']:
    STATIC_FOLDER = CONFIG['server']['templates'].get('static', 'static')

APP = Flask(__name__, static_folder=STATIC_FOLDER, static_url_path='/static')
APP.url_map.strict_slashes = False

BLUEPRINT = Blueprint('pygeoapi', __name__, static_folder=STATIC_FOLDER)

# CORS: optionally enable from config.
if CONFIG['server'].get('cors', False):
    from flask_cors import CORS

    CORS(APP)

APP.config['JSONIFY_PRETTYPRINT_REGULAR'] = CONFIG['server'].get(
    'pretty_print', True)

api_ = API(CONFIG)

OGC_SCHEMAS_LOCATION = CONFIG['server'].get('ogc_schemas_location', None)

if (OGC_SCHEMAS_LOCATION is not None and
        not OGC_SCHEMAS_LOCATION.startswith('http')):
    # serve the OGC schemas locally

    if not os.path.exists(OGC_SCHEMAS_LOCATION):
        raise RuntimeError('OGC schemas misconfigured')


    @BLUEPRINT.route('/schemas/<path:path>', methods=['GET'])
    def schemas(path):
        """
        Serve OGC schemas locally

        :param path: path of the OGC schema document

        :returns: HTTP response
        """

        full_filepath = os.path.join(OGC_SCHEMAS_LOCATION, path)
        dirname_ = os.path.dirname(full_filepath)
        basename_ = os.path.basename(full_filepath)

        # TODO: better sanitization?
        path_ = dirname_.replace('..', '').replace('//', '')
        return send_from_directory(path_, basename_,
                                   mimetype=get_mimetype(basename_))


def get_response(result: tuple):
    """
    Creates a Flask Response object and updates matching headers.

    :param result: The result of the API call.
                   This should be a tuple of (headers, status, content).

    :returns: A Response instance.
    """

    headers, status, content = result
    response = make_response(content, status)

    if headers:
        response.headers = headers
    return response


@BLUEPRINT.route('/')
def landing_page():
    """
    OGC API landing page endpoint

    :returns: HTTP response
    """
    return get_response(api_.landing_page(request))


@BLUEPRINT.route('/openapi')
def openapi():
    """
    OpenAPI endpoint

    :returns: HTTP response
    """
    with open(os.environ.get('PYGEOAPI_OPENAPI'), encoding='utf8') as ff:
        if os.environ.get('PYGEOAPI_OPENAPI').endswith(('.yaml', '.yml')):
            openapi_ = yaml_load(ff)
        else:  # JSON string, do not transform
            openapi_ = ff.read()

    return get_response(api_.openapi(request, openapi_))


@BLUEPRINT.route('/api')
def api():
    """
    OpenAPI endpoint

    :returns: HTTP response
    """
    with open(os.environ.get('PYGEOAPI_OPENAPI'), encoding='utf8') as ff:
        if os.environ.get('PYGEOAPI_OPENAPI').endswith(('.yaml', '.yml')):
            openapi_ = yaml_load(ff)
        else:  # JSON string, do not transform
            openapi_ = ff.read()

    return get_response(api_.openapi(request, openapi_))


@BLUEPRINT.route('/conformance')
def conformance():
    """
    OGC API conformance endpoint

    :returns: HTTP response
    """
    return get_response(api_.conformance(request))


@BLUEPRINT.route('/collections', methods=['GET', 'POST'])
@BLUEPRINT.route('/collections/<path:collection_id>', methods=['GET', 'PUT', 'DELETE'])
def collections(collection_id=None):
    """
    OGC API collections endpoint

    :param collection_id: collection identifier

    :returns: HTTP response
    """

    if collection_id is None:
        if request.method == 'GET':  # list items
            return get_response(api_.describe_collections(request))
        elif request.method == 'POST':  # filter or manage items
            return get_response(api_.manage_collection(request, 'create'))

    elif request.method == 'DELETE':
        return get_response(
            api_.manage_collection(request, 'delete',
                                   collection_id))
    elif request.method == 'PUT':
        return get_response(
            api_.manage_collection(request, 'update',
                                   collection_id))
    else:
        return get_response(
            api_.get_collection(request, collection_id))


@BLUEPRINT.route('/collections/<path:collection_id>/items', methods=['GET', 'POST'])
@BLUEPRINT.route('/collections/<path:collection_id>/items/<item_id>', methods=['GET', 'DELETE'])
def collection_items(collection_id, item_id=None):
    """
    OGC API collections items endpoint

    :param collection_id: collection identifier
    :param item_id: item identifier

    :returns: HTTP response
    """

    if item_id is None:
        if request.method == 'GET':  # list items
            return get_response(
                api_.get_collection_items(request, collection_id))
        elif request.method == 'POST':  # filter or manage items
            return get_response(api_.manage_collection_item(request, 'create', collection_id))

    elif request.method == 'DELETE':
        return get_response(
            api_.manage_collection_item(request, 'delete', collection_id, item_id))
    else:
        return get_response(
            api_.get_collection_item(request, collection_id, item_id))


@BLUEPRINT.route('/collections/<path:collection_id>/items/<item_id>/tgsequence',
                 methods=['GET', 'POST'])
@BLUEPRINT.route('/collections/<path:collection_id>/items/<item_id>/tgsequence/<tGeometry_id>',
                 methods=['DELETE'])
def collection_items_tgeometries(collection_id, item_id, tGeometry_id=None):
    """
    OGC API collections items endpoint

    :param collection_id: collection identifier
    :param item_id: item identifier

    :returns: HTTP response
    """

    if tGeometry_id is None:
        if request.method == 'GET':  # list items
            return get_response(
                api_.get_collection_items_tGeometry(request, collection_id, item_id))
        elif request.method == 'POST':  # filter or manage items
            return get_response(
                api_.manage_collection_item_tGeometry(request, 'create',collection_id, item_id))

    elif request.method == 'DELETE':
        return get_response(
            api_.manage_collection_item_tGeometry(request, 'delete', collection_id, item_id, tGeometry_id))


@BLUEPRINT.route('/collections/<path:collection_id>/items/<item_id>/tgsequence/<tGeometry_id>/velocity',
                 methods=['GET'])
def collection_items_tgeometries_velocity(collection_id, item_id, tGeometry_id):
    """
    OGC API collections items endpoint

    :param collection_id: collection identifier
    :param item_id: item identifier

    :returns: HTTP response
    """

    if request.method == 'GET':  # list items
        return get_response(
            api_.get_collection_items_tGeometry_velocity(request, collection_id, item_id, tGeometry_id))


@BLUEPRINT.route('/collections/<path:collection_id>/items/<item_id>/tgsequence/<tGeometry_id>/distance',
                 methods=['GET'])
def collection_items_tgeometries_distance(collection_id, item_id, tGeometry_id):
    """
    OGC API collections items endpoint

    :param collection_id: collection identifier
    :param item_id: item identifier

    :returns: HTTP response
    """

    if request.method == 'GET':  # list items
        return get_response(
            api_.get_collection_items_tGeometry_distance(request, collection_id, item_id, tGeometry_id))


@BLUEPRINT.route('/collections/<path:collection_id>/items/<item_id>/tgsequence/<tGeometry_id>/acceleration',
                 methods=['GET'])
def collection_items_tgeometries_acceleration(collection_id, item_id, tGeometry_id):
    """
    OGC API collections items endpoint

    :param collection_id: collection identifier
    :param item_id: item identifier

    :returns: HTTP response
    """

    if request.method == 'GET':  # list items
        return get_response(
            api_.get_collection_items_tGeometry_acceleration(request, collection_id, item_id, tGeometry_id))

@BLUEPRINT.route('/collections/<path:collection_id>/items/<item_id>/tProperties',
                 methods=['GET', 'POST'])
def collection_items_tproperties(collection_id, item_id):
    """
    OGC API collections items endpoint

    :param collection_id: collection identifier
    :param item_id: item identifier

    :returns: HTTP response
    """

    if request.method == 'GET':  # list items
        return get_response(
            api_.get_collection_items_tProperty(request, collection_id, item_id))
    elif request.method == 'POST':  # filter or manage items
        return get_response(api_.manage_collection_item_tProperty(request, 'create',
                                                                  collection_id, item_id))


@BLUEPRINT.route('/collections/<path:collection_id>/items/<item_id>/tProperties/<tProperty_id>',
                 methods=['GET', 'POST', 'DELETE'])
def collection_items_tproperties_values(collection_id, item_id, tProperty_id):
    """
    OGC API collections items endpoint

    :param collection_id: collection identifier
    :param item_id: item identifier

    :returns: HTTP response
    """

    if request.method == 'GET':  # list items
        return get_response(
            api_.get_collection_items_tProperty_value(request, collection_id, item_id, tProperty_id))
    elif request.method == 'POST':  # filter or manage items
        return get_response(api_.manage_collection_item_tProperty_value(request, 'create',
                                                                        collection_id, item_id, tProperty_id))
    elif request.method == 'DELETE':  # filter or manage items
        return get_response(api_.manage_collection_item_tProperty(request, 'delete',
                                                                  collection_id, item_id, tProperty_id))


APP.register_blueprint(BLUEPRINT)


@click.command()
@click.pass_context
@click.option('--debug', '-d', default=False, is_flag=True, help='debug')
def serve(ctx, server=None, debug=False):
    """
    Serve pygeoapi via Flask. Runs pygeoapi
    as a flask server. Not recommend for production.

    :param server: `string` of server type
    :param debug: `bool` of whether to run in debug mode

    :returns: void
    """

    # setup_logger(CONFIG['logging'])
    APP.run(debug=True, host=api_.config['server']['bind']['host'],
            port=api_.config['server']['bind']['port'])


if __name__ == '__main__':  # run locally, for testing
    serve()
