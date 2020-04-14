#===============================================================================
#
#  Flatmap viewer and annotation tool
#
#  Copyright (c) 2019  David Brooks
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#
#===============================================================================

import io
import json
import os.path
import pathlib
import sqlite3
import zlib

#===============================================================================

from flask import abort, Blueprint, Flask, jsonify, make_response, request, send_file
from flask_cors import CORS

from landez.sources import MBTilesReader, ExtractionError, InvalidFormatError

#===============================================================================

from PIL import Image

def blank_tile():
    tile = Image.new('RGBA', (1, 1), color=(255, 255, 255, 0))
    file = io.BytesIO()
    tile.save(file, 'png')
    file.seek(0)
    return file

#===============================================================================

map_blueprint = Blueprint('map', __name__, url_prefix='/', static_folder='static',
                          root_path=os.path.dirname(os.path.abspath(__file__)))

maps_root = os.path.normpath(os.path.join(map_blueprint.root_path, '../maps'))

#===============================================================================

app = Flask(__name__)

CORS(map_blueprint)

#===============================================================================

def tilejson(map_path, layer):
    try:
        mbtiles = os.path.join(maps_root, map_path,
                                '{}.mbtiles'.format(layer if layer else 'index'))
        reader = MBTilesReader(mbtiles)
        metadata = reader.metadata()
        tilejson = {}
        tilejson['tilejson'] = '2.2.0'
        if 'id' in metadata:
            tilejson['id'] = metadata['id']
        tilejson['bounds'] = [float(x) for x in metadata['bounds'].split(',')]
        tilejson['center'] = [float(x) for x in metadata['center'].split(',')] ## Ignored ??
        tilejson['maxzoom'] = int(metadata['maxzoom'])
        tilejson['minzoom'] = int(metadata['minzoom'])
        tilejson['format'] = 'pbf'
        tilejson['scheme'] = 'xyz'
        tilejson['tiles'] = [ '{}{}{}/mvtiles/{{z}}/{{x}}/{{y}}'
                                .format(request.url_root, map_path,
                                        '/{}'.format(layer) if layer else '') ]
        tilejson['vector_layers'] = json.loads(metadata['json'])['vector_layers']
        return jsonify(tilejson)
    except ExtractionError:
        pass
    except (InvalidFormatError, sqlite3.OperationalError):
        abort(404, 'Cannot read tile database')
    return make_response('', 204)

#===============================================================================

def vector_tiles(map_path, layer, z, y, x):
    try:
        mbtiles = os.path.join(maps_root, map_path, 'index.mbtiles')
        reader = MBTilesReader(mbtiles)
        tile = reader.tile(z, x, y)
        response = send_file(io.BytesIO(tile), mimetype='application/x-protobuf')
        if tile[0:2] == b'\x1f\x8b':
            response.headers['Content-Encoding'] = 'gzip';
        return response
    except ExtractionError:
        pass
    except (InvalidFormatError, sqlite3.OperationalError):
        abort(404, 'Cannot read tile database')
    return make_response('', 204)

#===============================================================================

@map_blueprint.route('/')
def maps():
    map_list = []
    for map_dir in pathlib.Path(maps_root).iterdir():
        index = map_dir.joinpath('index.json')
        mbtiles = map_dir.joinpath('index.mbtiles')
        style = map_dir.joinpath('style.json')
        if os.path.exists(index) and os.path.exists(mbtiles) and os.path.exists(style):
            id = map_dir.parts[-1]
            with open(index) as f:
                if json.load(f).get('id') == id:
                    map_list.append({'id': id})
    return jsonify(map_list)

@map_blueprint.route('/<string:map_path>/')
def map(map_path):
    filename = os.path.join(maps_root, map_path, 'index.json')
    return send_file(filename)

@map_blueprint.route('/<string:map_path>/tilejson')
def tilejson_base(map_path):
    return tilejson(map_path, '')

@map_blueprint.route('/<string:map_path>/<string:layer>/tilejson')
def tilejson_layer(map_path, layer):
    return tilejson(map_path, layer)

@map_blueprint.route('/<string:map_path>/mvtiles/<int:z>/<int:x>/<int:y>')
def vector_tiles_base(map_path, z, y, x):
    return vector_tiles(map_path, '', z, y, x)

@map_blueprint.route('/<string:map_path>/<string:layer>/mvtiles/<int:z>/<int:x>/<int:y>')
def vector_tiles_layer(map_path, layer, z, y, x):
    return vector_tiles(map_path, layer, z, y, x)

@map_blueprint.route('/<string:map_path>/style')
def style(map_path):
    filename = os.path.join(maps_root, map_path, 'style.json')
    return send_file(filename)

@map_blueprint.route('/<string:map_path>/images/<string:image>')
def map_background(map_path, image):
    filename = os.path.join(maps_root, map_path, 'images', image)
    if os.path.exists(filename):
        return send_file(filename)
    else:
        abort(404, 'Missing image: {}'.format(filename))

@map_blueprint.route('/<string:map_path>/tiles/<string:layer>/<int:z>/<int:x>/<int:y>')
def image_tiles(map_path, layer, z, y, x):
    try:
        mbtiles = os.path.join(maps_root, map_path, '{}.mbtiles'.format(layer))
        reader = MBTilesReader(mbtiles)
        return send_file(io.BytesIO(reader.tile(z, x, y)), mimetype='image/png')
    except ExtractionError:
        pass
    except (InvalidFormatError, sqlite3.OperationalError):
        abort(404, 'Cannot read tile database')
    return send_file(blank_tile(), mimetype='image/png')

#===============================================================================

app.register_blueprint(map_blueprint)

#===============================================================================

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='A web-server for maps.')
    parser.add_argument('--debug', action='store_true',
                        help="run in debugging mode (NOT FOR PRODUCTION)")
    parser.add_argument('--port', type=int, metavar='PORT', default=4328,
                        help='the port to listen on (default 4328)')

    args = parser.parse_args()

    app.run(debug=args.debug, host='localhost', port=args.port)

#===============================================================================
