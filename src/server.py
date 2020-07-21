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

import json
import os.path
import pathlib
import sqlite3

#===============================================================================

from sanic import Blueprint, Sanic, exceptions
import sanic.response as response
from sanic_cors import CORS

#===============================================================================

from landez.sources import MBTilesReader, ExtractionError, InvalidFormatError

#===============================================================================

import celeryapp
import socketio

#===============================================================================

from PIL import Image

def blank_tile():
    return Image.new('RGBA', (1, 1), color=(255, 255, 255, 0))

#===============================================================================

from urllib.parse import urljoin

## This needs to be in a config file or a runtime parameter...
## c.f. `port`??
## SERVER_URL = 'https://celldl.org/abi-covid-19/data/'
SERVER_URL = 'http://localhost:4329/'

def server_url(url):
    return urljoin(SERVER_URL, url[1:] if url.startswith('/') else url)

#===============================================================================

map_blueprint = Blueprint('map', url_prefix='/')

maps_root = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../maps'))

#===============================================================================

app = Sanic('map-server')
app.blueprint(map_blueprint)

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
        tilejson['tiles'] = [ server_url('{}{}/mvtiles/{{z}}/{{x}}/{{y}}'
                                .format(map_path, '/{}'.format(layer) if layer else '')) ]
        tilejson['vector_layers'] = json.loads(metadata['json'])['vector_layers']
        return response.json(tilejson)
    except ExtractionError:
        pass
    except (InvalidFormatError, sqlite3.OperationalError):
        exceptions.abort(404, 'Cannot read tile database')
    return response.empty(status=204)

#===============================================================================

def vector_tiles(map_path, layer, z, y, x):
    try:
        mbtiles = os.path.join(maps_root, map_path, 'index.mbtiles')
        reader = MBTilesReader(mbtiles)
        tile = reader.tile(z, x, y)
        headers={'Content-Type': 'application/x-protobuf'}
        if tile[0:2] == b'\x1f\x8b':
            headers['Content-Encoding'] = 'gzip';
        return response.raw(tile, headers=headers)
    except ExtractionError:
        pass
    except (InvalidFormatError, sqlite3.OperationalError):
        exceptions.abort(404, 'Cannot read tile database')
    return response.empty(status=204)

#===============================================================================

@map_blueprint.route('/')
async def maps(request):
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
    return response.json(map_list)

@map_blueprint.route('/<map_path>/')
async def map(request, map_path):
    filename = os.path.join(maps_root, map_path, 'index.json')
    if os.path.exists(filename):
        return await response.file(filename)
    else:
        exceptions.abort(404, 'Missing index file...')

@map_blueprint.route('/<map_path>/tilejson')
async def tilejson_base(request, map_path):
    return tilejson(map_path, '')

@map_blueprint.route('/<map_path>/<layer>/tilejson')
async def tilejson_layer(request, map_path, layer):
    return tilejson(map_path, layer)

@map_blueprint.route('/<map_path>/mvtiles/<z>/<x>/<y>')
def vector_tiles_base(request, map_path, z, y, x):
    return vector_tiles(map_path, '', int(z), int(y), int(x))

@map_blueprint.route('/<map_path>/<layer>/mvtiles/<z>/<x>/<y>')
def vector_tiles_layer(request, map_path, layer, z, y, x):
    return vector_tiles(map_path, layer, int(z), int(y), int(x))

@map_blueprint.route('/<map_path>/style')
async def style(request, map_path):
    filename = os.path.join(maps_root, map_path, 'style.json')
    if os.path.exists(filename):
        with open(filename) as style_data:
            style = json.load(style_data)
            # Resolve URLs
            for (name, source) in style['sources'].items():
                if 'url' in source:
                    source['url'] = server_url(source['url'])
                if 'tiles' in source:
                    tiles = []
                    for url in source['tiles']:
                        tiles.append(server_url(url))
                    source['tiles'] = tiles
#        print(style)
        return response.json(style)
    else:
        exceptions.abort(404, 'Missing style file...')

@map_blueprint.route('/<map_path>/images/<image>')
async def map_background(request, map_path, image):
    filename = os.path.join(maps_root, map_path, 'images', image)
    if os.path.exists(filename):
        return await response.file(filename)
    else:
        exceptions.abort(404, 'Missing image: {}'.format(filename))

@map_blueprint.route('/<map_path>/tiles/<layer>/<z>/<x>/<y>')
def image_tiles(request, map_path, layer, z, y, x):
    try:
        mbtiles = os.path.join(maps_root, map_path, '{}.mbtiles'.format(layer))
        reader = MBTilesReader(mbtiles)
        return response.raw(reader.tile(int(z), int(y), int(x)),
                            headers={'Content-Type': 'image/png'})
    except ExtractionError:
        pass
    except (InvalidFormatError, sqlite3.OperationalError):
        exceptions.abort(404, 'Cannot read tile database')
    return response.raw(blank_tile(), headers={'Content-Type': 'image/png'})

#===============================================================================

__sessions = {}

sio = socketio.AsyncServer(async_mode='sanic', cors_allowed_origins=[])
sio.attach(app)

@sio.event
async def connect(sid, data):
    #print('Connect:', sid, data)
    if 'HTTP_ORIGIN' in data:
        if sid not in __sessions:
            ## SERVER_URL ???
            __sessions[sid] = {'host': 'http://{}'.format(data.get('HTTP_HOST'))}
    elif 'HTTP_KEY' in data:
        __sessions[data['HTTP_KEY']]['simulation'] = sid

@sio.event
async def disconnect(sid):
    if sid in __sessions:
        del __sessions[sid]


def __send_message(msg_type, data, room):
    return sio.emit('msg', {'type': msg_type, 'data': data}, room=room)


@sio.event
async def msg(sid, msg):
    if msg.get('type') in ['data', 'metadata']:
        if msg.get('key') in __sessions:  # ==> message from worker
            await __send_message(msg['type'], msg['data'], msg['key'])

    elif msg.get('type') == 'control':
        msg_data = msg.get('data', {})
        control_type = msg_data.get('type')
        action = msg_data.get('action')

        if msg.get('key') in __sessions:  # ==> message from worker
            if control_type == 'simulation':
                if msg_data.get('action') == 'closedown':
                    await __send_message('control', {
                        'type': 'simulation',
                        'action': 'stopped'
                    }, msg['key'])
                    await sio.disconnect(sid)

        elif sid in __sessions:           # ==> message from browser
            session = __sessions[sid]
            if control_type == 'mouse':   # Example only...
                if action == 'click':
                    lng_lat = msg_data.get('data')
                    geojson = {
                        'type': 'FeatureCollection',
                        'features': [
                            { 'type': 'Feature',
                              'geometry': {
                                  'type': 'Point',
                                  'coordinates': [ lng_lat['lng'], lng_lat['lat']]
                                }
                            }
                        ]
                    }
                    await __send_message('data', {
                        'type': 'geojson',
                        'data': json.dumps(geojson)
                    }, sid)
            elif control_type == 'simulation':
                if action == 'start':
                    celeryapp.app.send_task('simulations.transportation.run', kwargs={
                        ## SERVER_URL
                        'host': session['host'],
                        'key': sid,
                        'params': msg_data.get('data')
                        })
                elif action == 'stop' and 'simulation' in session:
                    await __send_message('control', {
                        'type': 'simulation',
                        'action': 'stop'
                    }, session['simulation'])


#===============================================================================

app.register_blueprint(map_blueprint)

#===============================================================================

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='A web-server for maps.')
    parser.add_argument('--debug', action='store_true',
                        help="run in debugging mode (NOT FOR PRODUCTION)")
    parser.add_argument('--port', type=int, metavar='PORT', default=4329,
                        help='the port to listen on (default 4329)')

    args = parser.parse_args()

    ## Shouldn't we be using SERVER_URL ????
    app.run(debug=args.debug, host='localhost', port=args.port, access_log=False)
    ## Make `access_log` a runtime option...

#===============================================================================
