import json
import logging
import asyncio
import time

import websockets
import requests


class SignalKStorage:
    def __init__(self):
        self._data = {}

    def _store_path(self, path, value):
        logging.debug("_store_path(self, %s, %s)", path, value)
        key = self._data
        for p in path:
            if not p in key:
                key[p] = {}
            key = key[p]
        if 'value' in key:
            if isinstance(key['value'], dict):
                key['value'] = {**value, **key['value']}
            else:
                key['value'] = value
        else:
            key['value'] = value

    def add_update(self, update):
        if 'context' in update:
            context = update['context']
        else:
            context = 'self'
        context_path = context.split('.')
        for update in update['updates']:
            for value in update['values']:
                path_ = value['path']
                if path_:
                    full_path = context_path + path_.split('.')
                else:
                    full_path = context_path
                logging.debug("update: %s with %s", full_path, value['value'])
                self._store_path(full_path, value['value'])

    def vessels(self):
        if 'vessels' not in self._data:
            return []
        return self._data['vessels'].items()


async def listen_on_websocket(uri: str, storage: SignalKStorage, config):
    async with websockets.connect(uri) as websocket:
        # await websocket.send()
        response = json.loads(await websocket.recv())
        logging.debug("response: %s", response['roles'])
        await websocket.send(json.dumps({'context': '*',
                                         'subscribe': [{
                                             'path': "navigation.position",
                                             "period": 1000,
                                             "policy": "fixed"
                                         }, {
                                             'path': "name",
                                             "period": 130000,
                                             "policy": "fixed"
                                         }, {
                                             'path': "mmsi",
                                             "period": 130000,
                                             "policy": "fixed"
                                         }, {
                                             'path': "navigation.courseOverGroundTrue",
                                             "period": 1000,
                                             "policy": "fixed"
                                         }, {
                                             'path': "navigation.speedOverGround",
                                             "period": 1000,
                                             "policy": "fixed"
                                         },
                                         ]
                                         }))
        while config['keep_running']:
            resp = json.loads(await websocket.recv())
            logging.debug("got data: %s", resp)
            storage.add_update(resp)


async def display_data(storage: SignalKStorage, config):
    for n in range(3):
        await asyncio.sleep(5.0)
        logging.debug("Data: %s", storage._data)
        for vessel in storage.vessels():

            _id = vessel[0]
            _data = vessel[1]
            name_ = ''
            if 'value'  in _data and 'name' in _data['value']:
                name_ = _data['value']['name']
            mmsi_ = ''
            if 'value'  in _data and 'mmsi' in _data['value']*:
                mmsi_ = _data['value']['mmsi']
            if 'navigation' in _data:
                navigation_ = _data['navigation']
                position_ = {'latitude': 0.0, 'longitude': 0.0}
                course_ = 0.0
                speed_ = 0.0
                if 'position' in navigation_ and navigation_['position']['value']:
                    if navigation_['position']['value']['latitude'] and navigation_['position']['value']['longitude']:
                        position_ = {'latitude': float(navigation_['position']['value']['latitude']),
                                     'longitude': float(navigation_['position']['value']['longitude'])}
                if 'courseOverGroundTrue' in navigation_ and navigation_['courseOverGroundTrue']['value']:
                    course_ = float(navigation_['courseOverGroundTrue']['value'])
                if 'speedOverGround' in navigation_ and navigation_['speedOverGround']['value']:
                    speed_ = float(navigation_['speedOverGround']['value'])
            print(
                f"{_id:>60} :: {name_:>20} - {mmsi_:>20} :: ({position_['latitude']: 8.3f}, {position_['longitude']: 8.3f}) -> {course_: 4.1f} :: {speed_: 3.1f}")
    config['keep_running'] = False


async def main():
    # Get list of endpoints
    storage = SignalKStorage()
    response = requests.get("https://cloud.signalk.org/signalk")
    logging.debug("response: %s", response)
    if response.ok:
        json = response.json()
        logging.info("json: %s", json)
        config = {'keep_running': True}
        await asyncio.gather(listen_on_websocket(json['endpoints']['v1']['signalk-ws'], storage, config),
                             display_data(storage, config)
                             )


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main())
