###############################################################################
# Caleydo - Visualization for Molecular Biology - http://caleydo.org
# Copyright (c) The Caleydo Team. All rights reserved.
# Licensed under the new BSD license, available at http://caleydo.org/license
###############################################################################

from phovea_server.ns import Namespace, abort, Response
import requests
import logging

app = Namespace(__name__)
_log = logging.getLogger(__name__)


def _to_full_url(path):
  from phovea_server.config import view
  c = view('phovea_data_opencpu')
  return 'http://{host}:{port}/ocpu/{path}'.format(host=c.host, port=c.port, path=path)


@app.route('/<path:path>')
def _handle(path):
  _log.info('proxy request url: %s', path)
  url = _to_full_url(path)
  if url:
    _log.info('proxy request url: %s', url)
    r = requests.get(url)
    _log.info('proxy response status code: %s', r.status_code)
    return Response(r.text, status=r.status_code, content_type=r.headers['content-type'])
  abort(404)


def create():
  return app
