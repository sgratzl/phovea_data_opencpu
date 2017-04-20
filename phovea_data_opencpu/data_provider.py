from phovea_server.dataset_def import ATable, ADataSetProvider, AColumn, AVector, AMatrix
from logging import getLogger
import requests
import phovea_server
import numpy as np

__author__ = 'Samuel Gratzl'
_log = getLogger(__name__)
config = phovea_server.config.view('phovea_data_opencpu')


def _to_url(path):
  return 'http://{host}:{port}/ocpu/{path}'.format(host=config.host, port=config.port, path=path)


def assign_ids(ids, idtype):
  import phovea_server.plugin

  manager = phovea_server.plugin.lookup('idmanager')
  return np.array(manager(ids, idtype))


def create_session(init_script):
  import re
  code = """
  parse(text="
%s

# generate meta data for phovea
phoveaDatasets = (function(objs) {
  known_type = function(col) {
    clazz <- class(col)
    if (clazz == 'numeric' || clazz == 'integer' || clazz == 'double' || clazz == 'matrix') {
      if (typeof(col) == 'integer') {
        list(type='int', range=c(min(col),max(col)))
      } else {
        list(type='real', range=c(min(col),max(col)))
      }
    } else if (clazz == 'factor') {
      list(type='categorical', categories=levels(col))
    } else {
      list(type='string')
    }
  }
  columnDescription = function(col, colname) {
    list(name=colname, value=known_type(col))
  }
  tableDescription = function(dataset, data_name) {
    columns = mapply(columnDescription, dataset, colnames(dataset), SIMPLIFY='array')

    list(name=data_name,
         size=dim(dataset),
         type='table',
         columns=columns)
  }
  vectorDescription = function(dataset, data_name) {
    list(name=data_name,
         size=length(dataset),
         type='vector',
         value=known_type(dataset))
  }
  matrixDescription = function(dataset, data_name) {
    list(name=data_name,
         size=dim(dataset),
         type='matrix',
         value=known_type(dataset))
  }
  r = list()
  for (obj in objs) {
    value = get(obj)
    if (is.data.frame(value)) {
      r[[obj]] = tableDescription(value, obj)
    } else if (is.vector(value)) {
      r[[obj]] = vectorDescription(value, obj)
    } else if (is.matrix(value)) {
      r[[obj]] = matrixDescription(value, obj)
    }
  }
  r
})(ls())
")
""" % (init_script,)
  _log.debug(code)
  output = requests.post(_to_url('library/base/R/eval'), dict(expr=code))
  _log.debug(output.text)
  session = re.search('/tmp/(.*)/R', output.text).group(1)
  return session


def resolve_datasets(session):
  from itertools import izip
  # output = requests.get(_to_url('tmp/{s}/console'.format(s=session)))
  # print(output.text)

  # use the already computed list of datasets as part of session initializiation
  output = requests.get(_to_url('tmp/{s}/R/phoveaDatasets/json'.format(s=session)))
  desc = output.json()

  if not desc:
    return []

  # normalize description and remove single list entries

  def to_value(value):
    base = dict(type=value['type'][0])
    if 'range' in value:
      base['range'] = value['range']
    if 'categories' in value:
      base['categories'] = value['categories']
    return base

  def to_desc(d):
    base = dict(name=d['name'][0], type=d['type'][0], size=d['size'])
    if base['type'] == 'table':
      names = d['columns'][0]
      values = d['columns'][1]
      base['columns'] = [dict(name=name[0], value=to_value(value)) for name, value in izip(names, values)]
    if base['type'] == 'matrix' or base['type'] == 'vector':
      base['value'] = d['value']
    return base

  return [to_desc(d) for d in desc.values()]


def _dim_names(session, variable, expected_length, dim):
  import numpy as np
  output = requests.post(_to_url('library/base/R/{dim}names/json'.format(dim=dim)),
                         dict(x='{s}::{v}'.format(s=session, v=variable)))
  data = list(output.json())
  dim_name = dim.capitalize()
  if len(data) < expected_length:
    # generate dummy ids
    for i in range(len(data), expected_length):
      data.append(dim_name + str(i))
  return np.array(data)


def row_names(session, variable, expected_length):
  return _dim_names(session, variable, expected_length, 'row')


def col_names(session, variable, expected_length):
  return _dim_names(session, variable, expected_length, 'col')


def column_values(session, variable, column):
  import numpy as np
  output = requests.post(_to_url('library/base/R/identity/json'),
                         dict(x='{s}::{v}${c}'.format(s=session, v=variable, c=column)))
  data = list(output.json())
  return np.array(data)


def table_values(session, variable, columns):
  import pandas as pd
  output = requests.get(_to_url('tmp/{s}/R/{v}/json'.format(s=session, v=variable)))
  data = list(output.json())
  columns = [c.column for c in columns]
  return pd.DataFrame.from_records(data, columns=columns)


def vector_values(session, variable):
  import numpy as np
  output = requests.get(_to_url('tmp/{s}/R/{v}/json'.format(s=session, v=variable)))
  data = list(output.json())
  return np.array(data)


def matrix_values(session, variable):
  return vector_values(session, variable)


class OpenCPUColumn(AColumn):
  def __init__(self, desc, table):
    super(OpenCPUColumn, self).__init__(desc['name'], desc['value']['type'])
    self._desc = desc
    self.column = desc['name']
    self._table = table
    self._values = None

  def asnumpy(self, range=None):
    if self._values is None:
      self._values = self._table.column_values(self.column)
    if range is None:
      return self._values
    return self._values[range.asslice()]

  def dump(self):
    return self._desc


class OpenCPUTable(ATable):
  def __init__(self, entry, session, meta, session_name):
    ATable.__init__(self, entry['name'], 'opencpu/' + session_name, 'table', entry.get('id', None))
    self._session = session
    self._variable = entry['name']
    self.idtype = meta.get('idtype', 'Custom')
    self._entry = entry
    self.columns = [OpenCPUColumn(d, self) for d in entry['columns']]
    self.shape = entry['size']

    self._rows = None
    self._row_ids = None
    self._values = None

  def to_description(self):
    r = super(OpenCPUTable, self).to_description()
    r['idtype'] = self.idtype
    r['columns'] = [d.dump() for d in self.columns]
    r['size'] = self.shape
    return r

  def column_values(self, column):
    return column_values(self._session, self._variable, column)

  def rows(self, range=None):
    if self._rows is None:
      self._rows = row_names(self._session, self._variable, self.shape[0])
    if range is None:
      return self._rows
    return self._rows[range.asslice()]

  def rowids(self, range=None):
    if self._row_ids is None:
      self._row_ids = assign_ids(self.rows(), self.idtype)
    if range is None:
      return self._row_ids
    return self._row_ids[range.asslice()]

  def aspandas(self, range=None):
    if self._values is None:
      self._values = table_values(self._session, self._variable, self.columns)
    if range is None:
      return self._values
    return self._values.iloc[range.asslice(no_ellipsis=True)]


class OpenCPUVector(AVector):
  def __init__(self, entry, session, meta, session_name):
    super(OpenCPUVector, self).__init__(entry['name'], 'opencpu/' + session_name, 'vector', entry.get('id', None))
    self._session = session
    self._variable = entry['name']
    self.idtype = meta.get('idtype', 'Custom')
    self._entry = entry
    self.value = entry['value']['type']
    self.shape = entry['size']

    self._rows = None
    self._row_ids = None
    self._values = None

  def to_description(self):
    r = super(OpenCPUVector, self).to_description()
    r['idtype'] = self.idtype
    r['value'] = self._entry['value']
    r['size'] = self.shape
    return r

  def rows(self, range=None):
    if self._rows is None:
      self._rows = row_names(self._session, self._variable, self.shape[0])
    if range is None:
      return self._rows
    return self._rows[range.asslice()]

  def rowids(self, range=None):
    if self._row_ids is None:
      self._row_ids = assign_ids(self.rows(), self.idtype)
    if range is None:
      return self._row_ids
    return self._row_ids[range.asslice()]

  def asnumpy(self, range=None):
    if self._values is None:
      self._values = vector_values(self._session, self._variable)
    if range is None:
      return self._values
    return self._values[range[0].asslice()]


class OpenCPUMatrix(AMatrix):
  def __init__(self, entry, session, meta, session_name):
    super(OpenCPUMatrix, self).__init__(entry['name'], 'opencpu/' + session_name, 'matrix', entry.get('id', None))
    self._session = session
    self._variable = entry['name']
    self.rowtype = meta.get('rowtype', 'Custom')
    self.coltype = meta.get('coltype', 'Custom')
    self._entry = entry
    self.value = entry['value']['type']
    self.shape = entry['size']

    self._rows = None
    self._row_ids = None
    self._cols = None
    self._col_ids = None
    self._values = None

  def to_description(self):
    r = super(OpenCPUMatrix, self).to_description()
    r['rowtype'] = self.rowtype
    r['coltype'] = self.coltype
    r['value'] = self._entry['value']
    r['size'] = self.shape
    return r

  def rows(self, range=None):
    if self._rows is None:
      self._rows = row_names(self._session, self._variable, self.shape[0])
    if range is None:
      return self._rows
    return self._rows[range.asslice()]

  def rowids(self, range=None):
    if self._row_ids is None:
      self._row_ids = assign_ids(self.rows(), self.rowtype)
    if range is None:
      return self._row_ids
    return self._row_ids[range.asslice()]

  def cols(self, range=None):
    if self._cols is None:
      self._cols = col_names(self._session, self._variable, self.shape[0])
    if range is None:
      return self._cols
    return self._cols[range.asslice()]

  def colids(self, range=None):
    if self._row_ids is None:
      self._row_ids = assign_ids(self.rows(), self.coltype)
    if range is None:
      return self._row_ids
    return self._row_ids[range.asslice()]

  def asnumpy(self, range=None):
    if self._values is None:
      self._values = matrix_values(self._session, self._variable)
    if range is None:
      return self._values
    return self._values[range[0].asslice()]


class OpenCPUSession(object):
  def __init__(self, desc):
    self._desc = desc
    self._session = create_session(desc['script'])

    session_name = desc['name']
    entries = resolve_datasets(self._session)
    meta = desc.get('meta', dict())

    def to_dataset(entry):
      meta_data = meta.get(entry['name'], dict())
      if entry['type'] == 'table':
        return OpenCPUTable(entry, self._session, meta_data, session_name)
      elif entry['type'] == 'vector':
        return OpenCPUVector(entry, self._session, meta_data, session_name)
      elif entry['type'] == 'matrix':
        return OpenCPUMatrix(entry, self._session, meta_data, session_name)
      return None

    self._entries = [v for v in (to_dataset(entry) for entry in entries) if v is not None]

  def __iter__(self):
    return iter(self._entries)


class OpenCPUProvider(ADataSetProvider):
  """
  dataset provider for Caleydo from Calumma REST Api. It uses cached for common categorical properties and the
  authentication token
  """

  def __init__(self):
    self.c = config
    self._sessions = [OpenCPUSession(desc) for desc in config.sessions]

  def __len__(self):
    return len(self.entries)

  def __iter__(self):
    import itertools
    return itertools.chain(*self._sessions)

  def __getitem__(self, dataset_id):
    return next((e for e in self if e.id == dataset_id))


def create():
  return OpenCPUProvider()
