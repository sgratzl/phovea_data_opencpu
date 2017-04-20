from phovea_server.dataset_def import ATable, ADataSetProvider, AColumn
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
  columnDescription = function(col, colname) { 
    clazz <- class(col) 
    base <- list(name=colname, value=list(type='string'))
    if (clazz == 'numeric') {
      if (typeof(col) == 'integer') {
        base[['value']] <- list(type='int', range=c(min(col),max(col)))
      } else {
        base[['value']] <- list(type='real', range=c(min(col),max(col)))
      }
    } else if (clazz == 'factor') { 
      base[['value']] <- list(type='categorical', categories=levels(col))
    }
    base
  }
  tableDescription = function(dataset, data_name) {    
    columns = mapply(columnDescription, dataset, colnames(dataset), SIMPLIFY='array')
    
    list(name=data_name,
         size=dim(dataset),
         type='table',
         columns=columns)
  }
  r = list()
  for (obj in objs) {
    value = get(obj)
    if (is.data.frame(value)) {
      r[[obj]] = tableDescription(value, obj)
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
    return base

  return [to_desc(d) for d in desc.values()]


def row_names(session, variable):
  import numpy as np
  output = requests.post(_to_url('library/base/R/rownames/json'),
                         dict(x='{s}::{v}'.format(s=session, v=variable)))
  data = list(output.json())
  return np.array(data)


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
  def __init__(self, entry, session, meta):
    ATable.__init__(self, entry['name'], 'opencpu', 'table', entry.get('id', None))
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
      self._rows = row_names(self._session, self._variable)
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


class OpenCPUSession(object):
  def __init__(self, desc):
    self._desc = desc
    self._session = create_session(desc['initScript'])

    entries = resolve_datasets(self._session)
    meta = desc.get('meta', dict())
    self._entries = [OpenCPUTable(entry, self._session, meta.get(entry['name'], dict())) for entry in entries]

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
