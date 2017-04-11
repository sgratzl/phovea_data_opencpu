from phovea_server.dataset_def import ADataSetEntry, ADataSetProvider
import logging
import phovea_server.config

__author__ = 'Samuel Gratzl'
_log = getLogger(__name__)
config = phovea_server.config.view('phovea_data_opencpu')


def assign_ids(ids, idtype):
  import phovea_server.plugin

  manager = phovea_server.plugin.lookup('idmanager')
  return np.array(manager(ids, idtype))


class OpenCPUProvider(ADataSetProvider):
  """
  dataset provider for Caleydo from Calumma REST Api. It uses cached for common categorical properties and the
  authentication token
  """

  def __init__(self):
    self.c = config
    self.entries = []

  def __len__(self):
    return len(self.entries)

  def __iter__(self):
    return iter(self.entries)

  def __getitem__(self, dataset_id):
    return next((e for e in self.entries if e.id == dataset_id))


def create():
  return CalummaProvider()
