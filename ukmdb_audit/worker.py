#!/usr/bin/env python
# -*- coding: utf-8 -*-
# pylint: disable=C0330,C0103
"""UKMDB Worker.

Usage: ukm_audit [--help] [--debug ...]

Options:
  -d --debug               Show debug information (maybe multiple).

  ukm_audit (-h | --help)
  ukm_audit --version

"""

from __future__ import absolute_import
import logging
from pprint import pformat
from celery import Celery
from celery.signals import worker_process_init, worker_process_shutdown
from docopt import docopt
from ukmdb_worker.base import set_debug_level
from ukmdb_worker import __version__
from ukmdb_worker import queues
from ukmdb_settings import settings
from ukmdb_audit.cmreshandler import CMRESHandler


ukmdb_log = logging.getLogger("ukmdb")
es_log = None
app = Celery('worker',
             broker=settings.AMQP_BROKER_URL,
             )
queues.setup(app)


@app.task(serializer='json',
          name='ukmdb.add_object',
          queue='ukmdb_logst01',
          exchange='ukmdb_all_in',
          routing_key='#',
          bind=True
          )
def add_object(self, msg):
    ukmdb_log.info("-- ## self: '%s'", pformat(self))
    ukmdb_log.info("-------> self.request: '%s'", pformat(self.request))
    ukmdb_log.info("----------------> add_object: '%s'", str(msg))
    received_dict = {
        'cmd': 'add_object',
        'uuid': None,
        'props': None,
        'app_domain': None,
        'app_type': None,
        'app_name': None,
        'app_id': None,
        'comment': None,
    }
    for i in received_dict.keys():
        received_dict[i] = msg.get(i)
    ukmdb_log.info("received_dict: '%s'", pformat(received_dict))
    ukmdb_log.info("es_log: '%s'", pformat(es_log))
    es_log.info("add object",
                extra=received_dict)


@app.task(serializer='json',
          name='ukmdb.edit_object',
          queue='ukmdb_logst01',
          exchange='ukmdb_all_in',
          routing_key='#',
          bind=True
          )
def edit_object(self, msg):
    ukmdb_log.info("-- ## self: '%s'", pformat(self))
    ukmdb_log.info("-------> self.request: '%s'", pformat(self.request))
    ukmdb_log.info("----------------> edit_object: '%s'", str(msg))
    received_dict = {
        'cmd': 'edit_object',
        'uuid': None,
        'props': None,
        'app_domain': None,
        'app_type': None,
        'app_name': None,
        'app_id': None,
        'comment': None,
    }
    for i in received_dict.keys():
        received_dict[i] = msg.get(i)
    ukmdb_log.info("received_dict: '%s'", pformat(received_dict))
    ukmdb_log.info("es_log: '%s'", pformat(es_log))
    es_log.info("edit object",
                extra=received_dict)


@app.task(serializer='json',
          name='ukmdb.del_object',
          queue='ukmdb_logst01',
          exchange='ukmdb_all_in',
          routing_key='#',
          bind=True
          )
def del_object(self, msg):
    ukmdb_log.info("-- ## self: '%s'", pformat(self))
    ukmdb_log.info("-------> self.request: '%s'", pformat(self.request))
    ukmdb_log.info("----------------> del_object: '%s'", str(msg))
    received_dict = {
        'cmd': 'del_object',
        'uuid': None,
        'props': None,
        'app_domain': None,
        'app_type': None,
        'app_name': None,
        'app_id': None,
        'comment': None,
    }
    for i in received_dict.keys():
        received_dict[i] = msg.get(i)
    ukmdb_log.info("received_dict: '%s'", pformat(received_dict))
    ukmdb_log.info("es_log: '%s'", pformat(es_log))
    es_log.info("delete object",
                extra=received_dict)


@worker_process_init.connect
def init_worker(**kwargs):
    global es_log  # pylint: disable=W0603
    print('############# Initializing database connection for worker.')
    es_handler = CMRESHandler(hosts=[{'host': '10.200.1.165', 'port': 9200},
                                     {'host': '10.200.1.166', 'port': 9200},
                                     {'host': '10.200.1.167', 'port': 9200}],
                              auth_type=CMRESHandler.AuthType.NO_AUTH,
                              es_doc_type="ukmdb",
                              es_index_name="ukmdb")
    es_log = logging.getLogger("UKMDB ES")
    es_log.setLevel(logging.INFO)
    es_log.addHandler(es_handler)


@worker_process_shutdown.connect
def shutdown_worker(**kwargs):
    print('############# Closing database connectionn for worker.')


def main():
    print("KK" * 50)
    arguments = docopt(__doc__, options_first=True, version=__version__)
    set_debug_level(ukmdb_log, arguments)
    ukmdb_log.debug(u'program start')
    app.start()
    exit("See 'ukm_audit --help'.")
