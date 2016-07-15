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
from docopt import docopt
from celery.signals import worker_process_init, worker_process_shutdown
from ukmdb_audit.cmreshandler import CMRESHandler
from ukmdb_worker.base import set_debug_level
from ukmdb_worker import __version__
from ukmdb_worker import queues
from ukmdb_settings import settings


ukmdb_log = logging.getLogger("ukmdb")

es_log = None

# class myCelery(Celery):
#
#     def on_init(self):
#         print("oo" * 50)


app = Celery('worker',
             broker=settings.AMQP_BROKER_URL,
             )

queues.setup(app)

# app.conf.update(
#     CELERY_ROUTES={
#         'ukmdb_graph.worker.add_object': {
#             'queue': 'ukmdb_graph01',
#             'routing_key': '#',
#         },
#     },
# )
#


@app.task(serializer='json',
          name='worker.add_object',
          queue='ukmdb_logst01',
          exchange='ukmdb_all_in',
          routing_key='#',
          bind=True
          )
def add_object(self, msg):
    ukmdb_log.debug("-- ## self: '%s'", pformat(self))
    ukmdb_log.debug("-------> self.request: '%s'", pformat(self.request))
    ukmdb_log.info("----------------> add_object: '%s'", str(msg))
    received_dict = {
        'uuid': None,
        'type': None,
        'props': None,
        'app_domain': None,
        'app_type': None,
        'app_name': None,
        'app_id': None,
        'comment': None,
    }
    for i in received_dict.keys():
        received_dict[i] = msg.get(i)
    ukmdb_log.debug("received_dict: '%s'", pformat(received_dict))
    ukmdb_log.debug("es_log: '%s'", pformat(es_log))
    es_log.info("add object cmd",
                extra=received_dict)


@worker_process_init.connect
def init_worker(**kwargs):
    global es_log
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
    global es_log
    print('############# Closing database connectionn for worker.')


def main():
    print("KK" * 50)
    arguments = docopt(__doc__, options_first=True, version=__version__)
    set_debug_level(ukmdb_log, arguments)

    ukmdb_log.debug(u'program start')
    app.start()

    exit("See 'ukm_audit --help'.")
