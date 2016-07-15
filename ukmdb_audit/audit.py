# -*- coding: utf-8 -*-

import logging
from logging.handlers import SysLogHandler
from ukmdb_audit import __version__


def ukmdb_logger2(name='MyLogger'):
    log = logging.getLogger(name)
    log.setLevel(logging.DEBUG)
    handl = SysLogHandler(address=('10.200.1.10', 514),
                          facility=SysLogHandler.LOG_LOCAL3)
    formatter = logging.Formatter(
        "UKMDB %(name)s: '%(message)s' (%(filename)s)")
    handl.setFormatter(formatter)
    log.addHandler(handl)
    return log


def get_mod_version():
    return __version__
