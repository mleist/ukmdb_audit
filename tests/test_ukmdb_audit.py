#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
test_ukmdb_audit
----------------------------------

Tests for `ukmdb_audit` module.
"""

from unittest import TestCase

import ukmdb_audit
from ukmdb_audit import audit


class TestUkmdbAudit(TestCase):

    @classmethod
    def setup_class(cls):
        pass

    def test_something(self):
        pass

    def test_ver(self):
        self.assertEqual(audit.get_mod_version(),
                         ukmdb_audit.__version__)

    @classmethod
    def teardown_class(cls):
        pass
