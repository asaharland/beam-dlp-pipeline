#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from __future__ import absolute_import

import logging
import unittest

import apache_beam as beam
import dlp_pipeline
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to


class DlpPipelineTest(unittest.TestCase):

    def test_count1(self):
        self.run_pipeline()

    def run_pipeline(self):
        with TestPipeline() as p:
            rows = (p | 'create' >> beam.Create([
                {'id': 1,
                 'text': 'My credit card number is 3765-414016-21817.'},
                {'id': 2,
                 'text': 'My number should be 0333 7591 018.'}]))
            results = rows | dlp_pipeline.IdentifyAndRedactText('red-dog-piano', ['ALL_BASIC'])

            expected_results = [{'text': u'My credit card number is #################.', 'id': 1},
                                           {'text': u'My number should be ###########################', 'id': 2}]

            assert_that(results, equal_to(expected_results))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    unittest.main()
