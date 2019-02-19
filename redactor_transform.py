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

import apache_beam as beam

from redactor.redactor import DlpService


class RedactPersonalDataFn(beam.DoFn):
    """Redacts personal information by calling Cloud DLP on the free text field."""

    def __init__(self, project, info_types):
        self.dlp_client = None
        self.project = project
        self.info_types = info_types
        super(RedactPersonalDataFn, self).__init__()

    def start_bundle(self):
        self.dlp_client = DlpService(self.project)

    def process(self, elem):
        elem['text'] = self.dlp_client.deidentify_text(text=elem['text'], info_types=self.info_types)
        yield elem


class IdentifyAndRedactText(beam.PTransform):
    def __init__(self, project, info_types):
        self.project = project
        self.info_types = info_types

    def expand(self, pcoll):
        return (pcoll
                | 'RedactPersonalDataFn' >> beam.ParDo(RedactPersonalDataFn(self.project, self.info_types)))
