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

import argparse
import logging
import csv

from redactor.redactor_service import DlpService

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


class ParseFileFn(beam.DoFn):
    def __init__(self):
        super(ParseFileFn, self).__init__()

    def process(self, elem):
        row = list(csv.reader([elem]))[0]
        yield {
            'id': row[0],
            'text': row[1],
        }


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


def run(argv=None):
    """Pipeline for reading data from a CSV, redacting the data using Cloud DLP and writing the results to BigQuery"""
    parser = argparse.ArgumentParser()
    parser.add_argument('--input',
                        dest='input',
                        help='Input file to process.')
    parser.add_argument('--output',
                        dest='output',
                        help='BigQuery output dataset and table name in the format dataset.tablename')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # We use the save_main_session option because one or more DoFn's in this
    # workflow rely on global context (e.g., a module imported at module level).
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        # Read in the CSV file
        lines = (p
                 | 'ReadFromGCS' >> ReadFromText(known_args.input)
                 | 'ParseFileFn' >> beam.ParDo(ParseFileFn()))

        # Redact PII from the 'text' column.
        redacted_rows = lines | 'IdentifyAndRedactText' >> IdentifyAndRedactText('red-dog-piano', ['ALL_BASIC'])

        # Format rows and write to BigQuery.
        (redacted_rows
         | 'MapToTableRows' >> beam.Map(lambda row: {'id': row['id'], 'text': row['text']})
         | 'WriteToBigQuery' >> WriteToBigQuery(
                    'test',
                    dataset='finance',
                    schema='id:INTEGER, text:STRING',
                    project=p.options.display_data()['project'],
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
                ))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
