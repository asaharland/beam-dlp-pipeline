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

from google.cloud.dlp import DlpServiceClient


class DlpService:

    def __init__(self, project):
        self.project = project
        self.client = DlpServiceClient()

    def deidentify_text(self,
                        text=None,
                        info_types=None):
        parent = self.client.project_path(self.project)
        inspect_config = self.get_inspect_config(info_types)
        deidentify_config = self.get_deidentify_config('#', 0)

        response = self.client.deidentify_content(
            parent,
            inspect_config=inspect_config,
            deidentify_config=deidentify_config,
            item={'value': text})

        return response.item.value

    def get_inspect_config(self, info_types):
        inspect_config = {
            'info_types': [{'name': info_type} for info_type in info_types]
        }
        return inspect_config

    def get_deidentify_config(self, masking_character, number_to_mask):
        deidentify_config = {
            'info_type_transformations': {
                'transformations': [
                    {
                        'primitive_transformation': {
                            'character_mask_config': {
                                'masking_character': masking_character,
                                'number_to_mask': number_to_mask
                            }
                        }
                    }
                ]
            }
        }
        return deidentify_config
