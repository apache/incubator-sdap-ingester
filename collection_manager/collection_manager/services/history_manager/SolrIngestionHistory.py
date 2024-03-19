# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import hashlib
import logging
from datetime import datetime
import pysolr
import requests
from collection_manager.services.history_manager.IngestionHistory import (IngestionHistory, IngestionHistoryBuilder)
from common.async_utils.AsyncUtils import run_in_executor

logging.getLogger("pysolr").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def doc_key(dataset_id, file_name):
    return hashlib.sha1(f'{dataset_id}{file_name}'.encode('utf-8')).hexdigest()


class SolrIngestionHistoryBuilder(IngestionHistoryBuilder):
    def __init__(self, solr_url: str, signature_fun=None):
        self._solr_url = solr_url
        self._signature_fun = signature_fun

    def build(self, dataset_id: str):
        return SolrIngestionHistory(solr_url=self._solr_url,
                                    dataset_id=dataset_id,
                                    signature_fun=self._signature_fun)


class SolrIngestionHistory(IngestionHistory):
    _granule_collection_name = "nexusgranules"
    _dataset_collection_name = "nexusdatasets"
    _req_session = None

    def __init__(self, solr_url: str, dataset_id: str, signature_fun=None):
        try:
            self._url_prefix = f"{solr_url.strip('/')}/solr"
            self._create_collection_if_needed()
            self._solr_granules = pysolr.Solr(f"{self._url_prefix}/{self._granule_collection_name}")
            self._solr_datasets = pysolr.Solr(f"{self._url_prefix}/{self._dataset_collection_name}")
            self._dataset_id = dataset_id
            self._signature_fun = signature_fun
            self._latest_ingested_file_update = self._get_latest_file_update()
        except requests.exceptions.RequestException:
            raise DatasetIngestionHistorySolrException(f"solr instance unreachable {solr_url}")

    def __del__(self):
        self._req_session.close()

    @run_in_executor
    def _push_record(self, file_name, signature):
        hash_id = doc_key(self._dataset_id, file_name)
        self._solr_granules.delete(q=f"id:{hash_id}")
        self._solr_granules.add([{
            'id': hash_id,
            'dataset_s': self._dataset_id,
            'granule_s': file_name,
            'granule_signature_s': signature}])
        self._solr_granules.commit()
        return None

    @run_in_executor
    def _save_latest_timestamp(self):
        if self._solr_datasets:
            self._solr_datasets.delete(q=f"id:{self._dataset_id}")
            self._solr_datasets.add([{
                'id': self._dataset_id,
                'dataset_s': self._dataset_id,
                'latest_update_l': self._latest_ingested_file_update}])
            self._solr_datasets.commit()

    @run_in_executor
    def _push_dataset(self, dataset_id, type, config):
        if self._solr_datasets:
            if len(self._solr_datasets.search(q=f'id:{dataset_id}')) == 0:
                self._solr_datasets.add([{
                    'id': dataset_id,
                    'dataset_s': dataset_id,
                    'latest_update_l': int(datetime.now().timestamp()),
                    'store_type_s': type,
                    'config': config,
                    'source_s': 'collection_config'
                }])
                self._solr_datasets.commit()

    def _get_latest_file_update(self):
        results = self._solr_datasets.search(q=f"id:{self._dataset_id}")
        if results:
            return results.docs[0]['latest_update_l']
        else:
            return None

    @run_in_executor
    def _get_signature(self, file_name):
        hash_id = doc_key(self._dataset_id, file_name)
        results = self._solr_granules.search(q=f"id:{hash_id}")
        if results:
            return results.docs[0]['granule_signature_s']
        else:
            return None

    def _create_collection_if_needed(self):
        try:
            if not self._req_session:
                self._req_session = requests.session()

            payload = {'action': 'CLUSTERSTATUS'}
            collections_endpoint = f"{self._url_prefix}/admin/collections"
            result = self._req_session.get(collections_endpoint, params=payload)
            response = result.json()
            node_number = len(response['cluster']['live_nodes'])

            existing_collections = response['cluster']['collections'].keys()

            if self._granule_collection_name not in existing_collections:
                # Create collection
                payload = {'action': 'CREATE',
                           'name': self._granule_collection_name,
                           'numShards': node_number
                           }
                result = self._req_session.get(collections_endpoint, params=payload)
                response = result.json()
                logger.info(f"solr collection created {response}")

                # Update schema
                schema_endpoint = f"{self._url_prefix}/{self._granule_collection_name}/schema"
                self._add_field(schema_endpoint, "dataset_s", "string")
                self._add_field(schema_endpoint, "granule_s", "string")
                self._add_field(schema_endpoint, "granule_signature_s", "string")

            if self._dataset_collection_name not in existing_collections:
                # Create collection
                payload = {'action': 'CREATE',
                           'name': self._dataset_collection_name,
                           'numShards': node_number
                           }
                result = self._req_session.get(collections_endpoint, params=payload)
                response = result.json()
                logger.info(f"solr collection created {response}")

                # Update schema
                schema_endpoint = f"{self._url_prefix}/{self._dataset_collection_name}/schema"
                self._add_field(schema_endpoint, "dataset_s", "string")
                self._add_field(schema_endpoint, "latest_update_l", "TrieLongField")
                self._add_field(schema_endpoint, "store_type_s", "string", True)
                self._add_field(schema_endpoint, "source_s", "string", True)
                self._add_field(schema_endpoint, "config", "text_general", True)

        except requests.exceptions.RequestException as e:
            logger.error(f"solr instance unreachable {self._solr_url}")
            raise e

    def _add_field(self, schema_url, field_name, field_type, stored=False):
        """
        Helper to add a string field in a solr schema
        :param schema_url:
        :param field_name:
        :param field_type
        :return:
        """
        add_field_payload = {
            "add-field": {
                "name": field_name,
                "type": field_type,
                "stored": stored
            }
        }
        return self._req_session.post(schema_url, data=str(add_field_payload).encode('utf-8'))


class DatasetIngestionHistorySolrException(Exception):
    pass
