from gc import collect
import hashlib
from importlib.resources import path
import logging
from tkinter import W
from attr import s

import pysolr
import requests
from collection_manager.services.history_manager.IngestionHistory import (IngestionHistory, IngestionHistoryBuilder)
from collection_manager.entities.Collection import Collection
from collections import defaultdict
from common.async_utils.AsyncUtils import run_in_executor
from typing import Awaitable, Callable, Dict, List, Optional, Set

import yaml

logging.getLogger("pysolr").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def doc_key(dataset_id, file_name):
    return hashlib.sha1(f'{dataset_id}{file_name}'.encode('utf-8')).hexdigest()


class ZarrSolrIngestionHistoryBuilder(IngestionHistoryBuilder):
    def __init__(self, solr_url: str, collections_path: str, signature_fun=None):
        self._solr_url = solr_url
        self._signature_fun = signature_fun
        self.collections_path = collections_path

    def build(self, dataset_id: str):
        return ZarrSolrIngestionHistory(solr_url=self._solr_url,
                                    dataset_id=dataset_id,
                                    collections_path=self.collections_path,
                                    signature_fun=self._signature_fun)


class ZarrSolrIngestionHistory(IngestionHistory): 
    _granule_collection_name = "zarrgranules"
    _dataset_collection_name = "zarrdatasets"
    _req_session = None

    def __init__(self, solr_url: str, dataset_id: str, collections_path: str, signature_fun=None):
        try:
            self._url_prefix = f"{solr_url.strip('/')}/solr"
            # TODO check method
            self._create_collection_if_needed()
            self.collections_path = collections_path
            self.collections_by_dir: Dict[collections_path, Set[Collection]] = defaultdict(set)
            self._solr_granules = pysolr.Solr(f"{self._url_prefix}/{self._granule_collection_name}")
            self._solr_datasets = pysolr.Solr(f"{self._url_prefix}/{self._dataset_collection_name}")
            self._dataset_id = dataset_id
            self._signature_fun = signature_fun
            self._latest_ingested_file_update = self._get_latest_file_update()
            self._solr_url = solr_url
            self.collections_path = collections_path
        except requests.exceptions.RequestException:
            raise DatasetIngestionHistorySolrException(f"solr instance unreachable {solr_url}")

    def __del__(self):
        self._req_session.close()

    @run_in_executor
    def load_dataset_metadata(self):    # retrieve metadata from respective dataset in config yaml
        try:
            with open(self._collections_path, 'r') as f:
                collections_yaml = yaml.load(f, Loader=yaml.FullLoader)
            self._collections_by_dir.clear()
            for collection_dict in collections_yaml['collections']:
                try:
                    collection = Collection.from_dict(collection_dict)
                    if collection['id'] == self.dataset_id:
                        return collection
                except:
                    print("INNER LOOP ERROR")   #TODO add error handling
        except:
            print("OUTER LOOP ERROR")   #TODO add errors handling
        
        return None     
    
    def retrieve_variable_lists(self, collection):  # returns array of lists with variable and their fill values
        var_arr = [{"name_s": collection['dimensionNames']['variable'], 
                      "fill_d": collection['dimensionNames']['fill_value']}]
        return var_arr
    
    def retrieve_chunk_size(self, collection):
        chunkSize = [collection['slices']['time'], collection['slices']['lat'],
                         collection['slices']['lon']]
        return chunkSize
                     
    @run_in_executor
    def _push_record(self, file_name, signature):   # granule-level JSON entry
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
    def _save_latest_timestamp(self):   # dataset-level JSON entry
        if self._solr_datasets:
            collection = self.load_dataset_metadata
            self._solr_datasets.delete(q=f"id:{self._dataset_id}")
            chunkSize = [collection['slices']['time'], collection['slices']['lat'],
                         collection['slices']['lon']]
            self._solr_datasets.add([{
                'id': self._dataset_id,
                'latest_update_l': self._latest_ingested_file_update,
                'dataset_s': self._dataset_id,
                'variables': self.retrieve_variable_lists(collection),
                's3_url_s': collection['path'],
                'public_b': False, # TODO support for public buckets, make this dynamic
                'type_s': collection['projection'],
                'chunk_shape': self.retrieve_chunk_size(collection)}]) 
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
                self._add_field(schema_endpoint, "latest_update_l", "TrieLongField")     # TODO TrieLongField is depricated
                self._add_field(schema_endpoint, "dataset_s", "string")
                self._add_field(schema_endpoint, "variables", "list")
                self._add_field(schema_endpoint, "s2_uri_s", "string")
                self._add_field(schema_endpoint, "public_b", "bool")     
                self._add_field(schema_endpoint, "type_s", "string")
                self._add_field(schema_endpoint, "chunk_shape", "list")
        except requests.exceptions.RequestException as e:
            logger.error(f"solr instance unreachable {self._solr_url}")
            raise e

    def _add_field(self, schema_url, field_name, field_type):
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
                "stored": False
            }
        }
        return self._req_session.post(schema_url, data=str(add_field_payload).encode('utf-8'))


class DatasetIngestionHistorySolrException(Exception):
    pass
    