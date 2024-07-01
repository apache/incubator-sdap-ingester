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

import json
import logging
import os.path
from typing import Dict, Optional

import yaml
from collection_manager.entities import Collection
from collection_manager.services import MessagePublisher
from collection_manager.services.history_manager import (GranuleStatus,
                                                         IngestionHistory,
                                                         SolrIngestionHistory)
from collection_manager.services.history_manager.IngestionHistory import \
    IngestionHistoryBuilder

logger = logging.getLogger(__name__)

SUPPORTED_FILE_EXTENSIONS = ['.nc', '.nc4', '.h5']


class CollectionProcessor:

    def __init__(self, message_publisher: MessagePublisher, history_manager_builder: IngestionHistoryBuilder):
        self._publisher = message_publisher
        self._history_manager_builder = history_manager_builder
        self._history_manager_cache: Dict[str, IngestionHistory] = {}

    async def process_granule(self, granule: str, modified_time: int, collection: Collection):
        """
        Determine whether a granule needs to be ingested, and if so publish a RabbitMQ message for it.
        :param granule: A path to a granule file
        :param collection: A Collection against which to evaluate the granule
        :return: None
        """
        if not self._file_supported(granule):
            logger.warning(f'Tried to process unsupported file {granule}. Skipping.')
            return

        history_manager = self._get_history_manager(collection.dataset_id)
        granule_status = await history_manager.get_granule_status(granule,
                                                                  modified_time,
                                                                  collection.date_from,
                                                                  collection.date_to)

        if granule_status is GranuleStatus.DESIRED_FORWARD_PROCESSING:
            logger.info(f"New granule '{granule}' detected for forward-processing ingestion "
                        f"in collection '{collection.dataset_id}'.")
            if collection.forward_processing_priority is not None:
                use_priority = collection.forward_processing_priority
            else:
                use_priority = collection.historical_priority
        elif granule_status is GranuleStatus.DESIRED_HISTORICAL:
            logger.info(f"New granule '{granule}' detected for historical ingestion in collection "
                        f"'{collection.dataset_id}'.")
            use_priority = collection.historical_priority
        else:
            logger.debug(f"Granule '{granule}' detected but has already been ingested in "
                         f"collection '{collection.dataset_id}'. Skipping.")
            return

        dataset_config = self._generate_ingestion_message(granule, collection)
        await self._publisher.publish_message(body=dataset_config, priority=use_priority)
        await history_manager.push(granule, modified_time)

    def add_plugin_collection(self, collection: Collection):
        history_manager = self._get_history_manager(None)

        if isinstance(history_manager, SolrIngestionHistory):
            collection_config = {
                'path': collection.path,
                'config': collection.config
            }

            collection_dimensions = dict(collection.dimension_names)

            collection_config['config']['variables'] = collection_dimensions['variable']
            collection_config['config']['coords'] = {dim: collection_dimensions[dim]
                                                     for dim in collection_dimensions if dim != 'variable'}

            history_manager._push_dataset(collection.dataset_id, collection.store_type, json.dumps(collection_config))

    @staticmethod
    def _file_supported(file_path: str):
        ext = os.path.splitext(file_path)[-1]
        return ext in SUPPORTED_FILE_EXTENSIONS

    def _get_history_manager(self, dataset_id: Optional[str]) -> IngestionHistory:
        if dataset_id not in self._history_manager_cache:
            self._history_manager_cache[dataset_id] = self._history_manager_builder.build(dataset_id=dataset_id)
        return self._history_manager_cache[dataset_id]


    @staticmethod
    def _get_default_processors(collection: Collection):
        processors = [
            {
                'name': collection.projection,
                **dict(collection.dimension_names),
            },
            {'name': 'emptyTileFilter'},
            {'name': 'subtract180FromLongitude'}
        ]

        if collection.projection == 'Grid':
            processors.append({'name': 'forceAscendingLatitude'})

        processors.extend([
            {
                'name': 'kelvinToCelsius'
            },
            {
                'name': 'verifyShape'
            },
            {
                'name': 'tileSummary',
                'dataset_name': collection.dataset_id
            },
            {
                'name': 'generateTileId'
            },
        ])

        return processors
    

    @staticmethod
    def _generate_ingestion_message(granule_path: str, collection: Collection) -> str:

        config_dict = {
            'granule': {
                'resource': granule_path
            },
            'slicer': {
                'name': 'sliceFileByStepSize',
                'dimension_step_sizes': dict(collection.slices)
            },
            'processors': CollectionProcessor._get_default_processors(collection)
        }

        if collection.preprocess is not None:
            config_dict['preprocess'] = json.loads(collection.preprocess)

        if collection.processors is not None:
            config_dict['processors'].extend(json.loads(collection.processors))

        if collection.group is not None:
            config_dict['granule']['group'] = collection.group

        config_str = yaml.dump(config_dict)
        logger.debug(f"Templated dataset config:\n{config_str}")
        return config_str
