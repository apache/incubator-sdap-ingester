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

import asyncio
import functools
import math
import json
import logging
from asyncio import AbstractEventLoop
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Union, Tuple, Optional

import pysolr
from kazoo.exceptions import NoNodeError
from kazoo.handlers.threading import KazooTimeoutError
from tenacity import retry, stop_after_attempt, wait_exponential

from common.async_utils.AsyncUtils import run_in_executor
from granule_ingester.exceptions import (SolrFailedHealthCheckError,
                                         SolrLostConnectionError)
from granule_ingester.writers.MetadataStore import MetadataStore
from nexusproto.DataTile_pb2 import NexusTile, TileSummary

logger = logging.getLogger(__name__)

MAX_BATCH_SIZE = 128


class SolrStore(MetadataStore):
    def __init__(self, solr_url=None, zk_url=None):
        super().__init__()

        self.TABLE_NAME = "sea_surface_temp"
        self.iso: str = '%Y-%m-%dT%H:%M:%SZ'

        self._solr_url = solr_url
        self._zk_url = zk_url
        self.geo_precision: int = 3
        self._collection: str = "nexustiles"
        self.log: logging.Logger = logging.getLogger(__name__)
        self._solr: Optional[pysolr.Solr] = None
        self._zk: Optional[pysolr.ZooKeeper] = None

    def _get_collections(self, zk, parent_nodes):
        """
            try to get list of collection from zookeper, on a list of candidate nodes,
            return the first successful request result
        """

        try:
            logger.debug("getting solr configuration from zookeeper, node '%s'", parent_nodes[0])
            return parent_nodes[0], zk.zk.get_children(parent_nodes[0])
        except NoNodeError:
            logger.debug("solr configuration not found in node '%s'", parent_nodes[0])
            if len(parent_nodes)>1:
                return self._get_collections(zk, parent_nodes[1:])
            else:
                raise

    def _set_solr_status(self, zk):
        """ because of something not working right between zookeeper and solr
            we need to  manually update the solr status on zookeeper
            see https://github.com/django-haystack/pysolr/issues/189
        """
        collections = {}
        parent_node, zk_collections = self._get_collections(zk,
                                                            ['collections',
                                                             'solr/collections']
                                                            # with bitnami/solr 0.3.3 helm chart deployment
                                                            )

        for c in zk_collections:
            collections.update(json.loads(zk.zk.get(f"{parent_node}/{c}/state.json")[0].decode("utf-8")))
        zk.collections = collections

    def _get_connection(self) -> Tuple[pysolr.Solr, Union[pysolr.ZooKeeper, None]]:
        if self._zk_url:
            zk = pysolr.ZooKeeper(f"{self._zk_url}")
            self._set_solr_status(zk)
            return pysolr.SolrCloud(zk, self._collection, always_commit=True), zk
        elif self._solr_url:
            return pysolr.Solr(f'{self._solr_url}/solr/{self._collection}', always_commit=True), None
        else:
            raise RuntimeError("You must provide either solr_host or zookeeper_host.")

    def connect(self, loop: AbstractEventLoop = None):
        self._solr, self._zk = self._get_connection()

    def close(self):
        if self._solr is not None:
            self._solr.get_session().close()

        if self._zk is not None:
            self._zk.zk.stop()
            self._zk.zk.close()

    async def health_check(self):
        try:
            connection, _ = self._get_connection()
            connection.ping()

            self.close()
        except pysolr.SolrError:
            raise SolrFailedHealthCheckError("Cannot connect to Solr!")
        except NoNodeError:
            raise SolrFailedHealthCheckError("Connected to Zookeeper but cannot connect to Solr!")
        except KazooTimeoutError:
            raise SolrFailedHealthCheckError("Cannot connect to Zookeeper!")

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=12))
    async def save_metadata(self, nexus_tile: NexusTile) -> None:
        solr_doc = self._build_solr_doc(nexus_tile)
        logger.debug(f'solr_doc: {solr_doc}')
        await self._save_document(solr_doc)

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=12))
    async def save_batch(self, tiles: List[NexusTile]) -> None:
        solr_docs = [self._build_solr_doc(nexus_tile) for nexus_tile in tiles]
        logger.info(f'Writing {len(solr_docs)} metadata items to Solr')
        thetime = datetime.now()

        batches = [solr_docs[i:i+MAX_BATCH_SIZE] for i in range(0, len(solr_docs), MAX_BATCH_SIZE)]

        n_tiles = len(tiles)
        writing = 0

        for batch in batches:
            writing += len(batch)

            logger.info(f"Writing batch of {len(batch)} documents | ({writing}/{n_tiles}) [{writing/n_tiles*100:7.3f}%]")
            await self._save_document(batch)
        logger.info(f'Wrote {len(solr_docs)} metadata items to Solr in {str(datetime.now() - thetime)} seconds')

    @run_in_executor
    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=12))
    def _save_document(self, doc: Union[dict, list]):
        try:
            if not isinstance(doc, list):
                doc = [doc]

            self._solr.add(doc)
        except pysolr.SolrError as e:
            logger.warning("Failed to save metadata document to Solr")
            logger.exception(f'May have lost connection to Solr, and cannot save tiles. cause: {e}. creating SolrLostConnectionError')
            raise SolrLostConnectionError(f'Lost connection to Solr, and cannot save tiles. cause: {e}')

    def _build_solr_doc(self, tile: NexusTile) -> Dict:
        summary: TileSummary = tile.summary
        bbox: TileSummary.BBox = summary.bbox
        stats: TileSummary.DataStats = summary.stats

        min_time = datetime.strftime(datetime.utcfromtimestamp(stats.min_time), self.iso)
        max_time = datetime.strftime(datetime.utcfromtimestamp(stats.max_time), self.iso)
        day_of_year = datetime.utcfromtimestamp(stats.min_time).timetuple().tm_yday
        geo = self.determine_geo(bbox)

        granule_file_name: str = Path(summary.granule).name  # get base filename

        tile_type = tile.tile.WhichOneof("tile_type")
        tile_data = getattr(tile.tile, tile_type)

        var_names = json.loads(summary.data_var_name)
        standard_names = []
        if summary.standard_name:
            standard_names = json.loads(summary.standard_name)
        if not isinstance(var_names, list):
            var_names = [var_names]
        if not isinstance(standard_names, list):
            standard_names = [standard_names]

        input_document = {
            'table_s': self.TABLE_NAME,
            'geo': geo,
            'id': summary.tile_id,
            'solr_id_s': '{ds_name}!{tile_id}'.format(ds_name=summary.dataset_name, tile_id=summary.tile_id),
            'sectionSpec_s': summary.section_spec,
            'dataset_s': summary.dataset_name,
            'granule_s': granule_file_name,
            'tile_var_name_ss': var_names,
            'day_of_year_i': day_of_year,
            'tile_min_lon': bbox.lon_min,
            'tile_max_lon': bbox.lon_max,
            'tile_min_lat': bbox.lat_min,
            'tile_max_lat': bbox.lat_max,
            'tile_min_elevation_d': tile_data.min_elevation,
            'tile_max_elevation_d': tile_data.max_elevation,
            'tile_min_time_dt': min_time,
            'tile_max_time_dt': max_time,
            'tile_min_val_d': stats.min,
            'tile_max_val_d': stats.max,
            'tile_avg_val_d': stats.mean,
            'tile_count_i': int(stats.count)
        }

        for var_name, standard_name in zip(var_names, standard_names):
            if standard_name:
                input_document[f'{var_name}.tile_standard_name_s'] = standard_name

        ecco_tile_id = getattr(tile_data, 'tile', None)
        if ecco_tile_id:
            input_document['ecco_tile'] = ecco_tile_id

        for attribute in summary.global_attributes:
            input_document[attribute.getName()] = attribute.getValues(
                0) if attribute.getValuesCount() == 1 else attribute.getValuesList()

        return input_document

    @staticmethod
    def _format_latlon_string(value, rounding=0):
        if rounding == 0:
            rounded_value = round(value, 3)
        elif rounding == 1:
            rounded_value = math.ceil(value * 1000)/1000.0
        else:
            rounded_value = math.floor(value * 1000) / 1000.0
        return '{:.3f}'.format(rounded_value)

    @classmethod
    def determine_geo(cls, bbox: TileSummary.BBox) -> str:
        # Solr cannot index a POLYGON where all corners are the same point or when there are only
        # 2 distinct points (line). Solr is configured for a specific precision so we need to round
        # to that precision before checking equality.
        lat_min_str = cls._format_latlon_string(bbox.lat_min)
        lat_max_str = cls._format_latlon_string(bbox.lat_max)
        lon_min_str = cls._format_latlon_string(bbox.lon_min)
        lon_max_str = cls._format_latlon_string(bbox.lon_max)

        # If lat min = lat max and lon min = lon max, index the 'geo' bounding box as a POINT instead of a POLYGON
        if bbox.lat_min == bbox.lat_max and bbox.lon_min == bbox.lon_max:
            geo = 'POINT({} {})'.format(lon_min_str, lat_min_str)
        # If lat min = lat max but lon min != lon max, or lon min = lon max but lat min != lat max,
        # then we essentially have a line.
        elif bbox.lat_min == bbox.lat_max or bbox.lon_min == bbox.lon_max:
            # If the rounding makes the non-equal coordinates equal, ensure they're expanded so we have a line and not
            # a point
            if bbox.lat_min == bbox.lat_max and lon_min_str == lon_max_str:
                lon_min_str = cls._format_latlon_string(bbox.lon_min, rounding=-1)
                lon_max_str = cls._format_latlon_string(bbox.lon_max, rounding=1)
            elif bbox.lon_min == bbox.lon_max and lat_min_str == lat_max_str:
                lat_min_str = cls._format_latlon_string(bbox.lat_min, rounding=-1)
                lat_max_str = cls._format_latlon_string(bbox.lat_max, rounding=1)

            geo = 'LINESTRING({} {}, {} {})'.format(lon_min_str, lat_min_str, lon_max_str, lat_max_str)
        # All other cases should use POLYGON
        else:
            # If the rounding makes any min/max coord pairs equal, expand them so the polygon doesn't collapse into a
            # line
            if lon_min_str == lon_max_str:
                lon_min_str = cls._format_latlon_string(bbox.lon_min, rounding=-1)
                lon_max_str = cls._format_latlon_string(bbox.lon_max, rounding=1)

            if lat_min_str == lat_max_str:
                lat_min_str = cls._format_latlon_string(bbox.lat_min, rounding=-1)
                lat_max_str = cls._format_latlon_string(bbox.lat_max, rounding=1)

            geo = 'POLYGON(({} {}, {} {}, {} {}, {} {}, {} {}))'.format(lon_min_str, lat_min_str,
                                                                        lon_max_str, lat_min_str,
                                                                        lon_max_str, lat_max_str,
                                                                        lon_min_str, lat_max_str,
                                                                        lon_min_str, lat_min_str)

        return geo
