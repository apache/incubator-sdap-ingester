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
import logging
import uuid

from datetime import datetime

from tenacity import retry, stop_after_attempt, wait_exponential

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, Session, NoHostAvailable, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model
from cassandra.policies import RetryPolicy, ConstantReconnectionPolicy
from cassandra.query import BatchStatement, ConsistencyLevel
from nexusproto.DataTile_pb2 import NexusTile, TileData

from granule_ingester.exceptions import CassandraFailedHealthCheckError, CassandraLostConnectionError
from granule_ingester.writers.DataStore import DataStore

from typing import List
from time import sleep

logging.getLogger('cassandra').setLevel(logging.INFO)
logger = logging.getLogger(__name__)

MAX_BATCH_SIZE = 1024



class TileModel(Model):
    __keyspace__ = "nexustiles"
    __table_name__ = "sea_surface_temp"
    tile_id = columns.UUID(primary_key=True)
    tile_blob = columns.Bytes(index=True)


class CassandraStore(DataStore):
    def __init__(self, contact_points=None, port=9042, keyspace='nexustiles', username=None, password=None):
        self._contact_points = contact_points
        self._username = username
        self._password = password
        self._port = port
        self._keyspace = keyspace
        self._session = None

    async def health_check(self) -> bool:
        try:
            session = self._get_session()
            session.shutdown()
            return True
        except Exception:
            raise CassandraFailedHealthCheckError("Cannot connect to Cassandra!")

    def _get_session(self) -> Session:

        if self._username and self._password:
            auth_provider = PlainTextAuthProvider(username=self._username, password=self._password)
        else:
            auth_provider = None

        cluster = Cluster(contact_points=self._contact_points,
                          port=self._port,
                          # load_balancing_policy=
                          execution_profiles={
                              EXEC_PROFILE_DEFAULT: ExecutionProfile(
                                  request_timeout=60.0,
                                  retry_policy=RetryPolicy()
                              )
                          },
                          reconnection_policy=ConstantReconnectionPolicy(delay=5.0),
                          auth_provider=auth_provider)
        session = cluster.connect()
        session.set_keyspace(self._keyspace)
        return session

    def connect(self):
        self._session = self._get_session()

    def close(self):
        session: Session = self._session
        if session is not None:
            cluster = session.cluster

            session.shutdown()
            cluster.shutdown()

            del cluster, session

            self._session = None

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=3, max=30))
    async def save_data(self, tile: NexusTile) -> None:
        try:
            tile_id = uuid.UUID(tile.summary.tile_id)
            serialized_tile_data = TileData.SerializeToString(tile.tile)
            prepared_query = self._session.prepare("INSERT INTO sea_surface_temp (tile_id, tile_blob) VALUES (?, ?)")
            await self._execute_query_async(self._session, prepared_query,
                                            [tile_id, bytearray(serialized_tile_data)])
        except NoHostAvailable:
            logger.warning("Failed to save tile data to Cassandra")
            raise CassandraLostConnectionError(f"Lost connection to Cassandra, and cannot save tiles.")

    @retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=1, max=12))
    async def save_batch(self, tiles: List[NexusTile]) -> None:
        logger.info(f'Writing {len(tiles)} tiles to Cassandra')
        thetime = datetime.now()

        batches = [tiles[i:i + MAX_BATCH_SIZE] for i in range(0, len(tiles), MAX_BATCH_SIZE)]
        prepared_query = self._session.prepare("INSERT INTO sea_surface_temp (tile_id, tile_blob) VALUES (?, ?)")

        n_tiles = len(tiles)
        writing = 0

        for batch in batches:
            writing += len(batch)

            logger.info(f'Writing batch of {len(batch)} tiles to Cassandra | ({writing}/{n_tiles}) [{writing/n_tiles*100:7.3f}%]')

            while len(batch) > 0:
                futures = []
                failed = []

                for tile in batch:
                    tile_id = uuid.UUID(tile.summary.tile_id)
                    serialized_tile_data = TileData.SerializeToString(tile.tile)

                    cassandra_future = self._session.execute_async(prepared_query, [tile_id, bytearray(serialized_tile_data)])
                    asyncio_future = asyncio.Future()
                    cassandra_future.add_callbacks(asyncio_future.set_result, asyncio_future.set_exception)

                    futures.append((tile, asyncio_future))

                for t, f in futures:
                    try:
                        await f
                    except Exception:
                        failed.append(t)

                if len(failed) > 0:
                    logger.warning(f'Need to retry {len(failed)} tiles')
                    sleep(10)

                batch = failed

        logger.info(f'Wrote {len(tiles)} tiles to Cassandra in {str(datetime.now() - thetime)} seconds')

    @staticmethod
    async def _execute_query_async(session: Session, query, parameters=None):
        cassandra_future = session.execute_async(query, parameters)
        asyncio_future = asyncio.Future()
        cassandra_future.add_callbacks(asyncio_future.set_result, asyncio_future.set_exception)
        return await asyncio_future
