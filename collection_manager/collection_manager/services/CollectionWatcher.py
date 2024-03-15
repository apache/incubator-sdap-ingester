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
from datetime import datetime
from collection_manager.entities.Collection import CollectionStorageType, Collection
from collection_manager.services.S3Observer import S3Event, S3Observer
import logging
import os
import time
from collections import defaultdict
from glob import glob
from typing import Awaitable, Callable, Dict, List, Optional, Set

import yaml
from collection_manager.entities.exceptions import (CollectionConfigFileNotFoundError,
                                                    CollectionConfigParsingError,
                                                    ConflictingPathCollectionError,
                                                    MissingValueCollectionError,
                                                    RelativePathCollectionError,
                                                    RelativePathError)
from watchdog.events import FileCreatedEvent, FileModifiedEvent, FileSystemEventHandler
from watchdog.observers.polling import PollingObserver as Observer

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class CollectionWatcher:
    def __init__(self,
                 collections_path: str,
                 granule_updated_callback: Callable[[str, Collection], Awaitable],
                 s3_bucket: Optional[str] = None,
                 collections_refresh_interval: float = 30):
        if not os.path.isabs(collections_path):
            raise RelativePathError("Collections config  path must be an absolute path.")

        self._collections_path = collections_path
        self._granule_updated_callback = granule_updated_callback
        self._collections_refresh_interval = collections_refresh_interval

        self._collections_by_dir: Dict[str, Set[Collection]] = defaultdict(set)
        self._observer = S3Observer(s3_bucket, initial_scan=True) if s3_bucket else Observer()

        self._granule_watches = set()

    async def start_watching(self, loop: Optional[asyncio.AbstractEventLoop] = None):
        """
        Periodically load the Collections Configuration file to check for changes,
        and observe filesystem events for added/modified granules. When an event occurs,
        call the appropriate callback that was passed in during instantiation.
        :return: None
        """

        await self._run_periodically(loop=loop,
                                     wait_time=self._collections_refresh_interval,
                                     func=self._reload_and_reschedule)

        if isinstance(self._observer, S3Observer):
            await self._observer.start()
        else:
            self._observer.start()

    def _collections(self) -> Set[Collection]:
        """
        Return a set of all Collections being watched.
        :return: A set of Collections
        """
        return {collection for collections in self._collections_by_dir.values() for collection in collections}

    def _validate_collection(self, collection: Collection):
        if collection.storage_type() == CollectionStorageType.S3:
            # do some S3 path validation here
            return
        else:
            directory = collection.directory()
            if not os.path.isabs(directory):
                raise RelativePathCollectionError(collection=collection)
            if directory == os.path.dirname(self._collections_path):
                raise ConflictingPathCollectionError(collection=collection)

    def _load_collections(self):
        try:
            with open(self._collections_path, 'r') as f:
                collections_yaml = yaml.load(f, Loader=yaml.FullLoader)
            self._collections_by_dir.clear()
            logger.info('Refreshing collection config')

            for collection_dict in collections_yaml['collections']:
                try:
                    collection = Collection.from_dict(collection_dict)
                    if collection.storage_type() != CollectionStorageType.REMOTE:
                        self._validate_collection(collection)
                        self._collections_by_dir[collection.directory()].add(collection)
                except MissingValueCollectionError as e:
                    logger.error(f"A collection is missing '{e.missing_value}'. Ignoring this collection for now.")
                except RelativePathCollectionError as e:
                    logger.error(f"Relative paths are not allowed for the 'path' property of a collection. "
                                 f"Ignoring collection '{e.collection.dataset_id}' until its path is fixed.")
                except ConflictingPathCollectionError as e:
                    logger.error(f"Collection '{e.collection.dataset_id}' has granule path '{e.collection.path}' "
                                 f"which uses same directory as the collection configuration file, "
                                 f"'{self._collections_path}'. The granules need to be in their own directory. "
                                 f"Ignoring collection '{e.collection.dataset_id}' for now.")
        except FileNotFoundError:
            raise CollectionConfigFileNotFoundError("The collection config file could not be found at "
                                                    f"{self._collections_path}")
        except yaml.scanner.ScannerError:
            raise CollectionConfigParsingError("Bad YAML syntax in collection configuration file. Will attempt "
                                               "to reload collections after the next configuration change.")
        except KeyError:
            raise CollectionConfigParsingError("The collections configuration YAML file does not conform to the "
                                               "proper schema. Will attempt to reload collections config after the "
                                               "next file modification.")

    def _get_updated_collections(self) -> Set[Collection]:
        old_collections = self._collections()
        self._load_collections()
        return self._collections() - old_collections

    async def _call_callback_for_all_granules(self, collections: List[Collection]):
        logger.info(f"Scanning files for {len(collections)} collections...")
        start = time.perf_counter()
        for collection in collections:
            for granule_path in self._get_files_at_path(collection.path):
                modified_time = int(os.path.getmtime(granule_path))
                await self._granule_updated_callback(granule_path, modified_time, collection)
        logger.info(f"Finished scanning files in {time.perf_counter() - start} seconds.")

    def _get_files_at_path(self, path: str) -> List[str]:
        if os.path.isfile(path):
            return [path]
        elif os.path.isdir(path):
            return [f for f in glob(path + '/**', recursive=True) if os.path.isfile(f)]
        else:
            return [f for f in glob(path, recursive=True) if os.path.isfile(f)]

    async def _reload_and_reschedule(self):
        try:
            updated_collections = self._get_updated_collections()
            if len(updated_collections) > 0:
                # For S3 collections, the S3Observer will report as new any files that haven't already been scanned.
                # So we only need to rescan granules here if not using S3.
                if not isinstance(self._observer, S3Observer):
                    await self._call_callback_for_all_granules(collections=updated_collections)

                self._unschedule_watches()
                self._schedule_watches()
        except CollectionConfigParsingError as e:
            logger.error(e)

    def _unschedule_watches(self):
        for watch in self._granule_watches:
            self._observer.unschedule(watch)
        self._granule_watches.clear()

    def _schedule_watches(self):
        for directory, collections in self._collections_by_dir.items():
            granule_event_handler = _GranuleEventHandler(asyncio.get_running_loop(),
                                                         self._granule_updated_callback,
                                                         collections)
            # Note: the Watchdog library does not schedule a new watch
            # if one is already scheduled for the same directory
            try:
                if isinstance(self._observer, S3Observer):
                    self._granule_watches.add(self._observer.schedule(granule_event_handler, directory))
                else:
                    self._granule_watches.add(self._observer.schedule(granule_event_handler, directory, recursive=True))
            except (FileNotFoundError, NotADirectoryError):
                bad_collection_names = ' and '.join([col.dataset_id for col in collections])
                logger.error(f"Granule directory {directory} does not exist. Ignoring {bad_collection_names}.")

    @classmethod
    async def _run_periodically(cls,
                                loop: Optional[asyncio.AbstractEventLoop],
                                wait_time: float,
                                func: Callable[[any], Awaitable],
                                *args,
                                **kwargs):
        """
        Call a function periodically. This uses asyncio, and is non-blocking.
        :param loop: An optional event loop to use. If None, the current running event loop will be used.
        :param wait_time: seconds to wait between iterations of func
        :param func: the async function that will be awaited
        :param args: any args that need to be provided to func
        """
        if loop is None:
            loop = asyncio.get_running_loop()
        await func(*args, **kwargs)
        loop.call_later(wait_time, loop.create_task, cls._run_periodically(loop, wait_time, func, *args, **kwargs))


class _GranuleEventHandler(FileSystemEventHandler):
    """
    EventHandler that watches for new or modified granule files.
    """

    def __init__(self,
                 loop: asyncio.AbstractEventLoop,
                 callback: Callable[[str, Collection], Awaitable],
                 collections_for_dir: Set[Collection]):
        self._loop = loop
        self._callback = callback
        self._collections_for_dir = collections_for_dir

    def on_created(self, event):
        super().on_created(event)
        if isinstance(event, S3Event) or not event.is_directory:
            self._handle_event(event)

    def on_modified(self, event):
        super().on_modified(event)

        if isinstance(event, S3Event) or not event.is_directory:
            self._handle_event(event)

    def _handle_event(self, event):
        path = event.src_path
        for collection in self._collections_for_dir:
            try:
                if collection.owns_file(path):
                    if isinstance(event, S3Event):
                        modified_time = int(event.modified_time.timestamp())
                    else:
                        modified_time = int(os.path.getmtime(path))
                    self._loop.create_task(self._callback(path, modified_time, collection))
            except IsADirectoryError:
                return
