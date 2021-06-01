import logging
import os
import pathlib
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from fnmatch import fnmatch
from glob import glob
from typing import List, Optional
from urllib.parse import urlparse

from collection_manager.entities.exceptions import MissingValueCollectionError

logger = logging.getLogger(__name__)

class CollectionStorageType(Enum):
    LOCAL = 1
    S3 = 2


@dataclass(frozen=True)
class Collection:
    dataset_id: str
    projection: str
    dimension_names: frozenset
    slices: frozenset
    path: str
    historical_priority: int
    forward_processing_priority: Optional[int] = None
    date_from: Optional[datetime] = None
    date_to: Optional[datetime] = None

    @staticmethod
    def from_dict(properties: dict):
        logger.debug(f'incoming properties dict: {properties}')
        try:
            date_to = datetime.fromisoformat(properties['to']) if 'to' in properties else None
            date_from = datetime.fromisoformat(properties['from']) if 'from' in properties else None

            dimension_names = [(k, frozenset(v) if isinstance(v, list) else v) for k, v in properties['dimensionNames'].items()]
            collection = Collection(dataset_id=properties['id'],
                                    projection=properties['projection'],
                                    dimension_names=frozenset(dimension_names),
                                    slices=frozenset(properties['slices'].items()),
                                    path=properties['path'],
                                    historical_priority=properties['priority'],
                                    forward_processing_priority=properties.get('forward-processing-priority', None),
                                    date_to=date_to,
                                    date_from=date_from)
            return collection
        except KeyError as e:
            raise MissingValueCollectionError(missing_value=e.args[0])

    def storage_type(self):
        if urlparse(self.path).scheme == 's3':
            return CollectionStorageType.S3
        else:
            return CollectionStorageType.LOCAL

    def directory(self):
        if urlparse(self.path).scheme == 's3':
            return self.path
        elif os.path.isdir(self.path):
            return self.path
        else:
            return os.path.dirname(self.path)

    def owns_file(self, file_path: str) -> bool:
        if urlparse(file_path).scheme == 's3':
            return file_path.find(self.path) == 0
        else:
            if os.path.isdir(file_path):
                raise IsADirectoryError()

            if os.path.isdir(self.path):
                return pathlib.Path(self.path) in pathlib.Path(file_path).parents
            else:
                return fnmatch(file_path, self.path)
