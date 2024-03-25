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

from abc import ABC, abstractmethod

from nexusproto import DataTile_pb2 as nexusproto

from granule_ingester.healthcheck import HealthCheck

from typing import List


class DataStore(HealthCheck, ABC):
    @abstractmethod
    def connect(self):
        ...

    @abstractmethod
    def close(self):
        ...

    def __del__(self):
        self.close()


    @abstractmethod
    def save_data(self, nexus_tile: nexusproto.NexusTile) -> None:
        pass

    @abstractmethod
    def save_batch(self, tiles: List[nexusproto.NexusTile]) -> None:
        pass

