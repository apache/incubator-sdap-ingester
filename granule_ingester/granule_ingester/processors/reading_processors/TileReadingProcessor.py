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
import datetime
import json
import logging
from abc import ABC, abstractmethod
from typing import Dict, Union

import numpy as np
import xarray as xr
from granule_ingester.exceptions import TileProcessingError
from granule_ingester.processors.TileProcessor import TileProcessor
from nexusproto import DataTile_pb2 as nexusproto

logger = logging.getLogger(__name__)


class TileReadingProcessor(TileProcessor, ABC):

    def __init__(
            self,
            variable: Union[str, list],
            latitude: str,
            longitude: str,
            height: str = None,
            depth: str = None,
            *args,
            **kwargs
    ):
        try:
            self.variable = json.loads(variable)
        except Exception as e:
            logger.exception(f'failed to convert literal list to python list. using as a single variable: {variable}')
            self.variable = variable
        if isinstance(self.variable, list) and len(self.variable) < 1:
            logger.error(f'variable list is empty: {TileReadingProcessor}')
            raise RuntimeError(f'variable list is empty: {self.variable}')
        self.latitude = latitude
        self.longitude = longitude

        if height:
            self.height = height
            self.invert_z = False
            if depth:
                logger.warning('Both height and depth dimensions were specified. Favoring height')
        elif depth:
            self.height = depth
            self.invert_z = True
        else:
            self.height = None
            self.invert_z = None

        # self.invert_z: if depth is specified instead of height, multiply it by -1, so it becomes height

    def process(self, tile, dataset: xr.Dataset, *args, **kwargs):
        logger.debug(f'Reading Processor: {type(self)}')
        try:
            dimensions_to_slices = self._convert_spec_to_slices(tile.summary.section_spec)

            output_tile = nexusproto.NexusTile()
            output_tile.CopyFrom(tile)
            output_tile.summary.data_var_name = json.dumps(self.variable)

            return self._generate_tile(dataset, dimensions_to_slices, output_tile)
        except Exception as e:
            logger.exception(e)
            raise TileProcessingError(f"Could not generate tiles from the granule because of the following error: {e}.")

    @abstractmethod
    def _generate_tile(self, dataset: xr.Dataset, dimensions_to_slices: Dict[str, slice], tile):
        pass

    @classmethod
    def _parse_input(cls, the_input_tile, temp_dir):
        specs = the_input_tile.summary.section_spec
        tile_specifications = cls._convert_spec_to_slices(specs)

        file_path = the_input_tile.summary.granule
        file_path = file_path[len('file:'):] if file_path.startswith('file:') else file_path

        return tile_specifications, file_path

    @staticmethod
    def _slices_for_variable(variable: xr.DataArray, dimension_to_slice: Dict[str, slice]) -> Dict[str, slice]:
        return {dim_name: dimension_to_slice[dim_name] for dim_name in variable.dims}

    @staticmethod
    def _convert_spec_to_slices(spec):
        dim_to_slice = {}
        for dimension in spec.split(','):
            name, start, stop = dimension.split(':')
            dim_to_slice[name] = slice(int(start), int(stop))

        return dim_to_slice

    @staticmethod
    def _convert_to_timestamp(times: xr.DataArray) -> xr.DataArray:
        if times.dtype.type == np.timedelta64:  # If time is an array of offsets from a fixed reference
            reference = times.time.item() / 1e9  # Get the base time in seconds

            times = (times / 1e9).astype(int)  # Convert offset array to seconds
            times = times.where(times != -9223372036854775808)  # Replace NaT values with NaN

            return times + reference  # Add base to offsets
        elif np.issubdtype(times.dtype, np.number):
            return times

        epoch = np.datetime64(datetime.datetime(1970, 1, 1, 0, 0, 0))
        return ((times - epoch) / 1e9).astype(int)
