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

import logging
from typing import Dict

import numpy as np
import xarray as xr
from granule_ingester.processors.reading_processors.MultiBandUtils import MultiBandUtils
from nexusproto import DataTile_pb2 as nexusproto
from nexusproto.serialization import to_shaped_array

from granule_ingester.processors.reading_processors.TileReadingProcessor import TileReadingProcessor

logger = logging.getLogger(__name__)


class SwathMultiVariableReadingProcessor(TileReadingProcessor):
    def __init__(
            self,
            variable,
            latitude,
            longitude,
            height=None,
            depth=None,
            time=None,
            **kwargs
    ):
        super().__init__(variable, latitude, longitude, height, depth, **kwargs)
        self.time = time

    def _generate_tile(self, ds: xr.Dataset, dimensions_to_slices: Dict[str, slice], input_tile):
        if not isinstance(self.variable, list):
            raise ValueError(f'self.variable `{self.variable}` needs to be a list. use SwathReadingProcessor for single band Swath files.')
        logger.debug(f'reading as banded swath as self.variable is a list. self.variable: {self.variable}')
        if len(self.variable) < 1:
            raise ValueError(f'list of variable is empty. Need at least 1 variable')

        new_tile = nexusproto.SwathMultiVariableTile()
        lat_subset = ds[self.latitude][type(self)._slices_for_variable(ds[self.latitude], dimensions_to_slices)]
        lon_subset = ds[self.longitude][type(self)._slices_for_variable(ds[self.longitude], dimensions_to_slices)]
        lat_subset = np.ma.filled(lat_subset, np.NaN)
        lon_subset = np.ma.filled(lon_subset, np.NaN)

        time_subset = ds[self.time][type(self)._slices_for_variable(ds[self.time], dimensions_to_slices)]
        time_subset = np.ma.filled(type(self)._convert_to_timestamp(time_subset), np.NaN)

        data_subset = [ds[k][type(self)._slices_for_variable(ds[k], dimensions_to_slices)] for k in self.variable]
        updated_dims, updated_dims_indices = MultiBandUtils.move_band_dimension(list(data_subset[0].dims))
        data_subset = [ds.data for ds in data_subset]
        data_subset = np.array(data_subset)
        logger.debug(f'transposing data_subset')
        data_subset = data_subset.transpose(updated_dims_indices)
        logger.debug(f'adding summary.data_dim_names')
        input_tile.summary.data_dim_names.extend(updated_dims)

        if self.height:
            depth_dim, depth_slice = list(type(self)._slices_for_variable(ds[self.height],
                                                                          dimensions_to_slices).items())[0]
            depth_slice_len = depth_slice.stop - depth_slice.start
            if depth_slice_len > 1:
                raise RuntimeError(
                    "Depth slices must have length 1, but '{dim}' has length {dim_len}.".format(dim=depth_dim,
                                                                                                dim_len=depth_slice_len))

            if self.invert_z:
                ds[self.height] = ds[self.height] * -1

            new_tile.min_elevation = ds[self.height][depth_slice].item()
            new_tile.max_elevation = ds[self.height][depth_slice].item()

        new_tile.latitude.CopyFrom(to_shaped_array(lat_subset))
        new_tile.longitude.CopyFrom(to_shaped_array(lon_subset))
        new_tile.variable_data.CopyFrom(to_shaped_array(data_subset))
        new_tile.time.CopyFrom(to_shaped_array(time_subset))
        input_tile.tile.swath_multi_variable_tile.CopyFrom(new_tile)
        return input_tile
