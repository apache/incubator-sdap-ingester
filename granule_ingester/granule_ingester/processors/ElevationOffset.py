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

from granule_ingester.processors.TileProcessor import TileProcessor
import numpy as np
from nexusproto.serialization import from_shaped_array, to_shaped_array


logger = logging.getLogger(__name__)


class ElevationOffset(TileProcessor):
    def __init__(self, base, offset, **kwargs):
        self.base_dimension = base
        self.offset_dimension = offset
        self.flip_lat = kwargs.get('flipLatitude', False)

    def process(self, tile, dataset):
        slice_dims = {}

        tile_type = tile.tile.WhichOneof("tile_type")
        tile_data = getattr(tile.tile, tile_type)

        tile_summary = tile.summary

        spec_list = tile_summary.section_spec.split(',')

        height_index = None

        for spec in spec_list:
            v = spec.split(':')

            if v[0] == self.offset_dimension:
                height_index = int(v[1])
            elif v[0] in dataset[self.base_dimension].dims:
                slice_dims[v[0]] = slice(int(v[1]), int(v[2]))

        if height_index is None:
            logger.warning(f"Cannot compute heights for tile {str(tile.summary.tile_id)}. Unable to determine height index from spec")

            return tile

        height = dataset[self.offset_dimension][height_index].item()
        base_height = dataset[self.base_dimension].isel(slice_dims).data

        try:
            flip_lat, lat_axis = dataset.attrs.pop('_FlippedLat')
        except KeyError:
            flip_lat, lat_axis = (False, None)

        if flip_lat:
            base_height = np.flip(base_height, axis=lat_axis)

        computed_height = base_height + height

        # if tile_type in ['GridTile', 'GridMultiVariableTile']:
        #     elev_shape = (len(from_shaped_array(tile_data.latitude)), len(from_shaped_array(tile_data.longitude)))
        # else:
        #     elev_shape = from_shaped_array(tile_data.latitude).shape

        if self.flip_lat:
            computed_height = np.flip(computed_height, axis=0)

        tile_data.elevation.CopyFrom(
            to_shaped_array(computed_height)
        )

        tile_data.max_elevation = np.nanmax(computed_height).item()
        tile_data.min_elevation = np.nanmin(computed_height).item()

        return tile


