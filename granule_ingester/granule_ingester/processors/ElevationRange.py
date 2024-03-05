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


class ElevationRange(TileProcessor):
    def __init__(self, elevation_dimension_name, start, stop, step):
        self.dimension = elevation_dimension_name

        self.start = float(start)
        self.stop = float(stop)
        self.step = float(step)

        self.e = list(np.arange(self.start, self.stop + self.step, self.step))

    def process(self, tile, dataset):
        tile_type = tile.tile.WhichOneof("tile_type")
        tile_data = getattr(tile.tile, tile_type)

        tile_summary = tile.summary

        spec_list = tile_summary.section_spec.split(',')

        depth_index = None

        for spec in spec_list:
            v = spec.split(':')

            if v[0] == self.dimension:
                depth_index = int(v[1])
                break

        if depth_index is None:
            logger.warning(f"Cannot compute depth bounds for tile {str(tile.summary.tile_id)}. Unable to determine depth index from spec")

            return tile

        elevation = self.e[depth_index]

        # if tile_type in ['GridTile', 'GridMultiVariableTile']:
        #     elev_shape = (len(from_shaped_array(tile_data.latitude)), len(from_shaped_array(tile_data.longitude)))
        # else:
        #     elev_shape = from_shaped_array(tile_data.latitude).shape

        elev_shape = from_shaped_array(tile_data.variable_data).shape

        # print(f'Elev shape: {elev_shape}')

        tile_data.elevation.CopyFrom(
            to_shaped_array(
                np.full(
                    elev_shape,
                    elevation
                )
            )
        )

        tile_data.max_elevation = elevation
        tile_data.min_elevation = elevation

        return tile
