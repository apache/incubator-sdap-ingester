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

logger = logging.getLogger(__name__)


class HeightOffset(TileProcessor):
    def __init__(self, base, offset):
        self.base_dimension = base
        self.offset_dimension = offset

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
            elif v[0] in self.base_dimension.dims:
                slice_dims[v[0]] = slice(v[1], v[2])

        if height_index is None:
            logger.warning(f"Cannot compute heights for tile {str(tile.summary.tile_id)}. Unable to determine height index from spec")

            return tile

        height = dataset[self.offset_dimension][height_index].item()
        base_height = dataset[self.base_dimension].isel(slice_dims).data

        computed_height = base_height + height

        tile_data.max_depth = np.nanmax(computed_height).item()
        tile_data.min_depth = np.nanmin(computed_height).item()

        return tile

