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
from nexusproto.DataTile_pb2 import NexusTile
from nexusproto.serialization import from_shaped_array, to_shaped_array

logger = logging.getLogger(__name__)


class VerifyProcessor(TileProcessor):
    def process(self, tile: NexusTile, *args, **kwargs):
        the_tile_type: str = tile.tile.WhichOneof("tile_type")
        the_tile_data = getattr(tile.tile, the_tile_type)

        var_data = from_shaped_array(the_tile_data.variable_data)

        is_multi_var = 'multi' in the_tile_type.lower()

        n_valid_dims = 3 if is_multi_var else 2

        if len(var_data.shape) == n_valid_dims:
            return tile

        logger.debug(f'Incorrect tile data array shape created. Trying to squeeze out single length dimensions')

        diff = n_valid_dims - len(var_data.shape)

        dims = [(i, l) for i, l in enumerate(var_data.shape)]

        axes = [i for i, l in dims[slice(0, -3 if is_multi_var else -2)] if l == 1]

        new_var_data = var_data.squeeze(axis=tuple(axes))

        the_tile_data.variable_data.CopyFrom(to_shaped_array(new_var_data))

        if len(new_var_data.shape) != n_valid_dims:
            logger.warning(f'Squeezed tile is still the wrong number of dimensions. Shape = {new_var_data.shape} when '
                           f'we want a {n_valid_dims}-dimension tile. Proceeding, but the ingested data may not be '
                           f'usable')
        else:
            logger.debug('Tile shape now looks correct')

        return tile
