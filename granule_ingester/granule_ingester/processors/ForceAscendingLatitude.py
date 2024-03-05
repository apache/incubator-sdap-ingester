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

import numpy as np

from nexusproto.serialization import from_shaped_array, to_shaped_array
from granule_ingester.processors.TileProcessor import TileProcessor
logger = logging.getLogger(__name__)


class ForceAscendingLatitude(TileProcessor):
    def __init__(self, default_latitude_axis=0):
        self.__default_latitude_axis = default_latitude_axis
        self.__latitude_keywords = {'lat', 'latitude', 'latitudes', 'yc', 'ydim_grid'}

    def __get_latitude_axis(self, axis_tuple):
        if axis_tuple is None:
            logger.debug(f'axis_tuple is None. using the default axis')
            return self.__default_latitude_axis
        for i, k in enumerate(axis_tuple):
            if k.lower() in self.__latitude_keywords:
                logger.debug(f'found the latitude axis: {i}')
                return i
        logger.debug(f'cannot find one of latitude keywords from {self.__latitude_keywords} in axis_tuple: {axis_tuple}. using the default axis')
        return self.__default_latitude_axis

    def process(self, tile, *args, **kwargs):
        """
        This method will reverse the ordering of latitude values in a tile if necessary to ensure that the latitude values are ascending.
​
        :param self:
        :param tile: The nexus_tile
        :return: Tile data with altered latitude values
        """
        the_tile_type = tile.tile.WhichOneof("tile_type")
        logger.debug(f'processing granule: {tile.summary.granule}')

        the_tile_data = getattr(tile.tile, the_tile_type)

        latitudes = from_shaped_array(the_tile_data.latitude)
        data = from_shaped_array(the_tile_data.variable_data)
        if len(latitudes) < 2:
            logger.debug(f'Not enough latitude in data to flip. No need to do so..')

            if 'dataset' in kwargs:
                kwargs['dataset'].attrs['_FlippedLat'] = (False, None)

            return tile
        delta = latitudes[1] - latitudes[0]
        if delta >= 0:
            logger.debug(f'Only reverse latitude ordering if current ordering is descending.. No need to do so..')

            if 'dataset' in kwargs:
                kwargs['dataset'].attrs['_FlippedLat'] = (False, None)

            return tile
        logger.debug(f'flipping latitudes')
        latitudes = np.flip(latitudes)
        latitude_axis = self.__get_latitude_axis(tile.summary.data_dim_names)
        logger.debug(f'flipping data on axis: {latitude_axis}')
        data = np.flip(data, axis=latitude_axis)

        if 'dataset' in kwargs:
            kwargs['dataset'].attrs['_FlippedLat'] = (True, latitude_axis)

        the_tile_data.latitude.CopyFrom(to_shaped_array(latitudes))
        the_tile_data.variable_data.CopyFrom(to_shaped_array(data))
        return tile
