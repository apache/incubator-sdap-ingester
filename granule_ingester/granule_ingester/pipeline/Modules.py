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

from granule_ingester.processors import *
from granule_ingester.processors.reading_processors import *
from granule_ingester.slicers import SliceFileByStepSize
from granule_ingester.granule_loaders import GranuleLoader

modules = {
    "granule": GranuleLoader,
    "sliceFileByStepSize": SliceFileByStepSize,
    "generateTileId": GenerateTileId,
    "ECCO": EccoReadingProcessor,
    "Grid": GridReadingProcessor,
    "GridMulti": GridMultiVariableReadingProcessor,
    "TimeSeries": TimeSeriesReadingProcessor,
    "Swath": SwathReadingProcessor,
    "SwathMulti": SwathMultiVariableReadingProcessor,
    "tileSummary": TileSummarizingProcessor,
    "emptyTileFilter": EmptyTileFilter,
    "kelvinToCelsius": KelvinToCelsius,
    "subtract180FromLongitude": Subtract180FromLongitude,
    "forceAscendingLatitude": ForceAscendingLatitude,
    "elevationBounds": ElevationBounds,
    "elevationOffset": ElevationOffset,
    "elevationRange": ElevationRange,
    "verifyShape": VerifyProcessor
}
