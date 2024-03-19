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

import argparse
import asyncio
import logging
import os

from collection_manager.services import (CollectionProcessor,
                                         CollectionWatcher, MessagePublisher)
from collection_manager.services.history_manager import (
    FileIngestionHistoryBuilder, SolrIngestionHistoryBuilder,
    md5sum_from_filepath)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s")
logging.getLogger("pika").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def check_path(path) -> str:
    if not os.path.isabs(path):
        raise argparse.ArgumentError("Paths must be absolute.")
    return path


def get_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Watch the filesystem for new granules, and publish messages to "
                                                 "RabbitMQ whenever they become available.")
    parser.add_argument("--collections-path",
                        help="Absolute path to collections configuration file",
                        metavar="PATH",
                        required=True)
    history_group = parser.add_mutually_exclusive_group(required=True)
    history_group.add_argument("--history-path",
                               metavar="PATH",
                               help="Absolute path to ingestion history local directory")
    history_group.add_argument("--history-url",
                               metavar="URL",
                               help="URL to ingestion history solr database")
    parser.add_argument('--rabbitmq-host',
                        default='localhost',
                        metavar='HOST',
                        help='RabbitMQ hostname to connect to. (Default: "localhost")')
    parser.add_argument('--rabbitmq-username',
                        default='guest',
                        metavar='USERNAME',
                        help='RabbitMQ username. (Default: "guest")')
    parser.add_argument('--rabbitmq-password',
                        default='guest',
                        metavar='PASSWORD',
                        help='RabbitMQ password. (Default: "guest")')
    parser.add_argument('--rabbitmq-queue',
                        default="nexus",
                        metavar="QUEUE",
                        help='Name of the RabbitMQ queue to consume from. (Default: "nexus")')
    parser.add_argument('--refresh',
                        default='30',
                        metavar="INTERVAL",
                        help='Number of seconds after which to reload the collections config file. (Default: 30)')
    parser.add_argument('--s3-bucket',
                        metavar='S3-BUCKET',
                        help='Optional name of an AWS S3 bucket where granules are stored. If this option is set, then all collections to be scanned must have their granules on S3, not the local filesystem.')

    return parser.parse_args()


async def main():
    try:
        options = get_args()

        signature_fun = None if options.s3_bucket else md5sum_from_filepath

        if options.history_path:
            history_manager_builder = FileIngestionHistoryBuilder(history_path=options.history_path,
                                                                  signature_fun=signature_fun)
        else:
            history_manager_builder = SolrIngestionHistoryBuilder(solr_url=options.history_url,
                                                                  signature_fun=signature_fun)
        async with MessagePublisher(host=options.rabbitmq_host,
                                    username=options.rabbitmq_username,
                                    password=options.rabbitmq_password,
                                    queue=options.rabbitmq_queue) as publisher:
            collection_processor = CollectionProcessor(message_publisher=publisher,
                                                       history_manager_builder=history_manager_builder)
            collection_watcher = CollectionWatcher(collections_path=options.collections_path,
                                                   granule_updated_callback=collection_processor.process_granule,
                                                   dataset_added_callback=collection_processor.add_plugin_collection,
                                                   collections_refresh_interval=int(options.refresh),
                                                   s3_bucket=options.s3_bucket)

            await collection_watcher.start_watching()
            while True:
                try:
                    await asyncio.sleep(1)
                except KeyboardInterrupt:
                    return

    except Exception as e:
        logger.exception(e)
        return


if __name__ == "__main__":
    asyncio.run(main())
