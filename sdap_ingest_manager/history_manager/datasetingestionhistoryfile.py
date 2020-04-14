import os
from pathlib import Path
import logging
from sdap_ingest_manager.history_manager import md5sum_from_filepath


logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)


class DatasetIngestionHistoryFile:
    _signature_fun =  md5sum_from_filepath
    _dataset_id = None
    _history_file_path = None
    _history_file = None
    _history_dict = {}

    def __init__(self, history_path, dataset_id, signature_fun):
        """
        Constructor
        :param history_path:
        :param dataset_id:
        :param signature_fun: function which create the signature of the cache, a file path string as argument and returns a string (md5sum, time stamp)
        """
        self._dataset_id = dataset_id
        self._history_file_path = os.path.join(history_path, f'{dataset_id}.csv')
        self._signature_fun = signature_fun
        self._history_dict = {}
        self._load_history_dict()
        Path(history_path).mkdir(parents=True, exist_ok=True)
        self._history_file = open(f"{self._history_file_path}", 'a')

    def _load_history_dict(self):
        logger.info(f"loading history file {self._history_file_path}")
        try:
            with open(self._history_file_path, 'r') as f_history:
                for line in f_history:
                    filename, md5sum = line.strip().split(',')
                    logger.info(f"add to history file {filename} with md5sum {md5sum}")
                    self._history_dict[filename] = md5sum
        except FileNotFoundError:
            logger.info("no history file created yet")



    def __del__(self):
        self._history_file.close()
        self.purge()
        del self._history_dict

    def purge(self):
        logger.info("purge the history file from duplicates")
        unique_file_names = set()
        try:
            with open(f"{self._history_file_path}.buff", "w") as f:
                history_file = open(self._history_file_path, "r")
                for line in reversed(list(history_file)):
                    file_name = line.split(",")[0]
                    if file_name not in unique_file_names:
                        unique_file_names.add(file_name)
                        f.write(line)
                history_file.close()
            os.replace(f"{self._history_file_path}.buff", self._history_file_path)
        except FileNotFoundError:
            logger.info(f"no history file {self._history_file_path} to purge")

    def reset_cache(self):
        try:
            os.remove(self._history_file_path)
            logger.info(f"history cache {self._history_file_path} removed")
        except FileNotFoundError:
            logger.info(f"history cache {self._history_file_path} does not exist, does not need to be removed")

    def _push_record(self, file_name, md5sum):
        self._history_dict[file_name] = md5sum
        self._history_file.write(f'{file_name},{md5sum}\n')
        return None

    def push(self, file_path):
        file_path = file_path.strip()
        file_name = os.path.basename(file_path)
        md5sum = self._signature_fun(file_path)
        self._push_record(file_name, md5sum)

    def _get_md5sum(self, file_name):
        if file_name in self._history_dict.keys():
            return self._history_dict[file_name]
        else:
            return None

    def has_valid_cache(self, file_path):
        file_path = file_path.strip()
        file_name = os.path.basename(file_path)
        md5sum = self._signature_fun(file_path)
        logger.debug(f"compare {md5sum} with {self._get_md5sum(file_name)}")
        return md5sum == self._get_md5sum(file_name)
