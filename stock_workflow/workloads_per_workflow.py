from os import makedirs, listdir
from os.path import exists as file_is_exists

import json
from typing import List

from settings.logging_rules import logger
from settings.location_rules import LocationRules

from warnings import filterwarnings
filterwarnings("ignore")


"""

  -- Technical Indicator (Feature Engineering) --

  Writer : Al-Fariqy Raihan Azhwar
  NPM    : 202143501514
  Class  : R8Q
  Email  : alfariqyraihan@gmail.com

"""


class WorkloadsPerWorkflow(LocationRules):
  def generate_workloads(self) -> None:
    try:
      if not file_is_exists(self.DATASET_WOKLOADS_JSON_PATH):
        makedirs(self.DATASET_WOKLOADS_JSON_PATH)

      stock_name_modeling: List[str] = sorted(
        [
          item for item in listdir(self.DATASET_MODELING_CSV_PATH)
        ]
      )

      for _idx in range(len(stock_name_modeling) // 5):
        workloads: List[str] = stock_name_modeling[(_idx * 5) : 5 * (_idx + 1)]
        workloads_filename: str = f'{self.DATASET_WOKLOADS_JSON_PATH}/workloads_{_idx + 1}.json'

        with open(workloads_filename, 'w') as workloads_file:
          json.dump({'workloads': workloads}, workloads_file)

    except Exception as error_message:
      logger.error(error_message)
