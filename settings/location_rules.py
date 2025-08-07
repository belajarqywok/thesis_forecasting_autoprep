class LocationRules:
  # Dataset Main Location
  DATASET_MAIN_PATH:            str = 'indonesia_stocks'

  # Infographic Location
  DATASET_SECTOR_JSON_PATH:     str = f'{DATASET_MAIN_PATH}/sectors.json'
  DATASET_RANKING_CSV_PATH:     str = f'{DATASET_MAIN_PATH}/top_50_stocks.csv'
  DATASET_RANKING_JSON_PATH:    str = f'{DATASET_MAIN_PATH}/top_50_stocks.json'
  DATASET_INFOGRAPHIC_CSV_PATH: str = f'{DATASET_MAIN_PATH}/infographic_stocks.csv'

  # Historical Location
  DATASET_HISTORICAL_CSV_PATH:  str = f'{DATASET_MAIN_PATH}/historicals'

  # Indicator Location
  DATASET_INDICATOR_CSV_PATH:   str = f'{DATASET_MAIN_PATH}/indicators'

  # Modeling Location
  DATASET_MODELING_CSV_PATH:    str = f'{DATASET_MAIN_PATH}/modeling_datas'

  # Min-Max Location
  DATASET_MINMAX_CSV_PATH:      str = f'{DATASET_MAIN_PATH}/min_max'

  # Workloads
  DATASET_WOKLOADS_JSON_PATH:   str = f'{DATASET_MAIN_PATH}/workloads'
  