import numpy as np
from json import dump
from typing import List, Dict, Tuple
from pandas import Series, DataFrame, read_csv

from os import makedirs
from os.path import exists as file_is_exists

from settings.logging_rules_cythonize import logger
from settings.scraper_rules_cythonize import ScraperRules
from settings.location_rules_cythonize import LocationRules

from warnings import filterwarnings
filterwarnings("ignore")


"""

  -- Technical Indicator (Feature Engineering) --

  Writer : Al-Fariqy Raihan Azhwar
  NPM    : 202143501514
  Class  : R8Q
  Email  : alfariqyraihan@gmail.com

"""

LOCATION_RULES: LocationRules = LocationRules()
SCRAPER_RULES:  ScraperRules  = ScraperRules()


cdef class TechnicalIndicator:
  """
    [ name ]:
      __simple_moving_average (return dtype: Series)

    [ parameters ]
      - dataframe   (dtype: DataFrame)
      - column_name (dtype: str;       default: "Close")
      - window_size (dtype: int;       default: 20)

    [ description ]
      Simple moving average
  """
  cpdef object __simple_moving_average(self,
    object dataframe,
    str column_name = 'Close',
    int window_size = 20
  ):
    try:
      if column_name not in dataframe.columns:
        # raise KeyError(f'Column name "{column_name}" do not exist')
        logger.critical(f'Column name "{column_name}" do not exist')
        return dataframe

      single_column: DataFrame = dataframe[column_name].values.astype(float)
      if len(single_column) < window_size:
        # raise ValueError("amount of data is smaller than window")
        logger.critical('Amount of data is smaller than window')
        return dataframe

      simple_mov_avg: np.ndarray = np.full_like(single_column, np.nan, dtype = np.float64)

      for _idx_col in range(window_size - 1, len(single_column)):
        window_values: DataFrame = single_column[(_idx_col - window_size + 1) : (_idx_col + 1)]
        simple_mov_avg.append(sum(window_values) / window_size)

      return Series(simple_mov_avg, index = dataframe.index)

    except Exception as error_message:
      logger.error(error_message)
      return dataframe


  """
    [ name ]:
      __exponential_moving_average (return dtype: Series)

    [ parameters ]
      - dataframe   (dtype: DataFrame)
      - column_name (dtype: str;       default: "Close")
      - window_size (dtype: int;       default: 20)

    [ description ]
      Exponential moving average
  """
  cpdef object __exponential_moving_average(self,
    object dataframe,
    str column_name = 'Close',
    int window_size = 20
  ):
    try:
      if column_name not in dataframe.columns:
        # raise KeyError(f'Column name "{column_name}" do not exist')
        logger.critical(f'Column name "{column_name}" do not exist')
        return dataframe

      single_column: DataFrame = dataframe[column_name].values.astype(float)
      if len(single_column) < window_size:
        # raise ValueError("amount of data is smaller than window")
        logger.critical('Amount of data is smaller than window')
        return dataframe

      exponential_mov_avg: np.ndarray = np.full_like(single_column, np.nan, dtype = np.float64)
      exponential_mov_avg[window_size - 1] = np.mean(single_column[:window_size])

      alpha: float = 2 / (window_size + 1)

      for _idx_col in range(window_size, len(single_column)):
        exponential_mov_avg[_idx_col] = (
          single_column[_idx_col] * alpha) + \
            (exponential_mov_avg[_idx_col - 1] * (1 - alpha)
        )

      return Series(exponential_mov_avg, index = dataframe.index)
    
    except Exception as error_message:
      logger.error(error_message)
      return dataframe


  """
    [ name ]:
      __relative_strength_index (return dtype: Series)

    [ parameters ]
      - dataframe   (dtype: DataFrame)
      - column_name (dtype: str;       default: "Close")
      - window_size (dtype: int;       default: 14)

    [ description ]
      Relative Strength Index
  """
  cpdef object __relative_strength_index(self,
    # dtype: DataFrame
    object dataframe,
    str column_name = 'Close',
    int window_size = 14
  ):
    cdef str __INDICATOR__ = 'RSI'
    try:
      if column_name not in dataframe.columns:
        # raise KeyError(f'Column name "{column_name}" do not exist')
        logger.critical(f'Column name "{column_name}" do not exist')
        return dataframe

      single_column: DataFrame = dataframe[column_name].values.astype(float)
      if len(single_column) < window_size:
        # raise ValueError("amount of data is smaller than window")
        logger.critical('Amount of data is smaller than window')
        return dataframe

      delta: np.ndarray = np.diff(single_column, prepend = np.nan)

      gain_value: np.ndarray = np.where(delta > 0, delta, 0)
      loss_value: np.ndarray = np.where(delta < 0, -delta, 0)

      average_gain: np.ndarray = np.full_like(single_column, np.nan)
      average_gain[window_size - 1] = np.mean(gain_value[:window_size])

      average_loss: np.ndarray = np.full_like(single_column, np.nan)
      average_loss[window_size - 1] = np.mean(loss_value[:window_size])

      alpha: float = 2 / (window_size + 1)

      for _idx_col in range(window_size, len(single_column)):
        average_gain[_idx_col] = (gain_value[_idx_col] * alpha) + \
          (average_gain[_idx_col - 1] * (1 - alpha))
        average_loss[_idx_col] = (loss_value[_idx_col] * alpha) + \
          (average_loss[_idx_col - 1] * (1 - alpha))

      relative_strength: np.ndarray = average_gain / average_loss
      relative_strength_index: np.ndarray = 100 - (
        100 / (1 + np.where(average_loss == 0, np.nan, relative_strength))
      )               

      return Series(relative_strength_index, index = dataframe.index)

    except Exception as error_message:
      logger.error(error_message)
      return dataframe


  """
    [ name ]:
      __money_flow_index (return dtype: Series)

    [ parameters ]
      - dataframe   (dtype: DataFrame)
      - window_size (dtype: int;       default: 14)

    [ description ]
      Money Flow Index
  """
  def __money_flow_index(self,
    object dataframe,
    int window_size = 14
  ) -> Series:
    try:
      required_cols = ['High', 'Low', 'Close', 'Volume']
      if not all(col in dataframe.columns for col in required_cols):
        logger.critical(f'Dataframe missing required columns: {required_cols}')
        return dataframe

      high:   DataFrame = dataframe['High'].values.astype(float)
      low:    DataFrame = dataframe['Low'].values.astype(float)
      close:  DataFrame = dataframe['Close'].values.astype(float)
      volume: DataFrame = dataframe['Volume'].values.astype(float)

      typical_price: DataFrame = (high + low + close) / 3
      if len(typical_price) < window_size:
        logger.critical("Amount of data is smaller than window")
        return dataframe

      raw_money_flow: DataFrame = typical_price * volume

      delta_tp = np.diff(typical_price, prepend=np.nan)

      positive_flow: np.ndarray = np.where(delta_tp > 0, raw_money_flow, 0.0)
      negative_flow: np.ndarray = np.where(delta_tp < 0, raw_money_flow, 0.0)

      average_pos: np.ndarray = np.full_like(typical_price, np.nan)
      average_neg: np.ndarray = np.full_like(typical_price, np.nan)

      average_pos[window_size - 1] = np.mean(positive_flow[:window_size])
      average_neg[window_size - 1] = np.mean(negative_flow[:window_size])

      alpha: float = 2 / (window_size + 1)

      for _idx_col in range(window_size, len(typical_price)):
        average_pos[_idx_col] = (positive_flow[_idx_col] * alpha) + \
          (average_pos[_idx_col - 1] * (1 - alpha))
        average_neg[_idx_col] = (negative_flow[_idx_col] * alpha) + \
          (average_neg[_idx_col - 1] * (1 - alpha))

      money_flow_ratio: np.ndarray = np.divide(
        average_pos, average_neg,
        out   = np.full_like(average_pos, np.nan),
        where = average_neg != 0
      )

      money_flow_index: np.ndarray = 100 - (100 / (1 + money_flow_ratio))

      return Series(money_flow_index, index = dataframe.index)

    except Exception as error_message:
      logger.error(error_message)
      return dataframe



  """
    [ name ]:
      __moving_average_convergence_divergence (return dtype: Series)

    [ parameters ]
      - dataframe          (dtype: DataFrame)
      - column_name        (dtype: str;       default: "Close")
      - fast_window_size   (dtype: int;       default: 12)
      - slow_window_size   (dtype: int;       default: 26)
      - signal_window_size (dtype: int;       default: 9)

    [ description ]
      Moving Average Convergence Divergence
  """
  cpdef Dict[str, Series] __moving_average_convergence_divergence(self,
    object dataframe,
    str column_name = 'Close',
    int fast_window_size   = 12,
    int slow_window_size   = 26,
    int signal_window_size = 9
  ):
    try:
      if column_name not in dataframe.columns:
        # raise KeyError(f'Column name "{column_name}" do not exist')
        logger.critical(f'Column name "{column_name}" do not exist')
        return dataframe

      exp_mov_avg_fast: Series = self.__exponential_moving_average(
        dataframe, column_name, fast_window_size)
      exp_mov_avg_slow: Series = self.__exponential_moving_average(
        dataframe, column_name, slow_window_size)

      macd_line: Series = exp_mov_avg_fast - exp_mov_avg_slow
      macd_line.dropna(inplace = True)

      signal_dataframe: DataFrame = DataFrame({column_name: macd_line})
      macd_signal_line: Series  = self.__exponential_moving_average(
        signal_dataframe, column_name, signal_window_size)
      macd_signal_line.dropna(inplace = True)

      macd_histogram: Series = macd_line - macd_signal_line
      macd_histogram.dropna(inplace = True)

      return {
        "line":        macd_line,
        "signal_line": macd_signal_line,
        "histogram":   macd_histogram
      }
    
    except Exception as error_message:
      logger.error(error_message)
      return dataframe


  """ 
    [ name ]:
      __csv_store_validation (return dtype: bool)

    [ parameters ]:
      - file_path (dtype: str)

    [ description ]:
      To validate CSV
  """
  cpdef bint __csv_store_validation(self, str file_path):
    try:
      dataframe = read_csv(file_path)

      # validation: if dataframe is empty
      if dataframe.empty: return False

      # validation: header only, without values
      if len(dataframe) == 0: return False

      return True
      
    except Exception as error_message:
      logger.error(error_message)
      return False


  """ 
    [ name ]:
      __min_max_normalization (return dtype: Tuple[DataFrame, Dict[str, float]] or None)

    [ parameters ]:
      - dataframe (dtype: DataFrame)

    [ description ]:
      Min-Max Normalization
  """
  def __min_max_normalization(
    self, dataframe: DataFrame
  ) -> Tuple[DataFrame, Dict[str, float]] or None:
    try:
      min_value: Series = dataframe.min()
      max_value: Series = dataframe.max()
      normalization: DataFrame = (dataframe - min_value) / (max_value - min_value)

      return normalization, {
        'min_value': min_value.to_dict(),
        'max_value': max_value.to_dict()
      }

    except Exception as error_message:
      logger.error(error_message)
      return None


  """
    [ name ]:
      __retry_mechanism (return dtype: None)

    [ parameters ]
      - failed_symbols (dtype: List[str])

    [ description ]
      Retry mechanism with throttling and exponential back-off,
      to prevent scraping failure
  """
  cpdef void __retry_mechanism(self, List[str] failed_symbols):
    try:
      retry_count: int = 0
      max_retries: int = SCRAPER_RULES.SCRAPER_MAXIMUM_RETRY
      exponential_backoff: int = 0

      while failed_symbols and retry_count < max_retries:
        logger.warning(f'[ RETRY MECHANISM ] retry count: {retry_count + 1}')
        stock_failed: List[str] = failed_symbols.copy()
        failed_symbols.clear()
          
        for symbol in stock_failed:
          # json path
          min_max_json_path:   str = f'{LOCATION_RULES.DATASET_MINMAX_CSV_PATH}/{symbol}.json'

          # csv path
          historical_csv_path: str = f'{LOCATION_RULES.DATASET_HISTORICAL_CSV_PATH}/{symbol}.csv'
          indicator_csv_path:  str = f'{LOCATION_RULES.DATASET_INDICATOR_CSV_PATH}/{symbol}.csv'
          modeling_csv_path:   str = f'{LOCATION_RULES.DATASET_MODELING_CSV_PATH}/{symbol}.csv'
          
          dataframe: DataFrame = read_csv(historical_csv_path, index_col = 'Date')
          dataframe.dropna(inplace = True)
          dataframe['MFI'] = self.__money_flow_index(dataframe)

          dataframe: DataFrame = dataframe[['Close', 'Volume', 'MFI']]
          relative_strength_index: Series = \
            self.__relative_strength_index(dataframe)
          dataframe['RSI'] = relative_strength_index
          dataframe.dropna(inplace = True)
            
          moving_average_convergence_divergence: Dict[str, Series] = \
            self.__moving_average_convergence_divergence(dataframe)
          dataframe['MACD'] = moving_average_convergence_divergence.get('line')
          dataframe.dropna(inplace = True)

          dataframe_indicator: DataFrame = dataframe.copy()
          dataframe_indicator: DataFrame = dataframe_indicator[['MFI', 'RSI', 'MACD']]
          dataframe_indicator.to_csv(path_or_buf = indicator_csv_path)

          dataframe_norm, dataframe_min_max = \
            self.__min_max_normalization(dataframe)
            
          with open(min_max_json_path, "w") as min_max_value:
            dump(dataframe_min_max, min_max_value)
          dataframe_norm.to_csv(path_or_buf = modeling_csv_path)

          csv_file_is_valid: bool = self.__csv_store_validation(modeling_csv_path)
          if not csv_file_is_valid:
            failed_symbols.append(symbol)
            logger.warning(f'[ RETRY MECHANISM ] [ FAILED SYMBOL ] Append "{symbol}" to LIST -> failed_symbols: List[str]')
            
        retry_count += 1
        if failed_symbols:
          logger.info(f'[ RETRY MECHANISM ] Waiting {exponential_backoff} seconds before next retry...')
          # sleep(exponential_backoff)
          # exponential_backoff += (self.SCRAPER_EXPONENTIAL_RETRY + uniform(0.1, 0.4))

      if failed_symbols:
          logger.warning(f"[ RETRY MECHANISM ] Symbols failed after {max_retries} retries: {failed_symbols}")

    except Exception as error_message:
      logger.error(error_message)


  """ 
    [ name ]:
      generate_indicator_by_dataframe_sync (return dtype: None)

    [ parameters ]:
      - dataframe (dtype: DataFrame)

    [ description ]:
      Generate indicator by dataframe (Synchronous Process)
  """
  cpdef void generate_indicator_by_dataframe_sync(self, object dataframe):
    try:
      if not file_is_exists(LOCATION_RULES.DATASET_INDICATOR_CSV_PATH):
        makedirs(LOCATION_RULES.DATASET_INDICATOR_CSV_PATH)

      if not file_is_exists(LOCATION_RULES.DATASET_MODELING_CSV_PATH):
        makedirs(LOCATION_RULES.DATASET_MODELING_CSV_PATH)

      if not file_is_exists(LOCATION_RULES.DATASET_MINMAX_CSV_PATH):
        makedirs(LOCATION_RULES.DATASET_MINMAX_CSV_PATH)
      
      failed_symbols: List[str] = []

      for symbol in dataframe['symbol'].tolist():
        symbol: str = symbol[:len(symbol) - 3]
        # json path
        min_max_json_path:   str = f'{LOCATION_RULES.DATASET_MINMAX_CSV_PATH}/{symbol}.json'

        # csv path
        historical_csv_path: str = f'{LOCATION_RULES.DATASET_HISTORICAL_CSV_PATH}/{symbol}.csv'
        indicator_csv_path:  str = f'{LOCATION_RULES.DATASET_INDICATOR_CSV_PATH}/{symbol}.csv'
        modeling_csv_path:   str = f'{LOCATION_RULES.DATASET_MODELING_CSV_PATH}/{symbol}.csv'
        
        dataframe: DataFrame = read_csv(historical_csv_path, index_col = 'Date')
        dataframe.dropna(inplace = True)
        dataframe['MFI'] = self.__money_flow_index(dataframe)

        dataframe: DataFrame = dataframe[['Close', 'Volume', 'MFI']]
        relative_strength_index: Series = \
          self.__relative_strength_index(dataframe)
        dataframe['RSI'] = relative_strength_index
        dataframe.dropna(inplace = True)
          
        moving_average_convergence_divergence: Dict[str, Series] = \
          self.__moving_average_convergence_divergence(dataframe)
        dataframe['MACD'] = moving_average_convergence_divergence.get('line')
        dataframe.dropna(inplace = True)

        dataframe_indicator: DataFrame = dataframe.copy()
        dataframe_indicator: DataFrame = dataframe_indicator[['MFI', 'RSI', 'MACD']]
        dataframe_indicator.to_csv(path_or_buf = indicator_csv_path)

        dataframe_norm, dataframe_min_max = \
          self.__min_max_normalization(dataframe)

        with open(min_max_json_path, "w") as min_max_value:
          dump(dataframe_min_max, min_max_value)
        dataframe_norm.to_csv(path_or_buf = modeling_csv_path)

        csv_file_is_valid: bool = self.__csv_store_validation(modeling_csv_path)
        if not csv_file_is_valid:
          failed_symbols.append(symbol)
          logger.warning(f'[ FAILED SYMBOL ] Append "{symbol}" to LIST -> failed_symbols: List[str]')

      # Retry mechanism with throttling and exponential back-off
      # to prevent scraping failure
      if failed_symbols: self.__retry_mechanism(failed_symbols)

    except Exception as error_message:
      logger.error(error_message)

