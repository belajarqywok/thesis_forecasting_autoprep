import numpy as np
from json import dump, load
from typing import Any, List, Dict, Tuple
from pandas import Series, DataFrame, read_csv, to_datetime, isnull

from os import makedirs
from os.path import exists as file_is_exists

from settings.logging_rules import logger
from settings.scraper_rules import ScraperRules
from settings.location_rules import LocationRules

from stock_report.pdf_report import PdfReport

from warnings import filterwarnings
filterwarnings("ignore")


"""

  -- Technical Indicator (Feature Engineering) --

  Writer : Al-Fariqy Raihan Azhwar
  NPM    : 202143501514
  Class  : R8Q
  Email  : alfariqyraihan@gmail.com

"""


class TechnicalIndicator(ScraperRules, LocationRules):
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
  def __simple_moving_average(
    self, dataframe: DataFrame,
    column_name:     str = 'Close',
    window_size:     int = 20
  ) -> Series:
    try:
      if column_name not in dataframe.columns:
        logger.critical(f'Column name "{column_name}" do not exist')
        return dataframe

      single_column: DataFrame = dataframe[column_name].values.astype(float)
      if len(single_column) < window_size:
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
  def __exponential_moving_average(
    self, dataframe: DataFrame,
    column_name:     str = 'Close',
    window_size:     int = 20
  ) -> Series or np.ndarray:
    try:
      if column_name not in dataframe.columns:
        logger.critical(f'Column name "{column_name}" do not exist')
        return dataframe

      single_column: DataFrame = dataframe[column_name].values.astype(float)
      if len(single_column) < window_size:
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
  def __relative_strength_index(
    self, dataframe: DataFrame,
    column_name: str = 'Close',
    window_size: int = 14
  ) -> Series:
    try:
      if column_name not in dataframe.columns:
        logger.critical(f'Column name "{column_name}" do not exist')
        return dataframe

      single_column: DataFrame = dataframe[column_name].values.astype(float)
      if len(single_column) < window_size:
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
  def __money_flow_index(
    self, dataframe: DataFrame,
    window_size: int = 14
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
      __volume_flow_indicator (return dtype: Series)

    [ parameters ]
      - dataframe   (dtype: DataFrame)
      - window_size (dtype: int; default: 50)
      - coefficient (dtype: int; default: 0.2)

    [ description ]
      Volume Flow Indicator
  """
  def __volume_flow_indicator(
    self, dataframe: DataFrame,
    window:      int = 50,
    coefficient: int = 0.2
  ) -> Series:
    try:
      typical_price: Series = (
        dataframe["High"] + 
        dataframe["Low"]  + 
        dataframe["Close"]
      ) / 3

      money_flow:     Series = typical_price.diff(1)
      std_money_flow: Series = money_flow.rolling(window).std()
      cutoff_limit:   Series = coefficient * std_money_flow

      # volatility contraction pattern (VCP)
      volatility_contraction_pattern = DataFrame({
        "VCP": dataframe["Volume"] * (
          (money_flow >  cutoff_limit).astype(int) - \
          (money_flow < -cutoff_limit).astype(int)
        )
      })

      return self.__exponential_moving_average(
        dataframe    = volatility_contraction_pattern,
        column_name  = "VCP",
        window_size  = window
      )

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
  def __moving_average_convergence_divergence(
    self, dataframe: DataFrame,
    column_name:        str = "Close",
    fast_window_size:   int = 12,
    slow_window_size:   int = 26,
    signal_window_size: int = 9
  ) -> Dict[str, Series]:
    try:
      if column_name not in dataframe.columns:
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
      __bollinger_bands (return dtype: Dict[str, Series])

    [ parameters ]
      - dataframe   (dtype: DataFrame)
      - column_name (dtype: str; default: "Close")
      - window_size (dtype: int; default: 20)
      - num_std     (dtype: float; default: 2.0)

    [ description ]
      Bollinger Bands — upper, middle (SMA), and lower band.
      Also returns %B (position within the band, 0–1 range) which is
      more model-friendly than raw price levels.
  """
  def __bollinger_bands(
    self, dataframe: DataFrame,
    column_name: str   = 'Close',
    window_size: int   = 20,
    num_std:     float = 2.0
  ) -> Dict[str, Series]:
    try:
      if column_name not in dataframe.columns:
        logger.critical(f'Column name "{column_name}" do not exist')
        return dataframe

      single_column: Series = dataframe[column_name].astype(float)
      if len(single_column) < window_size:
        logger.critical('Amount of data is smaller than window')
        return dataframe

      rolling_mean: Series = single_column.rolling(window=window_size).mean()
      rolling_std:  Series = single_column.rolling(window=window_size).std(ddof=0)

      upper_band: Series = rolling_mean + (num_std * rolling_std)
      lower_band: Series = rolling_mean - (num_std * rolling_std)

      band_width: Series = upper_band - lower_band
      percent_b: Series  = np.where(
        band_width != 0,
        (single_column - lower_band) / band_width,
        np.nan
      )

      return {
        "upper":     upper_band,
        "middle":    rolling_mean,
        "lower":     lower_band,
        "percent_b": Series(percent_b, index=dataframe.index)
      }

    except Exception as error_message:
      logger.error(error_message)
      return dataframe


  """
    [ name ]:
      __average_true_range (return dtype: Series)

    [ parameters ]
      - dataframe   (dtype: DataFrame)
      - window_size (dtype: int; default: 14)

    [ description ]
      Average True Range — measures market volatility.
      Uses Wilder's smoothing (same as RSI) for consistency.
  """
  def __average_true_range(
    self, dataframe: DataFrame,
    window_size: int = 14
  ) -> Series:
    try:
      required_cols = ['High', 'Low', 'Close']
      if not all(col in dataframe.columns for col in required_cols):
        logger.critical(f'Dataframe missing required columns: {required_cols}')
        return dataframe

      high:  np.ndarray = dataframe['High'].values.astype(float)
      low:   np.ndarray = dataframe['Low'].values.astype(float)
      close: np.ndarray = dataframe['Close'].values.astype(float)

      prev_close: np.ndarray = np.roll(close, 1)
      prev_close[0] = np.nan

      true_range: np.ndarray = np.maximum(
        high - low,
        np.maximum(
          np.abs(high - prev_close),
          np.abs(low  - prev_close)
        )
      )

      atr: np.ndarray = np.full_like(close, np.nan)
      atr[window_size - 1] = np.nanmean(true_range[:window_size])

      # Wilder's smoothing
      wilder_alpha: float = 1 / window_size
      for _idx_col in range(window_size, len(close)):
        atr[_idx_col] = (true_range[_idx_col] * wilder_alpha) + \
          (atr[_idx_col - 1] * (1 - wilder_alpha))

      return Series(atr, index=dataframe.index)

    except Exception as error_message:
      logger.error(error_message)
      return dataframe


  """
    [ name ]:
      __stochastic_oscillator (return dtype: Dict[str, Series])

    [ parameters ]
      - dataframe      (dtype: DataFrame)
      - k_window_size  (dtype: int; default: 14)
      - d_window_size  (dtype: int; default: 3)

    [ description ]
      Stochastic Oscillator — %K and its smoothed signal %D.
      Bounded 0-100, low correlation with RSI despite similar range.
  """
  def __stochastic_oscillator(
    self, dataframe: DataFrame,
    k_window_size: int = 14,
    d_window_size: int = 3
  ) -> Dict[str, Series]:
    try:
      required_cols = ['High', 'Low', 'Close']
      if not all(col in dataframe.columns for col in required_cols):
        logger.critical(f'Dataframe missing required columns: {required_cols}')
        return dataframe

      high:  Series = dataframe['High'].astype(float)
      low:   Series = dataframe['Low'].astype(float)
      close: Series = dataframe['Close'].astype(float)

      if len(close) < k_window_size:
        logger.critical('Amount of data is smaller than window')
        return dataframe

      lowest_low:   Series = low.rolling(window=k_window_size).min()
      highest_high: Series = high.rolling(window=k_window_size).max()

      denominator: Series = highest_high - lowest_low
      stoch_k: Series = np.where(
        denominator != 0,
        100 * (close - lowest_low) / denominator,
        np.nan
      )
      stoch_k = Series(stoch_k, index=dataframe.index)
      stoch_d: Series = stoch_k.rolling(window=d_window_size).mean()

      return {
        "stoch_k": stoch_k,
        "stoch_d": stoch_d
      }

    except Exception as error_message:
      logger.error(error_message)
      return dataframe


  """
    [ name ]:
      __commodity_channel_index (return dtype: Series)

    [ parameters ]
      - dataframe   (dtype: DataFrame)
      - window_size (dtype: int; default: 20)

    [ description ]
      Commodity Channel Index — identifies cyclical trends and
      overbought/oversold conditions. Unbounded but typically ±100.
  """
  def __commodity_channel_index(
    self, dataframe: DataFrame,
    window_size: int = 20
  ) -> Series:
    try:
      required_cols = ['High', 'Low', 'Close']
      if not all(col in dataframe.columns for col in required_cols):
        logger.critical(f'Dataframe missing required columns: {required_cols}')
        return dataframe

      typical_price: Series = (
        dataframe['High'].astype(float) +
        dataframe['Low'].astype(float)  +
        dataframe['Close'].astype(float)
      ) / 3

      if len(typical_price) < window_size:
        logger.critical('Amount of data is smaller than window')
        return dataframe

      rolling_mean: Series = typical_price.rolling(window=window_size).mean()

      # mean absolute deviation (manual — pandas mad() deprecated)
      rolling_mad: Series = typical_price.rolling(window=window_size).apply(
        lambda x: np.mean(np.abs(x - np.mean(x))), raw=True
      )

      cci: Series = np.where(
        rolling_mad != 0,
        (typical_price - rolling_mean) / (0.015 * rolling_mad),
        np.nan
      )

      return Series(cci, index=dataframe.index)

    except Exception as error_message:
      logger.error(error_message)
      return dataframe


  """
    [ name ]:
      __on_balance_volume (return dtype: Series)

    [ parameters ]
      - dataframe (dtype: DataFrame)

    [ description ]
      On-Balance Volume — cumulative volume indicator that maps
      buying/selling pressure. Trend direction matters more than value.
  """
  def __on_balance_volume(
    self, dataframe: DataFrame
  ) -> Series:
    try:
      required_cols = ['Close', 'Volume']
      if not all(col in dataframe.columns for col in required_cols):
        logger.critical(f'Dataframe missing required columns: {required_cols}')
        return dataframe

      close:  np.ndarray = dataframe['Close'].values.astype(float)
      volume: np.ndarray = dataframe['Volume'].values.astype(float)

      delta:  np.ndarray = np.diff(close, prepend=np.nan)
      direction: np.ndarray = np.sign(delta)   # +1, 0, -1

      obv: np.ndarray = np.cumsum(np.where(
        np.isnan(delta), 0, direction * volume
      ))

      return Series(obv, index=dataframe.index)

    except Exception as error_message:
      logger.error(error_message)
      return dataframe


  """
    [ name ]:
      __chaikin_money_flow (return dtype: Series)

    [ parameters ]
      - dataframe   (dtype: DataFrame)
      - window_size (dtype: int; default: 20)

    [ description ]
      Chaikin Money Flow — measures buying/selling pressure as a
      ratio of volume-weighted price position. Bounded -1 to +1.
  """
  def __chaikin_money_flow(
    self, dataframe: DataFrame,
    window_size: int = 20
  ) -> Series:
    try:
      required_cols = ['High', 'Low', 'Close', 'Volume']
      if not all(col in dataframe.columns for col in required_cols):
        logger.critical(f'Dataframe missing required columns: {required_cols}')
        return dataframe

      high:   Series = dataframe['High'].astype(float)
      low:    Series = dataframe['Low'].astype(float)
      close:  Series = dataframe['Close'].astype(float)
      volume: Series = dataframe['Volume'].astype(float)

      if len(close) < window_size:
        logger.critical('Amount of data is smaller than window')
        return dataframe

      hl_range: Series = high - low
      money_flow_multiplier: Series = np.where(
        hl_range != 0,
        ((close - low) - (high - close)) / hl_range,
        0.0
      )
      money_flow_volume: Series = Series(
        money_flow_multiplier, index=dataframe.index) * volume

      cmf: Series = (
        money_flow_volume.rolling(window=window_size).sum() /
        volume.rolling(window=window_size).sum()
      )

      return cmf

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
  def __csv_store_validation(self, file_path: str) -> bool:
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
  def __retry_mechanism(self, failed_symbols: List[str]) -> None:
    try:
      retry_count: int = 0
      max_retries: int = self.SCRAPER_MAXIMUM_RETRY
      exponential_backoff: int = 0

      while failed_symbols and retry_count < max_retries:
        logger.warning(f'[ RETRY MECHANISM ] retry count: {retry_count + 1}')
        stock_failed: List[str] = failed_symbols.copy()
        failed_symbols.clear()
          
        for symbol in stock_failed:
          # json path
          min_max_json_path:   str = f'{self.DATASET_MINMAX_CSV_PATH}/{symbol}.json'

          # csv path
          historical_csv_path: str = f'{self.DATASET_HISTORICAL_CSV_PATH}/{symbol}.csv'
          indicator_csv_path:  str = f'{self.DATASET_INDICATOR_CSV_PATH}/{symbol}.csv'
          modeling_csv_path:   str = f'{self.DATASET_MODELING_CSV_PATH}/{symbol}.csv'
          
          dataframe: DataFrame = read_csv(historical_csv_path, index_col = 'Date')
          dataframe.dropna(inplace = True)

          # --- existing indicators ---
          dataframe['MFI']  = self.__money_flow_index(dataframe)

          dataframe = dataframe[['Close', 'Volume', 'High', 'Low', 'MFI']]
          dataframe['RSI']  = self.__relative_strength_index(dataframe)
          dataframe.dropna(inplace = True)
            
          macd_result: Dict[str, Series] = \
            self.__moving_average_convergence_divergence(dataframe)
          dataframe['MACD'] = macd_result.get('line')
          dataframe.dropna(inplace = True)

          # --- new indicators ---
          bb_result = self.__bollinger_bands(dataframe)
          dataframe['BB_PERCENT_B'] = bb_result.get('percent_b')

          dataframe['ATR'] = self.__average_true_range(dataframe)

          stoch_result = self.__stochastic_oscillator(dataframe)
          dataframe['STOCH_K'] = stoch_result.get('stoch_k')
          dataframe['STOCH_D'] = stoch_result.get('stoch_d')

          dataframe['CCI'] = self.__commodity_channel_index(dataframe)
          dataframe['OBV'] = self.__on_balance_volume(dataframe)
          dataframe['CMF'] = self.__chaikin_money_flow(dataframe)

          dataframe.dropna(inplace = True)

          dataframe_indicator: DataFrame = dataframe[
            ['MFI', 'RSI', 'MACD',
             'BB_PERCENT_B', 'ATR',
             'STOCH_K', 'STOCH_D',
             'CCI', 'OBV', 'CMF']
          ].copy()
          dataframe_indicator.to_csv(path_or_buf = indicator_csv_path)

          dataframe_modeling: DataFrame = dataframe[
            ['Close', 'Volume',
             'MFI', 'RSI', 'MACD',
             'BB_PERCENT_B', 'ATR',
             'STOCH_K', 'STOCH_D',
             'CCI', 'OBV', 'CMF']
          ].copy()

          dataframe_norm, dataframe_min_max = \
            self.__min_max_normalization(dataframe_modeling)
            
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
  def generate_indicator_by_dataframe_sync(self, dataframe: DataFrame) -> None:
    try:
      if not file_is_exists(self.DATASET_INDICATOR_CSV_PATH):
        makedirs(self.DATASET_INDICATOR_CSV_PATH)

      if not file_is_exists(self.DATASET_MODELING_CSV_PATH):
        makedirs(self.DATASET_MODELING_CSV_PATH)

      if not file_is_exists(self.DATASET_MINMAX_CSV_PATH):
        makedirs(self.DATASET_MINMAX_CSV_PATH)
      
      failed_symbols: List[str] = []

      for symbol in dataframe['symbol'].tolist():
        symbol: str = symbol[:len(symbol) - 3]

        # json path
        min_max_json_path:     str = f'{self.DATASET_MINMAX_CSV_PATH}/{symbol}.json'
        fundamental_json_path: str = f'{self.DATASET_FUNDAMENTAL_JSON_PATH}/{symbol}.json'

        # csv path
        historical_csv_path: str = f'{self.DATASET_HISTORICAL_CSV_PATH}/{symbol}.csv'
        indicator_csv_path:  str = f'{self.DATASET_INDICATOR_CSV_PATH}/{symbol}.csv'
        modeling_csv_path:   str = f'{self.DATASET_MODELING_CSV_PATH}/{symbol}.csv'
        
        dataframe: DataFrame = read_csv(historical_csv_path, index_col = 'Date')
        dataframe.index = to_datetime(dataframe.index, errors = 'coerce')

        with open(fundamental_json_path, 'r') as fundamental_json:
          fundamental_json_data: Dict[Any, Any] = load(fundamental_json)
          short_name_company: str = fundamental_json_data \
            .get('fundamentals').get('shortName')
        
        # day maping
        day_name_maping: Dict[str, str] = {
          'Monday':    'Senin',
          'Tuesday':   'Selasa',
          'Wednesday': 'Rabu',
          'Thursday':  'Kamis',
          'Friday':    'Jumat',
          'Saturday':  'Sabtu',
          'Sunday':    'Minggu',
        }

        # month maping
        month_name_maping: Dict[str, str] = {
          'January':   'Januari',
          'February':  'Februari',
          'March':     'Maret',
          'April':     'April',
          'May':       'Mei',
          'June':      'Juni',
          'July':      'Juli',
          'August':    'Agustus',
          'September': 'September',
          'October':   'Oktober',
          'November':  'November',
          'December':  'Desember',
        }

        logger.info(f'[ PROCESSED ] [ HISTORICAL ] [ {symbol} ] Generate Data...')

        historical_json: list[dict[str, str]] = []
        for dt, row in dataframe.iterrows():
          if isnull(dt):
            continue
          historical_json.append({
            "date":      dt.strftime("%Y-%m-%d"),
            "full_date": f"{day_name_maping[dt.strftime('%A')]}, {dt.strftime('%d')} {month_name_maping[dt.strftime('%B')]} {dt.strftime('%Y')}",
            "open":      row["Open"],
            "high":      row["High"],
            "low":       row["Low"],
            "close":     row["Close"],
            "volume":    row["Volume"]
          })
        logger.info(f'[ SUCCESS ] [ HISTORICAL ] [ {symbol} ] Generate Data Success...')


        # indicator / technical
        logger.info(f'[ PROCESSED ] [ INDICATOR/TECHNICAL ] [ {symbol} ] Generate Data...')

        # --- existing indicators ---
        dataframe['MFI'] = self.__money_flow_index(dataframe)

        dataframe = dataframe[['Close', 'Volume', 'High', 'Low', 'MFI']]
        dataframe['RSI']  = self.__relative_strength_index(dataframe)
        dataframe.dropna(inplace = True)

        macd_result: Dict[str, Series] = \
          self.__moving_average_convergence_divergence(dataframe)
        dataframe['MACD'] = macd_result.get('line')
        dataframe.dropna(inplace = True)

        # --- new indicators ---
        bb_result = self.__bollinger_bands(dataframe)
        dataframe['BB_PERCENT_B'] = bb_result.get('percent_b')

        dataframe['ATR'] = self.__average_true_range(dataframe)

        stoch_result = self.__stochastic_oscillator(dataframe)
        dataframe['STOCH_K'] = stoch_result.get('stoch_k')
        dataframe['STOCH_D'] = stoch_result.get('stoch_d')

        dataframe['CCI'] = self.__commodity_channel_index(dataframe)
        dataframe['OBV'] = self.__on_balance_volume(dataframe)
        dataframe['CMF'] = self.__chaikin_money_flow(dataframe)

        dataframe.dropna(inplace = True)
        logger.info(f'[ SUCCESS ] [ INDICATOR/TECHNICAL ] [ {symbol} ] Generate Data Success...')


        # --- save indicator CSV & JSON ---
        indicator_cols = ['MFI', 'RSI', 'MACD',
                          'BB_PERCENT_B', 'ATR',
                          'STOCH_K', 'STOCH_D',
                          'CCI', 'OBV', 'CMF']
        dataframe_indicator: DataFrame = dataframe[indicator_cols].copy()
        dataframe_indicator.to_csv(path_or_buf = indicator_csv_path)

        dataframe_indicator.index = to_datetime(dataframe_indicator.index, errors='coerce')
        indicator_json: list[dict[str, str]] = [
          {
            "date":      dt.strftime("%Y-%m-%d"),
            "full_date": f"{day_name_maping[dt.strftime('%A')]}, {dt.strftime('%d')} {month_name_maping[dt.strftime('%B')]} {dt.strftime('%Y')}",
            "MFI":         row["MFI"],
            "RSI":         row["RSI"],
            "MACD":        row["MACD"],
            "BB_PERCENT_B": row["BB_PERCENT_B"],
            "ATR":         row["ATR"],
            "STOCH_K":     row["STOCH_K"],
            "STOCH_D":     row["STOCH_D"],
            "CCI":         row["CCI"],
            "OBV":         row["OBV"],
            "CMF":         row["CMF"]
          }
          for dt, row in dataframe_indicator.iterrows()
          if not isnull(dt)
        ]

        indicator_json_path: str = f'{self.DATASET_INDICATOR_CSV_PATH}/{symbol}.json'
        with open(indicator_json_path, "w") as indicator_json_file:
          dump({'indicators': indicator_json}, indicator_json_file)
          logger.info(f'[ SAVED ] [ INDICATOR/TECHNICAL ] [ {symbol} ] Generate Data Saved on "{indicator_json_path}"...')

        historical_json = historical_json[-len(indicator_json):]
        historical_json_path: str = f'{self.DATASET_HISTORICAL_CSV_PATH}/{symbol}.json'
        with open(historical_json_path, "w") as historical_json_file:
          dump({'historicals': historical_json}, historical_json_file)
          logger.info(f'[ SAVED ] [ HISTORICAL ] [ {symbol} ] Generate Data Saved on "{historical_json_path}"...')


        # --- generate reports ---
        pdf_report: PdfReport = PdfReport()

        logger.info(f'[ PROCESSED ] [ HISTORICAL ] [ PDF REPORT ] [ {symbol} ] Generate Report...')
        pdf_report.generate_report_historicals(
          symbol      = symbol,
          short_name  = short_name_company,
          historicals = historical_json[::-1]
        )
        logger.info(f'[ SUCCESS ] [ HISTORICAL ] [ PDF REPORT ] [ {symbol} ] Generate Report Success...')

        logger.info(f'[ PROCESSED ] [ INDICATOR/TECHNICAL ] [ PDF REPORT ] [ {symbol} ] Generate Report...')
        pdf_report.generate_report_indicators(
          symbol     = symbol,
          short_name = short_name_company,
          indicators = indicator_json[::-1]
        )
        logger.info(f'[ SUCCESS ] [ INDICATOR/TECHNICAL ] [ PDF REPORT ] [ {symbol} ] Generate Report Success...')


        # --- normalization (modeling CSV) ---
        modeling_cols = ['Close', 'Volume',
                         'MFI', 'RSI', 'MACD',
                         'BB_PERCENT_B', 'ATR',
                         'STOCH_K', 'STOCH_D',
                         'CCI', 'OBV', 'CMF']
        dataframe_modeling: DataFrame = dataframe[modeling_cols].copy()

        dataframe_norm, dataframe_min_max = \
          self.__min_max_normalization(dataframe_modeling)

        with open(min_max_json_path, "w") as min_max_value:
          dump(dataframe_min_max, min_max_value)
          
        dataframe_norm.to_csv(path_or_buf = modeling_csv_path)

        csv_file_is_valid: bool = self.__csv_store_validation(modeling_csv_path)
        if not csv_file_is_valid:
          failed_symbols.append(symbol)
          logger.warning(f'[ FAILED SYMBOL ] Append "{symbol}" to LIST -> failed_symbols: List[str]')

      # Retry mechanism with throttling and exponential back-off
      if failed_symbols: self.__retry_mechanism(failed_symbols)

    except Exception as error_message:
      logger.error(error_message)