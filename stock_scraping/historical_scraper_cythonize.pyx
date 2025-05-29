from time import sleep
from random import uniform
from typing import List, Tuple
from datetime import datetime, timedelta

from yfinance import download
from pandas import DataFrame, read_csv, to_datetime

from os import makedirs
from os.path import exists as file_is_exists
from concurrent.futures import ThreadPoolExecutor, as_completed

from settings.logging_rules_cythonize import logger
from settings.scraper_rules_cythonize import ScraperRules
from settings.location_rules_cythonize import LocationRules

from warnings import filterwarnings
filterwarnings("ignore")


"""

  -- Historical Scraper --

  Writer : Al-Fariqy Raihan Azhwar
  NPM    : 202143501514
  Class  : R8Q
  Email  : alfariqyraihan@gmail.com

"""

LOCATION_RULES: LocationRules = LocationRules()
SCRAPER_RULES:  ScraperRules  = ScraperRules()


cdef class HistoricalScraper:
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
      dataframe: DataFrame = read_csv(file_path)

      # validation brohh..: if dataframe is empty
      if dataframe.empty: return False

      # validation brohh..: header only, without values
      if len(dataframe) == 0: return False

      return True
        
    except Exception as error_message:
      logger.error(error_message)
      return False


  """
    [ name ]:
      __get_historical (return dtype: str)

    [ parameters ]
      - symbol (dtype: str)

    [ description ]
      Get Historical Data
  """
  cpdef str __get_historical(self, str symbol):
    try:
      if not file_is_exists(LOCATION_RULES.DATASET_HISTORICAL_CSV_PATH):
        makedirs(LOCATION_RULES.DATASET_HISTORICAL_CSV_PATH)

      start_date: str = '2023-01-01'
      # end_date: str = datetime.now().strftime('%Y-%m-%d')
      end_date: str   = (datetime.now() + timedelta(days = 1)).strftime('%Y-%m-%d')

      historical: DataFrame = download(
        tickers  = symbol,
        start    = start_date,
        end      = end_date
        # progress = False,
        # interval ='1d'
      )

      symbol: str = symbol[:len(symbol) - 3]
      csv_filename: str = f"{LOCATION_RULES.DATASET_HISTORICAL_CSV_PATH}/{symbol}.csv"
      historical.to_csv(path_or_buf = csv_filename)

      return csv_filename
      
    except Exception as error_message:
      logger.error(f'{error_message} {symbol}')
      return None


  """
    [ name ]:
       get_by_symbol (return dtype: Tuple[bool, str, str])

    [ parameters ]
      - symbol (dtype: str)

    [ description ]
      Get historical data by symbol
  """
  cpdef Tuple[bool, str, str] get_by_symbol(self, str symbol):
    try:
      sleep(uniform(0.3, 0.5))
      csv_filename: str = self.__get_historical(symbol)
      if csv_filename is 'None': return False, symbol, csv_filename

      dataframe = read_csv(csv_filename, header = None)
      dataframe = dataframe.drop([0, 1, 2])
      dataframe.reset_index(drop = True, inplace = True)

      if dataframe.shape[1] == 7:
        dataframe.columns = ['Date', 'Adj Close', 'Close', 'High', 'Low', 'Open', 'Volume']
      elif dataframe.shape[1] == 6:
        dataframe.columns = ['Date', 'Close', 'High', 'Low', 'Open', 'Volume']
      else:
          logger.info(f"[Shape: {dataframe.shape[1]}] [Symbol: {symbol}] Number Of Columns Does'nt match")

      dataframe['Date'] = to_datetime(dataframe['Date']).dt.strftime('%Y-%m-%d')
      dataframe.to_csv(csv_filename, index = False)

      logger.info(f'[ SAVED ] Datasets are stored on "{csv_filename}"')
      return True, symbol, csv_filename

    except Exception as error_message:
      logger.error(f'{error_message} {symbol}')
      return False, symbol, csv_filename


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
      exponential_backoff: int = 3

      while failed_symbols and retry_count < max_retries:
        logger.warning(f'[ RETRY MECHANISM ] retry count: {retry_count + 1}')
        stock_failed: List[str] = failed_symbols.copy()
        failed_symbols.clear()
          
        for symbol in stock_failed:
          sleep(uniform(0.3, 0.5))
          is_success, symbol, csv_filename = self.get_by_symbol(symbol)
          csv_file_is_valid: bool = self.__csv_store_validation(csv_filename)

          if (not is_success) or (not csv_file_is_valid):
            failed_symbols.append(symbol)
            logger.warning(f'[ RETRY MECHANISM ] [ FAILED SYMBOL ] Append "{symbol}" to LIST -> failed_symbols: List[str]')
          
        retry_count += 1
        if failed_symbols:
          logger.info(f'[ RETRY MECHANISM ] Waiting {exponential_backoff} seconds before next retry...')
          sleep(exponential_backoff)
          exponential_backoff += (
            SCRAPER_RULES.SCRAPER_EXPONENTIAL_RETRY + uniform(0.1, 0.4)
          )

      if failed_symbols:
          logger.warning(f"Symbols failed after {max_retries} retries: {failed_symbols}")

    except Exception as error_message:
      logger.error(error_message) 


  """
    [ name ]:
       get_by_dataframe_sync (return dtype: None)

    [ parameters ]
      - dataframe (dtype: DataFrame)

    [ description ]
      Get historical data by DataFrame (Synchronous Process)
  """
  cpdef void get_by_dataframe_sync(self, object dataframe):
    try:
      failed_symbols: List[str] = []

      for symbol in dataframe['symbol'].tolist():
        is_success, symbol, csv_filename = self.get_by_symbol(symbol)
        csv_file_is_valid: bool = self.__csv_store_validation(csv_filename)

        if (not is_success) or (not csv_file_is_valid):
          failed_symbols.append(symbol)
          logger.warning(f'[ FAILED SYMBOL ] Append "{symbol}" to LIST -> failed_symbols: List[str]')

      # Retry mechanism with throttling and exponential back-off
      # to prevent scraping failure
      if failed_symbols: self.__retry_mechanism(failed_symbols)

    except Exception as error_message:
      logger.error(error_message)


  """
    [ name ]:
       get_by_dataframe_async (return dtype: None)

    [ parameters ]
      - dataframe (dtype: DataFrame)

    [ description ]
      Get historical data by DataFrame (Asynchronous Process)
  """
  cpdef void get_by_dataframe_async(self, object dataframe):
    try:
      # for symbol in dataframe['symbol'].tolist():
      #   self.get_by_symbol(symbol)
      failed_symbols: List[str] = []

      with ThreadPoolExecutor(max_workers = SCRAPER_RULES.SCRAPER_THREAD_WORKER) as executor:
        future_to_get_by_dataframe = {
          executor.submit(self.get_by_symbol, symbol):
            symbol for symbol in dataframe['symbol'].tolist()
        }

        for future in as_completed(future_to_get_by_dataframe):
          is_success, symbol, csv_filename = future.result()
          csv_file_is_valid: bool = self.__csv_store_validation(csv_filename)

          if (not is_success) or (not csv_file_is_valid):
            failed_symbols.append(symbol)
            logger.warning(f'[ FAILED SYMBOL ] Append "{symbol}" to LIST -> failed_symbols: List[str]')

      # Retry mechanism with throttling and exponential back-off
      # to prevent scraping failure
      if failed_symbols: self.__retry_mechanism(failed_symbols)

    except Exception as error_message:
      logger.error(error_message)


  """
    [ name ]:
      get_by_symbols_sync (return dtype: None)

    [ parameters ]
      - symbols (dtype: List[str])

    [ description ]
      Get historical data by symbols (Synchronous Process)
  """
  cpdef void get_by_symbols_sync(self, List[str] symbols):
    try:
      failed_symbols: List[str] = []
      for symbol in symbols:
        is_success, symbol, csv_filename = self.get_by_symbol(symbol)
        csv_file_is_valid: bool = self.__csv_store_validation(csv_filename)

        if (not is_success) or (not csv_file_is_valid):
          failed_symbols.append(symbol)
          logger.warning(f'[ FAILED SYMBOL ] Append "{symbol}" to LIST -> failed_symbols: List[str]')

      # Retry mechanism with throttling and exponential back-off
      # to prevent scraping failure
      if failed_symbols: self.__retry_mechanism(failed_symbols)
    
    except Exception as error_message:
      logger.error(error_message)


  """
    [ name ]:
      get_by_symbols_async (return dtype: None)

    [ parameters ]
      - symbols (dtype: List[str])

    [ description ]
      Get historical data by symbols (Asynchronous Process)
  """
  cpdef void get_by_symbols_async(self, List[str] symbols):
    try:
      # for symbol in symbols:
      #   self.get_by_symbol(symbol)
      failed_symbols: List[str] = []
      with ThreadPoolExecutor(max_workers = SCRAPER_RULES.SCRAPER_THREAD_WORKER) as executor:
        future_to_get_by_symbols = {
          executor.submit(self.get_by_symbol, symbol):
            symbol for symbol in symbols
        }

        for future in as_completed(future_to_get_by_symbols):
          is_success, symbol, csv_filename = future.result()
          csv_file_is_valid: bool = self.__csv_store_validation(csv_filename)

          if (not is_success) or (not csv_file_is_valid):
            failed_symbols.append(symbol)
            logger.warning(f'[ FAILED SYMBOL ] Append "{symbol}" to LIST -> failed_symbols: List[str]')

      # Retry mechanism with throttling and exponential back-off
      # to prevent scraping failure
      if failed_symbols: self.__retry_mechanism(failed_symbols)

    except Exception as error_message:
      logger.error(error_message)

