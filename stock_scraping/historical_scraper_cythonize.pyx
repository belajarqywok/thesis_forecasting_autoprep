from time import sleep
from typing import List, Dict
from datetime import datetime, timedelta

from yfinance import download
from pandas import DataFrame, read_csv, to_datetime

from os import makedirs
from os.path import exists as file_is_exists
from concurrent.futures import ThreadPoolExecutor, as_completed

from settings.logging_rules import logger
from settings.scraper_rules import ScraperRules
from settings.location_rules import LocationRules

from warnings import filterwarnings
filterwarnings("ignore")


"""

  -- Historical Scraper --

  Writer : Al-Fariqy Raihan Azhwar
  NPM    : 202143501514
  Class  : R8Q
  Email  : alfariqyraihan@gmail.com

"""


class HistoricalScraper(ScraperRules, LocationRules):
  """
    [ name ]:
       __get_historical (return dtype: str or None)

    [ parameters ]
      - symbol (dtype: str)

    [ description ]
      Get Historical Data
  """
  def __get_historical(self, symbol: str) -> str or None:
    try:
      if not file_is_exists(self.DATASET_HISTORICAL_CSV_PATH):
        makedirs(self.DATASET_HISTORICAL_CSV_PATH)

      start_date: str = '2000-01-01'
      # end_date: str = datetime.now().strftime('%Y-%m-%d')
      end_date:   str = (datetime.now() + timedelta(days = 1)).strftime('%Y-%m-%d')

      historical: DataFrame = download(
        tickers  = symbol,
        start    = start_date,
        end      = end_date,
        progress = False,
        interval ='1d'
      )

      csv_symbol:   str = symbol[:len(symbol) - 3]
      csv_filename: str = f"{self.DATASET_HISTORICAL_CSV_PATH}/{csv_symbol}.csv"
      historical.to_csv(path_or_buf = csv_filename)

      return csv_filename
    
    except Exception as error_message:
      logger.error(f'{error_message} {symbol}')
      return None


  """
    [ name ]:
       get_by_symbol (return dtype: Dict[bool, str])

    [ parameters ]
      - symbol (dtype: str)

    [ description ]
      Get historical data by symbol
  """
  def get_by_symbol(self, symbol: str) -> Dict[bool, str]:
    try:
      csv_filename: str or None = self.__get_historical(symbol)
      if csv_filename is None: return False, symbol

      dataframe = read_csv(filepath_or_buffer = csv_filename, header = None)
      dataframe = dataframe.drop([0, 1, 2])
      dataframe.reset_index(drop = True, inplace = True)

      if dataframe.shape[1] == 7:
        dataframe.columns = ['Date', 'Adj Close', 'Close', 'High', 'Low', 'Open', 'Volume']
      elif dataframe.shape[1] == 6:
        dataframe.columns = ['Date', 'Close', 'High', 'Low', 'Open', 'Volume']
      else:
          logger.info(f"[Shape: {dataframe.shape[1]}] [Symbol: {symbol}] Number Of Columns Does'nt match")

      dataframe['Date'] = to_datetime(dataframe['Date']).dt.strftime('%Y-%m-%d')
      dataframe.to_csv(path_or_buf = csv_filename, index = False)

      logger.info(f'Datasets Are Stored on "{csv_filename}"')
      return True, symbol

    except Exception as error_message:
      logger.error(f'{error_message} {symbol}')
      return False, symbol


  """
    [ name ]:
       get_by_dataframe (return dtype: None)

    [ parameters ]
      - dataframe (dtype: DataFrame)

    [ description ]
      Get historical data by DataFrame
  """
  def get_by_dataframe(self, dataframe: DataFrame) -> None:
    try:
      # for symbol in dataframe['symbol'].tolist():
      #   self.get_by_symbol(symbol)
      failed_symbols: List[str] = []
      with ThreadPoolExecutor(max_workers = self.SCRAPER_THREAD_WORKER) as executor:
        future_to_get_by_dataframe = {
          executor.submit(self.get_by_symbol, symbol):
            symbol for symbol in dataframe['symbol'].tolist()
        }

        for future in as_completed(future_to_get_by_dataframe):
          is_success, symbol = future.result()
          if not is_success: failed_symbols.append(symbol)

      # Retry mechanism with exponential back-off, wkwkwk...
      # to prevent scraping failure, bjirrr..
      retry_count: int = 0
      max_retries: int = self.SCRAPER_MAXIMUM_RETRY
      exponential_backoff: int = 3

      while failed_symbols and retry_count < max_retries:
        logger.warning(f'[ RETRY MECHANISM ] retry count: {retry_count + 1}')
        stock_failed: List[str] = failed_symbols.copy()
        failed_symbols.clear()
          
        for symbol in stock_failed:
          is_success, symbol = self.get_by_symbol(symbol)
          if not is_success:
            failed_symbols.append(symbol)
          
        retry_count += 1
        if failed_symbols:
          logger.info(f'[ RETRY MECHANISM ] Waiting {exponential_backoff} seconds before next retry...')
          sleep(exponential_backoff)
          exponential_backoff += self.SCRAPER_EXPONENTIAL_RETRY

      if failed_symbols:
          logger.warning(f"Symbols failed after {max_retries} retries: {failed_symbols}")

    except Exception as error_message:
      logger.error(error_message)


  """
    [ name ]:
      get_by_symbols (return dtype: None)

    [ parameters ]
      - symbols (dtype: List[str])

    [ description ]
      Get historical data by symbols
  """
  def get_by_symbols(self, symbols: List[str]) -> None:
    try:
      # for symbol in symbols:
      #   self.get_by_symbol(symbol)
      failed_symbols: List[str] = []
      with ThreadPoolExecutor(max_workers = self.SCRAPER_THREAD_WORKER) as executor:
        future_to_get_by_symbols = {
          executor.submit(self.get_by_symbol, symbol):
            symbol for symbol in symbols
        }

        for future in as_completed(future_to_get_by_symbols):
          is_success, symbol = future.result()
          if not is_success: failed_symbols.append(symbol)

      # Retry mechanism with exponential back-off, wkwkwk...
      # to prevent scraping failure, bjirrr..
      retry_count: int = 0
      max_retries: int = self.SCRAPER_MAXIMUM_RETRY
      exponential_backoff: int = 3

      while failed_symbols and retry_count < max_retries:
        logger.warning(f'[ RETRY MECHANISM ] retry count: {retry_count + 1}')
        stock_failed: List[str] = failed_symbols.copy()
        failed_symbols.clear()
          
        for symbol in stock_failed:
          is_success, symbol = self.get_by_symbol(symbol)
          if not is_success:
            failed_symbols.append(symbol)
          
        retry_count += 1
        if failed_symbols:
          logger.info(f'[ RETRY MECHANISM ] Waiting {exponential_backoff} seconds before next retry...')
          sleep(exponential_backoff)
          exponential_backoff += self.SCRAPER_EXPONENTIAL_RETRY

      if failed_symbols:
          logger.warning(f"Symbols failed after {max_retries} retries: {failed_symbols}")

    except Exception as error_message:
      logger.error(error_message)
