import re
from time import sleep
from random import choice, uniform
from typing import List, Dict, Any, Optional

from curl_cffi.requests import Session 
from os.path import exists as file_is_exists
from concurrent.futures import ThreadPoolExecutor, as_completed

from yfinance.ticker import Ticker
from pandas import DataFrame, read_csv
from investpy.stocks import get_stocks as investpy_get_stocks

from settings.logging_rules import logger
from settings.scraper_rules import ScraperRules
from settings.location_rules import LocationRules

from warnings import filterwarnings
filterwarnings("ignore")


"""

  -- Infographic Scraper --

  Writer : Al-Fariqy Raihan Azhwar
  NPM    : 202143501514
  Class  : R8Q
  Email  : alfariqyraihan@gmail.com

"""


class InfographicScraper(ScraperRules, LocationRules):
  # Browser Sessions
  # __BROWSER_SESSION: Session = Session(impersonate = 'chrome')
  __BROWSER_SESSIONS: List[Session] = [
    Session(impersonate = browser_agent) \
      for browser_agent in ScraperRules().SCRAPER_BROWSER_AGENTS
  ]


  """
    [ name ]:
       __is_valid_stock (return dtype: bool)

    [ parameters ]
      - stock_info (dtype: Dict[str, Any])

    [ description ]
      Validation: Is Valid Stock ?.
  """
  def __is_valid_stock(
    self, stock_info: Dict[str, Any]
  ) -> bool:
    try:
      # Mandatory Fields Validation
      mandatory_fields: List[str] = [
        'longName', 'address1', 'address2',
        'city', 'zip', 'industry', 'sector',
      ]
      for field in mandatory_fields:
        if (field not in stock_info) or (not stock_info.get(field)):
          logger.info(f'[REJECT] Stock "{stock_info.get("symbol")}" missing mandatory field: "{field}".')
          return False

      # Optional Fields Validation
      optional_fields: List[str] = ['website', 'phone', 'fax']
      # if not any(stock_info.get(field) for field in optional_fields):
      for field in optional_fields:
        if (field not in stock_info) or (not stock_info.get(field)):
          logger.info(f'Stock "{stock_info.get("symbol")}" missing optional field: "{field}".')

      return True

    except Exception as error_message:
      logger.error(error_message)
      return False
  

  """
    [ name ]:
      __fetch_stock_info (return dtype: Optional[Dict[str, Any]])

    [ parameters ]
      - symbol (dtype: str)

    [ description ]
      Fetch Stock Info
  """
  def __fetch_stock_info(
    self,
    symbol:  str,
    process: str
  ) -> Optional[Dict[str, Any]]:
    try:
      ticker: Ticker = Ticker(
        ticker  = symbol, 
        session = choice(self.__BROWSER_SESSIONS)
      )

      stock_info: Dict[str, Any] = ticker.info
      stock_info['symbol']       = symbol

      # Validation: Is Valid Stock ?.
      is_valid: bool = self.__is_valid_stock(stock_info)
      if process == 'ASYNC':
        return stock_info, 'VALIDATION_STEP' if (is_valid == True) \
          else None, 'VALIDATION_STEP' 

      elif process == 'SYNC':
        return stock_info if (is_valid == True) else None

    except Exception as error_message:
      logger.error(f"{symbol} {error_message}")
      if process == 'ASYNC':
        if re.search(r'http.*404', error_message, re.IGNORECASE):
          return None, 'NOT_FOUND'
        else:
          return None, 'EXCEPTION_STEP'
      else: return None
  

  """
    [ name ]:
      get_stocks_symbol (return dtype: List[str])

    [ description ]
      Get Stock Symbol
  """
  def get_stocks_symbol(self) -> List[str]:
    try:
      country_stocks = investpy_get_stocks(country = "Indonesia")
      return [f"{row['symbol']}.JK" for _, row in country_stocks.iterrows()]

    except Exception as error_message:
      logger.error(error_message)
      return []


  """
    [ name ]:
      __get_stocks_data_sync (return dtype: Optional[Dict[str, Any]])

    [ description ]
      Get Stocks Data (Synchronous Process)
  """
  def __get_stocks_data_sync(self) -> Optional[Dict[str, Any]]:
    PROCESS: str = 'SYNC'
    try:
      stock_datas: List[Dict[str, Any]] = []
      iteration:   int = 1

      for stock_symbol in self.get_stocks_symbol():
        stock_info: Optional[Dict[str, Any]] = \
          self.__fetch_stock_info(
            symbol  = stock_symbol,
            process = PROCESS
          )

        if stock_info:
          stock_datas.append(stock_info)
          logger.info(
            f"[iter: {iteration};stocks: {len(stock_datas)}] " +
            f"[{stock_symbol} | {stock_info['longName']}]"
          )

        iteration += 1
        sleep(self.SCRAPER_RATE_LIMIT_HANDLE)

      return stock_datas
      
    except Exception as error_message:
      logger.error(error_message)
      return None


  """
    [ name ]:
      __get_stocks_data_async (return dtype: Optional[Dict[str, Any]])

    [ description ]
      Get Stocks Data (Asynchronous Process) [ -- ON DEVELOPMENT -- ]

    def __fetch_with_throttle(self, symbol: str):
      sleep(uniform(0.3, 0.8))
      return self.__fetch_stock_info(symbol, 'ASYNC')
  """
  def __get_stocks_data_async(self) -> Optional[Dict[str, Any]]:
    PROCESS: str = 'ASYNC'
    try:
      failed_symbols: List[str] = []
      stock_datas:    List[Dict[str, Any]] = []

      with ThreadPoolExecutor(max_workers = self.SCRAPER_THREAD_WORKER) as executor:
        future_to_fetch_stock_info = {
          executor.submit(self.__fetch_stock_info, stock_symbol, PROCESS):
            stock_symbol for stock_symbol in self.get_stocks_symbol()
        }

        for future in as_completed(future_to_fetch_stock_info):
          stock_info, step = future.result()
          if (stock_info) and (step == 'VALIDATION_STEP'):
            stock_symbol: str = stock_info.get('symbol')
            stock_datas.append(stock_info)
            logger.info(f"[stocks: {len(stock_datas)}] [{stock_symbol} | {stock_info.get('longName')}]")

          elif (not stock_info) and (step == 'VALIDATION_STEP'): pass
          elif (not stock_info) and (step == 'NOT_FOUND'): pass
          else: failed_symbols.append(stock_symbol)

      # Retry mechanism with exponential back-off
      # to prevent scraping failure
      retry_count: int = 0
      max_retries: int = self.SCRAPER_MAXIMUM_RETRY
      exponential_backoff: int = 3

      while failed_symbols and retry_count < max_retries:
        logger.warning(f'[ RETRY MECHANISM ] retry count: {retry_count + 1}')
        stock_failed: List[str] = failed_symbols.copy()
        failed_symbols.clear()
          
        for symbol in stock_failed:
          # throttling mechanism
          sleep(uniform(0.3, 0.8))
          stock_info: Optional[Dict[str, Any]] = \
            self.__fetch_stock_info(symbol, process = PROCESS)
            
          if stock_info:
            stock_symbol: str = stock_info.get('symbol')
            stock_datas.append(stock_info)
            logger.info(f"[stocks: {len(stock_datas)}] [{stock_symbol} | {stock_info.get('longName')}]")
          else: failed_symbols.append(symbol)
          
        retry_count += 1
        if failed_symbols:
          logger.info(f'[ RETRY MECHANISM ] Waiting {exponential_backoff} seconds before next retry...')
          sleep(exponential_backoff)
          exponential_backoff += self.SCRAPER_EXPONENTIAL_RETRY

      if failed_symbols:
        logger.warning(f"Symbols failed after {max_retries} retries: {failed_symbols}")
        return None
      
      return stock_datas
      
    except Exception as error_message:
      logger.error(error_message)
      return None


  """
    [ name ]:
      get_stocks_infographic (return dtype: Optional[DataFrame])

    [ parameters ]
      - generate_new_data (dtype: bool; default: False)

    [ description ]
      Get Stocks Infographic
  """
  def get_stocks_infographic(
    self, generate_new_data: bool = False,
    get_stocks_process:      str  = 'SYNC' # SYNC, ASYNC
  ) -> Optional[DataFrame]:
    try:
      sector_translation = {
        "Financial Services":     "Jasa Keuangan",
        "Consumer Cyclical":      "Barang Non-Primer",
        "Consumer Defensive":     "Barang Primer",
        "Communication Services": "Komunikasi",
        "Healthcare":             "Kesehatan",
        "Energy":                 "Energi",
        "Industrials":            "Industri",
        "Basic Materials":        "Bahan Baku",
        "Real Estate":            "Properti",
        "Technology":             "Teknologi",
        "Utilities":              "Utilitas"
      }

      sector_fontawesome_icons = {
        "Financial Services":     "fa-building-columns",
        "Consumer Cyclical":      "fa-cart-shopping",
        "Consumer Defensive":     "fa-apple-whole",
        "Communication Services": "fa-tower-broadcast",
        "Healthcare":             "fa-briefcase-medical",
        "Energy":                 "fa-bolt",
        "Industrials":            "fa-industry",
        "Basic Materials":        "fa-boxes-stacked",
        "Real Estate":            "fa-building-user",
        "Technology":             "fa-microchip",
        "Utilities":              "fa-lightbulb"
      }

      if not file_is_exists(self.DATASET_INFOGRAPHIC_CSV_PATH) or generate_new_data:
        stocks_data: Optional[Dict[str, Any]] = \
          self.__get_stocks_data_async() if get_stocks_process is 'ASYNC' \
            else self.__get_stocks_data_sync()

        indonesia_stocks_dataframe: DataFrame = DataFrame(stocks_data)
        indonesia_stocks_dataframe['sector_id'] = \
          indonesia_stocks_dataframe['sector'].map(sector_translation)
        indonesia_stocks_dataframe['fontawesome_icon'] = \
          indonesia_stocks_dataframe['sector'].map(sector_fontawesome_icons)
        indonesia_stocks_dataframe.to_csv(
          index       = False,
          path_or_buf = self.DATASET_INFOGRAPHIC_CSV_PATH
        )
        
        logger.info(f'Stocks infographic saved on {self.DATASET_INFOGRAPHIC_CSV_PATH}')
        
      else:
        indonesia_stocks_dataframe: DataFrame = \
          read_csv(filepath_or_buffer = self.DATASET_INFOGRAPHIC_CSV_PATH)
        indonesia_stocks_dataframe['sector_id'] = \
          indonesia_stocks_dataframe['sector'].map(sector_translation)
        indonesia_stocks_dataframe['fontawesome_icon'] = \
          indonesia_stocks_dataframe['sector'].map(sector_fontawesome_icons)
        logger.info(f'"{self.DATASET_INFOGRAPHIC_CSV_PATH}" already exists.')

      return indonesia_stocks_dataframe

    except Exception as error_message:
      logger.error(error_message)
      return None
     