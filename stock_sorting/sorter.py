import re
from json import dump
from typing import List, Dict
from pandas import DataFrame, isna

from os import makedirs
from os.path import exists as file_is_exists

from settings.logging_rules import logger
from settings.location_rules import LocationRules

from warnings import filterwarnings
filterwarnings("ignore")


"""

  -- Sorter --

  Writer : Al-Fariqy Raihan Azhwar
  NPM    : 202143501514
  Class  : R8Q
  Email  : alfariqyraihan@gmail.com

"""


class Sorter(LocationRules):
  """ 
    [ name ]:
      __dataframe_imputation (return dtype: DataFrame)

    [ parameters ]:
      - dataframe (dtype: DataFrame)
      - columns   (dtype: List[str])

    [ description ]:
      DataFrame Imputation
  """
  def __dataframe_imputation(
    self, dataframe: DataFrame,
    columns: List[str]
  ) -> DataFrame:
    try:
      for column in columns:
        if column not in dataframe.columns:
          dataframe[column] = 0.0
        dataframe[column]   = dataframe[column].fillna(0.0)

    except Exception as error_message:
      logger.error(error_message)

    return dataframe

  
  def __format_phone(self, phone):
    digits = re.sub(r"\D", "", phone)
    if digits.startswith("62"):
      return "+{}".format(digits)
    elif digits.startswith("0"):
      return "+62{}".format(digits[1:])
    else:
      return "+62{}".format(digits)


  """ 
    [ name ]:
      __data_rangking (return dtype: DataFrame)

    [ parameters ]:
      - dataframe      (dtype: DataFrame)
      - ranking        (dtype: Dict[str, str or int])
      - columns        (dtype: List[str])
      - is_ascendings  (dtype: List[bool])

    [ description ]:
      DataFrame Rangking
  """
  def __data_rangking(
    self, dataframe: DataFrame,
    ranking:       Dict[str, str or int],
    columns:       List[str],
    is_ascendings: List[bool]
  ) -> DataFrame:
    try:
      if ranking.get('ranking_by') == 'HEAD_RANK':
        ranking_stocks: DataFrame = dataframe.sort_values(
          by        = columns,
          ascending = is_ascendings
        ).head(ranking.get('number'))

      elif ranking.get('ranking_by') == 'TAIL_RANK':
        ranking_stocks: DataFrame = dataframe.sort_values(
          by        = columns,
          ascending = is_ascendings
        ).tail(ranking.get('number'))

      else:
        ranking_stocks: DataFrame = dataframe.sort_values(
          by        = columns,
          ascending = is_ascendings
        ).head(ranking.get('number'))

      return ranking_stocks
    
    except Exception as error_message:
      logger.error(error_message)
      return dataframe

  
  """ 
    [ name ]:
      by_default_infographic (return dtype: bool)

    [ parameters ]:
      - infographic   (dtype: DataFrame)
      - ranking       (dtype: Dict[str, str or int])

    [ description ]:
      Sorting By Default Infographic
  """
  def by_default_infographic(
    self, infographic: DataFrame,
    ranking: Dict[str, str or int] = \
      {
        'ranking_by': 'HEAD_RANK', #Option: HEAD_RANK, TAIL_RANK
        'number'    : 50
      }
  ) -> bool:
    try:
      required_columns: List[str] = [
        'marketCap',         # Market Capitalization;      Descending brohh...
        'returnOnEquity',    # Return On Equity;           Descending brohh...

        'revenueGrowth',     # Revenue Growth;             Descending brohh...
        'trailingPE',        # Trailing Price-to-Earnings; Ascending brohh...

        'operatingMargins',  # Operating Margins;          Descending brohh...
        'freeCashflow',      # Free Cash Flow;             Descending brohh...

        'priceToBook',       # Price to Book Ratio;        Ascending brohh...
        'debtToEquity',      # Debt to Equity Ration;      Ascending brohh...

        'dividendYield',     # Dividend Yield;             Descending brohh...
        'earningsGrowth'     # Earnings Growth;            Descending brohh...
      ]

      is_ascendings: List[bool] = [
        False, False,        # marketCap, returnOnEquity
        False, True,         # revenueGrowth, trailingPE
        False, False,        # operatingMargins, freeCashflow
        True,  True,         # priceToBook, debtToEquity
        False, False         # dividendYield, earningsGrowth
      ]

      # Imputation if "required_columns" value is NaN
      infographic = self.__dataframe_imputation(
        dataframe = infographic, 
        columns   = required_columns
      )

      # Rangking by 'required_columns' and 'is_ascendings'
      infographic = self.__data_rangking(
        dataframe     = infographic,
        ranking       = ranking,
        columns       = required_columns,
        is_ascendings = is_ascendings
      )

      # generate csv file
      infographic.to_csv(
        index       = False,
        path_or_buf = self.DATASET_RANKING_CSV_PATH
      )


      # infographic information
      infographic_json: list[dict[str, str]] = [
        {
          "fontawesome_icon": row["fontawesome_icon"],
          "symbol":           row["symbol"][:len(row["symbol"]) - 3], 
          "sector_id":        row["sector_id"],
          "shortName":        row["shortName"],
          "beta":             '0.0' if isna(row["beta"]) else str(row["beta"]),
          "dividendYield":    '0.0 %' if isna(row["dividendYield"]) else f'{row["dividendYield"]} %'
        }
        for _, row in infographic.iterrows()
      ]

      with open(self.DATASET_RANKING_JSON_PATH, "w") as infographic_json_value:
        dump({'infographics': infographic_json}, infographic_json_value)


      # sectors information
      sectors_json = list(infographic['sector_id'].unique())
      with open(self.DATASET_SECTOR_JSON_PATH, "w") as sectors_json_value:
        dump({'sectors': sectors_json}, sectors_json_value)

      # fundamental information
      if not file_is_exists(self.DATASET_FUNDAMENTAL_JSON_PATH):
        makedirs(self.DATASET_FUNDAMENTAL_JSON_PATH)

      for _, row in infographic.iterrows():
        issuer_symbol: str = row['symbol'][:len(row['symbol']) - 3]
        fundamentals_json: dict[str, str] = {
          # header data
          'symbol':       issuer_symbol,
          'sector_id':    row['sector_id'],
          'shortName':    row['shortName'],

          # issuer data
          'address':      f'{row["address1"]}; {row["address2"]}; {row["city"]}; {row["zip"]}',
          'phone':        'Tidak Ada' if isna(row["phone"]) else self.__format_phone(row["phone"]),
          'website':      'Tidak Ada' if isna(row["website"]) else str(row["website"]),

          # fundamental data
          'marketCap':           '0.0' if isna(row["marketCap"]) else str(row["marketCap"]),
          'dividendRate':        '0.0' if isna(row["dividendRate"]) else str(row["dividendRate"]),
          'dividendYield':       '0.0 %' if isna(row["dividendYield"]) else f'{row["dividendYield"]} %',
          'earningsGrowth':      '0.0 %' if isna(row["earningsGrowth"]) else f'{row["earningsGrowth"]} %',
          'profitMargins':       '0.0 %' if isna(row["profitMargins"]) else f'{row["profitMargins"]} %',
          'grossMargins':        '0.0 %' if isna(row["grossMargins"]) else f'{row["grossMargins"]} %',
          'beta':                '0.0' if isna(row["beta"]) else str(row["beta"]),
          'bookValue':           '0.0' if isna(row["bookValue"]) else str(row["bookValue"]),
          'priceToBook':         '0.0' if isna(row["priceToBook"]) else str(row["priceToBook"]),
          'quickRatio':          '0.0' if isna(row["quickRatio"]) else str(row["quickRatio"]),
          'currentRatio':        '0.0' if isna(row["currentRatio"]) else str(row["currentRatio"]),
          'debtToEquity':        '0.0' if isna(row["debtToEquity"]) else str(row["debtToEquity"]),
          'revenuePerShare':     '0.0' if isna(row["revenuePerShare"]) else str(row["revenuePerShare"]),
          'revenueGrowth':       '0.0 %' if isna(row["revenueGrowth"]) else f'{row["revenueGrowth"]} %',
          'ebitda':              '0.0' if isna(row["ebitda"]) else str(row["ebitda"]),
          'regularMarketChange': '0.0' if isna(row["regularMarketChange"]) else str(row["regularMarketChange"]),
          'payoutRatio':         '0.0 %' if isna(row["payoutRatio"]) else f'{row["payoutRatio"]} %',
          'trailingPE':          '0.0' if isna(row["trailingPE"]) else str(row["trailingPE"]),
          'forwardPE':           '0.0' if isna(row["forwardPE"]) else str(row["forwardPE"]),
          'trailingEps':         '0.0' if isna(row["trailingEps"]) else str(row["trailingEps"]),
          'forwardEps':          '0.0' if isna(row["forwardEps"]) else str(row["forwardEps"])
        }

        with open(f'{self.DATASET_FUNDAMENTAL_JSON_PATH}/{issuer_symbol}.json', 'w') as fundamentals_json_value:
          dump({'fundamentals': fundamentals_json}, fundamentals_json_value)

      return infographic
    
    except Exception as error_message:
      logger.error(error_message)
      return False


  """ 
    [ name ]:
      by_custom_infographic (return dtype: bool)

    [ parameters ]:
      - infographic      (dtype: DataFrame)
      - required_columns (dtype: List[str])
      - is_ascendings    (dtype: List[bool])
      - ranking          (dtype: Dict[str, str or int])

    [ description ]:
      Sorting By Custom Infographic
  """
  def by_custom_infographic(
    self, infographic:  DataFrame,

    required_columns:   List[str], 
    is_ascendings:      List[bool],

    ranking: Dict[str, str or int] = \
      {
        'ranking_by': 'HEAD_RANK', #Option: HEAD_RANK, TAIL_RANK
        'number'    : 50
      }
  ) -> bool:
    try:
      # Validation
      if type(required_columns).__name__ != 'list':
        logger.error('the "required_columns" parameter must be a list')
        return False

      if type(is_ascendings).__name__ != 'list':
        logger.error('the "is_ascendings" parameter must be a list')
        return False

      # Imputation if "required_columns" value is NaN
      infographic = self.__dataframe_imputation(
        dataframe = infographic, 
        columns   = required_columns
      )

      # Rangking by 'required_columns' and 'is_ascendings'
      infographic = self.__data_rangking(
        dataframe     = infographic,
        ranking       = ranking,
        columns       = required_columns,
        is_ascendings = is_ascendings
      )

      # Generate CSV file
      infographic.to_csv(
        index       = False,
        path_or_buf = self.DATASET_RANKING_CSV_PATH
      )

      return infographic
    
    except Exception as error_message:
      logger.error(error_message)
      return False
