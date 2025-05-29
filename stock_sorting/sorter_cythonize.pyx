from pandas import DataFrame
from typing import List, Dict

from settings.logging_rules_cythonize import logger
from settings.location_rules_cythonize import LocationRules

from warnings import filterwarnings
filterwarnings("ignore")


"""

  -- Sorter --

  Writer : Al-Fariqy Raihan Azhwar
  NPM    : 202143501514
  Class  : R8Q
  Email  : alfariqyraihan@gmail.com

"""

LOCATION_RULES: LocationRules = LocationRules()


cdef class Sorter:
  """ 
    [ name ]:
      __dataframe_imputation (return dtype: DataFrame)

    [ parameters ]:
      - dataframe (dtype: DataFrame)
      - columns   (dtype: List[str])

    [ description ]:
      DataFrame Imputation
  """
  cpdef object __dataframe_imputation(
    self, object dataframe,
    List[str] columns
  ):
    try:
      for column in columns:
        if column not in dataframe.columns:
          dataframe[column] = 0.0
        dataframe[column]   = dataframe[column].fillna(0.0)

    except Exception as error_message:
      logger.error(error_message)

    return dataframe


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
  cpdef object __data_rangking(
    self, object dataframe, dict ranking,
    List[str] columns,
    List[bool] is_ascendings
  ):
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

      # Generate CSV file
      infographic.to_csv(
        index       = False,
        path_or_buf = self.DATASET_RANKING_CSV_PATH
      )

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
