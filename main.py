from pandas import DataFrame
from argparse import ArgumentParser, Namespace

from stock_sorting.sorter import Sorter
from stock_scraping.infographic_scraper import InfographicScraper
from stock_workflow.workloads_per_workflow import WorkloadsPerWorkflow
# from stock_scraping.historical_scraper import HistoricalScraper
from stock_scraping.historical_scraper_cythonize import HistoricalScraper
from stock_indicator.technical_indicator import TechnicalIndicator
# from stock_indicator.technical_indicator_cythonize import TechnicalIndicator

from settings.logging_rules import logger

from warnings import filterwarnings
filterwarnings("ignore")


"""

  -- Main --

  Writer : Al-Fariqy Raihan Azhwar (qywok)
  NPM    : 202143501514
  Class  : R8Q
  Email  : alfariqyraihan@gmail.com

"""


def gen_new_data_requirements(v: str) -> bool:
  return v.lower() in ('true', '1', 'yes', 'y')

def run_pipeline(arguments: Namespace) -> None:
  try:
    stocks_infographic: DataFrame = \
      InfographicScraper().get_stocks_infographic(
        generate_new_data  = arguments.gen_new_data, 
        get_stocks_process = arguments.process
      )

    sorting_by_infographic: DataFrame = \
      Sorter().by_default_infographic(
        infographic = stocks_infographic,
        ranking = {
          'ranking_by': arguments.ranking_by,
          'number'    : int(arguments.ranking_number)
        }
      )

    historical: HistoricalScraper = HistoricalScraper()
    historical.get_by_dataframe_sync(dataframe = sorting_by_infographic)

    technical: TechnicalIndicator = TechnicalIndicator()
    technical.generate_indicator_by_dataframe_sync(dataframe = sorting_by_infographic)

    workloads_per_workflow: WorkloadsPerWorkflow = WorkloadsPerWorkflow()
    workloads_per_workflow.generate_workloads()

  except Exception as error_message:
    logger.error(error_message)


def main() -> None:
  try:
    parser: ArgumentParser = ArgumentParser(description = "thesis_forecasting_autoprep")

    parser.add_argument(
      '-gnd', '--gen_new_data',
      type = gen_new_data_requirements, required = True,
      help = 'Generate New Data  [options: True, False]'
    )
    parser.add_argument(
      '-proc', '--process',
      type = str, required = True, choices = ['SYNC', 'ASYNC'],
      help = 'Get Stocks Process [options: SYNC, ASYNC]'
    )
    parser.add_argument(
      '-rank_by',  '--ranking_by',
      type = str, required = True, choices = ['HEAD_RANK', 'TAIL_RANK'],
      help = 'Ranking By [options: HEAD_RANK, TAIL_RANK]'
    )
    parser.add_argument(
      '-rank_num', '--ranking_number',
      type = int, required = True, help = 'Ranking Number'
    )

    arguments: Namespace = parser.parse_args()
    run_pipeline(arguments)

  except Exception as error_message:
    logger.error(error_message)


if __name__ == "__main__": main()
