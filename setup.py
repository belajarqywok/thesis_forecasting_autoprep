from setuptools import setup, Extension
from Cython.Build import cythonize
import numpy

setup(
  name        = 'thesis_forecasting_dataprep',
  packages    = ['stock_indicator', 'stock_scraping'],
  package_dir = {'': '.'},

  ext_modules = cythonize([
    # stock indicator extension
    Extension(
      'stock_indicator.technical_indicator_cythonize',
      ['stock_indicator/technical_indicator_cythonize.pyx'],
      include_dirs = [ numpy.get_include() ]
    ),

    # stock scraping extension
    Extension(
      'stock_scraping.historical_scraper_cythonize',
      ['stock_scraping/historical_scraper_cythonize.pyx'],
      include_dirs = [ numpy.get_include() ]
    ),

    Extension(
      'stock_scraping.infographic_scraper_cythonize',
      ['stock_scraping/infographic_scraper_cythonize.pyx'],
      include_dirs = [ numpy.get_include() ]
    )

    # stock sorting extension
  ]),
  zip_safe = False,
)
