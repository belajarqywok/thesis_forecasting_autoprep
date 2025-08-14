import os
from weasyprint import HTML
from typing import Any, List, Dict
from datetime import datetime, timedelta
from jinja2 import Template, Environment, FileSystemLoader




class PdfReport:
  CONTRACTS_PATH:      str = './stock_report'

  ISSUER_REPORT:       str = './indonesia_stocks'
  TECHNICAL_REPORT:    str = './indonesia_stocks/indicators'
  HISTORICAL_REPORT:   str = './indonesia_stocks/historicals'
  FUNDAMENTAL_REPORT:  str = './indonesia_stocks/fundamentals'

  environment: Environment = Environment(
    loader = FileSystemLoader(CONTRACTS_PATH)
  )


  def __init__(self) -> None:
    self.full_name:    str = 'Al-Fariqy Raihan Azhwar'
    self.npm_numbers:  str = '202143501514'

    self.month_name_mapping = {
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


  """ 
    [ name ]:
      generate_report_indicators (return dtype: None)

    [ parameters ]:
      - symbol     (dtype: str)
      - short_name (dtype: str)
      - indicators (dtype: List)

    [ description ]:
      Generate Report Indicators
  """
  def generate_report_indicators(
    self, symbol: str, 
    short_name:   str, 
    indicators:   List[Any]
  ) -> None:
    try:
      template: Template = self.environment \
        .get_template('technical_report.jinja2')

      tomorrow: datetime = datetime.now() + timedelta(days = 1)
      full_date: str = f"{tomorrow.strftime('%d')} {self.month_name_mapping[tomorrow.strftime('%B')]} {tomorrow.strftime('%Y')}"

      template_context: Dict[str, Any] = {
        'symbol':      symbol,
        'short_name':  short_name,
        'indicators':  indicators,

        'full_name':   self.full_name,
        'npm_numbers': self.npm_numbers,
        'full_date':   full_date
      }

      template_render: str = template.render(template_context)
      HTML(
        string   = template_render, 
        base_url = os.getcwd()
      ).write_pdf(f'{self.TECHNICAL_REPORT}/{symbol}.pdf')
    except Exception as error_message:
      print(error_message)


  """ 
    [ name ]:
      generate_report_historicals (return dtype: None)

    [ parameters ]:
      - symbol      (dtype: str)
      - short_name  (dtype: str)
      - historicals (dtype: List)

    [ description ]:
      Generate Report Historicals
  """
  def generate_report_historicals(
    self, symbol: str, 
    short_name:   str, 
    historicals:  List[Any]
  ) -> None:
    try:
      template: Template = self.environment \
        .get_template('historical_report.jinja2')

      tomorrow: datetime = datetime.now() + timedelta(days = 1)
      full_date: str = f"{tomorrow.strftime('%d')} {self.month_name_mapping[tomorrow.strftime('%B')]} {tomorrow.strftime('%Y')}"

      template_context: Dict[str, Any] = {
        'symbol':       symbol,
        'short_name':   short_name,
        'historicals':  historicals,

        'full_name':   self.full_name,
        'npm_numbers': self.npm_numbers,
        'full_date':   full_date
      }

      template_render: str = template.render(template_context)
      HTML(
        string   = template_render, 
        base_url = os.getcwd()
      ).write_pdf(f'{self.HISTORICAL_REPORT}/{symbol}.pdf')
    except Exception as error_message:
      print(error_message)


  """ 
    [ name ]:
      generate_report_issuers (return dtype: None)

    [ parameters ]:
      - symbol      (dtype: str)
      - short_name  (dtype: str)
      - issuers     (dtype: List)

    [ description ]:
      Generate Report Issuers
  """
  def generate_report_issuers(self, issuers: List[Any]) -> None:
    try:
      template: Template = self.environment \
        .get_template('issuers_report.jinja2')

      tomorrow: datetime = datetime.now() + timedelta(days = 1)
      full_date: str = f"{tomorrow.strftime('%d')} {self.month_name_mapping[tomorrow.strftime('%B')]} {tomorrow.strftime('%Y')}"

      template_context: Dict[str, Any] = {
        'issuers':      issuers,

        'full_name':   self.full_name,
        'npm_numbers': self.npm_numbers,
        'full_date':   full_date
      }

      template_render: str = template.render(template_context)
      HTML(
        string   = template_render, 
        base_url = os.getcwd()
      ).write_pdf(f'{self.ISSUER_REPORT}/emiten_saham.pdf')
    except Exception as error_message:
      print(error_message)


  """ 
    [ name ]:
      generate_report_fundamental (return dtype: None)

    [ parameters ]:
      - symbol       (dtype: str)
      - short_name   (dtype: str)
      - issuer_label (dtype: Dict[str, str])
      - issuer_data  (dtype: Dict[str, Any])

    [ description ]:
      Generate Report Historicals
  """
  def generate_report_fundamental(
    self, symbol: str, 
    short_name:   str, 
    issuer_labels:  Dict[str, str],
    issuer_datas:   Dict[str, Any]
  ) -> None:
    try:
      template: Template = self.environment \
        .get_template('fundamental_report.jinja2')

      tomorrow: datetime = datetime.now() + timedelta(days = 1)
      full_date: str = f"{tomorrow.strftime('%d')} {self.month_name_mapping[tomorrow.strftime('%B')]} {tomorrow.strftime('%Y')}"

      template_context: Dict[str, Any] = {
        'symbol':       symbol,
        'short_name':   short_name,

        'fundamental_labels':  issuer_labels,
        'fundamental_datas':   issuer_datas,

        'full_name':   self.full_name,
        'npm_numbers': self.npm_numbers,
        'full_date':   full_date
      }

      template_render: str = template.render(template_context)
      HTML(
        string   = template_render, 
        base_url = os.getcwd()
      ).write_pdf(f'{self.FUNDAMENTAL_REPORT}/{symbol}.pdf')
    except Exception as error_message:
      print(error_message)

