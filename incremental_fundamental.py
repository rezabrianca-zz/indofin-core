#!/usr/bin/python3
import os
import sqlalchemy as s
import psycopg2 as ps
import pandas as pd
import numpy as np

from requests import get
from multiprocessing import Pool
from numpy import ceil
from time import time, sleep
from datetime import datetime

from db import *
from slack_message import sendMessage

os.chdir('/home/ubuntu/indofin-core/')

def process_response(company, last_quarter, last_profit_report):
    today = datetime.today()
    last_year = today.year - 1
    if last_quarter == 'TW3':
        try:
            url_main = 'https://www.idx.co.id/Portals/0/StaticData/ListedCompanies/Corporate_Actions/New_Info_JSX/Jenis_Informasi/01_Laporan_Keuangan/02_Soft_Copy_Laporan_Keuangan//Laporan%20Keuangan%20Tahun%20{year}/Audit/{stock}/FinancialStatement-{year}-Tahunan-{stock}.xlsx'.format(stock=company, year=last_year)
            response_main = get(url_main)
            if response_main.status_code == 200: # if Excel file submitted
                quarter_data = pd.read_excel(url_main, sheet_name=3, skiprows=2, usecols='A:D')
                profit = quarter_data[quarter_data[quarter_data.columns[3]] == 'Total profit (loss)'].iloc[:,1].values[0]
                new_data = pd.DataFrame({'company_code': [company], 'quarter':['Tahunan'], 'year':[last_year], 'net_profit_report':[profit], 'net_profit_quarter':[profit - last_profit_report]})
                return new_data

            elif response_main.status_code != 200:
                try:
                    url_pdf = 'https://www.idx.co.id/Portals/0/StaticData/ListedCompanies/Corporate_Actions/New_Info_JSX/Jenis_Informasi/01_Laporan_Keuangan/02_Soft_Copy_Laporan_Keuangan//Laporan%20Keuangan%20Tahun%20{year}/Audit/{stock}/FinancialStatement-{year}-Tahunan-{stock}.pdf'.format(stock=company, year=last_year)
                    response_pdf = get(url_pdf) # check if PDF submitted
                    if response_pdf.status_code == 200:
                        print('{0} submitted in PDF for {1} annual report'.format(company, last_year))
                        sendMessage('{0} submitted in PDF for {1} annual report'.format(company, last_year))
                except Exception as e:
                    print('PDF file not found for {0}'.format(company))
                    print('Error Type:', e.__class__.__name__)
                    print('Error Message:', e)
                    pass
        except Exception as e:
            print('File not found for {0}'.format(company))
            print('Error Type:', e.__class__.__name__)
            print('Error Message:', e)
            pass

    elif last_quarter == 'Tahunan':
        try:
            url_main = 'https://www.idx.co.id/Portals/0/StaticData/ListedCompanies/Corporate_Actions/New_Info_JSX/Jenis_Informasi/01_Laporan_Keuangan/02_Soft_Copy_Laporan_Keuangan//Laporan%20Keuangan%20Tahun%20{year}/{quarter}/{stock}/FinancialStatement-{year}-I-{stock}.xlsx'.format(stock=company, year=str(today.year), quarter='TW1')
            response_main = get(url_main)
            if response_main.status_code == 200: # if Excel file submitted
                quarter_data = pd.read_excel(url_main, sheet_name=3, skiprows=2, usecols='A:D')
                profit = quarter_data[quarter_data[quarter_data.columns[3]] == 'Total profit (loss)'].iloc[:,1].values[0]
                new_data = pd.DataFrame({'company_code': [company], 'quarter':['TW1'], 'year':[today.year], 'net_profit_report':[profit], 'net_profit_quarter':[profit]})
                return new_data

            elif response_main.status_code != 200:
                try:
                    url_pdf = 'https://www.idx.co.id/Portals/0/StaticData/ListedCompanies/Corporate_Actions/New_Info_JSX/Jenis_Informasi/01_Laporan_Keuangan/02_Soft_Copy_Laporan_Keuangan//Laporan%20Keuangan%20Tahun%20{year}/{quarter}/{stock}/FinancialStatement-{year}-I-{stock}.pdf'.format(stock=company, year=str(today.year), quarter='TW1')
                    response_pdf = get(url_pdf) # check if PDF submitted
                    if response_pdf.status_code == 200:
                        print('{0} submitted in PDF for TW1 - {1} report'.format(company, str(today.year)))
                        sendMessage('{0} submitted in PDF for TW1 - {1} report'.format(company, str(today.year)))
                except Exception as e:
                    print('PDF file not found for {0}'.format(company))
                    print('Error Type:', e.__class__.__name__)
                    print('Error Message:', e)
                    pass
        except Exception as e:
            print('File not found for {0}'.format(company))
            print('Error Type:', e.__class__.__name__)
            print('Error Message:', e)
            pass

    elif last_quarter == 'TW1':
        try:
            url_main = 'https://www.idx.co.id/Portals/0/StaticData/ListedCompanies/Corporate_Actions/New_Info_JSX/Jenis_Informasi/01_Laporan_Keuangan/02_Soft_Copy_Laporan_Keuangan//Laporan%20Keuangan%20Tahun%20{year}/{quarter}/{stock}/FinancialStatement-{year}-II-{stock}.xlsx'.format(stock=company, year=str(today.year), quarter='TW2')
            response_main = get(url_main)
            if response_main.status_code == 200: # if Excel file submitted
                quarter_data = pd.read_excel(url_main, sheet_name=3, skiprows=2, usecols='A:D')
                profit = quarter_data[quarter_data[quarter_data.columns[3]] == 'Total profit (loss)'].iloc[:,1].values[0]
                new_data = pd.DataFrame({'company_code': [company], 'quarter':['TW2'], 'year':[today.year], 'net_profit_report':[profit], 'net_profit_quarter':[profit - last_profit_report]})
                return new_data

            elif response_main.status_code != 200:
                try:
                    url_pdf = 'https://www.idx.co.id/Portals/0/StaticData/ListedCompanies/Corporate_Actions/New_Info_JSX/Jenis_Informasi/01_Laporan_Keuangan/02_Soft_Copy_Laporan_Keuangan//Laporan%20Keuangan%20Tahun%20{year}/{quarter}/{stock}/FinancialStatement-{year}-II-{stock}.pdf'.format(stock=company, year=str(today.year), quarter='TW2')
                    response_pdf = get(url_pdf) # check if PDF submitted
                    if response_pdf.status_code == 200:
                        print('{0} submitted in PDF for TW2 - {1} report'.format(company, str(today.year)))
                        sendMessage('{0} submitted in PDF for TW2 - {1} report'.format(company, str(today.year)))
                except Exception as e:
                    print('PDF file not found for {0}'.format(company))
                    print('Error Type:', e.__class__.__name__)
                    print('Error Message:', e)
                    pass
        except Exception as e:
            print('File not found for {0}'.format(company))
            print('Error Type:', e.__class__.__name__)
            print('Error Message:', e)
            pass

    elif last_quarter == 'TW2':
        try:
            url_main = 'https://www.idx.co.id/Portals/0/StaticData/ListedCompanies/Corporate_Actions/New_Info_JSX/Jenis_Informasi/01_Laporan_Keuangan/02_Soft_Copy_Laporan_Keuangan//Laporan%20Keuangan%20Tahun%20{year}/{quarter}/{stock}/FinancialStatement-{year}-III-{stock}.xlsx'.format(stock=company, year=str(today.year), quarter='TW3')
            response_main = get(url_main)
            if response_main.status_code == 200: # if Excel file submitted
                quarter_data = pd.read_excel(url_main, sheet_name=3, skiprows=2, usecols='A:D')
                profit = quarter_data[quarter_data[quarter_data.columns[3]] == 'Total profit (loss)'].iloc[:,1].values[0]
                new_data = pd.DataFrame({'company_code': [company], 'quarter':['TW2'], 'year':[today.year], 'net_profit_report':[profit], 'net_profit_quarter':[profit - last_profit_report]})
                return new_data

            elif response_main.status_code != 200:
                try:
                    url_pdf = 'https://www.idx.co.id/Portals/0/StaticData/ListedCompanies/Corporate_Actions/New_Info_JSX/Jenis_Informasi/01_Laporan_Keuangan/02_Soft_Copy_Laporan_Keuangan//Laporan%20Keuangan%20Tahun%20{year}/{quarter}/{stock}/FinancialStatement-{year}-III-{stock}.pdf'.format(stock=company, year=str(today.year), quarter='TW3')
                    response_pdf = get(url_pdf) # check if PDF submitted
                    if response_pdf.status_code == 200:
                        print('{0} submitted in PDF for TW3 - {1} report'.format(company, str(today.year)))
                        sendMessage('{0} submitted in PDF for TW3 - {1} report'.format(company, str(today.year)))
                except Exception as e:
                    print('PDF file not found for {0}'.format(company))
                    print('Error Type:', e.__class__.__name__)
                    print('Error Message:', e)
                    pass

        except Exception as e:
            print('File not found for {0}'.format(company))
            print('Error Type:', e.__class__.__name__)
            print('Error Message:', e)
            pass

def get_last_profit(company):
    today = datetime.today()
    last_profit_q = '''
    SELECT * FROM "fundamental"."{company_code}"
    '''.format(company_code=company)
    print('Processing for {0}'.format(company))
    try:
        conn = pg_connect()
        last_profit = pd.read_sql(last_profit_q, conn).tail(1).reset_index()
        last_profit_report = last_profit.net_profit_report[0]
        last_quarter = last_profit.quarter[0]
        if last_quarter == 'TW3' and last_profit.year[0] == today.year - 1 and today.month in range(1,7):
            new_data = process_response(company=company, last_quarter=last_quarter, last_profit_report=last_profit_report)
            new_data.to_sql('{0}'.format(company), conn, schema='fundamental', if_exists='append', index=False)
            new_data.to_sql('all_fundamental'.format(company), conn, schema='fundamental', if_exists='append', index=False)
            # print(new_data)

        elif last_quarter == 'Tahunan' and last_profit.year[0] == today.year - 1 and today.month in range(4,10):
            new_data = process_response(company=company, last_quarter=last_quarter, last_profit_report=None)
            new_data.to_sql('{0}'.format(company), conn, schema='fundamental', if_exists='append', index=False)
            new_data.to_sql('all_fundamental'.format(company), conn, schema='fundamental', if_exists='append', index=False)
            # print(new_data)

        elif last_quarter == 'TW1' and last_profit.year[0] == today.year and today.month in range(7,12):
            new_data = process_response(company=company, last_quarter=last_quarter, last_profit_report=last_profit_report)
            new_data.to_sql('{0}'.format(company), conn, schema='fundamental', if_exists='append', index=False)
            new_data.to_sql('all_fundamental'.format(company), conn, schema='fundamental', if_exists='append', index=False)
            # print(new_data)

        elif last_quarter == 'TW2' and last_profit.year[0] == today.year and (today.month in range(10,13) or today.month in range(1,4)):
            new_data = process_response(company=company, last_quarter=last_quarter, last_profit_report=last_profit_report)
            new_data.to_sql('{0}'.format(company), conn, schema='fundamental', if_exists='append', index=False)
            new_data.to_sql('all_fundamental'.format(company), conn, schema='fundamental', if_exists='append', index=False)
            # print(new_data)

    except IndexError as e:
        print('Empty table for {0}'.format(company))
        print('Error Type:', e.__class__.__name__)
        print('Error Message:', e)

        last_year = today.year - 1
        if today.month in range(1,7):
            try:
                new_data = process_response(company=company, last_quarter='TW3', last_profit_report=0)
                new_data.loc[0, 'net_profit_quarter'] = None
                new_data.to_sql('{0}'.format(company), conn, schema='fundamental', if_exists='append', index=False)
                new_data.to_sql('all_fundamental'.format(company), conn, schema='fundamental', if_exists='append', index=False)
                # print(new_data)

            except Exception as e:
                print('File not found for {0}'.format(company))
                print('Error Type:', e.__class__.__name__)
                print('Error Message:', e)
                pass

        elif today.month in range(4,10):
            try:
                new_data = process_response(company=company, last_quarter='Tahunan', last_profit_report=0)
                new_data.loc[0, 'net_profit_quarter'] = None
                new_data.to_sql('{0}'.format(company), conn, schema='fundamental', if_exists='append', index=False)
                new_data.to_sql('all_fundamental'.format(company), conn, schema='fundamental', if_exists='append', index=False)
                # print(new_data)

            except Exception as e:
                print('File not found for {0}'.format(company))
                print('Error Type:', e.__class__.__name__)
                print('Error Message:', e)
                pass

        elif today.month in range(7,12):
            try:
                new_data = process_response(company=company, last_quarter='TW1', last_profit_report=0)
                new_data.loc[0, 'net_profit_quarter'] = None
                new_data.to_sql('{0}'.format(company), conn, schema='fundamental', if_exists='append', index=False)
                new_data.to_sql('all_fundamental'.format(company), conn, schema='fundamental', if_exists='append', index=False)
                # print(new_data)

            except Exception as e:
                print('File not found for {0}'.format(company))
                print('Error Type:', e.__class__.__name__)
                print('Error Message:', e)
                pass

        elif today.month in [1,2,3,10,11,12]:
            try:
                new_data = process_response(company=company, last_quarter='TW2', last_profit_report=0)
                new_data.loc[0, 'net_profit_quarter'] = None
                new_data.to_sql('{0}'.format(company), conn, schema='fundamental', if_exists='append', index=False)
                new_data.to_sql('all_fundamental'.format(company), conn, schema='fundamental', if_exists='append', index=False)
                # print(new_data)

            except Exception as e:
                print('File not found for {0}'.format(company))
                print('Error Type:', e.__class__.__name__)
                print('Error Message:', e)
                pass

    except AttributeError as e:
        print('Error Type:', e.__class__.__name__)
        # print('Error Message:', e)
        pass

    except s.exc.ProgrammingError as e:
        print('Table not exist for {0}'.format(company))
        print('Error Type:', e.__class__.__name__)
        new_data = pd.DataFrame({'company_code': [], 'quarter':[], 'year':[], 'net_profit_report':[], 'net_profit_quarter':[]})
        # new_data.to_sql('{0}'.format(company), conn, schema='fundamental', if_exists='append', index=False)
        print(new_data)
        pass

    except Exception as e:
        # print('File not found for {0}'.format(company))
        print('Error Class:', e.__class__)
        print('Error Type:', e.__class__.__name__)
        print('Error Message:', e)
        sendMessage('Error Class: {0}'.format(e.__class__))
        sendMessage('Error Type: {0}'.format(e.__class__.__name__))
        sendMessage('Error Message: {0}'.format(e))
        pass


if __name__ == "__main__":
    print_date = datetime.today().date().strftime('%Y-%m-%d')
    print('Begin incremental fundamental load at {0}'.format(print_date))
    sendMessage('Begin incremental fundamental load at {0}'.format(print_date))
    conn = pg_connect()

    active_company_q = '''
    SELECT company_code
    FROM "company"."active_company"
    ORDER BY 1
    '''

    active_company = pd.read_sql(active_company_q, conn)
    company_code_list = list(set(active_company.company_code))
    n_process = 5 #int(ceil(len(company_code_list) / 100))

    mp = Pool(n_process)
    print('Start with {0} parallel'.format(n_process))
    sendMessage('Start with {0} parallel processing'.format(n_process))
    start = time()
    for result in mp.map(get_last_profit, company_code_list):
        if result is not None:
            print(result)
    duration = time() - start
    print('Finished incremental fundamental load at {0} with duration {1:.2f} seconds'.format(print_date, duration))
    sendMessage('Finished incremental fundamental load at {0} with duration {1:.2f} seconds'.format(print_date, duration))
    grant_access()
    sendMessage('Access granted.')
