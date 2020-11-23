import requests
import json
import re
import pdfkit
import os
import pandas as pd
import numpy as np

from configparser import ConfigParser
from pathlib import Path
from datetime import datetime
from shutil import copyfile


def main():
    parser = ConfigParser()
    parser.read('options.ini')

    path = Path(parser.get('settings', 'path'))    
    path_pdf = path / 'PDF_Reports/'
    path_excel = path / 'EXCEL_Reports/'
    path_excel_total = path_excel / 'BPL_raport_zbiorczy/'

    current_time = datetime.now()

    report_excel_file_name = 'BLP_' + current_time.strftime("%Y-%m-%d_%H%M") \
                             + '.xlsx'
    report_excel_file_name_org = path_excel / report_excel_file_name
    report_excel_file_name_copy = path_excel_total / report_excel_file_name

    pliki_csv = []
    for currentFile in path.glob("sap*.csv"):
        pliki_csv.append(currentFile)

    ile_csv = len(pliki_csv)

    if not pliki_csv:
        print(
            '\nNie widzę w folderze ani jednego pliku csv z danymi do '
            'analizy. Spróbuj ponownie.\n')
        input('Kliknij dowolny przycisk, żeby zakończyć program.')

    else:
        print(f'\nLiczba plików csv: {ile_csv}.')

        colnames = ['jednostka', 'numer dostawcy', 'NIP', 'nazwa dost',
                    'numer dokumentu', 'faktura', 'data faktury',
                    'kwota faktury', 'waluta', 'rachunek', 'data płatności',
                    'metoda', 'rok', 'jed_dok_rok'
                    ]

        plik = pd.concat(map(
            lambda file: pd.read_csv(file, sep=';', names=colnames, dtype=str),
            pliki_csv))

        plik['Czy odszukał po rachunku?'] = ''
        plik['Czy odszukał po NIPie?'] = ''
        plik['Czy jest w bazie MF?'] = ''
        plik['Nip zgodny?'] = ''
        plik['Czy ma rachunki wirtualne?'] = ''
        plik['status VAT'] = ''
        plik['requestId'] = ''
        plik['code'] = ''
        plik['message'] = ''

        plik['minus'] = plik['kwota faktury'].apply(
            lambda x: -1 if '-' in x else 1)  # Handle negative numbers

        plik['kwota faktury'] = plik['kwota faktury'].apply(
            lambda x: x.replace('.', ''))
        plik['kwota faktury'] = plik['kwota faktury'].apply(
            lambda x: x.replace(',', '.'))
        plik['kwota faktury'] = plik['kwota faktury'].apply(
            lambda x: x.replace('-', '') if '-' in x else x)
        plik['kwota faktury'] = plik['kwota faktury'].astype(float)

        if any(plik['kwota faktury'] < 0):
            plik.loc[plik.minus == -1, 'kwota faktury'] = - plik[
                'kwota faktury']

        plik = plik.drop(['jed_dok_rok', 'minus'], axis=1)

        columns = plik.columns.tolist()
        plik = plik.drop_duplicates()

        plik['numer dostawcy'] = plik['numer dostawcy'].astype(int)
        plik['pracownicy_1'] = plik['numer dostawcy'] < 1200000
        plik['pracownicy_2'] = plik['numer dostawcy'] > 1499999

        plik = plik[(plik['pracownicy_1'] == True) |
                    (plik['pracownicy_2'] == True)]

        plik = plik.drop(['pracownicy_1', 'pracownicy_2'], axis=1)

        plik_unikaty = plik.drop_duplicates(['NIP', 'rachunek'])

        ile_unikatow = len(plik_unikaty.index)
        print(f'Do sprawdzenia {ile_unikatow} par NIP - rachunek.\n')

        plik_dict = plik_unikaty.to_dict('records')

        today = datetime.now().strftime('%Y-%m-%d')

        licznik = 0
        for podmiot in plik_dict:
            licznik += 1
            nip = podmiot['NIP']
            rachunek = podmiot['rachunek']
            print(f'Sprawdzam: {licznik} na {ile_unikatow}', nip, rachunek)
            # szukamy po rachunku
            url = 'https://wl-api.mf.gov.pl/api/search/bank-account/' \
                  f'{rachunek}?date={today }'

            headers = {
                        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) '
                        'AppleWebKit/537.36 (KHTML, like Gecko) '
                        'Chrome/56.0.2924.76 Safari/537.36'
                        }

            try:
                r = requests.get(url, headers=headers, verify=True)
                response_dict = r.json()
                odpowiedz = json.dumps(response_dict)

                if 'code' in odpowiedz:
                    podmiot['code'] = response_dict['code']
                    podmiot['message'] = response_dict['message']
                    podmiot['Czy odszukał po rachunku?'] = False

                    url = f'https://wl-api.mf.gov.pl/api/search/nip/{nip}?'\
                          f'date={today }'
                    r = requests.get(url, headers=headers, verify=True)
                    response_dict = r.json()
                    odpowiedz_nip = json.dumps(response_dict)
                    if 'code' in odpowiedz_nip:
                        podmiot['code'] = response_dict['code']
                        podmiot['message'] = response_dict['message']
                        podmiot['Czy odszukał po NIPie?'] = False
                    if 'result' in odpowiedz_nip:
                        if 'name' in odpowiedz_nip:
                            podmiot['requestId'] = response_dict['result'][
                                'requestId']
                            podmiot['Czy jest w bazie MF?'] = True
                            podmiot['Czy odszukał po NIPie?'] = True
                            if 'subjects' in odpowiedz_nip:
                                podmiot['status VAT'] = \
                                    response_dict['result']['subjects'] \
                                    [0]['statusVat']
                            else:
                                podmiot['status VAT'] = \
                                    response_dict['result'] \
                                    ['subject']['statusVat']

                        else:
                            podmiot['Czy jest w bazie MF?'] = False
                            podmiot['Czy odszukał po NIPie?'] = False
                            podmiot['requestId'] = response_dict['result'][
                                'requestId']

                if 'result' in odpowiedz:
                    if 'name' in odpowiedz:
                        podmiot['Czy odszukał po rachunku?'] = True
                        podmiot['Czy ma rachunki wirtualne?'] = response_dict[
                            'result']['subjects'][0]['hasVirtualAccounts']
                        podmiot['requestId'] = response_dict['result'] \
                            ['requestId']
                        podmiot['Nip zgodny?'] = (nip ==
                                                  response_dict['result'][
                                                      'subjects'][0]['nip'])
                        podmiot['Czy jest w bazie MF?'] = True
                    else:
                        podmiot['Czy odszukał po rachunku?'] = False
                        url = 'https://wl-api.mf.gov.pl/api/search/nip/' \
                              f'{nip}?date={today }'
                        r = requests.get(url, headers=headers, verify=True)
                        response_dict = r.json()
                        odpowiedz_nip = json.dumps(response_dict)

                        if 'code' in odpowiedz_nip:
                            podmiot['code'] = response_dict['code']
                            podmiot['message'] = response_dict['message']
                            podmiot['Czy odszukał po NIPie?'] = False
                        if 'result' in odpowiedz_nip:
                            if 'name' in odpowiedz_nip:
                                podmiot['requestId'] = response_dict['result'][
                                    'requestId']
                                podmiot['Czy jest w bazie MF?'] = True
                                podmiot['Czy odszukał po NIPie?'] = True
                                if 'subjects' in odpowiedz_nip:
                                    podmiot['status VAT'] = \
                                    response_dict['result']['subjects'][0][
                                        'statusVat']
                                else:
                                    podmiot['status VAT'] = \
                                    response_dict['result']['subject'][
                                        'statusVat']

                            else:
                                podmiot['Czy jest w bazie MF?'] = False
                                podmiot['Czy odszukał po NIPie?'] = False
                                podmiot['requestId'] = response_dict['result'][
                                    'requestId']

            except:
                print('Coś nie gra. Czy masz połączenie z internetem?')

        output_df = pd.DataFrame(plik_dict)

        output_df['jednostka'] = output_df['jednostka'].astype(str)
        output_df['numer dostawcy'] = output_df['numer dostawcy'].astype(str)
        output_df['rachunek'] = output_df['rachunek'].astype(str)

        output_df['index'] = output_df['numer dostawcy'] + output_df[
            'rachunek']
        output_df = output_df.set_index('index')
        output_df['Czy odszukał po rachunku?'] = output_df[
            'Czy odszukał po rachunku?'].astype(str)
        output_df['Czy jest w bazie MF?'] = output_df[
            'Czy jest w bazie MF?'].astype(str)
        output_df['Czy odszukał po NIPie?'] = output_df[
            'Czy odszukał po NIPie?'].astype(str)
        output_df['Nip zgodny?'] = output_df['Nip zgodny?'].astype(str)
        output_df['Czy ma rachunki wirtualne?'] = output_df[
            'Czy ma rachunki wirtualne?'].astype(str)

        output_df = output_df[columns]

        plik['jednostka'] = plik['jednostka'].astype(str)
        plik['numer dostawcy'] = plik['numer dostawcy'].astype(str)
        plik['rachunek'] = plik['rachunek'].astype(str)

        plik['index'] = plik['numer dostawcy'] + plik['rachunek']
        plik = plik.set_index('index')
        plik = plik.replace('', np.NaN)
        plik = plik.combine_first(output_df)

        writer = pd.ExcelWriter(report_excel_file_name_org,
                                engine='xlsxwriter')
        plik.to_excel(writer, index=False, sheet_name='report')
        workbook = writer.book
        worksheet = writer.sheets['report']

        header_format = workbook.add_format({
            'bold': True,
            'text_wrap': True,  
            'align': 'vcenter',  # formatowanie w pionie
            'valign': 'center',  # formatowanie w poziomie
            'fg_color': '#cceeff',
            'border': 1})

        # Write the column headers with the defined format.
        for col_num, value in enumerate(plik.columns.values):
            worksheet.write(0, col_num, value, header_format)

        # format_columns = workbook.add_format({'text_wrap': True})

        # worksheet.set_column('A:T', None, format_columns)
        worksheet.set_column('A:A', 9)
        worksheet.set_column('C:C', 11)
        worksheet.set_column('D:D', 35)
        worksheet.set_column('E:E', 11)
        worksheet.set_column('F:F', 20)
        worksheet.set_column('G:G', 10)
        worksheet.set_column('H:H', 13)
        worksheet.set_column('J:J', 30)
        worksheet.set_column('K:K', 10)
        worksheet.set_column('L:P', 10)
        worksheet.set_column('Q:Q', 17)
        worksheet.set_column('R:R', 14)
        worksheet.freeze_panes(1, 0)

        writer.save()

        copyfile(report_excel_file_name_org, report_excel_file_name_copy)

        print('\nRaport excel jest gotowy. Przygotowuję raport pdf.\n')

        mniejsza_tabela = plik[
            ['jednostka', 'numer dokumentu', 'NIP', 'nazwa dost', 'faktura',
             'data faktury', 'kwota faktury',
             'rachunek', 'Czy odszukał po rachunku?', 'Czy odszukał po NIPie?',
             'Czy jest w bazie MF?', 'Nip zgodny?',
             'status VAT', 'requestId']]

        html = mniejsza_tabela.to_html(index=False, na_rep='', col_space=110)

        # poniższego nie używam. generalnie border=0 oznacza brak krawędzi
        html2 = re.sub('table border="1"', 'table border="0"', html)

        # poniższa zmienna nie jest potrzebna. domyślne kodowanie działa dobrze
        kodowanie = """
        <head>
            <meta charset="UTF-8">
        </head>
        """

        styl = """
        <style>
            table {
                border-collapse: collapse;
            }
            th, td {
                border: 1px solid #ff6600;
                padding: 10px;
                text-align: left;
            }
        </style>
        """

        report_pdf_file_name = 'BLP_' + current_time.strftime(
            "%Y-%m-%d_%H%M") + '.pdf'

        wkhtmltopdf = Path(parser.get('settings', 'wkhtmltopdf'))
        str_wkhtmltopdf=str(wkhtmltopdf)
        str_wkhtmltopdf_corrected = str_wkhtmltopdf.replace('\\', '\\\\')
        config = pdfkit.configuration(wkhtmltopdf=str_wkhtmltopdf_corrected)

        options = {'orientation': 'Landscape',
                   'encoding': 'UTF-8',
                   'header-center': \
                       'Biała Lista Podatników weryfikacja w dniu ' \
                       + current_time.strftime("%Y/%m/%d"),
                   'dpi': 400}

        os.chdir(path_pdf)

        pdfkit.from_string(styl + html, report_pdf_file_name, options=options,
                           configuration=config)

        print('\nRaport pdf jest gotowy.\n')
        input('Kliknij dowolny przycisk, żeby zakończyć program.')


if __name__ == '__main__':
    main()
