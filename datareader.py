"""
    Modulo com funcoes para leitura de dados do Yahoo Finance
"""


from pandas_datareader import data as pdr
from datetime import timedelta, datetime
import pandas as pd
import fix_yahoo_finance as yf

yf.pdr_override()  # Ajuste para funcionamento do get_data_yahoo

PATH_ACOES = 'data/acoes/'


def read_stock_data(ticker, suf='.SA'):
    """
        Retorna o dataframe dos dados guardados de uma acao
    """
    return pd.read_pickle(PATH_ACOES + 'pickle/' + ticker + suf)


def get_stock_data(ticker, start_date, end_date, suf='.SA'):
    """
        Retorna um dataframe com a leitura do Yahoo Finances para a acao de
        start_date ate end_date

        As datas devem ser passadas no formato yyyy-mm-dd

        suf sera o sufixo do nome da acao para leitura do yahoo
    """
    data = pdr.get_data_yahoo(ticker + suf, start_date, end_date)
    data.reset_index(inplace=True)
    return data


def download_stock_data(ticker, start_date='2013-01-01', suf='.SA'):
    """
        Baixa todas as cotacoes de uma acao de start_date ate o dia anterior
        a execucao do script. Salva os dados em um arquivo CSV e um pickle

        start_date deve estar no formato yyyy-mm-dd

        suf sera o sufixo do nome da acao para leitura do yahoo
    """
    end_date = (datetime.now().date() - timedelta(days=1)).strftime('%Y-%m-%d')
    print('Baixando informacoes da acao ' + ticker + ' das datas: '
          + start_date + ' ate ' + end_date)
    data = get_stock_data(ticker, start_date, end_date, suf=suf)
    data.to_pickle(PATH_ACOES + 'pickle/' + ticker + suf)
    data.to_csv(PATH_ACOES + 'csv/' + ticker + suf + '.csv', index=False)


def update_stock_data(ticker, suf='.SA'):
    """
        Atualiza os dados de uma acao do ultimo dia salvo ate o dia anterior
        a execucao do script
    """
    df = read_stock_data(ticker, suf=suf)
    start_date = df.tail(1)['Date'].iloc[0].date() + timedelta(days=1)
    end_date = datetime.now().date() - timedelta(days=1)
    data = get_stock_data(ticker,
                          start_date.strftime('%Y-%m-%d'),
                          end_date.strftime('%Y-%m-%d'), suf=suf)
    new_df = df.append(data, ignore_index=True)
    csv_info = data.to_csv(index=False, header=False)
    new_df.to_pickle(PATH_ACOES + 'pickle/' + ticker + suf)
    with open(PATH_ACOES + 'csv/' + ticker + suf + '.csv', "a") as f:
        f.write(csv_info)


def download_stock_list(stock_list):
    """
        Baixa as informacoes de todas as acoes em stock_list
    """
    for stock in stock_list:
        download_stock_data(stock)


def update_stock_list(stock_list):
    """
        Atualiza a informacao de todas as acoes em stock_list
    """
    for stock in stock_list:
        update_stock_data(stock)


def main():
    """
        Faz o download das acoes do arquivo lista_acoes.
        Para fazer o update basta trocar a funcao da ultima linha
    """
    stock_list = []
    with open('data/lista_acoes') as f:
        for line in f.readlines():
            stock_list.append(line.split()[0])
    download_stock_list(stock_list)
    # update_stock_list(stock_list)


if __name__ == '__main__':
    main()
