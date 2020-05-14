import os
import click
import requests
import pandas as pd
from tinydb import TinyDB, where


def get_xlsx_files(folder):
    return [os.path.join(folder, file) for file in
                  os.listdir(folder) if file.endswith('.xlsx')]


def xlsx_to_dict(xlsx_file):
    dataframe = pd.read_excel(xlsx_file)
    return dataframe.to_dict('records')


def parse_xlsx(db, applicants, folder):
    xlsx_files = get_xlsx_files(folder)
    for file in xlsx_files:
        applicants += xlsx_to_dict(file)
    for applicant in applicants:
        db.insert({'name': applicant.get('ФИО'),
                   'position': applicant.get('Должность'),
                   'salary': applicant.get('Ожидания по ЗП'),
                   'comment': applicant.get('Комментарий'),
                   'status': applicant.get('Статус'),
                   'resume': None,
                   'loaded': False
                   })


def get_folders_with_resumes(folder):
    return [os.path.join(folder, name)
            for name in os.listdir(folder)
            if os.path.isdir(os.path.join(folder, name))]


def get_resumes(resumes_folder):
    return [os.path.join(resumes_folder, resume)
            for resume in os.listdir(resumes_folder)]


def get_account_id(apikey):
    url = f'{api_endpoint}/accounts'
    headers = {
        'User-Agent': 'huntflow-test/0.1 (lialinvitalii@gmail.com)',
        'Authorization': f'Bearer {apikey}'
    }
    response = requests.get(url, headers=headers)
    response_json = response.json()
    accounts = response_json.get('items')
    if not accounts:
        click.echo('Извиняемся, ни одной компании не найдено :(')
    else:
        if len(accounts) == 1:
            return accounts[0].get('id')
        else:
            click.echo('Доступные компании: ')
            for number, account in enumerate(accounts):
                click.echo(f'{number + 1} - {account.get("name")}')
            value = click.prompt(
                'Для какой компании добавить соискателей? Введите номер',
                type=int
            )
            return accounts[value - 1].get('id')


def load_applicant(apikey, applicant, account_id):
    pass


@click.command()
@click.option('--apikey', help='huntflow api key')
@click.option('--folder', type=click.Path(exists=True), help='folder with applicants')
def main(apikey, folder):
    applicants = []
    dbname = f'{folder}.json'
    db = TinyDB(dbname, ensure_ascii=False, encoding='utf-8')
    if not len(db):
        resumes = []
        resumes_folders = get_folders_with_resumes(folder)
        for resumes_folder in resumes_folders:
            resumes += get_resumes(resumes_folder)
        parse_xlsx(db, applicants, folder)
    else:
        applicants = db.search(where('loaded') == False)
    account_id = get_account_id(apikey)
    for applicant in applicants:
        load_applicant(apikey, applicant, account_id)


if __name__ == '__main__':
    api_endpoint = 'https://dev-100-api.huntflow.ru'
    main()
