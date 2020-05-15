import click
import requests
import mimetypes
import pandas as pd
from pathlib import Path
from tinydb import TinyDB, where


def xlsx_to_dict(xlsx_file):
    dataframe = pd.read_excel(xlsx_file)
    return dataframe.to_dict('records')


def get_request(headers, api_method):
    url = f'{api_endpoint}{api_method}'
    response = requests.get(url, headers=headers)
    return response.json()


def applicants_to_db(db, root):
    applicants = []
    xlsx_files = list(root.glob('*.xlsx'))
    for file in xlsx_files:
        applicants += xlsx_to_dict(file)
    for applicant in applicants:
        db.insert({'name': applicant.get('ФИО'),
                   'position': applicant.get('Должность'),
                   'salary': applicant.get('Ожидания по ЗП'),
                   'comment': applicant.get('Комментарий'),
                   'status': applicant.get('Статус'),
                   'loaded': False
                   })


def resumes_to_db(db, root):
    resumes = list(root.glob('**/*.doc')) + list(root.glob('**/*.pdf'))
    for resume in resumes:
        applicant = resume.stem
        resume_file = resume.name
        mimetype = mimetypes.guess_type(resume_file, strict=True)[0]
        db.insert({'applicant': applicant,
                   'filename': resume_file,
                   'path': str(resume),
                   'mimetype': mimetype,
                   'loaded': False})



def get_account_id(headers):
    accounts = get_request(headers, '/accounts').get('items')
    if not accounts:
        click.echo('Извиняемся, ни одной компании не найдено :(')
        return False
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


def upload_resume(headers, account_id, resume):
    api_method = f'/account/{account_id}/upload'
    headers.update({'X-File-Parse': 'true'})
    url = f'{api_endpoint}{api_method}'
    filename = resume.get('filename')
    path = resume.get('path')
    mimetype = resume.get('mimetype')
    files = {
        'file': (filename, open(path, 'rb'), mimetype),
    }
    try:
        response = requests.post(url, headers=headers, files=files)
        response.raise_for_status()
    except requests.Timeout:
        click.echo('Время ожидания истекло, попробуйте позже.')
    except requests.HTTPError as err:
        click.echo(f'Загрузка файла {resume.get("filename")} '
                   f'завершилась с ошибкой: {err.response.status_code}.')
    except requests.ConnectionError as err:
        click.echo(f'Сетевые проблемы, попробуйте чуть позднее. Текст ошибки: {err}')
    except requests.RequestException as err:
        click.echo(f'Трудноуловимая ошибка: {err}')
    else:
        click.echo(f'Файл {filename} загружен.')
        return response.json()
    return


def get_vacancies(headers, account_id):
    return get_request(
        headers, f'/account/{account_id}/vacancies').get('items')


def load_applicant(headers, account_id, applicant):
    api_method = f'/account/{account_id}/applicants'
    url = f'{api_endpoint}{api_method}'
    response = requests.post(url, headers=headers)
    return response.json()


@click.command()
@click.option('--apikey', help='huntflow api key')
@click.option('--folder', type=click.Path(exists=True), help='folder with applicants')
def main(apikey, folder):
    root_folder = Path(folder)
    headers = {
        'User-Agent': 'huntflow-test/0.1 (lialinvitalii@gmail.com)',
        'Authorization': f'Bearer {apikey}'
    }
    account_id = get_account_id(headers)
    if not account_id:
        exit(1)
    vacancies = get_vacancies(headers, account_id)
    applicants_dbname = f'{folder}-applicants.json'
    resumes_dbname = f'{folder}-resumes.json'
    applicants_db = TinyDB(applicants_dbname, ensure_ascii=False, encoding='utf-8')
    resumes_db = TinyDB(resumes_dbname, ensure_ascii=False, encoding='utf-8')
    if not len(resumes_db):
        resumes_to_db(resumes_db, root_folder)
    unloaded_resumes = resumes_db.search(where('loaded') == False)
    for resume in unloaded_resumes:
        parsed_resume = upload_resume(headers, account_id, resume)
        resumes_db.update({'loaded': True, 'parsed_resume' : parsed_resume} ,doc_ids=[resume.doc_id])
    if not len(applicants_db):
        applicants_to_db(applicants_db, root_folder)
    unloaded_applicants = applicants_db.search(where('loaded') == False)
    # for applicant in unloaded_applicants:
    #     load_applicant(headers, account_id, applicant)

    # vacancies = get_vacancies(account_id, headers)
    # print(vacancies)



if __name__ == '__main__':
    api_endpoint = 'https://dev-100-api.huntflow.ru'
    main()
