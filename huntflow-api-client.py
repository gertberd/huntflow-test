import json
import click
import requests
import mimetypes
import pandas as pd
from pathlib import Path
from tinydb import TinyDB, where


statuses_dict = {
    'New Lead': [],
    'Submitted': [],
    'Contacted': ['Отправлено письмо'],
    'HR Interview': ['Интервью с HR'],
    'Client Interview': [],
    'Offered': ['Выставлен оффер'],
    'Offer Accepted': [],
    'Hired': [],
    'Trial passed': [],
    'Declined': ['Отказ'],
}


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
        db.insert({'name': applicant.get('ФИО').strip(),
                   'position': applicant.get('Должность'),
                   'salary': applicant.get('Ожидания по ЗП'),
                   'comment': applicant.get('Комментарий'),
                   'status': applicant.get('Статус'),
                   'parsed_resume': None,
                   'huntflow_response': None,
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
                   'parsed': False,
                   })


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


def parse_resume(headers, account_id, resume):
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
        headers, f'/account/{account_id}/vacancies'
    ).get('items')


def get_statuses(headers, account_id):
    return get_request(
        headers, f'/account/{account_id}/vacancy/statuses'
    ).get('items')


def get_sources(headers, account_id):
    return get_request(
        headers, f'/account/{account_id}/applicant/sources'
    ).get('items')


# TODO: try to use json_proccesor
def prepare_applicant(applicant):
    resume = applicant.get('parsed_resume')
    fields = resume.get('fields')
    middle_name, last_name, first_name  = fields.get('name').values()
    phone = fields.get('phones')[0]
    email = fields.get('email')
    position = fields.get('position') # или fields.get('experience')[0].get('position')
    company = fields.get('experience')[0].get('company')
    money = applicant.get('salary')
    birthday_month = birthday_day = birthday_year = None
    birthdate = fields.get('birthdate')
    photo = resume.get('photo').get('id')
    body = resume.get('text')
    id = resume.get('id')
    if birthdate:
        birthday_month, birthday_day, _, birthday_year = birthdate.values()
    prepared_applicant = {
        "last_name": last_name,
        "first_name": first_name,
        "middle_name": middle_name,
        "phone": phone,
        "email": email,
        "position": position,
        "company": company,
        "money": money,
        "birthday_day": birthday_day,
        "birthday_month": birthday_month,
        "birthday_year": birthday_year,
        "photo": photo,
        "externals": [
            {
                "data": {
                    "body": body
                },
                "auth_type": 'NATIVE',  # Не знаю, что здесь должно быть, значение взял из примера на github
                "files": [
                    {
                        "id": id
                    }
                ],
                "account_source": None  # Нет этих данных
            }
        ]
    }
    return prepared_applicant


# def prepare_applicant(applicant, vacancy):
#     vacancy_id = 1
#     comment = ''
#     file_id = 1
#     prepared_applicant = {
#         "vacancy": vacancy_id,
#         "status": 1230,
#         "comment": comment,
#         "files": [
#             {
#                 "id": file_id
#             }
#         ],
#         "rejection_reason": None
#     }
#     return prepared_applicant


def load_applicant(headers, account_id, applicant):
    api_method = f'/account/{account_id}/applicants'
    url = f'{api_endpoint}{api_method}'
    try:
        response = requests.post(url, headers=headers, data=json.dumps(applicant))
        response.raise_for_status()
    except requests.Timeout:
        click.echo('Время ожидания истекло, попробуйте позже.')
    except requests.HTTPError as err:
        click.echo(f'Загрузка кандидата \
                   завершилась с ошибкой: {err.response.status_code}.')
    except requests.ConnectionError as err:
        click.echo(f'Сетевые проблемы, \
                   попробуйте чуть позднее. Текст ошибки: {err}')
    except requests.RequestException as err:
        click.echo(f'Трудноуловимая ошибка: {err}')
    else:
        click.echo(f'Кандидат загружен.')
        return response.json()
    return


# def add_applicant_to_vacancy(headers, account_id, prepared_applicant):
#     api_method = f'/account/{account_id}/applicants/{applicant_id}/vacancy'
#     url = f'{api_endpoint}{api_method}'
#     try:
#         response = requests.post(url, headers=headers, data=json.dumps(applicant))
#         response.raise_for_status()
#     except requests.Timeout:
#         click.echo('Время ожидания истекло, попробуйте позже.')
#     except requests.HTTPError as err:
#         click.echo(f'Загрузка файла {err} \
#                    завершилась с ошибкой: {err.response.status_code}.')
#     except requests.ConnectionError as err:
#         click.echo(f'Сетевые проблемы, \
#                    попробуйте чуть позднее. Текст ошибки: {err}')
#     except requests.RequestException as err:
#         click.echo(f'Трудноуловимая ошибка: {err}')
#     else:
#         click.echo(f'Файл загружен.')
#         return response.json()
#     return


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
        click.echo('Выход из программы')
        exit(1)
    vacancies = get_vacancies(headers, account_id)
    statuses = get_statuses(headers, account_id)
    # sources = get_sources(headers, account_id)
    applicants_dbname = f'{folder}-applicants.json'
    resumes_dbname = f'{folder}-resumes.json'
    applicants_db = TinyDB(applicants_dbname, ensure_ascii=False, encoding='utf-8')
    resumes_db = TinyDB(resumes_dbname, ensure_ascii=False, encoding='utf-8')
    if not len(applicants_db):
        applicants_to_db(applicants_db, root_folder)
    if not len(resumes_db):
        resumes_to_db(resumes_db, root_folder)
    unparsed_resumes = resumes_db.search(where('parsed') == False)
    for resume in unparsed_resumes:
        parsed_resume = parse_resume(headers, account_id, resume)
        resumes_db.update({'parsed': True}, doc_ids=[resume.doc_id])
        name_from_resume_set = set(parsed_resume.get('fields').get('name').values())
        for applicant in applicants_db:
            applicant_name_set = set(applicant.get('name').split(' '))
            if applicant_name_set == name_from_resume_set or \
                    applicant_name_set.issubset(name_from_resume_set) or \
                    applicant_name_set.issuperset(name_from_resume_set):
                applicants_db.update({'parsed_resume': parsed_resume}, doc_ids=[applicant.doc_id])
    unloaded_applicants = applicants_db.search(where('loaded') == False)
    for applicant in unloaded_applicants:
        doc_id = applicant.doc_id
        salary_text = str(applicant.get('salary'))
        salary = ''.join(filter(str.isdigit, salary_text))
        applicants_db.update({'salary': f'{salary} руб.'}, doc_ids=[doc_id])
        position_text = applicant.get('position')
        status_text = applicant.get('status')
        for vacancy in vacancies:
            if position_text == vacancy.get('position'):
                applicants_db.update({'vacancy': vacancy.get('id')}, doc_ids=[doc_id])
        for key, value in statuses_dict.items():
            if status_text in value:
                applicants_db.update({'status': key}, doc_ids=[applicant.doc_id])
                for status in statuses:
                    if status.get('name') == key:
                        status_id = status.get('id')
                        applicants_db.update({'status_id': status_id}, doc_ids=[doc_id])
        prepared_applicant = prepare_applicant(applicant)
        huntflow_response = load_applicant(headers, account_id, prepared_applicant)
        applicants_db.update({'huntflow_response': huntflow_response}, doc_ids=[doc_id])


if __name__ == '__main__':
    api_endpoint = 'https://dev-100-api.huntflow.ru'
    main()
