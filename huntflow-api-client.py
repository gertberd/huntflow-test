import json
import shutil
import mimetypes
from pathlib import Path

import click
import requests
import colorama  # used by click for color output in MS Windows
import pandas as pd
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
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
    except requests.Timeout:
        click.secho('Время ожидания истекло, попробуйте позже.',
                    fg='red')
    except requests.HTTPError as err:
        click.secho(f'Ошибка HTTP, код ошибки: {err.response.status_code}.',
                    fg='red')
    except requests.ConnectionError as err:
        click.secho(f'Сетевые проблемы, попробуйте чуть позднее. Текст ошибки: {err}',
                    fg='red')
    except requests.RequestException as err:
        click.secho(f'Трудноуловимая ошибка: {err}',
                    fg='red')
    else:
        return response.json()
    finally:
        if response.status_code != 200:
            click.secho('Невозможно продолжить работу, '
                        'проверьте валидность токена и доступность API!',
                        bg='red',
                        fg='yellow')
            exit(1)



def get_vacancies(headers, account_id):
    click.echo('Получение вакансий...')
    vacancies = get_request(
        headers, f'/account/{account_id}/vacancies'
    ).get('items')
    if not vacancies:
        click.secho('Ни одной вакансии не найдено.',
                    fg='red')
        return
    return vacancies


def get_statuses(headers, account_id):
    click.echo('Получение статусов...')
    statuses = get_request(
        headers, f'/account/{account_id}/vacancy/statuses'
    ).get('items')
    if not statuses:
        click.secho('Ни одного статуса не найдено.',
                    fg='red')
        return
    return statuses


def get_sources(headers, account_id):
    click.echo('Получение источников резюме...')
    sources = get_request(
        headers, f'/account/{account_id}/applicant/sources'
    ).get('items')
    if not sources:
        click.secho('Ни одного источника резюме не найдено.',
                    fg='red')
        return
    return sources


def get_account_id(headers):
    click.echo('Получение id компании...')
    accounts = get_request(headers, '/accounts').get('items')
    if not accounts:
        click.secho('Ни одной компании не найдено.',
                    fg='red')
        return
    else:
        if len(accounts) == 1:
            return accounts[0].get('id')
        else:
            click.secho('Доступные компании: ',
                        fg='yellow')
            for number, account in enumerate(accounts):
                click.secho(f'{number + 1} - {account.get("name")}',
                            fg='yellow')
            value = click.prompt(
                'Для какой компании добавить соискателей? Введите номер',
                type=int
            )
            return accounts[value - 1].get('id')


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
                   'loaded': False,
                   'attached': False
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
        click.secho('Время ожидания истекло, попробуйте позже.',
                    fg='red')
    except requests.HTTPError as err:
        click.secho(f'Загрузка файла {resume.get("filename")} '
                    f'завершилась с ошибкой: {err.response.status_code}.',
                    fg='red')
    except requests.ConnectionError as err:
        click.secho(f'Сетевые проблемы, попробуйте чуть позднее.'
                    f' Текст ошибки: {err}',
                    fg='red')
    except requests.RequestException as err:
        click.secho(f'Трудноуловимая ошибка: {err}',
                    fg='red')
    else:
        click.secho(f'Файл {filename} загружен.',
                    fg='green')
        return response.json()
    return


def bind_resume_to_applicant(headers,
                             account_id,
                             resumes_db,
                             applicants_db,
                             resume):
    parsed_resume = parse_resume(headers, account_id, resume)
    resumes_db.update({'parsed': True},
                      doc_ids=[resume.doc_id])
    name_from_resume_set = set(parsed_resume.get('fields').get('name').values())
    for applicant in applicants_db:
        applicant_name_set = set(applicant.get('name').split(' '))
        if applicant_name_set == name_from_resume_set or \
                applicant_name_set.issubset(name_from_resume_set) or \
                applicant_name_set.issuperset(name_from_resume_set):
            applicants_db.update({'parsed_resume': parsed_resume},
                                 doc_ids=[applicant.doc_id])


# TODO: try to use json_proccesor
def prepare_to_load(applicant):
    resume = applicant.get('parsed_resume')
    fields = resume.get('fields')
    middle_name, last_name, first_name = fields.get('name').values()
    phone = fields.get('phones')[0]
    email = fields.get('email')
    position = fields.get('experience')[0].get('position')
    company = fields.get('experience')[0].get('company')
    money = applicant.get('salary')
    birthday_month = birthday_day = birthday_year = None
    birthdate = fields.get('birthdate')
    photo = resume.get('photo').get('id')
    body = resume.get('text')
    file_id = resume.get('id')
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
                "auth_type": 'NATIVE',
                "files": [
                    {
                        "id": file_id
                    }
                ],
                "account_source": None  # Нет этих данных
            }
        ]
    }
    return prepared_applicant


def load_to_huntflow(headers, account_id, applicant):
    api_method = f'/account/{account_id}/applicants'
    url = f'{api_endpoint}{api_method}'
    try:
        response = requests.post(url, headers=headers, data=json.dumps(applicant))
        response.raise_for_status()
    except requests.Timeout:
        click.secho('Время ожидания истекло, попробуйте позже.',
                    fg='red')
    except requests.HTTPError as err:
        click.secho(f'Загрузка кандидата '
                    f'завершилась с ошибкой: {err.response.status_code}.',
                    fg='red')
    except requests.ConnectionError as err:
        click.secho(f'Сетевые проблемы, '
                    f'попробуйте чуть позднее. Текст ошибки: {err}',
                    fg='red')
    except requests.RequestException as err:
        click.secho(f'Трудноуловимая ошибка: {err}',
                    fg='red')
    else:
        click.secho(f'Кандидат загружен.',
                    fg='green')
        return response.json()
    return


def load_applicant(headers,
                   account_id,
                   applicants_db,
                   vacancies,
                   statuses,
                   applicant):
    salary_text = str(applicant.get('salary'))
    salary = ''.join(filter(str.isdigit, salary_text))
    applicant['salary'] = f'{salary} руб.'
    position_text = applicant.get('position')
    status_text = applicant.get('status')
    for vacancy in vacancies:
        if position_text == vacancy.get('position'):
            applicants_db.update({'vacancy': vacancy.get('id')},
                                 doc_ids=[applicant.doc_id])
    for key, value in statuses_dict.items():
        if status_text in value:
            applicants_db.update({'status': key},
                                 doc_ids=[applicant.doc_id])
            if key == 'Declined':
                rejection_reason = 21  # грязный хак, не нашёл где взять все id через api
                applicants_db.update({'rejection_reason': rejection_reason},
                                     doc_ids=[applicant.doc_id])
            for status in statuses:
                if status.get('name') == key:
                    status_id = status.get('id')
                    applicants_db.update({'status_id': status_id},
                                         doc_ids=[applicant.doc_id])
    prepared_applicant = prepare_to_load(applicant)
    huntflow_response = load_to_huntflow(headers, account_id, prepared_applicant)
    if huntflow_response:
        applicants_db.update({
            'huntflow_response': huntflow_response,
            'loaded': True},
            doc_ids=[applicant.doc_id])


# TODO: try to use json_proccesor
def prepare_to_attach(applicant):
    vacancy = applicant.get('vacancy')
    status_id = applicant.get('status_id')
    comment = applicant.get('comment')
    file_id = applicant.get('parsed_resume').get('id')
    rejection_reason = applicant.get('rejection_reason')
    prepared_applicant = {
        "vacancy": vacancy,
        "status": status_id,
        "comment": comment,
        "files": [
            {
                "id": file_id
            }
        ],
        "rejection_reason": rejection_reason
    }
    return prepared_applicant


def attach_to_vacancy(headers, account_id, applicant, prepared_applicant):
    applicant_id = applicant.get('huntflow_response').get('id')
    api_method = f'/account/{account_id}/applicants/{applicant_id}/vacancy'
    url = f'{api_endpoint}{api_method}'
    try:
        response = requests.post(url, headers=headers, data=json.dumps(prepared_applicant))
        response.raise_for_status()
    except requests.Timeout:
        click.secho('Время ожидания истекло, '
                    'попробуйте позже.',
                    fg='red')
    except requests.HTTPError as err:
        click.secho(f'Добавление кандидата на вакансию '
                    f'завершилось с ошибкой: {err.response.status_code}.',
                    fg='red')
    except requests.ConnectionError as err:
        click.secho(f'Сетевые проблемы, '
                    f'попробуйте чуть позднее. Текст ошибки: {err}',
                    fg='red')
    except requests.RequestException as err:
        click.secho(f'Трудноуловимая ошибка: {err}',
                    fg='red')
    else:
        click.secho(f'Кандидат добавлен.',
                    fg='green')
        return response.json()
    return


@click.command()
@click.option('--token', help='huntflow-api token')
@click.option('--folder',
              type=click.Path(exists=True),
              help='folder with applicants')
def main(token, folder):
    root_folder = Path(folder)
    headers = {
        'User-Agent': 'huntflow-test/0.1 (lialinvitalii@gmail.com)',
        'Authorization': f'Bearer {token}'
    }
    account_id = get_account_id(headers)
    vacancies = get_vacancies(headers, account_id)
    statuses = get_statuses(headers, account_id)
    # sources = get_sources(headers, account_id)
    if None in [account_id, vacancies, statuses]:
        click.secho('Выход из программы',
                    fg='red')
        exit(1)
    applicants_dbname = f'{folder}-applicants.json'
    resumes_dbname = f'{folder}-resumes.json'
    applicants_db = TinyDB(applicants_dbname, ensure_ascii=False, encoding='utf-8')
    resumes_db = TinyDB(resumes_dbname, ensure_ascii=False, encoding='utf-8')
    if not len(applicants_db):
        applicants_to_db(applicants_db, root_folder)
    if not len(resumes_db):
        resumes_to_db(resumes_db, root_folder)
    unparsed_resumes = resumes_db.search(
        where('parsed') == False
    )
    if unparsed_resumes:
        for resume in unparsed_resumes:
            bind_resume_to_applicant(headers,
                                     account_id,
                                     resumes_db,
                                     applicants_db,
                                     resume
                                     )
    else:
        click.secho('Все резюме загружены и распарсены.',
                    fg='green')
    unloaded_applicants = applicants_db.search(
        where('loaded') == False
    )
    if unloaded_applicants:
        for applicant in unloaded_applicants:
            load_applicant(headers,
                           account_id,
                           applicants_db,
                           vacancies,
                           statuses,
                           applicant
                           )
    else:
        click.secho('Все кандидаты добавлены в базу.',
                    fg='green')
    unattached_applicants = applicants_db.search(
        where('attached') == False
    )
    if unattached_applicants:
        for applicant in unattached_applicants:
            prepared_applicant = prepare_to_attach(applicant)
            if attach_to_vacancy(headers, account_id, applicant, prepared_applicant):
                applicants_db.update({'attached': True}, doc_ids=[applicant.doc_id])
    else:
        click.secho('Все кандидаты добавлены на вакансии.',
                    fg='green')
        if click.confirm(f'Удалить папку {root_folder}?'):
            shutil.rmtree(root_folder)


if __name__ == '__main__':
    api_endpoint = 'https://dev-100-api.huntflow.ru'
    main()
