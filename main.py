import os
import re 
import requests
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

spreadId = '1Tn0Y02OMSAGprBsLm_gM9c0awUHDVEI48TCACQEYJY4'


def get_vk_ads_campaigns(access_token, account_id):
    url = 'https://api.vk.com/method/ads.getCampaigns'
    
    params = {
        'access_token': access_token,
        'account_id': account_id,
        'include_deleted': 0,
        'v': '5.199'
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        if 'response' in data:
            campaign_ids = [str(campaign['id']) for campaign in data['response']]
            return ','.join(campaign_ids)
        else:
            return 'No response data in VK API response'
    else:
        return f'Failed to get data from VK API, status code: {response.status_code}'

def get_vk_ads_statistics(access_token, account_id, ids, date_from, date_to):
    url = 'https://api.vk.com/method/ads.getStatistics'
    
    params = {
        'access_token': access_token,   
        'account_id': account_id,
        'ids_type': "campaign",
        'ids': ids,
        'period': 'day',
        'date_from': date_from,
        'date_to': date_to,
        'stats_fields': 'views_times',
        'v': '5.199'
    }
    
    response = requests.get(url, params=params)
    
    if response.status_code == 200:
        print("Статистика получена")
        return response.json()
    else:
        return {'error': 'Failed to get data from VK API', 'status_code': response.status_code}

def get_column_data(sheet_id, column, credentials):
    service = build('sheets', 'v4', credentials=credentials)
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=sheet_id, range=f'{column}4:{column}').execute()
    return result.get('values', [])

def update_sheet(sheet_id, range_name, values, credentials):
    service = build('sheets', 'v4', credentials=credentials)
    sheet = service.spreadsheets()
    
    body = {
        "values": values
    }
    
    result = sheet.values().update(
        spreadsheetId=sheet_id,
        range=range_name,
        valueInputOption="RAW",
        body=body
    ).execute()

def find_row_for_date(date, dates):
    """ Ищет нужную дату в списке и возвращает индекс строки """
    date_formatted = date.split('-')[2] + '.' + date.split('-')[1] + '.' + date.split('-')[0]
    for i, row_date in enumerate(dates):
        if row_date[0] == date_formatted:
            return i +4
    return None


access_token = 'vk1.a.L7MN8WFIqscT58a8Br-iknODfkaaemH1oOguHoqVqzWtM0i55Ra5FFZKH5w2SINXEseYFCIrrnq5ipUKCKIE3WC0WAGnc-x8RcS0_kWa2MsTwz-A5pvRa4wewxVpttKLaWXw595I1rvw0ZNJdarFvJDHmg3QsdUD2evifC4wNoX7sxhizfql5SukEX6LfCGEZzrwrLgYi3msYbdp1aHy7g'
account_id = '1607984698'#id кабинета клиента
ids = get_vk_ads_campaigns(access_token, account_id)

def main():
    credentials = None
    if os.path.exists("token.json"):
        credentials = Credentials.from_authorized_user_file("token.json", SCOPES)
    if not credentials or not credentials.valid:
        if credentials and credentials.expired and credentials.refresh_token:
            credentials.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("creds.json", SCOPES)
            credentials = flow.run_local_server(port=0)
        with open("token.json", "w") as token:
            token.write(credentials.to_json())

    date_from = input("Статистика от (в формате YYYY-MM-DD): ")
    date_to = input("Статистика до (в формате YYYY-MM-DD): ")

    statistics = get_vk_ads_statistics(access_token, account_id, ids, date_from, date_to)
    
    if 'response' in statistics:
        daily_stats = {}

        for stat in statistics['response']:
            for daily_stat in stat['stats']:
                date = daily_stat['day']
                spent = float(daily_stat.get('spent', 0))
                impressions = int(daily_stat.get('impressions', 0))
                clicks = int(daily_stat.get('clicks', 0))

                if date not in daily_stats: 
                    daily_stats[date] = {'spent': 0, 'impressions': 0, 'clicks': 0}

                daily_stats[date]['spent'] += spent
                daily_stats[date]['impressions'] += impressions
                daily_stats[date]['clicks'] += clicks

        dates_column = get_column_data(spreadId, 'A', credentials)

        for date, stats in daily_stats.items():
            row = find_row_for_date(date, dates_column)
            if row:
                print(f"Итог за {date}:")
                print(f"Расходы: {stats['spent']}")
                print(f"Показы: {stats['impressions']}")
                print(f"Переходы: {stats['clicks']}")
                update_sheet(spreadId, f'B{row}', [[stats['spent']]], credentials)
                update_sheet(spreadId, f'C{row}', [[stats['impressions']]], credentials)
                update_sheet(spreadId, f'F{row}', [[stats['clicks']]], credentials)
                print("Обновлена таблица\n")
            else:
                print(f"Дата {date} не найдена в таблице.")
    else:
        print("Error:", statistics.get('error', 'Unknown error'))

if __name__ == '__main__':
    main()
