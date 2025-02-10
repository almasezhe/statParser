import os
import logging
import re
import aiohttp
import requests
from aiogram import Bot, Dispatcher, types,F
from aiogram.types import Message
from aiogram.filters import Command
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from datetime import date,timedelta,datetime

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]
API_TOKEN = '7050119926:AAEnaPW3qkW1tlKuUsb0mvRy6ffIUxEHo2g'
access_token = 'vk1.a.mnqrB7IXrSY418ujhynNlTVqvvSqa9deMLGpzy4y8gr9fp_VbuZ4BW4vz92IY_wQfVDx1KPTm_o3SfAyEcCtGCSqUxLrQlgI4EJSl22hnNbijXWGaZVWicoYCsxItoZGWElqh-2qnhYMztcGin4Aum_Oys2V1e5VOozZL4sOQLth92sl0HYPpHOC5ozEqC3CD99mM61PwoGpTVjqcTySeQ'
account_id = '1607984698'
headers={
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-KZ,en;q=0.9,ru-KZ;q=0.8,ru;q=0.7,es-KZ;q=0.6,es;q=0.5,ru-RU;q=0.4,en-US;q=0.3",
        "Connection": "keep-alive",
        "Content-Type": "application/json",
        "Origin": "https://lk.scananalytics.ru",
        "Pathname": "/users/universal",
        "Referer": "https://lk.scananalytics.ru/",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-site",
        "Token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJhbmFseXRpY3MiLCJhdWQiOiJhbmFseXRpY3MiLCJpYXQiOjE3Mzg4NTk1OTIsImV4cCI6MTczOTQ2NDM5MiwidXNlcl9pZCI6MTc0MiwidXNlcl9yb2xlIjoidXNlciJ9.3dfScBmvlD6H8Drwy9KxcfuLkgRI-Q1Vv6Tu19cEa0Y",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
        "sec-ch-ua": "\"Not A(Brand\";v=\"8\", \"Chromium\";v=\"132\", \"Google Chrome\";v=\"132\"",
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "\"Windows\""
    }
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bot = Bot(token=API_TOKEN)
dp = Dispatcher()
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


async def get_column_data(sheet_id, column_range, credentials):
    """
    Получает данные из указанного диапазона столбцов и строк таблицы Google Sheets.
    """
    try:
        service = build('sheets', 'v4', credentials=credentials)
        sheet = service.spreadsheets()
        result = sheet.values().get(
            spreadsheetId=sheet_id,
            range=column_range
        ).execute()

        return result.get('values', [])
    except Exception as e:
        print(f"Ошибка при получении данных из Google Sheets: {str(e)}")
        return []


async def update_sheet(sheet_id, range_name, values, credentials):
    """
    Обновляет ячейки Google Sheets по указанному диапазону.
    """
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
    """
    Ищет нужную дату в списке и возвращает индекс строки
    (пример функции, если вдруг понадобится).
    """
    date_formatted = date.split('-')[2] + '.' + date.split('-')[1] + '.' + date.split('-')[0]
    for i, row_date in enumerate(dates):
        if row_date[0] == date_formatted:
            print(f"{i+1}:{row_date[0]}")
            return i+1
    return None
