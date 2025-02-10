import asyncio
import json
import requests
from datetime import datetime, timedelta
from botdata import *
import aiohttp

# Импорт всего нужного из botdata и botmain
from botdata import dp, logger, credentials, get_column_data, update_sheet, headers
from aiogram import types
from aiogram.types import Message
from aiogram.filters import Command
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.context import FSMContext

# Данные для работы
access_token_metrika = "y0__wgBEJDI-fYCGNSUNCCx_r3yEbTLqNV_9H8_e5TvCDozoBjhrOdC"  # Твой токен Я.Метрики
sheet_id = "10BEyd4h3pdau_3kJIzOfBJ8nJa5EGNnIuZOyJmblQUA"

class VKStatsStates(StatesGroup):
    waiting_for_date_from = State()
    waiting_for_date_to = State()


################################################################################
#                         SCANANALYTICS API
################################################################################
def get_data_from_scananalytics(period_from, period_to, source):
    """Запрашивает данные из ScanAnalytics."""
    try:
        with open('scananalytics.json', 'r', encoding='utf-8') as file:
            json_data_all = json.load(file)
    except FileNotFoundError:
        return "Ошибка: Файл scananalytics.json не найден"
    except json.JSONDecodeError as e:
        return f"Ошибка при разборе JSON: {e}"

    json_data = json_data_all.get(str(source))
    if not json_data:
        return "Invalid tag selection"

    json_data["period"]["start"] = period_from
    json_data["period"]["end"] = period_to

    response = requests.post(
        'https://api.scananalytics.ru/api/filter/table',
        headers=headers,
        json=json_data
    )

    if response.status_code != 200:
        print(response)
        return f"API Error: {response.status_code}"

    try:
        data = response.json().get('total', {})
        if not isinstance(data, dict):
            return "Некорректный формат ответа API"

        return (
            data.get('users_count', 0),
            data.get('orders_count', 0),
            float(data.get('orders_sum', 0)),
            float(data.get('payments_sum', 0))
        )
    except Exception as e:
        return f"Ошибка при разборе API ответа: {str(e)}"


def get_column_letter(col_index):
    """
    Преобразует индекс (0-based) в букву столбца: 0->A,25->Z,26->AA.
    """
    col_letter = ""
    while col_index >= 0:
        col_letter = chr(col_index % 26 + 65) + col_letter
        col_index = col_index // 26 - 1
    return col_letter

################################################################################
#                         ОБРАБОТКА SCANANALYTICS
################################################################################
async def process_scananalytics_data(date_from, date_to, source):
    """Обрабатывает данные ScanAnalytics и обновляет Google Sheets."""
    start_date = datetime.strptime(date_from, '%Y-%m-%d')
    end_date = datetime.strptime(date_to, '%Y-%m-%d')

    while start_date <= end_date:
        date_str = start_date.strftime('%Y-%m-%d')
        response_data = get_data_from_scananalytics(date_str, date_str, source)

        if isinstance(response_data, str):
            print(f"Ошибка ScanAnalytics: {response_data}")
            start_date += timedelta(days=1)
            continue

        user_count, order_count, order_sum, payment_sum = response_data

        date_column = await find_column_by_date(date_str)
        if not date_column:
            print(f"Дата {date_str} не найдена в таблице.")
            start_date += timedelta(days=1)
            continue

        row_map = {
            "vk_1": [79, 88, 90, 91],
            "vk_2": [112, 121, 123, 124],
            "vk_ads": [178, 187, 189, 190],
            "yandex_danil1": [12, 21, 23, 24],
            "tg_in": [236, 243, 245, 246],
            "yandex_danil2": [47, 54, 56, 57]
        }

        if source in row_map:
            rows = row_map[source]
            print(f"Обновляется таблица для {source}, дата {date_str}:")
            await update_sheet(sheet_id, f'{date_column}{rows[0]}', [[user_count]], credentials)
            print(f"{date_column}{rows[0]}: {[[user_count]]}")
            await update_sheet(sheet_id, f'{date_column}{rows[1]}', [[order_count]], credentials)
            print(f"{date_column}{rows[1]}: {[[order_count]]}")
            await update_sheet(sheet_id, f'{date_column}{rows[2]}', [[order_sum]], credentials)
            print(f"{date_column}{rows[2]}: {[[order_sum]]}")
            await update_sheet(sheet_id, f'{date_column}{rows[3]}', [[payment_sum]], credentials)
            print(f"{date_column}{rows[3]}: {[[payment_sum]]}")



        start_date += timedelta(days=1)

################################################################################
#                         ПОИСК КОЛОНКИ ПО ДАТЕ
################################################################################
async def find_column_by_date(date_str):
    """
    Ищет колонку, где дата (число месяца) совпадает со значением в строке 3 Google Sheets.
    """
    try:
        year, month, day = date_str.split('-')
        months = {
            "01": "Январь", "02": "Февраль", "03": "Март",
            "04": "Апрель", "05": "Май", "06": "Июнь",
            "07": "Июль", "08": "Август", "09": "Сентябрь",
            "10": "Октябрь", "11": "Ноябрь", "12": "Декабрь"
        }
        sheet_name = f"'{months[month]}, {year}'"
        range_name = f"{sheet_name}!A2:ZZ2"  # Загружаем 3-ю строку целиком

        data = await get_column_data(sheet_id, range_name, credentials)
        if not data or len(data) < 1:
            print("Ошибка: Не удалось получить данные из Google Sheets.")
            return None

        header_row = data[0]  # Строка с датами
        for col_index, cell_value in enumerate(header_row):
            if cell_value and str(cell_value).strip() == str(int(day)):  # Сравниваем с днём
                column_letter = get_column_letter(col_index)
                print(f"Найдена колонка: {column_letter} для даты {date_str}")
                return column_letter

        print(f"Дата {date_str} не найдена в таблице.")
        return None

    except Exception as e:
        print(f"Ошибка при поиске колонки по дате: {str(e)}")
        return None


################################################################################
#                                VK Ads
################################################################################
async def get_vkAds_campaigns(access_token, which_vk):
    """
    Получаем список рекламных кампаний для аккаунта which_vk (1 или 2).
    """
    url = 'https://api.vk.com/method/ads.getCampaigns'
    account = {
        1: '1607984698',
        2: '1608399135',
    }
    params = {
        'access_token': access_token,
        'account_id': account[which_vk],
        'include_deleted': 0,
        'v': '5.199'
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, ssl=False) as response:
            if response.status == 200:
                data = await response.json()
                if 'response' in data:
                    campaign_ids = [str(campaign['id']) for campaign in data['response']]
                    return ','.join(campaign_ids)
                else:
                    return 'No response data in VK API response'
            else:
                return f'Failed to get data from VK API, status code: {response.status}'


async def get_vkAds_campStatistics(access_token, ids, date_from, date_to, which_vk):
    """
    Получаем статистику по этим кампаниям за период [date_from, date_to]
    """
    url = 'https://api.vk.com/method/ads.getStatistics'
    account = {
        1: '1607984698',
        2: '1608399135'
    }
    params = {
        'access_token': access_token,
        'account_id': account[which_vk],
        'ids_type': "campaign",
        'ids': ids,
        'period': 'day',
        'date_from': date_from,
        'date_to': date_to,
        'stats_fields': 'views_times',
        'v': '5.199'
    }
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, ssl=False) as response:
            if response.status == 200:
                return await response.json()
            else:
                return {'error': 'Failed to get data from VK API', 'status_code': response.status}


################################################################################
#                         Обработка ScanAnalytics
################################################################################
async def process_date_toScanAnalytics(message, state, which_one,date_from,date_to):
    """
    На каждую дату из диапазона (сейчас фиксирован: 2024-10-01..2024-10-02)
    шлём запрос в ScanAnalytics и записываем результат в Google Sheet.
    """


    try:
        start_date = datetime.strptime(date_from, '%Y-%m-%d')
        end_date = datetime.strptime(date_to, '%Y-%m-%d')
        if start_date > end_date:
            print("Ошибка: Начальная дата больше конечной.")
            return
        
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            print(f"--- ScanAnalytics: {which_one}, {date_str} ---")

            # Получаем данные
            response_data = get_data_from_scananalytics(date_str, date_str, which_one)
            print("ScanAnalytics response_data:", response_data)

            # Проверка, строка или кортеж
            if isinstance(response_data, str):
                # Это ошибка
                print(f"ScanAnalytics Error: {response_data}")
                current_date += timedelta(days=1)
                continue

            if not isinstance(response_data, tuple) or len(response_data) != 4:
                print("Некорректный формат ответа ScanAnalytics:", response_data)
                current_date += timedelta(days=1)
                continue

            user_count, order_count, order_sum, payment_sum = response_data
            print(f"  user_count={user_count}, orders={order_count}, order_sum={order_sum}, payment_sum={payment_sum}")

            # Ищем колонку в Google Sheets
            date_column = await find_column_by_date(date_str)
            if not date_column:
                print(f"Дата {date_str} не найдена в таблице.")
                current_date += timedelta(days=1)
                continue

            # Соответствие which_one -> строчки
            row_map = {
                "vk_1": [79, 88, 90, 91],
                "vk_2": [112,121,123,124],
                "vk_ads": [178,187,189,190],
                "yandex_danil1": [12,21,23,24],
                "tg_in": [236,243,245,246],
                "yandex_danil2": [47,54,56,57]
            }
            # Обновляем
            if which_one in row_map:
                r = row_map[which_one]
                await update_sheet(sheet_id, f'{date_column}{r[0]}', [[user_count]], credentials)
                await update_sheet(sheet_id, f'{date_column}{r[1]}', [[order_count]], credentials)
                await update_sheet(sheet_id, f'{date_column}{r[2]}', [[order_sum]], credentials)
                await update_sheet(sheet_id, f'{date_column}{r[3]}', [[payment_sum]], credentials)

                print(f"Обновлена Google-таблица [{which_one}, {date_str}]")
            else:
                print(f"Неизвестный which_one: {which_one}")

            current_date += timedelta(days=1)

    except ValueError as e:
        print(f"ValueError: {e}")
        print(f"Failed to parse dates: date_from={date_from}, date_to={date_to}")
    except Exception as e:
        print(f"Unexpected error in process_date_toScanAnalytics: {e}")


################################################################################
#                     Обработка VK Ads + ScanAnalytics
################################################################################
async def process_date_to(message, state, which_vk,date_from, date_to):
    """
    Получаем название источника (vk_1 или vk_2),
    сперва обрабатываем ScanAnalytics,
    потом обрабатываем VK Ads.
    """
    # Мапим which_vk -> (название для ScanAnalytics) и строки для расход, показы, клики
    # Пример: rows[1] = ["vk_1", [72,74,77]]
    #         rows[2] = ["vk_2", [105,107,110]]
    rows = {
        1: ["vk_1", [72, 74, 77]],
        2: ["vk_2", [105, 107, 110]]
    }

    # Достаём нужные данные
    if which_vk not in rows:
        print(f"Ошибка: неизвестный which_vk={which_vk}")
        return

    scan_source = rows[which_vk][0]  # "vk_1" или "vk_2"
    stats_rows = rows[which_vk][1]   # [72,74,77] для записи расход/показы/клики

    # Сначала заполним ScanAnalytics
    await process_date_toScanAnalytics(message, state, scan_source,date_from,date_to)

    # Теперь VK Ads

    # Список кампаний
    ids = await get_vkAds_campaigns(access_token, which_vk)
    if ids.startswith("Failed to get data") or ids.startswith("No response") or 'error' in ids:
        print(f"Ошибка получения списка кампаний VK: {ids}")
        return

    # Запрашиваем статистику
    statistics = await get_vkAds_campStatistics(access_token, ids, date_from, date_to, which_vk)
    if 'response' not in statistics:
        print(f"Error: {statistics.get('error', 'Unknown error')}")
        return

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

    # Записываем в Google Sheets
    for day, stats in daily_stats.items():
        date_column = await find_column_by_date(day)
        print(f"VK Ads day={day}, column={date_column}")
        if date_column:
            spent = stats['spent']
            impr = stats['impressions']
            clk = stats['clicks']
            print(f"  Расход: {spent}, Показы: {impr}, Клики: {clk}")

            # stats_rows = [72,74,77] для vk_1 или [105,107,110] для vk_2
            await update_sheet(sheet_id, f'{date_column}{stats_rows[0]}', [[spent]], credentials)
            await update_sheet(sheet_id, f'{date_column}{stats_rows[1]}', [[impr]], credentials)
            await update_sheet(sheet_id, f'{date_column}{stats_rows[2]}', [[clk]], credentials)

        else:
            print(f"Дата {day} не найдена в таблице.")


################################################################################
#                            YANDEX DIRECT
################################################################################
async def get_ya_direct_statistics(access_token, client_login, date_from, date_to):
    """
    Запрашивает статистику Яндекс.Директ (TSV).
    """
    url = 'https://api.direct.yandex.com/json/v5/reports'
    headers_yd = {
        'Authorization': f'Bearer {access_token}',
        'Client-Login': client_login,
        'Accept-Language': 'ru',
        'Content-Type': 'application/json'
    }
    body = {
        "params": {
            "SelectionCriteria": {
                "DateFrom": date_from,
                "DateTo": date_to
            },
            "FieldNames": [
                "CampaignId",
                "Impressions",
                "Clicks",
                "Cost",
                "Date"
            ],
            "ReportName": "Sample Report",
            "ReportType": "CAMPAIGN_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "NO",
            "IncludeDiscount": "YES",
            "OrderBy": [
                {"Field": "Date", "SortOrder": "ASCENDING"} 
            ]
        }
    }
    print(f"Request Headers: {headers_yd}")
    print(f"Request Body: {body}")

    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers_yd, json=body, ssl=False) as response:
            if response.status == 200:
                return await response.text()
            else:
                return {'error': 'Failed to get data from Yandex API', 'status_code': response.status}


async def process_date_to_yandex(message, state, access_token, client_login,date_from,date_to):
    """
    Запрашиваем статистику Яндекс.Директа, затем заполняем Google Sheets.
    Параллельно можем дёрнуть ScanAnalytics, если нужно.
    """


    # Сопоставляем кабинет -> строки в Google Sheets
    # [расход, показы, клики], плюс ключ для ScanAnalytics
    cabinet_rows = {
        "adilmuratwork": [5, 7, 10, "yandex_danil1"],
        "muratadilwork": [38, 40, 43, "yandex_danil2"]
    }

    if client_login not in cabinet_rows:
        print(f"Неизвестный логин Я.Директа: {client_login}")
        return

    # Если надо обновить ScanAnalytics (раскомментируй):
    # scan_source = cabinet_rows[client_login][3]
    # await process_date_toScanAnalytics(message, state, scan_source)

    response = await get_ya_direct_statistics(access_token, client_login, date_from, date_to)
    print(f"Full API response:\n{response}\n")

    if isinstance(response, dict) and 'error' in response:
        print(f"Ошибка от Yandex Direct: {response}")
        return

    if isinstance(response, str):  # формат TSV
        stats_rows = response.split('\n')
        daily_stats = {}
        # Пропускаем первые две строки (там служебная инфа + заголовки)
        for row in stats_rows[2:]:
            if not row or row.startswith('Total rows:'):
                continue
            fields = row.split('\t')
            if len(fields) >= 5:
                campaign_id = fields[0]
                date_yd = fields[4]
                impressions = fields[1]
                clicks = fields[2]
                cost = fields[3]
                
                if date_yd not in daily_stats:
                    daily_stats[date_yd] = {'cost': 0, 'impressions': 0, 'clicks': 0}

                daily_stats[date_yd]['cost'] += float(cost) / 1000000
                daily_stats[date_yd]['impressions'] += int(impressions)
                daily_stats[date_yd]['clicks'] += int(clicks)

        # Теперь daily_stats содержит данные по датам
        row_spend, row_impr, row_click, _ = cabinet_rows[client_login]

        for day_str, statvals in daily_stats.items():
            date_column = await find_column_by_date(day_str)
            if not date_column:
                print(f"Дата {day_str} не найдена в таблице.")
                continue

            cst = statvals['cost']
            imp = statvals['impressions']
            clk = statvals['clicks']

            print(f"Я.Директ {client_login}, дата {day_str}: cost={cst}, impr={imp}, clk={clk}")
            await update_sheet(sheet_id, f'{date_column}{row_spend}', [[cst]], credentials)
            await update_sheet(sheet_id, f'{date_column}{row_impr}', [[imp]], credentials)
            await update_sheet(sheet_id, f'{date_column}{row_click}', [[clk]], credentials)
    else:
        print(f"Неизвестный тип ответа Yandex Direct: {response}")


################################################################################
#                          ЯНДЕКС.МЕТРИКА
################################################################################
async def get_metrika(date_from, date_to, which_one):
    """
    Запрашивает visits и bounceRate из Я.Метрики, записывает в таблицу.
    """
    # Счётчики
    counter_id = {
        "yandex_danil1": "91579912",
        "yandex_danil2": "96996332",
        "main": "88447893"
    }
    # Фильтры
    utm_filters = {
        'yandex_danil1': "ym:s:UTMSource=='yandex-danil'",
        'yandex_danil2': "ym:s:UTMSource=='yandex-danil2'",
        'vk_1': "(ym:s:UTMSource=='vk') AND ((ym:s:UTMMedium=='kornelyuk') OR (ym:s:UTMMedium=='semenov'))",
        'vk_2': "(ym:s:UTMSource=='vk') AND (ym:s:UTMMedium=='kornelyuk2')",
        'vk_market': "ym:s:UTMSource=='vk_market'",
        'tg_in': "ym:s:UTMSource=='tg_in'",
        'vk_ads': "ym:s:UTMSource=='vk_ads'",
    }
    # Куда пишем в таблицу
    row_mapping = {
        "yandex_danil1": [14, 13],  # [visits, bounceRate]
        "yandex_danil2": [47, 46],
        "vk_1": [81, 80],
        "vk_2": [114, 113],
        "vk_market": [208, 207],
        "tg_in": [236, 235],
        "vk_ads": [180, 179],
    }

    if which_one in ["yandex_danil1", "yandex_danil2"]:
        filters = utm_filters[which_one]
        counter = counter_id[which_one]
    elif which_one in utm_filters:
        filters = utm_filters[which_one]
        counter = counter_id["main"]
    else:
        print(f"Invalid selection for which_one: {which_one}")
        return

    try:
        start_date = datetime.strptime(date_from, '%Y-%m-%d')
        end_date   = datetime.strptime(date_to, '%Y-%m-%d')
        if start_date > end_date:
            print("Ошибка: Начальная дата больше конечной.")
            return

        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            print(f"--- Metrika {which_one}, {date_str} ---")

            url = 'https://api-metrika.yandex.net/stat/v1/data'
            params = {
                'ids': counter,
                'metrics': 'ym:s:visits,ym:s:bounceRate',
                'filters': filters,
                'date1': date_str,
                'date2': date_str,
                'accuracy': 'full'
            }
            head_metrika = {
                'Authorization': f'OAuth {access_token_metrika}'
            }
            try:
                resp = requests.get(url, params=params, headers=head_metrika)
                if resp.status_code == 200:
                    data = resp.json()
                    totals = data.get('totals', [])
                    if len(totals) < 2:
                        print("Пустые данные totals:", totals)
                        current_date += timedelta(days=1)
                        continue

                    visits = totals[0]
                    bounce_rate = totals[1] / 100.0

                    print(f"  visits={visits}, bounce={bounce_rate}")
                else:
                    print(f"Ошибка API Метрики: {resp.status_code} => {resp.text}")
                    current_date += timedelta(days=1)
                    continue

            except Exception as e:
                print(f"Ошибка запроса в Метрику: {e}")
                current_date += timedelta(days=1)
                continue

            # Обновляем таблицу
            date_column = await find_column_by_date(date_str)
            if date_column and which_one in row_mapping:
                row_visits, row_bounce = row_mapping[which_one]
                await update_sheet(sheet_id, f'{date_column}{row_visits}', [[visits]], credentials)
                await update_sheet(sheet_id, f'{date_column}{row_bounce}', [[bounce_rate]], credentials)
                print(f"Обновили таблицу Metrika [{which_one}, {date_str}]")
            else:
                print(f"Дата {date_str} не найдена или неизвестный which_one={which_one}")

            current_date += timedelta(days=1)

    except ValueError as e:
        print(f"ValueError: {e}")
        print(f"Failed to parse dates: date_from={date_from}, date_to={date_to}")
    except Exception as e:
        print(f"Unexpected error in get_metrika: {e}")


################################################################################
#                         ОБЩИЙ ЗАПУСК
################################################################################
async def run_all():
    """Запускает все процессы обработки данных."""
    date_from = "2024-10-01"
    date_to = "2024-10-31"

    print("==== Обработка ScanAnalytics ====")
    scan_sources = ["vk_1", "vk_2", "vk_ads", "yandex_danil1", "tg_in", "yandex_danil2", "vk_market"]
    for source in scan_sources:
        print(f"Запрос данных ScanAnalytics для {source}...")
        await process_scananalytics_data(date_from, date_to, source)

    print("==== VK Ads ====")
    vk_accounts = [1, 2]  # 1 - vk_1, 2 - vk_2
    for vk in vk_accounts:
        print(f"Обработка VK Ads для which_vk={vk}...")
        await process_date_to(message=None, state=None, which_vk=vk, date_from=date_from, date_to=date_to)

    print("==== Yandex Direct ====")
    yandex_accounts = [
        ('y0_AgAAAAB0cXbUAAHlKAAAAAETmvKuAAClpXNw7_lGpqSv7aDgBsont46UTQ', 'adilmuratwork', 'yandex_danil1'),
        ('y0__wgBEJDI-fYCGL-SNCCju_XwEX3M-U-jfQR4F2EYAZKYdsfarc-U', 'getuniq-u126030-5', "yandex_danil2")
    ]
    for token_direct, client_login, yandex_source in yandex_accounts:
        print(f"Обработка Yandex Direct для {client_login} | {yandex_source}...")
        await process_date_to_yandex(message=None, state=None, access_token=token_direct, client_login=client_login,date_from=date_from, date_to=date_to)

    metrika_sources = ["yandex_danil1", "yandex_danil2", "vk_1", "vk_2", "vk_market", "tg_in", "vk_ads"]

    print("==== Get Metrika (запуск get_metrika) ====")
    # Дополнительный запуск функции get_metrika
    for source in metrika_sources:
        print(f"Запуск get_metrika для {source}...")
        await get_metrika(date_from, date_to, source)
        
if __name__ == "__main__":
    asyncio.run(run_all())
