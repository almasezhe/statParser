from botdata import *
import aiohttp
import json

class YDStatsStates(StatesGroup):
    waiting_for_date_from = State()
    waiting_for_date_to = State()

# Получение данных по кампаниям Яндекс.Директа
async def get_ya_direct_statistics(access_token, client_login, date_from, date_to):
    url = 'https://api.direct.yandex.com/json/v5/reports' #Yandex Direct
    #url = 'https://api-sandbox.direct.yandex.com/json/v5/reports'  #Yandex Direct Sandbox
    headers = {
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
    
    async with aiohttp.ClientSession() as session:
        async with session.post(url, headers=headers, json=body, ssl=False) as response:
            if response.status == 200:
                print(response)    
                return await response.text()
            else:
                print(response)
                return {'error': 'Failed to get data from Yandex API', 'status_code': response.status}

# Команда для получения статистики Яндекс.Директа
@dp.message(Command('ydstats'))
async def ya_stats(message: Message, state: FSMContext):
    logger.info(f"yaStats from {message.from_user.id}")
    await message.answer("Введите дату от (в формате YYYY-MM-DD):")
    await state.set_state(YDStatsStates.waiting_for_date_from)

@dp.message(YDStatsStates.waiting_for_date_from)
async def process_date_from_yandex(message: Message, state: FSMContext):
    await state.update_data(date_from=message.text)
    await message.answer("Введите дату до (в формате YYYY-MM-DD):")
    await state.set_state(YDStatsStates.waiting_for_date_to)

@dp.message(YDStatsStates.waiting_for_date_to)
async def process_date_to_yandex(message: Message, state: FSMContext):
    spreadID = '1g7XWd9n00ngnr2ZjHijqrt3Rxx8hSxaIx-V_999kwG8'
    user_data = await state.get_data()
    date_from = user_data['date_from']
    date_to = message.text

    access_token = 'y0_AgAAAAB0cXbUAAHlKAAAAAETmvKuAAClpXNw7_lGpqSv7aDgBsont46UTQ'  # Установите ваш OAuth токен
    client_login = 'adilmuratwork'  # Установите логин клиента

    response = await get_ya_direct_statistics(access_token, client_login, date_from, date_to)

    if isinstance(response, str):  # Если ответ в формате TSV
        print(response + "\n")
        stats = response.split('\n')
        daily_stats = {}
        for row in stats[2:]:  # Пропускаем первые строки с заголовками
            print(row)
            if row and not row.startswith('Total rows:'):
                fields = row.split('\t')
                if len(fields) >= 5:
                    campaign_id, date, impressions, clicks, cost = fields[0], fields[4], fields[1], fields[2], fields[3]
                    
                    # Сохраняем статистику за каждый день
                    if date not in daily_stats:
                        daily_stats[date] = {'cost': 0, 'impressions': 0, 'clicks': 0}
                    daily_stats[date]['cost'] += float(cost)
                    daily_stats[date]['impressions'] += int(impressions)
                    daily_stats[date]['clicks'] += int(clicks)
        # Получаем столбец с датами из таблицы
        dates_column = await get_column_data(spreadID, 'A', credentials)
        # Обновляем таблицу данными
        for date, stats in daily_stats.items():
            row = find_row_for_date(date, dates_column)
            if row:
                await message.answer(f"Итог за {date}:\nРасходы: {stats['cost']}\nПоказы: {stats['impressions']}\nКлики: {stats['clicks']}")
                await update_sheet(spreadID, f'B{row}', [[stats['cost']]], credentials)
                await update_sheet(spreadID, f'C{row}', [[stats['impressions']]], credentials)
                await update_sheet(spreadID, f'F{row}', [[stats['clicks']]], credentials)
                await message.answer("Обновлена таблица\n")
            else:
                await message.answer(f"Дата {date} не найдена в таблице.")
    else:
        await message.answer(f"Error: {response.get('error', 'Unknown error')}")
    await state.clear()