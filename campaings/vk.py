from botdata import *
spreadID = '1Tn0Y02OMSAGprBsLm_gM9c0awUHDVEI48TCACQEYJY4'

#Как работает:
#получаются айдишники всех кампании через get_vKAds_Campaings и после запрашивается статистика этих кампании по определенным датам
class VKStatsStates(StatesGroup):
    waiting_for_date_from = State()
    waiting_for_date_to = State()
    
async def get_vkAds_campaigns(access_token, account_id):
    url = 'https://api.vk.com/method/ads.getCampaigns'
    params = {
        'access_token': access_token,
        'account_id': account_id,
        'include_deleted': 0,
        'v': '5.199'
    }#
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params,ssl=False) as response:
            if response.status == 200:
                data = await response.json()
                if 'response' in data:
                    campaign_ids = [str(campaign['id']) for campaign in data['response']]
                    return ','.join(campaign_ids)
                else:
                    return 'No response data in VK API response'
            else:
                return f'Failed to get data from VK API, status code: {response.status}'

async def get_vkAds_campStatistics(access_token, account_id, ids, date_from, date_to):
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
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params,ssl=False) as response:
            if response.status == 200:
                return await response.json()
            else:
                return {'error': 'Failed to get data from VK API', 'status_code': response.status}

# Команда для получения статистики ВКонтакте
@dp.message(Command('vkstats'))
async def vk_stats(message: Message, state: FSMContext):
    logger.info(f"vkStats from {message.from_user.id}")
    await message.answer("Введите дату от (в формате YYYY-MM-DD):")
    await state.set_state(VKStatsStates.waiting_for_date_from)

@dp.message(VKStatsStates.waiting_for_date_from)
async def process_date_from(message: Message, state: FSMContext):
    await state.update_data(date_from=message.text)
    await message.answer("Введите дату до (в формате YYYY-MM-DD):")
    await state.set_state(VKStatsStates.waiting_for_date_to)

@dp.message(VKStatsStates.waiting_for_date_to)
async def process_date_to(message: Message, state: FSMContext):
    user_data = await state.get_data()
    date_from = user_data['date_from']
    date_to = message.text

    ids = await get_vkAds_campaigns(access_token, account_id)
    statistics = await get_vkAds_campStatistics(access_token, account_id, ids, date_from, date_to)
    
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

        dates_column = await get_column_data(spreadID, 'A:A', credentials)

        for date, stats in daily_stats.items():
            row = find_row_for_date(date, dates_column)
            if row:
                await message.answer(f"Итог за {date}:\nРасходы: {stats['spent']}\nПоказы: {stats['impressions']}\nПереходы: {stats['clicks']}")
                await update_sheet(spreadID, f'B{row}', [[stats['spent']]], credentials)
                await update_sheet(spreadID, f'C{row}', [[stats['impressions']]], credentials)
                await update_sheet(spreadID, f'F{row}', [[stats['clicks']]], credentials)
                await message.answer("Обновлена таблица\n")
            else:
                await message.answer(f"Дата {date} не найдена в таблице.")
    else:
        await message.answer(f"Error: {statistics.get('error', 'Unknown error')}")
    await state.clear()