from botdata import *

class Form(StatesGroup):
    waiting_for_spread_id = State()

@dp.message(Command('setexcel'))
async def set_spread_id(message: Message, state: FSMContext):
    await message.answer("Пожалуйста, отправьте ссылку на Google Sheets.")
    await state.set_state(Form.waiting_for_spread_id)

@dp.message(Form.waiting_for_spread_id)
async def process_spread_id(message: Message, state: FSMContext):
    global spreadID
    args = message.text
    match = re.search(r'/spreadsheets/d/([a-zA-Z0-9-_]+)', args)
    if match:
        spreadID = match.group(1)
        await message.answer(f"spreadId успешно обновлен на: {spreadID}")
        await state.clear() 
    else:
        await message.answer("Не удалось извлечь spreadId. Убедитесь, что вы ввели правильную ссылку.")


@dp.message(Command('getexcel'))
async def get_spread_id(message: types.Message, state: FSMContext):
    await message.answer(f"Текущий spreadId: {spreadID}\nСсылка на таблицу: https://docs.google.com/spreadsheets/d/{spreadID}/")