import pandas as pd

file_path = "tg_in.xlsx" 
df = pd.read_excel(file_path)


df = df.dropna(subset=["Завершен"])

df["Завершен"] = pd.to_datetime(df["Завершен"], format="%d-%m-%Y %H:%M")

df["Дата"] = df["Завершен"].dt.date

numeric_columns = ["Цена", "Кол-во подписчиков", "Просмотры фактические", "Клики"]
df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors="coerce")

result = df.groupby("Дата").agg({
    "Цена": "sum",
    "Кол-во подписчиков": "sum",
    "Просмотры фактические": "sum",
    "Клики": "sum"
}).reset_index()

result = result.sort_values(by="Дата")

print(result)
