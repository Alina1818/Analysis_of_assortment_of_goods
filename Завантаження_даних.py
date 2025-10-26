import requests, time, re
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import pandas as pd
from collections import OrderedDict
import os

# Завантаження списку категорій

# 1. Збираємо дані каталогу
#
# Створюємо список усіх можливих сторінок каталогу
base_url = '...'
list_all_url = []
for i in range(0, 556):
    list_all_url.append(base_url + str(i))

# Створюємо запит на збирання посилань на товари каталогу

session = requests.Session()

urls_all_product_list = []
for i in list_all_url:
    r = session.get(i)
    p = BeautifulSoup(r.text, "html.parser")

    # Занурюємося у список де лежать товари
    view_content = p.find(class_="view-content")

    # Дістаємо посилання на товари
    titles = view_content.find_all(class_="b-product-small__title")
    urls = []
    for title in titles:
        a_tag = title.find("a", href=True)
        if a_tag:
            urls.append(a_tag["href"])

    # Доповнюємо загальний список
    urls_all_product_list.append(urls)
    del urls
    print("Done urls: ", i)
    time.sleep(1)

# Цикл утворив список списків. Зробимо один єдиний список товарів
urls_all_product_list_all = [item for sublist in urls_all_product_list for item in sublist]
# Зберігаємо перелік товарів
pd.to_pickle(urls_all_product_list_all, "urls_all_product_list.pkl")

from joblib import Parallel, delayed

def load_data(path_urls: str) -> pd.DataFrame:
    session = requests.Session()

    try:
        r = session.get(path_urls)
        r.raise_for_status()
    except Exception as e:
        session.close()
        empty = pd.DataFrame(columns=["name_attr","value_attr","price","urls","goods_name","goods_category","goods_status","goods_code"])
        print(f"[ERROR GET] {path_urls} -> {e}")
        return empty

    p = BeautifulSoup(r.text, "html.parser")

    # Назва
    title_tag = p.find(class_="b-product__title")
    goods_name = title_tag.get_text(strip=True) if title_tag else 'no_data'

    # Категорія
    category_block = p.find(class_="b-product__topline_bottom")
    if category_block:
        field_item = category_block.find(class_="field__item")
        goods_category = field_item.get_text(strip=True) if field_item else 'no_data'
    else:
        goods_category = 'no_data'

    # Статус
    container = p.find("div", class_="b-product__topline_bottom")
    if container:
        all_classes = set(container.get("class", []))
        for tag in container.find_all(True):
            all_classes.update(tag.get("class", []))
        if {"product-avail", "on"}.issubset(all_classes):
            status_tag = container.find(class_="product-avail on")
            goods_status = status_tag.get_text(strip=True) if status_tag else 'no_data'
        elif {"product-avail", "off"}.issubset(all_classes):
            status_tag = container.find(class_="product-avail off")
            goods_status = status_tag.get_text(strip=True) if status_tag else 'no_data'
        else:
            goods_status = 'no_data'
    else:
        goods_status = 'no_data'

    # Ціна
    price_tag = p.find(class_='b-price')
    goods_price = price_tag.get_text(strip=True) if price_tag else 'no_data'

    # Код товару
    code_block = p.find("div", class_=lambda x: x and "field--name-field-product-vendor-code" in x)
    if code_block:
        code_item = code_block.find(class_="field__item")
        goods_code = code_item.get_text(strip=True) if code_item else 'no_data'
    else:
        goods_code = 'no_data'


    # Перший блок характеристик
    attr_block = p.find(
        class_='field field--name-field-product-specifications field--type-vocabilary-terms-field field--label-hidden field__items'
    )
    if attr_block:
        name_attr_1 = [x.get_text(strip=True) for x in attr_block.find_all(class_='label')]
        value_attr_1 = [x.get_text(strip=True) for x in attr_block.find_all(class_='name')]
    else:
        name_attr_1, value_attr_1 = [], []

    # Другий блок характеристик
    container2 = p.find(class_='container-inline field__item')
    if container2:
        name_attr_2 = [x.get_text(strip=True) for x in container2.find_all(class_='double-field-first')]
        value_attr_2 = [x.get_text(strip=True) for x in container2.find_all(class_='double-field-second')]
    else:
        name_attr_2, value_attr_2 = [], []

    # Об’єднання
    name_all = name_attr_1 + name_attr_2
    value_all = value_attr_1 + value_attr_2
    max_len = max(len(name_all), len(value_all), 1)
    while len(name_all) < max_len:
        name_all.append('no_data')
    while len(value_all) < max_len:
        value_all.append('no_data')

    goods_attr_df = pd.DataFrame({'name_attr': name_all, 'value_attr': value_all})
    goods_attr_df['price'] = goods_price
    goods_attr_df['urls'] = path_urls
    goods_attr_df['goods_name'] = goods_name
    goods_attr_df['goods_category'] = goods_category
    goods_attr_df['goods_status'] = goods_status
    goods_attr_df['goods_code'] = goods_code


    col_order = [
    "goods_name",
    "goods_category",
    "goods_code",
    "goods_status",
    "price",
    "urls",
    "name_attr",
    "value_attr"
    ]
    goods_attr_df = goods_attr_df[col_order]

    session.close()
    return goods_attr_df

# 3. Читаємо список URL-ів товарів і формуємо абсолютні посилання
urls_all_product_list_all = pd.read_pickle("urls_all_product_list.pkl")
list_all_url_goods = ['...' + u for u in urls_all_product_list_all]

# 4. Підхоплюємо попередні результати за потреби

if os.path.exists("goods_info_all_df.pkl"):
    goods_info_all_df = pd.read_pickle("goods_info_all_df.pkl")
elif os.path.exists("goods_info_all_df.csv"):
    goods_info_all_df = pd.read_csv("goods_info_all_df.csv")
else:
    goods_info_all_df = pd.DataFrame(columns=[
        "name_attr","value_attr","price","urls","goods_name","goods_category","goods_status", "goods_code"
    ])

processed_urls = set(goods_info_all_df['urls'].unique()) if not goods_info_all_df.empty else set()

print(f"Всього товарів: {len(list_all_url_goods)}")
print(f"Вже оброблено: {len(processed_urls)}")
print(f"Залишилось: {len(list_all_url_goods) - len(processed_urls)}")

# Пропустити вже оброблені урли:
urls_for_run = [u for u in list_all_url_goods if u not in processed_urls]
#list_all_url_goods
 #

results_list = Parallel(n_jobs=-1, verbose=10, prefer="threads")(
    delayed(load_data)(path_urls=u) for u in urls_for_run
)

results_df_all = pd.concat(results_list, ignore_index=True) if results_list else pd.DataFrame(
    columns = ["goods_code", "goods_name", "goods_category", "goods_status", "price", "urls", "name_attr", "value_attr"]

)

# Додаємо до вже наявних 
goods_info_all_df = pd.concat([goods_info_all_df, results_df_all], ignore_index=True)

goods_info_all_df = goods_info_all_df[[c for c in col_order if c in goods_info_all_df.columns]]
goods_info_all_df.to_pickle("goods_info_all_df.pkl")
goods_info_all_df.to_csv("goods_info_all_df.csv", index=False)


