import asyncio
import math

import numpy as np
from jinja2 import Environment, FileSystemLoader
import os
from playwright.async_api import async_playwright
import pathlib
import os

from processing import csv_to_unixtime_df
from repository import HouseRepository
import router
from schemas import ValuePeriod

async def dataLoader(flatId):
    # Формирование html
    filename = "raai_school_2024.csv"
    data = csv_to_unixtime_df(filename)
    data = data[data['flat_tkn'] == flatId].fillna('Н/А')

    env = Environment(loader=FileSystemLoader('.'))
    template = env.get_template("pdf_template.html")

    anomaly = await HouseRepository.get_house_anomaly(flatId)
    flat_percentElectr = await HouseRepository.get_energy_percent_flat(flatId)
    flat_percentWater = await HouseRepository.get_water_percent_flat(flatId)
    flat_percentDebt = await HouseRepository.get_dept_percent_flat(flatId)
    flat = await HouseRepository.get_flat_by_id(flatId)
    people_in_flat = await HouseRepository.get_flat_avg_people_by_id(flatId)

    HouseRepository.plot_linear_regression_with_dates(data['payment_period'], data['debt'], flat.ratio)
    ratio = ''
    stability = ''
    amount = ''
    family = ''
    situation = ''
    stonks = ''
    backImg = ''
    textColor = 'black'
    backColor = 'white'
    stonksTitle = ''
    stonksText = ''
    logo = 'assets/гол.png'
    logoText = 'Медвежья услуга'
    fontTitle = 70
    if math.ceil(anomaly[0]) == 1:
        logo = 'assets/img.png'
        logoText = '[ДАННЫЕ УДАЛЕНЫ]'
        fontTitle = 60

    if people_in_flat[0] == 0:
        amount = ''
    else:
        amount = round(people_in_flat[0])

    if round(people_in_flat[0]) == 1:
        family = 'assets/холостяк.svg'
    elif round(people_in_flat[0]) == 2:
        family = 'assets/skuf.svg'
    elif round(people_in_flat[0]) == 3:
        family = 'assets/семья.svg'
    elif round(people_in_flat[0]) >= 4:
        family = 'assets/многочелов.svg'

    if flat.class_debt == 4:
        situation = 'assets/min100.svg'
    elif flat.class_debt == 3:
        situation = 'assets/min50.svg'
    elif flat.class_debt == 2:
        situation = 'assets/0.svg'
    elif flat.class_debt == 1:
        situation = 'assets/max50.svg'
    elif flat.class_debt == 0:
        situation = 'assets/max100.svg'

    if flat.ratio > 0:
        ratio = "Должник"
    elif flat.ratio < 0:
        ratio = "Переплата"
    else:
        ratio = "Без изменений"

    if flat.stability:
        stability = "Стабильно"
        stonks = 'assets/СТОНКС.svg'
        stonksTitle = "Стонкс."
        stonksText = ("На основе данных наша система присвоила этой "
                      "квартире класс 'Стабильно', "
                      "поскольку у данной квартиры нет задолжностей")
    else:
        stability = "Не стабильно"
        stonks = 'assets/НЕСТОНКС.svg'
        stonksTitle = "Не Стонкс."
        stonksText = ("На основе данных наша система присвоила этой "
                      "квартире класс 'Не Стабильно', "
                      "поскольку у данной квартиры есть задолжности")

    if flat_percentElectr != None:
        flat_percentElectr = "{:.2f}".format(flat_percentElectr)
    else:
        flat_percentElectr = 'N/A'

    if flat_percentWater != None:
        flat_percentWater = "{:.2f}".format(flat_percentWater)
    else:
        flat_percentWater = 'N/A'

    if flat_percentDebt != None:
        flat_percentDebt = "{:.2f}".format(flat_percentDebt)
    else:
        flat_percentDebt = '0'

    pdf_template = template.render(
        {'data': data.to_dict('records'),
         'debtPercentElectr': flat_percentElectr,
         'debtPercentWater': flat_percentWater,
         'debtPercentDebt': flat_percentDebt,
         'houseId': data.iloc[0]['house_tkn'],
         'flatId': data.iloc[0]['flat_tkn'],
         'amount': amount,
         'ratio': ratio,
         'stability': stability,
         'logo': logo,
         'family': family,
         'situation': situation,
         'stonks': stonks,
         'graph': 'assets/linear_regression_plot.png',
         'logoText': logoText,
         'fontTitle': fontTitle,
         'stonksTitle': stonksTitle,
         'stonksText': stonksText}
    )
    with open('output.html', 'w', encoding='utf-8') as f:
        f.write(pdf_template)
    return pdf_template


async def formPdf(flatId):
    await dataLoader(flatId)

    # Формирование пдфа
    filePath = os.path.abspath("output.html")
    fileUrl = pathlib.Path(filePath).as_uri()
    playwright = await async_playwright().start()
    browser = await playwright.chromium.launch()
    page = await browser.new_page()
    await page.goto(fileUrl)
    await page.emulate_media(media="screen")
    await page.pdf(path="output.pdf", format="A4")
    await browser.close()
    await playwright.stop()
