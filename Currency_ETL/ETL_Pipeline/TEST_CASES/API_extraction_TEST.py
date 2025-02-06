import requests
from datetime import datetime

url_year = datetime.now().year-1

def fetch_currency_data():
    url = f"https://api.frankfurter.dev/v1/{url_year}-01-01.."
    response = requests.get(url)
    data = response.json()
    return data

currency_data = fetch_currency_data()

print(currency_data)