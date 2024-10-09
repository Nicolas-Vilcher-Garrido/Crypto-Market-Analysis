import pyodbc 
import requests
import pandas as pd
from datetime import datetime


# 1 - Conectar ao banco de dados
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=localhost\\SQLEXPRESS01;DATABASE=criptodb;Trusted_Connection=yes;')
cursor = conn.cursor()

url = 'https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=10&page=1&sparkline=false'

response = requests.get(url)
data = response.json()


# 2 - Preparando os dados para inserir ao Banco de dados
cripto_data = []
for coin in data:
    last_updated = datetime.strptime(coin['last_updated'], '%Y-%m-%dT%H:%M:%S.%fZ')
    cripto_data.append((coin['name'], coin['symbol'], coin['current_price'], last_updated))

# 3 - Inserindo os dados acima no banco de dados

cursor = conn.cursor()
cursor.executemany("""INSERT INTO crypto_data (name, symbol, price, last_updated) VALUES (?, ?, ?, ?) """, cripto_data)
conn.commit()

cursor.close()
conn.close()

print("Dados armazenados com sucesso")
