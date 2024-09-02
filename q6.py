import redis
from datetime import datetime
import time

# Conectar ao Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Parâmetros da consulta
start_date = datetime.strptime('1994-01-01', '%Y-%m-%d')
end_date = datetime.strptime('1995-01-01', '%Y-%m-%d')
discount_min = 0.05
discount_max = 0.07
quantity_max = 24

# Medir o tempo de execução
start_time = time.time()

# Inicializar o acumulador de receita
total_revenue = 0.0

# Filtrar e processar os dados
for key in r.keys('lineitem:*'):
    lineitem = r.hgetall(key)

    # Verificar os critérios de filtragem
    l_shipdate = datetime.fromtimestamp(float(lineitem[b'l_shipdate'].decode('utf-8')))
    l_discount = float(lineitem[b'l_discount'].decode('utf-8'))
    l_quantity = float(lineitem[b'l_quantity'].decode('utf-8'))

    if start_date <= l_shipdate < end_date and discount_min <= l_discount <= discount_max and l_quantity < quantity_max:
        l_extendedprice = float(lineitem[b'l_extendedprice'].decode('utf-8'))
        total_revenue += l_extendedprice * l_discount

# Medir o tempo de execução total
end_time = time.time()
execution_time = end_time - start_time

# Exibir o resultado
print(f"Receita total (revenue): {total_revenue:.2f}")
print(f"Tempo de execução total: {execution_time:.4f} segundos")


