import redis
from datetime import datetime, timedelta
import time

# Conectar ao Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Parâmetros da consulta
start_date = datetime.strptime('1996-01-01', '%Y-%m-%d')
end_date = start_date + timedelta(days=90)  # Intervalo de 3 meses

# Medir o tempo de execução
start_time = time.time()

# Inicializar dicionário para armazenar a receita por fornecedor
revenue_by_supplier = {}

# Processar cada item de linha (`lineitem`)
for lineitem_key in r.keys('lineitem:*'):
    lineitem = r.hgetall(lineitem_key)
    l_suppkey = lineitem[b'l_suppkey'].decode('utf-8')
    l_shipdate = datetime.fromtimestamp(float(lineitem[b'l_shipdate'].decode('utf-8')))
    
    if not (start_date <= l_shipdate < end_date):
        continue

    l_extendedprice = float(lineitem[b'l_extendedprice'].decode('utf-8'))
    l_discount = float(lineitem[b'l_discount'].decode('utf-8'))

    # Calcular a receita para o fornecedor
    revenue = l_extendedprice * (1 - l_discount)
    
    if l_suppkey not in revenue_by_supplier:
        revenue_by_supplier[l_suppkey] = 0
    revenue_by_supplier[l_suppkey] += revenue

# Identificar a receita máxima
max_revenue = max(revenue_by_supplier.values())

# Selecionar o(s) fornecedor(es) com a receita máxima
top_suppliers = [suppkey for suppkey, revenue in revenue_by_supplier.items() if revenue == max_revenue]

# Medir o tempo de execução total
end_time = time.time()
execution_time = end_time - start_time

# Exibir os resultados
for suppkey in top_suppliers:
    supplier = r.hgetall(f'supplier:{suppkey}')
    s_name = supplier[b's_name'].decode('utf-8')
    s_address = supplier[b's_address'].decode('utf-8')
    s_phone = supplier[b's_phone'].decode('utf-8')
    print(f"Supplier Key: {suppkey}, Name: {s_name}, Address: {s_address}, Phone: {s_phone}, Total Revenue: {max_revenue:.2f}")

# Exibir o tempo de execução total
print(f"\nTempo de execução total: {execution_time:.2f} segundos")
