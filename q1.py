import redis
from datetime import datetime, timedelta
import time

# Conectar ao Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Parâmetros da consulta
cutoff_date = datetime.strptime("1998-12-01", "%Y-%m-%d")
interval_days = 90
filtered_date = cutoff_date - timedelta(days=interval_days)

# Dicionário para armazenar os resultados por (l_returnflag, l_linestatus)
results = {}

# Medir o tempo de execução
start_time = time.time()

# Processar os dados
for key in r.keys('lineitem:*'):
    lineitem = r.hgetall(key)
    
    # Verificar se os campos necessários existem
    if b'l_shipdate' not in lineitem or b'l_returnflag' not in lineitem or b'l_linestatus' not in lineitem:
        continue
    
    # Filtragem pela data de envio
    l_shipdate = datetime.fromtimestamp(float(lineitem[b'l_shipdate'].decode('utf-8')))
    if l_shipdate > filtered_date:
        continue
    
    l_returnflag = lineitem[b'l_returnflag'].decode('utf-8')
    l_linestatus = lineitem[b'l_linestatus'].decode('utf-8')
    l_quantity = float(lineitem[b'l_quantity'])
    l_extendedprice = float(lineitem[b'l_extendedprice'])
    l_discount = float(lineitem[b'l_discount'])
    l_tax = float(lineitem[b'l_tax'])
    
    group_key = (l_returnflag, l_linestatus)
    
    if group_key not in results:
        results[group_key] = {
            'sum_qty': 0.0,
            'sum_base_price': 0.0,
            'sum_disc_price': 0.0,
            'sum_charge': 0.0,
            'count_order': 0
        }
    
    results[group_key]['sum_qty'] += l_quantity
    results[group_key]['sum_base_price'] += l_extendedprice
    results[group_key]['sum_disc_price'] += l_extendedprice * (1 - l_discount)
    results[group_key]['sum_charge'] += l_extendedprice * (1 - l_discount) * (1 + l_tax)
    results[group_key]['count_order'] += 1

# Calcular as médias
final_results = []
for group_key, values in results.items():
    avg_qty = values['sum_qty'] / values['count_order']
    avg_price = values['sum_base_price'] / values['count_order']
    avg_disc = (values['sum_base_price'] - values['sum_disc_price']) / values['count_order']
    final_results.append((
        group_key[0],
        group_key[1],
        values['sum_qty'],
        values['sum_base_price'],
        values['sum_disc_price'],
        values['sum_charge'],
        avg_qty,
        avg_price,
        avg_disc,
        values['count_order']
    ))

# Ordenar os resultados
final_results.sort(key=lambda x: (x[0], x[1]))

# Medir o tempo de execução total
end_time = time.time()
execution_time = end_time - start_time

# Exibir os resultados
for result in final_results:
    print(f"Return Flag: {result[0]}, Line Status: {result[1]}, "
          f"Sum Qty: {result[2]:.2f}, Sum Base Price: {result[3]:.2f}, "
          f"Sum Disc Price: {result[4]:.2f}, Sum Charge: {result[5]:.2f}, "
          f"Avg Qty: {result[6]:.2f}, Avg Price: {result[7]:.2f}, "
          f"Avg Disc: {result[8]:.2f}, Count Order: {result[9]}")

# Exibir o tempo de execução total
print(f"\nTempo de execução total: {execution_time:.4f} segundos")







