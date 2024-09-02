import redis
from datetime import datetime, timedelta
import time

# Conectar ao Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Parâmetros da consulta
start_date = datetime.strptime('1993-07-01', '%Y-%m-%d')
end_date = start_date + timedelta(days=90)  # Intervalo de 3 meses

# Medir o tempo de execução
start_time = time.time()

# Inicializar o dicionário para armazenar a contagem de pedidos por prioridade
order_count_by_priority = {}

# Processar pedidos (`orders`) de forma mais eficiente
for order_key in r.keys('orders:*'):
    order = r.hgetall(order_key)
    
    o_orderdate = datetime.fromtimestamp(float(order[b'o_orderdate'].decode('utf-8')))
    if not (start_date <= o_orderdate < end_date):
        continue

    o_orderkey = order[b'o_orderkey'].decode('utf-8')
    o_orderpriority = order[b'o_orderpriority'].decode('utf-8')

    # Tente buscar diretamente o lineitem correspondente sem iterar por todos
    lineitem_keys = r.keys(f'lineitem:{o_orderkey}:*')
    exists = False
    for lineitem_key in lineitem_keys:
        lineitem = r.hgetall(lineitem_key)
        l_commitdate = datetime.fromtimestamp(float(lineitem[b'l_commitdate'].decode('utf-8')))
        l_receiptdate = datetime.fromtimestamp(float(lineitem[b'l_receiptdate'].decode('utf-8')))
        if l_commitdate < l_receiptdate:
            exists = True
            break

    if exists:
        # Incrementar a contagem de pedidos para a prioridade correspondente
        if o_orderpriority not in order_count_by_priority:
            order_count_by_priority[o_orderpriority] = 0
        order_count_by_priority[o_orderpriority] += 1

# Ordenar os resultados por `o_orderpriority`
sorted_results = sorted(order_count_by_priority.items())

# Medir o tempo de execução total
end_time = time.time()
execution_time = end_time - start_time

# Exibir os resultados
for priority, count in sorted_results:
    print(f"Order Priority: {priority}, Order Count: {count}")

# Exibir o tempo de execução total
print(f"\nTempo de execução total: {execution_time:.2f} segundos")




