import redis
from datetime import datetime
import time

# Conectar ao Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Parâmetros da consulta
start_date = datetime.strptime('1994-01-01', '%Y-%m-%d')
end_date = datetime.strptime('1995-01-01', '%Y-%m-%d')

# Medir o tempo de execução
start_time = time.time()

# Inicializar os contadores
results = {
    'MAIL': {'high_line_count': 0, 'low_line_count': 0},
    'SHIP': {'high_line_count': 0, 'low_line_count': 0}
}

# Processar cada item na tabela lineitem
for key in r.keys('lineitem:*'):
    lineitem = r.hgetall(key)
    
    # Aplicar a filtragem nas datas e no modo de envio
    l_shipmode = lineitem[b'l_shipmode'].decode('utf-8')
    if l_shipmode not in results:
        continue
    
    l_commitdate = datetime.fromtimestamp(float(lineitem[b'l_commitdate'].decode('utf-8')))
    l_receiptdate = datetime.fromtimestamp(float(lineitem[b'l_receiptdate'].decode('utf-8')))
    l_shipdate = datetime.fromtimestamp(float(lineitem[b'l_shipdate'].decode('utf-8')))

    if not (l_commitdate < l_receiptdate and l_shipdate < l_commitdate and start_date <= l_receiptdate < end_date):
        continue
    
    # Recuperar o pedido correspondente na tabela orders
    l_orderkey = lineitem[b'l_orderkey'].decode('utf-8')
    order = r.hgetall(f'orders:{l_orderkey}')
    o_orderpriority = order[b'o_orderpriority'].decode('utf-8')

    # Contagem de alta e baixa prioridade
    if o_orderpriority in ['1-URGENT', '2-HIGH']:
        results[l_shipmode]['high_line_count'] += 1
    else:
        results[l_shipmode]['low_line_count'] += 1

# Medir o tempo de execução total
end_time = time.time()
execution_time = end_time - start_time

# Exibir os resultados
for shipmode, counts in results.items():
    print(f"Ship Mode: {shipmode}, High Line Count: {counts['high_line_count']}, Low Line Count: {counts['low_line_count']}")

# Exibir o tempo de execução total
print(f"\nTempo de execução total: {execution_time:.4f} segundos")

