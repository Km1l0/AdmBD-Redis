import redis
from datetime import datetime, timedelta
import time

# Conectar ao Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Parâmetros da consulta
start_date = datetime.strptime('1993-10-01', '%Y-%m-%d')
end_date = start_date + timedelta(days=90)  # 3 meses de intervalo

# Medir o tempo de execução
start_time = time.time()

# Inicializar o dicionário para armazenar a receita por cliente
revenue_by_customer = {}

# Processar cada item de linha (`lineitem`)
for lineitem_key in r.keys('lineitem:*'):
    lineitem = r.hgetall(lineitem_key)
    l_orderkey = lineitem[b'l_orderkey'].decode('utf-8')
    l_returnflag = lineitem[b'l_returnflag'].decode('utf-8')
    l_extendedprice = float(lineitem[b'l_extendedprice'].decode('utf-8'))
    l_discount = float(lineitem[b'l_discount'].decode('utf-8'))

    # Considerar apenas itens devolvidos
    if l_returnflag != 'R':
        continue

    # Encontrar o pedido (`orders`) relacionado
    order = r.hgetall(f'orders:{l_orderkey}')
    o_orderdate = datetime.fromtimestamp(float(order[b'o_orderdate'].decode('utf-8')))
    if not (start_date <= o_orderdate < end_date):
        continue

    o_custkey = order[b'o_custkey'].decode('utf-8')

    # Encontrar o cliente (`customer`) relacionado
    customer = r.hgetall(f'customer:{o_custkey}')
    c_custkey = customer[b'c_custkey'].decode('utf-8')
    c_name = customer[b'c_name'].decode('utf-8')
    c_acctbal = float(customer[b'c_acctbal'].decode('utf-8'))
    c_address = customer[b'c_address'].decode('utf-8')
    c_phone = customer[b'c_phone'].decode('utf-8')
    c_comment = customer[b'c_comment'].decode('utf-8')
    c_nationkey = customer[b'c_nationkey'].decode('utf-8')

    # Encontrar a nação (`nation`) relacionada
    nation = r.hget(f'nation:{c_nationkey}', 'n_name').decode('utf-8')

    # Calcular a receita perdida (revenue)
    revenue = l_extendedprice * (1 - l_discount)

    # Agrupar e acumular a receita por cliente
    key = (c_custkey, c_name, c_acctbal, nation, c_address, c_phone, c_comment)
    if key not in revenue_by_customer:
        revenue_by_customer[key] = 0
    revenue_by_customer[key] += revenue

# Ordenar os resultados por receita decrescente
sorted_results = sorted(revenue_by_customer.items(), key=lambda x: -x[1])

# Medir o tempo de execução total
end_time = time.time()
execution_time = end_time - start_time

# Exibir os resultados
for (c_custkey, c_name, c_acctbal, nation, c_address, c_phone, c_comment), revenue in sorted_results:
    print(f"Customer Key: {c_custkey}, Name: {c_name}, Revenue: {revenue:.2f}, Balance: {c_acctbal}, Nation: {nation}")
    print(f"Address: {c_address}, Phone: {c_phone}, Comment: {c_comment}")
    print("---------------------------------------------------------------")

# Exibir o tempo de execução total
print(f"\nTempo de execução total: {execution_time:.2f} segundos")
