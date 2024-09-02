import redis
from datetime import datetime
import time

# Conectar ao Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Parâmetros da consulta
start_date = datetime.strptime('1995-01-01', '%Y-%m-%d')
end_date = datetime.strptime('1996-12-31', '%Y-%m-%d')
target_nations = {'FRANCE', 'GERMANY'}

# Medir o tempo de execução
start_time = time.time()

# Inicializar o dicionário para armazenar a receita por fornecedor, cliente e ano
revenue_by_nation_year = {}

# Processar cada item de linha (`lineitem`)
for lineitem_key in r.keys('lineitem:*'):
    lineitem = r.hgetall(lineitem_key)
    l_shipdate = datetime.fromtimestamp(float(lineitem[b'l_shipdate'].decode('utf-8')))
    if not (start_date <= l_shipdate <= end_date):
        continue

    l_suppkey = lineitem[b'l_suppkey'].decode('utf-8')
    l_orderkey = lineitem[b'l_orderkey'].decode('utf-8')
    l_extendedprice = float(lineitem[b'l_extendedprice'].decode('utf-8'))
    l_discount = float(lineitem[b'l_discount'].decode('utf-8'))

    # Encontrar o fornecedor (`supplier`) relacionado
    supplier = r.hgetall(f'supplier:{l_suppkey}')
    s_nationkey = supplier[b's_nationkey'].decode('utf-8')

    # Encontrar a nação do fornecedor
    supp_nation = r.hget(f'nation:{s_nationkey}', 'n_name').decode('utf-8')
    if supp_nation not in target_nations:
        continue

    # Encontrar o pedido (`orders`) relacionado
    order = r.hgetall(f'orders:{l_orderkey}')
    c_custkey = order[b'o_custkey'].decode('utf-8')

    # Encontrar o cliente (`customer`) relacionado
    customer = r.hgetall(f'customer:{c_custkey}')
    c_nationkey = customer[b'c_nationkey'].decode('utf-8')

    # Encontrar a nação do cliente
    cust_nation = r.hget(f'nation:{c_nationkey}', 'n_name').decode('utf-8')
    if cust_nation not in target_nations or cust_nation == supp_nation:
        continue

    # Calcular o volume (receita)
    volume = l_extendedprice * (1 - l_discount)
    l_year = l_shipdate.year

    # Agrupar e acumular a receita por fornecedor, cliente e ano
    key = (supp_nation, cust_nation, l_year)
    if key not in revenue_by_nation_year:
        revenue_by_nation_year[key] = 0
    revenue_by_nation_year[key] += volume

# Ordenar os resultados por fornecedor, cliente e ano
sorted_results = sorted(revenue_by_nation_year.items(), key=lambda x: (x[0][0], x[0][1], x[0][2]))

# Medir o tempo de execução total
end_time = time.time()
execution_time = end_time - start_time

# Exibir os resultados
for (supp_nation, cust_nation, l_year), revenue in sorted_results:
    print(f"Supplier Nation: {supp_nation}, Customer Nation: {cust_nation}, Year: {l_year}, Revenue: {revenue:.2f}")

# Exibir o tempo de execução total
print(f"\nTempo de execução total: {execution_time:.2f} segundos")
