import redis
from datetime import datetime
import time

# Conectar ao Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Parâmetros da consulta
start_date = datetime.strptime('1994-01-01', '%Y-%m-%d')
end_date = datetime.strptime('1995-01-01', '%Y-%m-%d')
target_region = 'ASIA'

# Medir o tempo de execução
start_time = time.time()

# Inicializar o dicionário para armazenar a receita por nação
revenue_by_nation = {}

# Filtrar as regiões para obter as nações na região `ASIA`
region_keys = [key for key in r.keys('region:*') if r.hget(key, 'r_name').decode('utf-8') == target_region]
nation_keys = []
for region_key in region_keys:
    region_id = region_key.decode('utf-8').split(':')[-1]
    nation_keys.extend([key for key in r.keys('nation:*') if r.hget(key, 'n_regionkey').decode('utf-8') == region_id])

# Processar cada nação na região `ASIA`
for nation_key in nation_keys:
    nation = r.hgetall(nation_key)
    n_nationkey = nation[b'n_nationkey'].decode('utf-8')
    n_name = nation[b'n_name'].decode('utf-8')

    # Encontrar os fornecedores (`supplier`) relacionados
    supplier_keys = [key for key in r.keys('supplier:*') if r.hget(key, 's_nationkey').decode('utf-8') == n_nationkey]

    for supplier_key in supplier_keys:
        supplier = r.hgetall(supplier_key)
        s_suppkey = supplier[b's_suppkey'].decode('utf-8')

        # Encontrar os pedidos (`orders`) relacionados
        for order_key in r.keys('orders:*'):
            order = r.hgetall(order_key)
            o_orderdate = datetime.fromtimestamp(float(order[b'o_orderdate'].decode('utf-8')))
            if not (start_date <= o_orderdate < end_date):
                continue

            o_orderkey = order[b'o_orderkey'].decode('utf-8')
            c_custkey = order[b'o_custkey'].decode('utf-8')

            # Verificar se o cliente (`customer`) pertence à nação correta
            customer = r.hget(f'customer:{c_custkey}', 'c_nationkey')
            if customer.decode('utf-8') != n_nationkey:
                continue

            # Encontrar os itens de linha (`lineitem`) relacionados
            for lineitem_key in r.keys(f'lineitem:{o_orderkey}:*'):
                lineitem = r.hgetall(lineitem_key)
                l_suppkey = lineitem[b'l_suppkey'].decode('utf-8')
                if l_suppkey != s_suppkey:
                    continue

                # Calcular a receita (revenue)
                l_extendedprice = float(lineitem[b'l_extendedprice'].decode('utf-8'))
                l_discount = float(lineitem[b'l_discount'].decode('utf-8'))
                revenue = l_extendedprice * (1 - l_discount)

                # Agrupar e acumular a receita por nação
                if n_name not in revenue_by_nation:
                    revenue_by_nation[n_name] = 0
                revenue_by_nation[n_name] += revenue

# Ordenar os resultados por receita decrescente
sorted_results = sorted(revenue_by_nation.items(), key=lambda x: -x[1])

# Medir o tempo de execução total
end_time = time.time()
execution_time = end_time - start_time

# Exibir os resultados
for nation, revenue in sorted_results:
    print(f"Nation: {nation}, Revenue: {revenue:.2f}")

# Exibir o tempo de execução total
print(f"\nTempo de execução total: {execution_time:.2f} segundos")


