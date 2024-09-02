import redis
import time
from datetime import datetime

# Conectar ao Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Medir o tempo de execução
start_time = time.time()

# Verificação inicial: Contagem dos registros que atendem às condições de filtragem
def count_filtered_records():
    count_customers = 0
    count_orders = 0
    count_lineitems = 0

    # Contagem de clientes filtrados
    for key in r.keys('customer:*'):
        customer = r.hgetall(key)
        if customer[b'c_mktsegment'].decode('utf-8') == 'BUILDING':
            count_customers += 1

            # Contagem de pedidos filtrados
            c_custkey = customer[b'c_custkey'].decode('utf-8')
            for order_key in r.keys(f'orders:*:{c_custkey}'):
                order = r.hgetall(order_key)
                o_orderdate = datetime.strptime(order[b'o_orderdate'].decode('utf-8'), "%Y-%m-%d")
                if o_orderdate < datetime.strptime('1995-03-15', "%Y-%m-%d"):
                    count_orders += 1

                    # Contagem de itens filtrados
                    o_orderkey = order[b'o_orderkey'].decode('utf-8')
                    for lineitem_key in r.keys(f'lineitem:{o_orderkey}:*'):
                        lineitem = r.hgetall(lineitem_key)
                        l_shipdate = datetime.fromtimestamp(float(lineitem[b'l_shipdate'].decode('utf-8')))
                        if l_shipdate > datetime.strptime('1995-03-15', "%Y-%m-%d"):
                            count_lineitems += 1

    return count_customers, count_orders, count_lineitems

# Chame a função de contagem para verificar a quantidade de registros que atendem a cada filtro
filtered_customers, filtered_orders, filtered_lineitems = count_filtered_records()
print(f"Clientes filtrados: {filtered_customers}")
print(f"Pedidos filtrados: {filtered_orders}")
print(f"Itens de linha filtrados: {filtered_lineitems}")

# Se houver registros filtrados, continue com a execução da consulta
if filtered_customers > 0 and filtered_orders > 0 and filtered_lineitems > 0:
    results = []

    # Processar cada cliente filtrado
    for key in r.keys('customer:*'):
        customer = r.hgetall(key)
        if customer[b'c_mktsegment'].decode('utf-8') != 'BUILDING':
            continue

        c_custkey = customer[b'c_custkey'].decode('utf-8')

        # Filtrar pedidos (`orders`) com `o_orderdate < '1995-03-15'`
        for order_key in r.keys(f'orders:*:{c_custkey}'):
            order = r.hgetall(order_key)
            o_orderdate = datetime.strptime(order[b'o_orderdate'].decode('utf-8'), "%Y-%m-%d")
            if o_orderdate >= datetime.strptime('1995-03-15', "%Y-%m-%d"):
                continue

            o_orderkey = order[b'o_orderkey'].decode('utf-8')
            o_shippriority = order[b'o_shippriority'].decode('utf-8')

            # Filtrar itens (`lineitem`) com `l_shipdate > '1995-03-15'`
            for lineitem_key in r.keys(f'lineitem:{o_orderkey}:*'):
                lineitem = r.hgetall(lineitem_key)
                l_shipdate = datetime.fromtimestamp(float(lineitem[b'l_shipdate'].decode('utf-8')))
                if l_shipdate <= datetime.strptime('1995-03-15', "%Y-%m-%d"):
                    continue

                # Calcular a receita (revenue)
                l_extendedprice = float(lineitem[b'l_extendedprice'])
                l_discount = float(lineitem[b'l_discount'])
                revenue = l_extendedprice * (1 - l_discount)

                # Armazenar o resultado agregado
                results.append({
                    'l_orderkey': o_orderkey,
                    'revenue': revenue,
                    'o_orderdate': o_orderdate.strftime("%Y-%m-%d"),
                    'o_shippriority': o_shippriority
                })

    # Agrupar e somar receitas por `l_orderkey`, `o_orderdate`, `o_shippriority`
    aggregated_results = {}
    for result in results:
        group_key = (result['l_orderkey'], result['o_orderdate'], result['o_shippriority'])
        if group_key not in aggregated_results:
            aggregated_results[group_key] = 0
        aggregated_results[group_key] += result['revenue']

    # Converter os resultados agrupados em uma lista ordenada
    final_results = [{
        'l_orderkey': key[0],
        'revenue': revenue,
        'o_orderdate': key[1],
        'o_shippriority': key[2]
    } for key, revenue in aggregated_results.items()]

    # Ordenar os resultados por `revenue` desc, `o_orderdate`
    final_results.sort(key=lambda x: (-x['revenue'], x['o_orderdate']))

    # Exibir os resultados
    for result in final_results:
        print(result)

# Medir o tempo de execução total
end_time = time.time()
execution_time = end_time - start_time

# Exibir o tempo de execução total
print(f"\nTempo de execução total: {execution_time:.2f} segundos")



