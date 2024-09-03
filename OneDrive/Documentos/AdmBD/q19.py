import redis
import time

# Conectar ao Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Medir o tempo de execução
start_time = time.time()

# Inicializar variável para acumular a receita total
total_revenue = 0

# Processar cada item de linha (`lineitem`)
for lineitem_key in r.keys('lineitem:*'):
    lineitem = r.hgetall(lineitem_key)
    l_partkey = lineitem[b'l_partkey'].decode('utf-8')
    l_quantity = float(lineitem[b'l_quantity'].decode('utf-8'))
    l_extendedprice = float(lineitem[b'l_extendedprice'].decode('utf-8'))
    l_discount = float(lineitem[b'l_discount'].decode('utf-8'))
    l_shipmode = lineitem[b'l_shipmode'].decode('utf-8')
    l_shipinstruct = lineitem[b'l_shipinstruct'].decode('utf-8')

    # Encontrar a peça (`part`) relacionada
    part = r.hgetall(f'part:{l_partkey}')
    p_brand = part[b'p_brand'].decode('utf-8')
    p_container = part[b'p_container'].decode('utf-8')
    p_size = int(part[b'p_size'].decode('utf-8'))

    # Aplicar as condições da consulta
    if (
        (p_brand == 'Brand#12' and p_container in ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG') and
         1 <= l_quantity <= 11 and 1 <= p_size <= 5 and
         l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct == 'DELIVER IN PERSON')
        or
        (p_brand == 'Brand#23' and p_container in ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK') and
         10 <= l_quantity <= 20 and 1 <= p_size <= 10 and
         l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct == 'DELIVER IN PERSON')
        or
        (p_brand == 'Brand#34' and p_container in ('LG CASE', 'LG BOX', 'LG PACK', 'LG PKG') and
         20 <= l_quantity <= 30 and 1 <= p_size <= 15 and
         l_shipmode in ('AIR', 'AIR REG') and l_shipinstruct == 'DELIVER IN PERSON')
    ):
        # Calcular a receita para os itens que atendem aos critérios
        revenue = l_extendedprice * (1 - l_discount)
        total_revenue += revenue

# Medir o tempo de execução total
end_time = time.time()
execution_time = end_time - start_time

# Exibir o resultado
print(f"Total Revenue: {total_revenue:.2f}")

# Exibir o tempo de execução total
print(f"\nTempo de execução total: {execution_time:.2f} segundos")
