import redis
from datetime import datetime, timedelta
import time

# Conectar ao Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Parâmetros da consulta
start_date = datetime.strptime('1994-01-01', '%Y-%m-%d')
end_date = start_date + timedelta(days=365)  # Intervalo de 1 ano

# Medir o tempo de execução
start_time = time.time()

# Identificar as peças cujo nome começa com 'forest'
forest_parts = set()
for part_key in r.keys('part:*'):
    part = r.hgetall(part_key)
    p_name = part[b'p_name'].decode('utf-8')
    if p_name.startswith('forest'):
        forest_parts.add(part[b'p_partkey'].decode('utf-8'))

# Identificar os fornecedores que atendem ao critério de quantidade disponível > 50% da soma de quantidades
valid_suppliers = set()
for ps_key in r.keys('partsupp:*'):
    partsupp = r.hgetall(ps_key)
    ps_partkey = partsupp[b'ps_partkey'].decode('utf-8')
    ps_suppkey = partsupp[b'ps_suppkey'].decode('utf-8')
    ps_availqty = float(partsupp[b'ps_availqty'].decode('utf-8'))

    if ps_partkey in forest_parts:
        # Calcular a soma das quantidades de itens para este partkey e suppkey
        total_quantity = 0
        for lineitem_key in r.keys(f'lineitem:*:{ps_suppkey}'):
            lineitem = r.hgetall(lineitem_key)
            l_partkey = lineitem[b'l_partkey'].decode('utf-8')
            l_suppkey = lineitem[b'l_suppkey'].decode('utf-8')
            l_shipdate = datetime.fromtimestamp(float(lineitem[b'l_shipdate'].decode('utf-8')))

            if l_partkey == ps_partkey and l_suppkey == ps_suppkey and start_date <= l_shipdate < end_date:
                total_quantity += float(lineitem[b'l_quantity'].decode('utf-8'))

        # Verificar se a quantidade disponível é maior que 50% da soma das quantidades
        if ps_availqty > 0.5 * total_quantity:
            valid_suppliers.add(ps_suppkey)

# Filtrar fornecedores da nação 'CANADA' e ordenar por nome
results = []
for supplier_key in valid_suppliers:
    supplier = r.hgetall(f'supplier:{supplier_key}')
    s_nationkey = supplier[b's_nationkey'].decode('utf-8')
    s_name = supplier[b's_name'].decode('utf-8')
    s_address = supplier[b's_address'].decode('utf-8')

    # Verificar se o fornecedor está na nação 'CANADA'
    nation = r.hget(f'nation:{s_nationkey}', 'n_name').decode('utf-8')
    if nation == 'CANADA':
        results.append((s_name, s_address))

# Ordenar os resultados pelo nome do fornecedor
results.sort(key=lambda x: x[0])

# Medir o tempo de execução total
end_time = time.time()
execution_time = end_time - start_time

# Exibir os resultados
for s_name, s_address in results:
    print(f"Supplier Name: {s_name}, Address: {s_address}")

# Exibir o tempo de execução total
print(f"\nTempo de execução total: {execution_time:.2f} segundos")
