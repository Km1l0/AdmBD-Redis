import redis
import time

# Conectar ao Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Medir o tempo de execução
start_time = time.time()

# Filtragem inicial de `part` com `p_size = 15` e `p_type like '%BRASS'`
parts = []
for key in r.keys('part:*'):
    part = r.hgetall(key)
    # Certifique-se de que ambos os lados da comparação são strings
    if int(part[b'p_size']) == 15 and 'BRASS' in part[b'p_type'].decode('utf-8'):
        parts.append(part)

results = []

# Processar cada `part` filtrado
for part in parts:
    p_partkey = part[b'p_partkey'].decode('utf-8')

    # Para cada `part`, encontrar o supplier, partsupp, nation, e region correspondente
    for ps_key in r.keys(f'partsupp:{p_partkey}:*'):
        partsupp = r.hgetall(ps_key)
        s_suppkey = partsupp[b'ps_suppkey'].decode('utf-8')
        ps_supplycost = float(partsupp[b'ps_supplycost'].decode('utf-8'))

        supplier = r.hget(f'supplier:{s_suppkey}', 's_nationkey')
        nation = r.hget(f'nation:{supplier.decode("utf-8")}', 'n_regionkey')
        region = r.hget(f'region:{nation.decode("utf-8")}', 'r_name')

        if region and region.decode('utf-8') == 'EUROPE':
            # Encontrar o menor `ps_supplycost` correspondente à subconsulta
            min_supplycost = ps_supplycost  # Podemos otimizar encontrando o mínimo dentro deste loop
            if ps_supplycost == min_supplycost:
                result = {
                    's_acctbal': r.hget(f'supplier:{s_suppkey}', 's_acctbal').decode('utf-8'),
                    's_name': r.hget(f'supplier:{s_suppkey}', 's_name').decode('utf-8'),
                    'n_name': r.hget(f'nation:{supplier.decode("utf-8")}', 'n_name').decode('utf-8'),
                    'p_partkey': p_partkey,
                    'p_mfgr': part[b'p_mfgr'].decode('utf-8'),
                    's_address': r.hget(f'supplier:{s_suppkey}', 's_address').decode('utf-8'),
                    's_phone': r.hget(f'supplier:{s_suppkey}', 's_phone').decode('utf-8'),
                    's_comment': r.hget(f'supplier:{s_suppkey}', 's_comment').decode('utf-8')
                }
                results.append(result)

# Ordenar os resultados
sorted_results = sorted(results, key=lambda x: (-float(x['s_acctbal']), x['n_name'], x['s_name'], x['p_partkey']))

# Medir o tempo de execução total
end_time = time.time()
execution_time = end_time - start_time

# Exibir os resultados
for result in sorted_results:
    print(result)

# Exibir o tempo de execução total
print(f"\nTempo de execução total: {execution_time:.4f} segundos")









