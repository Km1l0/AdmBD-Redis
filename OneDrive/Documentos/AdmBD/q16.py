import redis
import time

# Conectar ao Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Medir o tempo de execução
start_time = time.time()

# Subconsulta: Filtrar fornecedores com comentários que mencionam "Customer Complaints"
excluded_suppliers = set()
for supplier_key in r.keys('supplier:*'):
    s_comment = r.hget(supplier_key, 's_comment').decode('utf-8')
    if 'Customer Complaints' in s_comment:
        s_suppkey = supplier_key.decode('utf-8').split(':')[-1]
        excluded_suppliers.add(s_suppkey)

# Inicializar dicionário para contar fornecedores únicos por combinação de marca, tipo e tamanho
supplier_count_by_part = {}

# Processar cada item de peça (`part`) e de fornecimento (`partsupp`)
for part_key in r.keys('part:*'):
    part = r.hgetall(part_key)
    p_partkey = part[b'p_partkey'].decode('utf-8')
    p_brand = part[b'p_brand'].decode('utf-8')
    p_type = part[b'p_type'].decode('utf-8')
    p_size = int(part[b'p_size'].decode('utf-8'))

    # Aplicar filtros na marca, tipo e tamanho
    if p_brand == 'Brand#45' or p_type.startswith('MEDIUM POLISHED') or p_size not in [49, 14, 23, 45, 19, 3, 36, 9]:
        continue

    # Verificar fornecedores para cada peça
    for partsupp_key in r.keys(f'partsupp:{p_partkey}:*'):
        partsupp = r.hgetall(partsupp_key)
        ps_suppkey = partsupp[b'ps_suppkey'].decode('utf-8')

        # Excluir fornecedores que aparecem na subconsulta
        if ps_suppkey in excluded_suppliers:
            continue

        # Agrupar e contar fornecedores únicos
        key = (p_brand, p_type, p_size)
        if key not in supplier_count_by_part:
            supplier_count_by_part[key] = set()
        supplier_count_by_part[key].add(ps_suppkey)

# Converter contagem de conjuntos para inteiros
final_count = {key: len(supp_set) for key, supp_set in supplier_count_by_part.items()}

# Ordenar os resultados por contagem de fornecedores (decrescente), marca, tipo e tamanho
sorted_results = sorted(final_count.items(), key=lambda x: (-x[1], x[0][0], x[0][1], x[0][2]))

# Medir o tempo de execução total
end_time = time.time()
execution_time = end_time - start_time

# Exibir os resultados
for (p_brand, p_type, p_size), supplier_cnt in sorted_results:
    print(f"Brand: {p_brand}, Type: {p_type}, Size: {p_size}, Supplier Count: {supplier_cnt}")

# Exibir o tempo de execução total
print(f"\nTempo de execução total: {execution_time:.2f} segundos")

