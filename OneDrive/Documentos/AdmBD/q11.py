import redis
import time

# Conectar ao Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Medir o tempo de execução
start_time = time.time()

# Inicializar dicionários para armazenar os valores do estoque por peça e o valor total do estoque
stock_value_by_part = {}
total_stock_value = 0

# Filtrar os fornecedores na nação 'GERMANY'
nation_name = 'GERMANY'

supplier_keys = [key for key in r.keys('supplier:*') if r.hget(key, 's_nationkey')]
nation_keys = [key for key in supplier_keys if r.hget(f'nation:{r.hget(key, "s_nationkey").decode("utf-8")}', 'n_name').decode('utf-8') == nation_name]

# Processar cada fornecedor relacionado
for supplier_key in nation_keys:
    s_suppkey = supplier_key.decode('utf-8').split(':')[-1]
    
    # Encontrar os itens de `partsupp` relacionados
    partsupp_keys = [key for key in r.keys(f'partsupp:*:{s_suppkey}')]

    for partsupp_key in partsupp_keys:
        partsupp = r.hgetall(partsupp_key)
        ps_partkey = partsupp[b'ps_partkey'].decode('utf-8')
        ps_supplycost = float(partsupp[b'ps_supplycost'].decode('utf-8'))
        ps_availqty = float(partsupp[b'ps_availqty'].decode('utf-8'))

        # Calcular o valor do estoque
        stock_value = ps_supplycost * ps_availqty

        # Acumular o valor total do estoque
        total_stock_value += stock_value

        # Agrupar o valor do estoque por peça
        if ps_partkey not in stock_value_by_part:
            stock_value_by_part[ps_partkey] = 0
        stock_value_by_part[ps_partkey] += stock_value

# Calcular o limite da subconsulta (0,01% do valor total do estoque)
threshold_value = total_stock_value * 0.0001

# Filtrar peças cujo valor do estoque exceda o limite da subconsulta
filtered_parts = {part: value for part, value in stock_value_by_part.items() if value > threshold_value}

# Ordenar os resultados por valor do estoque em ordem decrescente
sorted_results = sorted(filtered_parts.items(), key=lambda x: -x[1])

# Medir o tempo de execução total
end_time = time.time()
execution_time = end_time - start_time

# Exibir os resultados
for ps_partkey, value in sorted_results:
    print(f"Part Key: {ps_partkey}, Stock Value: {value:.2f}")

# Exibir o tempo de execução total
print(f"\nTempo de execução total: {execution_time:.2f} segundos")
