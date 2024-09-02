import redis
from datetime import datetime
import time

# Conectar ao Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Medir o tempo de execução
start_time = time.time()

# Inicializar o dicionário para armazenar os resultados agregados
profit_by_nation_year = {}

# Filtrar as peças (`part`) com `p_name` contendo 'green'
parts = []
for part_key in r.keys('part:*'):
    part = r.hgetall(part_key)
    if 'green' in part[b'p_name'].decode('utf-8'):
        parts.append(part)

# Processar cada peça filtrada
for part in parts:
    p_partkey = part[b'p_partkey'].decode('utf-8')

    # Encontrar os itens de linha (`lineitem`) relacionados
    for lineitem_key in r.keys(f'lineitem:*:{p_partkey}'):
        lineitem = r.hgetall(lineitem_key)
        
        l_suppkey = lineitem[b'l_suppkey'].decode('utf-8')
        l_orderkey = lineitem[b'l_orderkey'].decode('utf-8')
        l_extendedprice = float(lineitem[b'l_extendedprice'].decode('utf-8'))
        l_discount = float(lineitem[b'l_discount'].decode('utf-8'))
        l_quantity = float(lineitem[b'l_quantity'].decode('utf-8'))

        # Encontrar o fornecedor (`supplier`) relacionado
        supplier = r.hgetall(f'supplier:{l_suppkey}')
        s_nationkey = supplier[b's_nationkey'].decode('utf-8')

        # Encontrar o custo do suprimento (`partsupp`)
        partsupp = r.hgetall(f'partsupp:{p_partkey}:{l_suppkey}')
        ps_supplycost = float(partsupp[b'ps_supplycost'].decode('utf-8'))

        # Encontrar o pedido (`orders`) relacionado
        order = r.hgetall(f'orders:{l_orderkey}')
        # Converter o timestamp para uma data
        o_orderdate = datetime.fromtimestamp(float(order[b'o_orderdate'].decode('utf-8')))
        o_year = o_orderdate.year

        # Encontrar a nação (`nation`) relacionada
        nation = r.hget(f'nation:{s_nationkey}', 'n_name')
        nation_name = nation.decode('utf-8')

        # Calcular o lucro (amount)
        amount = (l_extendedprice * (1 - l_discount)) - (ps_supplycost * l_quantity)

        # Agrupar e acumular o lucro por nação e ano
        key = (nation_name, o_year)
        if key not in profit_by_nation_year:
            profit_by_nation_year[key] = 0
        profit_by_nation_year[key] += amount

# Ordenar os resultados
sorted_results = sorted(profit_by_nation_year.items(), key=lambda x: (x[0][0], -x[0][1]))

# Medir o tempo de execução total
end_time = time.time()
execution_time = end_time - start_time

# Exibir os resultados
for (nation, year), profit in sorted_results:
    print(f"Nation: {nation}, Year: {year}, Profit: {profit:.2f}")

# Exibir o tempo de execução total
print(f"\nTempo de execução total: {execution_time:.2f} segundos")

