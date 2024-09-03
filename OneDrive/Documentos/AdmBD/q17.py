import redis
import time

# Conectar ao Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Medir o tempo de execução
start_time = time.time()

# Inicializar variáveis para acumular a receita e armazenar a média da quantidade por `l_partkey`
revenue_sum = 0
avg_quantity_by_partkey = {}

# Primeira passagem para calcular a média da quantidade (`l_quantity`) por `l_partkey`
for lineitem_key in r.keys('lineitem:*'):
    lineitem = r.hgetall(lineitem_key)
    l_partkey = lineitem[b'l_partkey'].decode('utf-8')
    l_quantity = float(lineitem[b'l_quantity'].decode('utf-8'))

    if l_partkey not in avg_quantity_by_partkey:
        avg_quantity_by_partkey[l_partkey] = []
    avg_quantity_by_partkey[l_partkey].append(l_quantity)

# Calcular a média de quantidade por `l_partkey`
for partkey in avg_quantity_by_partkey:
    avg_quantity_by_partkey[partkey] = sum(avg_quantity_by_partkey[partkey]) / len(avg_quantity_by_partkey[partkey])

# Segunda passagem para filtrar e calcular a receita
for lineitem_key in r.keys('lineitem:*'):
    lineitem = r.hgetall(lineitem_key)
    l_partkey = lineitem[b'l_partkey'].decode('utf-8')
    l_quantity = float(lineitem[b'l_quantity'].decode('utf-8'))
    l_extendedprice = float(lineitem[b'l_extendedprice'].decode('utf-8'))

    # Filtrar com base na média da quantidade
    if l_quantity >= 0.2 * avg_quantity_by_partkey[l_partkey]:
        continue

    # Verificar se a peça atende aos critérios de marca e contêiner
    part = r.hgetall(f'part:{l_partkey}')
    p_brand = part[b'p_brand'].decode('utf-8')
    p_container = part[b'p_container'].decode('utf-8')

    if p_brand != 'Brand#23' or p_container != 'MED BOX':
        continue

    # Acumular a receita
    revenue_sum += l_extendedprice

# Calcular a média anual
avg_yearly_revenue = revenue_sum / 7.0

# Medir o tempo de execução total
end_time = time.time()
execution_time = end_time - start_time

# Exibir o resultado
print(f"Average Yearly Revenue: {avg_yearly_revenue:.2f}")

# Exibir o tempo de execução total
print(f"\nTempo de execução total: {execution_time:.2f} segundos")


