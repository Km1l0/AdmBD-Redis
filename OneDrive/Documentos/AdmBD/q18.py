import redis
import time

# Conectar ao Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Medir o tempo de execução
start_time = time.time()

# Subconsulta: Identificar `l_orderkey` com soma de `l_quantity` superior a 300
order_quantity = {}

for lineitem_key in r.keys('lineitem:*'):
    lineitem = r.hgetall(lineitem_key)
    l_orderkey = lineitem[b'l_orderkey'].decode('utf-8')
    l_quantity = float(lineitem[b'l_quantity'].decode('utf-8'))

    if l_orderkey not in order_quantity:
        order_quantity[l_orderkey] = 0  # Inicializar a soma

    order_quantity[l_orderkey] += l_quantity

# Filtrar `l_orderkey` onde a soma das quantidades excede 300
orders_with_high_quantity = {order_key for order_key, quantity in order_quantity.items() if quantity > 300}

# Dicionário para armazenar resultados agregados
aggregated_results = []

# Filtragem e junção com `customer` e `orders`
for order_key in orders_with_high_quantity:
    order = r.hgetall(f'orders:{order_key}')
    o_custkey = order[b'o_custkey'].decode('utf-8')
    o_orderdate = order[b'o_orderdate'].decode('utf-8')
    o_totalprice = float(order[b'o_totalprice'].decode('utf-8'))

    customer = r.hgetall(f'customer:{o_custkey}')
    c_name = customer[b'c_name'].decode('utf-8')
    c_custkey = customer[b'c_custkey'].decode('utf-8')

    # Soma das quantidades para o pedido
    total_quantity = sum(
        float(r.hget(f'lineitem:{order_key}:{i}', 'l_quantity').decode('utf-8'))
        for i in range(len(r.keys(f'lineitem:{order_key}:*')))
        if r.hget(f'lineitem:{order_key}:{i}', 'l_quantity') is not None
    )

    # Armazenar o resultado agregado
    aggregated_results.append({
        'c_name': c_name,
        'c_custkey': c_custkey,
        'o_orderkey': order_key,
        'o_orderdate': o_orderdate,
        'o_totalprice': o_totalprice,
        'total_quantity': total_quantity
    })

# Ordenar os resultados por `o_totalprice` (descendente) e `o_orderdate`
sorted_results = sorted(aggregated_results, key=lambda x: (-x['o_totalprice'], x['o_orderdate']))

# Medir o tempo de execução total
end_time = time.time()
execution_time = end_time - start_time

# Exibir os resultados
for result in sorted_results:
    print(f"Customer Name: {result['c_name']}, Customer Key: {result['c_custkey']}, Order Key: {result['o_orderkey']}, "
          f"Order Date: {result['o_orderdate']}, Total Price: {result['o_totalprice']:.2f}, "
          f"Total Quantity: {result['total_quantity']}")

# Exibir o tempo de execução total
print(f"\nTempo de execução total: {execution_time:.2f} segundos")


