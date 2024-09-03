import redis
import time

# Conectar ao Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Medir o tempo de execução
start_time = time.time()

# Inicializar dicionário para armazenar a contagem de pedidos por cliente
order_count_by_customer = {}

# Processar cada cliente (`customer`)
for customer_key in r.keys('customer:*'):
    c_custkey = customer_key.decode('utf-8').split(':')[-1]
    order_count = 0

    # Verificar pedidos (`orders`) relacionados ao cliente
    for order_key in r.keys(f'orders:{c_custkey}:*'):
        order = r.hgetall(order_key)
        o_comment = order.get(b'o_comment', b'').decode('utf-8')

        # Excluir pedidos com comentários que contenham 'special requests'
        if 'special requests' in o_comment:
            continue

        # Contar o pedido se não for excluído
        order_count += 1

    # Armazenar a contagem de pedidos por cliente
    if order_count not in order_count_by_customer:
        order_count_by_customer[order_count] = 0
    order_count_by_customer[order_count] += 1

# Ordenar os resultados por número de clientes (custdist) e número de pedidos (c_count)
sorted_results = sorted(order_count_by_customer.items(), key=lambda x: (-x[1], -x[0]))

# Medir o tempo de execução total
end_time = time.time()
execution_time = end_time - start_time

# Exibir os resultados
for c_count, custdist in sorted_results:
    print(f"Order Count: {c_count}, Customer Distribution: {custdist}")

# Exibir o tempo de execução total
print(f"\nTempo de execução total: {execution_time:.4f} segundos")
