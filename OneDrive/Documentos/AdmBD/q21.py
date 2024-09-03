import redis
import time
from datetime import datetime

# Conectar ao Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Medir o tempo de execução
start_time = time.time()

# Inicializar dicionário para contar atrasos por fornecedor
delay_count_by_supplier = {}

# Filtrar linhas relevantes e processar cada item de linha (`lineitem`)
lineitems = r.keys('lineitem:*')

for lineitem_key in lineitems:
    lineitem = r.hgetall(lineitem_key)
    l_suppkey = lineitem.get(b'l_suppkey', None)
    l_orderkey = lineitem.get(b'l_orderkey', None)
    l_receiptdate = lineitem.get(b'l_receiptdate', None)
    l_commitdate = lineitem.get(b'l_commitdate', None)

    if l_suppkey and l_orderkey and l_receiptdate and l_commitdate:
        l_suppkey = l_suppkey.decode('utf-8')
        l_orderkey = l_orderkey.decode('utf-8')
        l_receiptdate = datetime.fromtimestamp(float(l_receiptdate.decode('utf-8')))
        l_commitdate = datetime.fromtimestamp(float(l_commitdate.decode('utf-8')))

        # Verificar se o pedido foi finalizado e teve atraso
        order = r.hgetall(f'orders:{l_orderkey}')
        o_orderstatus = order.get(b'o_orderstatus', None)

        if o_orderstatus and o_orderstatus.decode('utf-8') == 'F' and l_receiptdate > l_commitdate:
            # Verificar se outros fornecedores estão envolvidos e não tiveram atrasos
            other_suppliers = []
            lineitem_keys_for_order = r.keys(f'lineitem:{l_orderkey}:*')
            for i in lineitem_keys_for_order:
                other_suppkey = r.hget(i, 'l_suppkey')
                if other_suppkey and other_suppkey.decode('utf-8') != l_suppkey:
                    other_suppliers.append(other_suppkey.decode('utf-8'))

            if other_suppliers:
                no_other_delays = not any(
                    datetime.fromtimestamp(float(r.hget(i, 'l_receiptdate').decode('utf-8'))) >
                    datetime.fromtimestamp(float(r.hget(i, 'l_commitdate').decode('utf-8')))
                    for i in lineitem_keys_for_order
                    if r.hget(i, 'l_suppkey') and r.hget(i, 'l_suppkey').decode('utf-8') in other_suppliers
                )

                if no_other_delays:
                    # Contar atrasos para o fornecedor
                    if l_suppkey not in delay_count_by_supplier:
                        delay_count_by_supplier[l_suppkey] = 0
                    delay_count_by_supplier[l_suppkey] += 1

# Filtrar fornecedores da nação 'SAUDI ARABIA' e ordenar por contagem de atrasos
results = []
for suppkey, numwait in delay_count_by_supplier.items():
    supplier = r.hgetall(f'supplier:{suppkey}')
    s_nationkey = supplier.get(b's_nationkey', None)
    s_name = supplier.get(b's_name', None)

    if s_nationkey and s_name:
        s_nationkey = s_nationkey.decode('utf-8')
        s_name = s_name.decode('utf-8')

        # Verificar se o fornecedor está na nação 'SAUDI ARABIA'
        nation = r.hget(f'nation:{s_nationkey}', 'n_name')
        if nation and nation.decode('utf-8') == 'SAUDI ARABIA':
            results.append((s_name, numwait))

# Ordenar os resultados por `numwait` (descendente) e `s_name`
results.sort(key=lambda x: (-x[1], x[0]))

# Medir o tempo de execução total
end_time = time.time()
execution_time = end_time - start_time

# Exibir os resultados
for s_name, numwait in results:
    print(f"Supplier Name: {s_name}, NumWait: {numwait}")

# Exibir o tempo de execução total
print(f"\nTempo de execução total: {execution_time:.2f} segundos")



