import redis
import time

# Conectar ao Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Parâmetros da consulta
country_codes = ('13', '31', '23', '29', '30', '18', '17')

# Medir o tempo de execução
start_time = time.time()

# Subconsulta: Calcular a média dos saldos de contas positivas para os códigos de país especificados
total_acctbal = 0
count_acctbal = 0

for customer_key in r.keys('customer:*'):
    customer = r.hgetall(customer_key)
    c_acctbal = float(customer[b'c_acctbal'].decode('utf-8'))
    c_phone = customer[b'c_phone'].decode('utf-8')
    cntrycode = c_phone[:2]
    
    if cntrycode in country_codes and c_acctbal > 0.00:
        total_acctbal += c_acctbal
        count_acctbal += 1

avg_acctbal = total_acctbal / count_acctbal if count_acctbal > 0 else 0

# Filtrar os clientes que atendem às condições da subconsulta interna
filtered_customers = {}

for customer_key in r.keys('customer:*'):
    customer = r.hgetall(customer_key)
    c_custkey = customer[b'c_custkey'].decode('utf-8')
    c_acctbal = float(customer[b'c_acctbal'].decode('utf-8'))
    c_phone = customer[b'c_phone'].decode('utf-8')
    cntrycode = c_phone[:2]
    
    # Verificar condições
    if cntrycode in country_codes and c_acctbal > avg_acctbal:
        # Verificar se o cliente não tem pedidos
        if not r.exists(f'orders:{c_custkey}'):
            if cntrycode not in filtered_customers:
                filtered_customers[cntrycode] = {'numcust': 0, 'totacctbal': 0}
            
            filtered_customers[cntrycode]['numcust'] += 1
            filtered_customers[cntrycode]['totacctbal'] += c_acctbal

# Ordenar os resultados por `cntrycode`
sorted_results = sorted(filtered_customers.items(), key=lambda x: x[0])

# Medir o tempo de execução total
end_time = time.time()
execution_time = end_time - start_time

# Exibir os resultados
for cntrycode, data in sorted_results:
    print(f"Country Code: {cntrycode}, NumCust: {data['numcust']}, Total AcctBal: {data['totacctbal']:.2f}")

# Exibir o tempo de execução total
print(f"\nTempo de execução total: {execution_time:.2f} segundos")
