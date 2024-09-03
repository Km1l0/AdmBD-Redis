import redis
from datetime import datetime, timedelta
import time

# Conectar ao Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Parâmetros da consulta
start_date = datetime.strptime('1995-09-01', '%Y-%m-%d')
end_date = start_date + timedelta(days=30)  # Intervalo de 1 mês

# Medir o tempo de execução
start_time = time.time()

# Inicializar variáveis para acumular as receitas
total_revenue = 0
promo_revenue = 0

# Processar cada item de linha (`lineitem`)
for lineitem_key in r.keys('lineitem:*'):
    lineitem = r.hgetall(lineitem_key)
    l_partkey = lineitem[b'l_partkey'].decode('utf-8')
    l_shipdate = datetime.fromtimestamp(float(lineitem[b'l_shipdate'].decode('utf-8')))
    
    if not (start_date <= l_shipdate < end_date):
        continue

    l_extendedprice = float(lineitem[b'l_extendedprice'].decode('utf-8'))
    l_discount = float(lineitem[b'l_discount'].decode('utf-8'))

    # Encontrar a peça (`part`) relacionada
    part = r.hgetall(f'part:{l_partkey}')
    p_type = part[b'p_type'].decode('utf-8')

    # Calcular a receita total
    revenue = l_extendedprice * (1 - l_discount)
    total_revenue += revenue

    # Calcular a receita de promoções
    if p_type.startswith('PROMO'):
        promo_revenue += revenue

# Calcular a porcentagem de receita de promoções
promo_percentage = (promo_revenue / total_revenue) * 100 if total_revenue > 0 else 0

# Medir o tempo de execução total
end_time = time.time()
execution_time = end_time - start_time

# Exibir o resultado
print(f"Promo Revenue Percentage: {promo_percentage:.2f}%")

# Exibir o tempo de execução total
print(f"\nTempo de execução total: {execution_time:.2f} segundos")


