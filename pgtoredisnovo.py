import psycopg2
import redis

# Conectar ao PostgreSQL
pg_conn = psycopg2.connect(
    dbname="tpch",
    user="postgres",
    password="asd12345",  # Substitua pela senha correta
    host="localhost"
)

# Conectar ao Redis
r = redis.StrictRedis(host='localhost', port=6379, db=0)

# Função auxiliar para adicionar dados a Sorted Sets e Sets
def add_to_sorted_set(redis_conn, key, score, member):
    redis_conn.zadd(key, {member: float(score)})  # Converte o score para float

def add_to_set(redis_conn, key, member):
    redis_conn.sadd(key, member)

# Função para carregar a tabela PART
def load_part_table(pg_cursor, redis_conn):
    pg_cursor.execute("SELECT P_PARTKEY, P_NAME, P_MFGR, P_BRAND, P_TYPE, P_SIZE, P_CONTAINER, P_RETAILPRICE, P_COMMENT FROM tpch.PART")
    rows = pg_cursor.fetchall()
    for row in rows:
        partkey, name, mfgr, brand, part_type, size, container, retailprice, comment = row
        redis_conn.hset(f"part:{partkey}", mapping={
            "P_NAME": name,
            "P_MFGR": mfgr,
            "P_BRAND": brand,
            "P_TYPE": part_type,
            "P_SIZE": size,
            "P_CONTAINER": container,
            "P_RETAILPRICE": str(retailprice),  # Converte Decimal para string
            "P_COMMENT": comment
        })

# Função para carregar a tabela SUPPLIER
def load_supplier_table(pg_cursor, redis_conn):
    pg_cursor.execute("SELECT S_SUPPKEY, S_NAME, S_ADDRESS, S_NATIONKEY, S_PHONE, S_ACCTBAL, S_COMMENT FROM tpch.SUPPLIER")
    rows = pg_cursor.fetchall()
    for row in rows:
        suppkey, name, address, nationkey, phone, acctbal, comment = row
        redis_conn.hset(f"supplier:{suppkey}", mapping={
            "S_NAME": name,
            "S_ADDRESS": address,
            "S_NATIONKEY": nationkey,
            "S_PHONE": phone,
            "S_ACCTBAL": str(acctbal),  # Converte Decimal para string
            "S_COMMENT": comment
        })

# Função para carregar a tabela PARTSUPP
def load_partsupp_table(pg_cursor, redis_conn):
    pg_cursor.execute("SELECT PS_PARTKEY, PS_SUPPKEY, PS_AVAILQTY, PS_SUPPLYCOST, PS_COMMENT FROM tpch.PARTSUPP")
    rows = pg_cursor.fetchall()
    for row in rows:
        partkey, suppkey, availqty, supplycost, comment = row
        redis_conn.hset(f"partsupp:{partkey}:{suppkey}", mapping={
            "PS_PARTKEY": partkey,  # Adiciona o campo PS_PARTKEY
            "PS_SUPPKEY": suppkey,  # Adiciona o campo PS_SUPPKEY
            "PS_AVAILQTY": str(availqty),  # Converte Decimal para string, se necessário
            "PS_SUPPLYCOST": str(supplycost),  # Converte Decimal para string
            "PS_COMMENT": comment
        })
        # Adiciona ao Sorted Set para consultas por preço
        add_to_sorted_set(redis_conn, f"partsupp:sorted:{partkey}", supplycost, f"{partkey}:{suppkey}")
        

# Função para carregar a tabela CUSTOMER
def load_customer_table(pg_cursor, redis_conn):
    pg_cursor.execute("SELECT C_CUSTKEY, C_NAME, C_ADDRESS, C_NATIONKEY, C_PHONE, C_ACCTBAL, C_MKTSEGMENT, C_COMMENT FROM tpch.CUSTOMER")
    rows = pg_cursor.fetchall()
    for row in rows:
        custkey, name, address, nationkey, phone, acctbal, mktsegment, comment = row
        redis_conn.hset(f"customer:{custkey}", mapping={
            "C_NAME": name,
            "C_ADDRESS": address,
            "C_NATIONKEY": nationkey,
            "C_PHONE": phone,
            "C_ACCTBAL": str(acctbal),  # Converte Decimal para string
            "C_MKTSEGMENT": mktsegment,
            "C_COMMENT": comment
        })

# Função para carregar a tabela ORDERS e criar estruturas auxiliares
def load_orders_table(pg_cursor, redis_conn):
    pg_cursor.execute("SELECT O_ORDERKEY, O_CUSTKEY, O_ORDERSTATUS, O_TOTALPRICE, O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, O_SHIPPRIORITY, O_COMMENT FROM tpch.ORDERS")
    rows = pg_cursor.fetchall()
    for row in rows:
        orderkey, custkey, orderstatus, totalprice, orderdate, orderpriority, clerk, shippriority, comment = row
        redis_conn.hset(f"order:{orderkey}", mapping={
            "O_CUSTKEY": custkey,
            "O_ORDERSTATUS": orderstatus,
            "O_TOTALPRICE": str(totalprice),  # Converte Decimal para string
            "O_ORDERDATE": orderdate.isoformat(),  # Converte datetime para string
            "O_ORDERPRIORITY": orderpriority,
            "O_CLERK": clerk,
            "O_SHIPPRIORITY": shippriority,
            "O_COMMENT": comment
        })
        # Adiciona a um Sorted Set para ordenar por data
        add_to_sorted_set(redis_conn, f"orders:by_date", orderdate.timestamp(), f"{orderkey}")

        # Adiciona o pedido ao cliente correspondente
        add_to_set(redis_conn, f"customer:{custkey}:orders", f"{orderkey}")

# Função para carregar a tabela LINEITEM e criar estruturas auxiliares
def load_lineitem_table(pg_cursor, redis_conn):
    pg_cursor.execute("SELECT L_ORDERKEY, L_PARTKEY, L_SUPPKEY, L_LINENUMBER, L_QUANTITY, L_EXTENDEDPRICE, L_DISCOUNT, L_TAX, L_RETURNFLAG, L_LINESTATUS, L_SHIPDATE, L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT, L_SHIPMODE, L_COMMENT FROM tpch.LINEITEM")
    rows = pg_cursor.fetchall()
    for row in rows:
        orderkey, partkey, suppkey, linenumber, quantity, extendedprice, discount, tax, returnflag, linestatus, shipdate, commitdate, receiptdate, shipinstruct, shipmode, comment = row
        redis_conn.hset(f"lineitem:{orderkey}:{linenumber}", mapping={
            "L_PARTKEY": partkey,
            "L_SUPPKEY": suppkey,
            "L_QUANTITY": str(quantity),  # Converte Decimal para string
            "L_EXTENDEDPRICE": str(extendedprice),  # Converte Decimal para string
            "L_DISCOUNT": str(discount),  # Converte Decimal para string
            "L_TAX": str(tax),  # Converte Decimal para string
            "L_RETURNFLAG": returnflag,
            "L_LINESTATUS": linestatus,
            "L_SHIPDATE": shipdate.isoformat(),  # Converte datetime para string
            "L_COMMITDATE": commitdate.isoformat(),  # Converte datetime para string
            "L_RECEIPTDATE": receiptdate.isoformat(),  # Converte datetime para string
            "L_SHIPINSTRUCT": shipinstruct,
            "L_SHIPMODE": shipmode,
            "L_COMMENT": comment
        })
        # Adiciona ao Sorted Set para facilitar a consulta por data de envio
        add_to_sorted_set(redis_conn, f"lineitem:by_shipdate", shipdate.timestamp(), f"{orderkey}:{linenumber}")

# Função principal para carregar todas as tabelas
def main():
    pg_cursor = pg_conn.cursor()
    
    load_part_table(pg_cursor, r)
    load_supplier_table(pg_cursor, r)
    load_partsupp_table(pg_cursor, r)
    load_customer_table(pg_cursor, r)
    load_orders_table(pg_cursor, r)
    load_lineitem_table(pg_cursor, r)

    pg_cursor.close()
    pg_conn.close()

if __name__ == "__main__":
    main()



