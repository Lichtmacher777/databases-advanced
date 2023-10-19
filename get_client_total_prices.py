import psycopg2

# Database connection parameters for default PostgreSQL installation
default_db_connection_params = {
    'database': 'postgres',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5432,
}

# Connect to the default PostgreSQL database
default_conn = psycopg2.connect(**default_db_connection_params)
default_conn.autocommit = True

# Create the 'my_database' database
with default_conn.cursor() as default_cur:
    default_cur.execute("CREATE DATABASE my_database")

# Close the connection to the default database
default_conn.close()

# Database connection parameters for the created database
db_connection_params = {
    'database': 'my_database',
    'user': 'postgres',
    'password': 'postgres',
    'host': 'localhost',
    'port': 5432,
}

# Connect to the 'my_database' database
conn = psycopg2.connect(**db_connection_params)

# Create a cursor object
cur = conn.cursor()

# Create the 'clients' table
cur.execute('''
    CREATE TABLE clients (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255) NOT NULL
    );
''')

# Create the 'products' table
cur.execute('''
    CREATE TABLE products (
        id SERIAL PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        price DECIMAL NOT NULL
    );
''')

# Create the 'orders' table
cur.execute('''
    CREATE TABLE orders (
        id SERIAL PRIMARY KEY,
        client_id INT NOT NULL REFERENCES clients(id),
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
''')

# Create the 'order_items' table
cur.execute('''
    CREATE TABLE order_items (
        id SERIAL PRIMARY KEY,
        order_id INT NOT NULL REFERENCES orders(id),
        product_id INT NOT NULL REFERENCES products(id)
    );
''')

# Insert sample data into the 'clients' table
cur.execute('''
    INSERT INTO clients (name, email) VALUES
    ('Client1', 'client1@example.com'),
    ('Client2', 'client2@example.com');
''')

# Insert sample data into the 'products' table
cur.execute('''
    INSERT INTO products (name, price) VALUES
    ('Product1', 10.99),
    ('Product2', 20.99);
''')

# Insert sample data into the 'orders' table
cur.execute('''
    INSERT INTO orders (client_id) VALUES
    (1),
    (2);
''')

# Insert sample data into the 'order_items' table
cur.execute('''
    INSERT INTO order_items (order_id, product_id) VALUES
    (1, 1),
    (1, 2),
    (2, 1);
''')

# Commit the changes
conn.commit()

# Function to execute a SQL query and print results
def execute_and_print_query(sql_query, message):
    try:
        # Execute the query
        cur.execute(sql_query)

        # Fetch the results
        results = cur.fetchall()

        # Print the results to the console
        print(message)
        for row in results:
            print('Client name:', row[0])
            print('Total price:', row[1])
            print('---')

    except Exception as e:
        print("Error executing SQL:", e)

# INNER JOIN
sql_inner_join = """
SELECT clients.name, SUM(products.price) AS total_price
FROM clients
INNER JOIN orders ON clients.id = orders.client_id
INNER JOIN order_items ON orders.id = order_items.order_id
INNER JOIN products ON order_items.product_id = products.id
GROUP BY clients.name;
"""

# LEFT JOIN
sql_left_join = """
SELECT clients.name, SUM(products.price) AS total_price
FROM clients
LEFT JOIN orders ON clients.id = orders.client_id
LEFT JOIN order_items ON orders.id = order_items.order_id
LEFT JOIN products ON order_items.product_id = products.id
GROUP BY clients.name;
"""

# FULL OUTER JOIN
sql_full_outer_join = """
SELECT clients.name, SUM(products.price) AS total_price
FROM clients
FULL OUTER JOIN orders ON clients.id = orders.client_id
FULL OUTER JOIN order_items ON orders.id = order_items.order_id
FULL OUTER JOIN products ON order_items.product_id = products.id
GROUP BY clients.name;
"""

# Display clients with the sum of prices
sql_display_clients_total_prices = """
SELECT clients.name, COALESCE(SUM(products.price), 0) AS total_price
FROM clients
LEFT JOIN orders ON clients.id = orders.client_id
LEFT JOIN order_items ON orders.id = order_items.order_id
LEFT JOIN products ON order_items.product_id = products.id
GROUP BY clients.name;
"""

# Execute and print results for each query
execute_and_print_query(sql_inner_join, "Results for INNER JOIN:")
execute_and_print_query(sql_left_join, "Results for LEFT JOIN:")
execute_and_print_query(sql_full_outer_join, "Results for FULL OUTER JOIN:")
execute_and_print_query
'''
Results for INNER JOIN:
Client name: Client2
Total price: 10.99
---
Client name: Client1
Total price: 31.98
---
Results for LEFT JOIN:
Client name: Client2
Total price: 10.99
---
Client name: Client1
Total price: 31.98
---
Results for FULL OUTER JOIN:
Client name: Client2
Total price: 10.99
---
Client name: Client1
Total price: 31.98
---
'''