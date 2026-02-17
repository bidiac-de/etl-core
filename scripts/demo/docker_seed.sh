#!/usr/bin/env bash
set -euo pipefail

POSTGRES_NAME="${POSTGRES_NAME:-demo-postgres}"
MARIADB_NAME="${MARIADB_NAME:-demo-mariadb}"
MONGO_NAME="${MONGO_NAME:-demo-mongo}"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker command not found." >&2
  exit 1
fi

docker exec -i "${POSTGRES_NAME}" psql -U postgres -d srcdb <<'SQL'
CREATE TABLE IF NOT EXISTS source_customers (
  id INT PRIMARY KEY,
  name TEXT,
  city TEXT,
  amount NUMERIC(10,2)
);
CREATE TABLE IF NOT EXISTS mongo_sales_agg (
  category TEXT,
  total_amount NUMERIC(12,2),
  row_count INT
);
CREATE TABLE IF NOT EXISTS story_orders (
  id INT PRIMARY KEY,
  customer TEXT,
  city TEXT,
  amount NUMERIC(10,2),
  channel TEXT
);
CREATE TABLE IF NOT EXISTS story_unified_book (
  id INT,
  customer TEXT,
  city TEXT,
  amount NUMERIC(10,2),
  channel TEXT
);
CREATE TABLE IF NOT EXISTS story_city_summary (
  city TEXT,
  total_amount NUMERIC(12,2),
  order_count INT
);
TRUNCATE source_customers;
TRUNCATE mongo_sales_agg;
TRUNCATE story_orders;
TRUNCATE story_unified_book;
TRUNCATE story_city_summary;
INSERT INTO source_customers (id, name, city, amount) VALUES
(1, 'Alice', 'Berlin', 120.50),
(2, 'Bob',   'Paris',   80.00),
(3, 'Cara',  'Rome',   300.00);
INSERT INTO story_orders (id, customer, city, amount, channel) VALUES
(101, 'Alice', 'Berlin',  95.00, 'web'),
(102, 'Bob',   'Berlin', 130.00, 'store'),
(103, 'Cara',  'Paris',  220.00, 'web'),
(104, 'Dora',  'Rome',   180.00, 'partner'),
(105, 'Eli',   'Paris',  260.00, 'store'),
(106, 'Faye',  'Rome',    75.00, 'web'),
(107, 'Gus',   'Berlin', 155.00, 'partner'),
(108, 'Hana',  'Paris',  310.00, 'web');
SQL

docker exec -i "${MARIADB_NAME}" mariadb -uroot -proot dstdb <<'SQL'
CREATE TABLE IF NOT EXISTS filtered_customers (
  id INT,
  name VARCHAR(255),
  city VARCHAR(255),
  amount DECIMAL(10,2)
);
CREATE TABLE IF NOT EXISTS story_vip_customers (
  id INT,
  customer VARCHAR(255),
  city VARCHAR(255),
  amount DECIMAL(10,2),
  channel VARCHAR(255)
);
TRUNCATE filtered_customers;
TRUNCATE story_vip_customers;
SQL

docker exec -i "${MONGO_NAME}" mongosh \
  --quiet \
  --username admin \
  --password admin \
  --authenticationDatabase admin <<'JS'
const dbConn = db.getSiblingDB("mongosrc");
dbConn.orders_src.drop();
dbConn.orders_src.insertMany([
  { customer: "Alice", category: "A", amount: 120.50 },
  { customer: "Bob", category: "B", amount: 80.00 },
  { customer: "Cara", category: "A", amount: 300.00 },
  { customer: "Dora", category: "B", amount: 150.00 },
  { customer: "Eli", category: "A", amount: 50.00 }
]);
JS

echo "Seeded demo data in PostgreSQL, MariaDB, and MongoDB."
