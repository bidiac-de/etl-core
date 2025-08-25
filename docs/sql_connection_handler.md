# SQLConnectionHandler - Central Database Interface

## 🎯 **Overview**

The `SQLConnectionHandler` is a **central interface** (Abstraction Layer) for all SQL databases in the ETL-Core. It uses SQLAlchemy as a universal database engine and provides a unified API for PostgreSQL, MySQL, MariaDB and SQLite.

## 🏗️ **Architecture Principles**

### **1. Database-Agnostic Design**
- The handler doesn't know which specific database is behind it
- Unified API for all supported databases
- Automatic driver selection based on `db_type`

### **2. Layered Architecture**
```
┌─────────────────────────────────────┐
│        SQLConnectionHandler         │ ← Your Application
├─────────────────────────────────────┤
│         SQLAlchemy Engine           │ ← Mediator
├─────────────────────────────────────┤
│    Specific DB-Drivers              │ ← psycopg2, mysqlconnector, etc.
├─────────────────────────────────────┤
│        Native DB-Protocol           │ ← PostgreSQL, MySQL, etc.
└─────────────────────────────────────┘
```

### **3. Connection Pooling**
- Uses the `ConnectionPoolRegistry` for efficient connection management
- Automatic leasing and release of connections
- Thread-safe connection management

## 🔧 **Supported Databases and Drivers**

| Database | SQLAlchemy Dialect | Native Driver | Special Features |
|----------|-------------------|---------------|------------------|
| **PostgreSQL** | `postgresql+psycopg2` | psycopg2 | Bulk operations, COPY FROM |
| **MySQL** | `mysql+mysqlconnector` | mysql-connector-python | Replication, Clustering |
| **MariaDB** | `mysql+mysqlconnector` | mysql-connector-python | MySQL compatibility |
| **SQLite** | `sqlite` | built-in | File-based, single application |

## 📋 **Core Functionalities**

### **1. URL Building**
```python
@staticmethod
def build_url(
    *,
    db_type: str,
    user: Optional[str] = None,
    password: Optional[str] = None,
    host: Optional[str] = None,
    port: Optional[int] = None,
    database: Optional[str] = None,
) -> str
```

**Examples:**
```python
# PostgreSQL
url = SQLConnectionHandler.build_url(
    db_type="postgres",
    user="myuser",
    password="mypass",
    host="localhost",
    port=5432,
    database="mydb"
)
# Result: "postgresql+psycopg2://myuser:mypass@localhost:5432/mydb"

# SQLite
url = SQLConnectionHandler.build_url(
    db_type="sqlite",
    database="/path/to/database.db"
)
# Result: "sqlite:////path/to/database.db"
```

### **2. Connection Setup**
```python
def connect(
    self,
    *,
    url: str,
    engine_kwargs: Optional[Dict[str, Any]] = None
) -> Tuple[PoolKey, Engine]
```

**Example:**
```python
handler = SQLConnectionHandler()
key, engine = handler.connect(
    url="postgresql+psycopg2://user:pass@localhost:5432/db",
    engine_kwargs={"pool_size": 10, "max_overflow": 20}
)
```

### **3. Connection Leasing**
```python
@contextmanager
def lease(self) -> Generator[Connection, None, None]
```

**Example:**
```python
with handler.lease() as conn:
    result = conn.execute("SELECT * FROM users")
    users = result.fetchall()
```

### **4. Pool Management**
```python
def close_pool(self, *, force: bool = False) -> bool
def stats(self) -> dict
```

## 🚀 **Practical Usage**

### **Basic Usage**
```python
from src.etl_core.components.databases.sql_connection_handler import SQLConnectionHandler

# Initialize handler
handler = SQLConnectionHandler()

# Create connection URL
url = handler.build_url(
    db_type="postgres",
    user="etl_user",
    password="secure_password",
    host="db.example.com",
    port=5432,
    database="etl_database"
)

# Establish connection
key, engine = handler.connect(url=url)

# Database operations
with handler.lease() as conn:
    # Simple query
    result = conn.execute("SELECT COUNT(*) FROM users")
    count = result.fetchone()[0]

    # Parameterized query
    result = conn.execute(
        "SELECT * FROM users WHERE age > %s",
        (18,)
    )
    users = result.fetchall()

# Close pool (optional)
handler.close_pool()
```

### **Bulk Operations with PostgreSQL (psycopg2)**
```python
with handler.lease() as conn:
    # psycopg2-specific bulk operations
    from psycopg2.extras import RealDictCursor, execute_batch

    # Fast bulk inserts
    users_data = [
        ("Alice", "alice@example.com", 25),
        ("Bob", "bob@example.com", 30),
        ("Charlie", "charlie@example.com", 35)
    ]

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        execute_batch(
            cur,
            "INSERT INTO users (name, email, age) VALUES (%s, %s, %s)",
            users_data,
            page_size=1000
        )

    # COPY FROM for extremely fast bulk inserts
    with conn.cursor() as cur:
        cur.execute("COPY users (name, email, age) FROM STDIN")

        # Write data directly to COPY stream
        for user in users_data:
            cur.write_row(user)

        cur.close()
```

### **Error Handling**
```python
try:
    with handler.lease() as conn:
        result = conn.execute("SELECT * FROM non_existent_table")
except Exception as e:
    print(f"Database error: {e}")
    # Handler automatically cleans up
finally:
    # Show pool statistics
    stats = handler.stats()
    print(f"Active connections: {stats}")
```

## 🔒 **Security Aspects**

### **1. Credential Management**
- Passwords are not stored in code
- Use of `SecureContextAdapter` for secure parameter management
- Support for environment variables and secret stores

### **2. Connection Pooling**
- Automatic connection management
- Prevents connection leaks
- Thread-safe implementation

### **3. SQL Injection Protection**
```python
# ✅ Secure - Parameterized queries
with handler.lease() as conn:
    result = conn.execute(
        "SELECT * FROM users WHERE name = %s AND age > %s",
        (user_name, min_age)
    )

# ❌ Insecure - String formatting
query = f"SELECT * FROM users WHERE name = '{user_name}'"
```

## 📊 **Performance Optimizations**

### **1. Connection Pooling**
```python
# Optimized pool settings
engine_kwargs = {
    "pool_size": 20,           # Base pool size
    "max_overflow": 30,        # Maximum additional connections
    "pool_timeout": 30,        # Timeout for pool connections
    "pool_recycle": 3600,      # Renew connections after 1 hour
    "pool_pre_ping": True      # Test connections before use
}

key, engine = handler.connect(url=url, engine_kwargs=engine_kwargs)
```

### **2. Bulk Operations**
```python
# For large data volumes
with handler.lease() as conn:
    # Transaction for bulk inserts
    with conn.begin():
        for batch in data_batches:
            conn.execute(
                "INSERT INTO large_table (col1, col2) VALUES (%s, %s)",
                batch
            )
```

### **3. Query Optimization**
```python
with handler.lease() as conn:
    # Explicit transactions for complex operations
    with conn.begin():
        # Multiple operations in one transaction
        conn.execute("UPDATE table1 SET status = 'processing'")
        conn.execute("INSERT INTO table2 SELECT * FROM table1 WHERE status = 'processing'")
        conn.execute("UPDATE table1 SET status = 'completed'")
```

## 🔄 **Backward Compatibility**

### **1. SQLAlchemy Versions**
- **SQLAlchemy 1.4+**: Fully supported
- **SQLAlchemy 2.0**: Compatible with legacy syntax
- **Future versions**: Automatic compatibility through SQLAlchemy

### **2. Database Versions**
- **PostgreSQL**: 9.6+ (through psycopg2)
- **MySQL**: 5.7+ (through mysql-connector-python)
- **MariaDB**: 10.2+ (through mysql-connector-python)
- **SQLite**: 3.0+ (built-in)

### **3. Framework Integration**
```python
# Works with various ORMs
from sqlalchemy.orm import sessionmaker

# Create session
Session = sessionmaker(bind=engine)
session = Session()

# ORM operations
users = session.query(User).filter(User.age > 18).all()
```

## 🧪 **Testing and Debugging**

### **1. Pool Statistics**
```python
# Show current pool status
stats = handler.stats()
print(f"SQL Pools: {stats['sql']}")
print(f"MongoDB Pools: {stats['mongo']}")
```

### **2. Connection Monitoring**
```python
# Monitor connection status
with handler.lease() as conn:
    # PostgreSQL-specific information
    if hasattr(conn, 'info'):
        print(f"Server Version: {conn.info.server_version}")
        print(f"Protocol Version: {conn.info.protocol_version}")
```

### **3. Error Handling**
```python
try:
    with handler.lease() as conn:
        conn.execute("SELECT * FROM table")
except Exception as e:
    # Specific error handling
    if "relation" in str(e).lower():
        print("Table does not exist")
    elif "connection" in str(e).lower():
        print("Connection error")
    else:
        print(f"Unknown error: {e}")
```

## 📝 **Best Practices**

### **1. Resource Management**
```python
# ✅ Correct - Use context manager
with handler.lease() as conn:
    result = conn.execute("SELECT * FROM table")
    data = result.fetchall()

# ❌ Wrong - Manual connection management
conn = handler._engine.connect()
try:
    result = conn.execute("SELECT * FROM table")
finally:
    conn.close()
```

### **2. Transaction Management**
```python
with handler.lease() as conn:
    # Explicit transactions for critical operations
    with conn.begin():
        conn.execute("UPDATE accounts SET balance = balance - %s", (amount,))
        conn.execute("UPDATE accounts SET balance = balance + %s", (amount,))
```

### **3. Connection Pooling**
```python
# Adapt pool settings to application
if is_production:
    engine_kwargs = {"pool_size": 50, "max_overflow": 100}
else:
    engine_kwargs = {"pool_size": 5, "max_overflow": 10}

key, engine = handler.connect(url=url, engine_kwargs=engine_kwargs)
```

## 🔮 **Future Extensions**

### **1. New Database Support**
- **Oracle**: `oracle+cx_oracle`
- **SQL Server**: `mssql+pyodbc`
- **DB2**: `ibm_db_sa`

### **2. Enhanced Features**
- Automatic failover
- Read-replica support
- Connection health checks
- Metrics and monitoring

### **3. Cloud Integration**
- AWS RDS Support
- Azure SQL Database
- Google Cloud SQL
- Kubernetes-native connection management

## 📚 **Additional Resources**

- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
- [psycopg2 Documentation](https://www.psycopg.org/docs/)
- [MySQL Connector/Python](https://dev.mysql.com/doc/connector-python/en/)
- [Connection Pooling Best Practices](https://docs.sqlalchemy.org/en/14/core/pooling.html)

---

**Note**: The `SQLConnectionHandler` is designed so that you **don't need to worry** about specific database drivers. Simply choose the `db_type` and the rest is handled automatically! 🎯
