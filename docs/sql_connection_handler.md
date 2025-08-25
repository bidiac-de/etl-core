# SQLConnectionHandler - Zentrale Datenbank-Schnittstelle

## 🎯 **Überblick**

Der `SQLConnectionHandler` ist eine **zentrale Schnittstelle** (Abstraction Layer) für alle SQL-Datenbanken in der ETL-Core. Er nutzt SQLAlchemy als universelle Datenbank-Engine und bietet eine einheitliche API für PostgreSQL, MySQL, MariaDB und SQLite.

## 🏗️ **Architektur-Prinzipien**

### **1. Database-Agnostic Design**
- Der Handler weiß nicht, welche spezifische Datenbank dahinter liegt
- Einheitliche API für alle unterstützten Datenbanken
- Automatische Driver-Auswahl basierend auf dem `db_type`

### **2. Layered Architecture**
```
┌─────────────────────────────────────┐
│        SQLConnectionHandler         │ ← Deine Anwendung
├─────────────────────────────────────┤
│         SQLAlchemy Engine           │ ← Vermittler
├─────────────────────────────────────┤
│    Spezifische DB-Drivers          │ ← psycopg2, mysqlconnector, etc.
├─────────────────────────────────────┤
│        Native DB-Protokoll         │ ← PostgreSQL, MySQL, etc.
└─────────────────────────────────────┘
```

### **3. Connection Pooling**
- Nutzt den `ConnectionPoolRegistry` für effiziente Verbindungsverwaltung
- Automatisches Leasing und Freigabe von Verbindungen
- Thread-sichere Verbindungsverwaltung

## 🔧 **Unterstützte Datenbanken und Drivers**

| Datenbank | SQLAlchemy Dialekt | Native Driver | Besonderheiten |
|-----------|-------------------|---------------|----------------|
| **PostgreSQL** | `postgresql+psycopg2` | psycopg2 | Bulk-Operationen, COPY FROM |
| **MySQL** | `mysql+mysqlconnector` | mysql-connector-python | Replikation, Clustering |
| **MariaDB** | `mysql+mysqlconnector` | mysql-connector-python | Kompatibilität mit MySQL |
| **SQLite** | `sqlite` | built-in | Datei-basiert, Einzelanwendung |

## 📋 **Kern-Funktionalitäten**

### **1. URL-Building**
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

**Beispiele:**
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
# Ergebnis: "postgresql+psycopg2://myuser:mypass@localhost:5432/mydb"

# SQLite
url = SQLConnectionHandler.build_url(
    db_type="sqlite",
    database="/path/to/database.db"
)
# Ergebnis: "sqlite:////path/to/database.db"
```

### **2. Verbindungsaufbau**
```python
def connect(
    self, 
    *, 
    url: str, 
    engine_kwargs: Optional[Dict[str, Any]] = None
) -> Tuple[PoolKey, Engine]
```

**Beispiel:**
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

**Beispiel:**
```python
with handler.lease() as conn:
    result = conn.execute("SELECT * FROM users")
    users = result.fetchall()
```

### **4. Pool-Verwaltung**
```python
def close_pool(self, *, force: bool = False) -> bool
def stats(self) -> dict
```

## 🚀 **Praktische Nutzung**

### **Grundlegende Verwendung**
```python
from src.etl_core.components.databases.sql_connection_handler import SQLConnectionHandler

# Handler initialisieren
handler = SQLConnectionHandler()

# Verbindungs-URL erstellen
url = handler.build_url(
    db_type="postgres",
    user="etl_user",
    password="secure_password",
    host="db.example.com",
    port=5432,
    database="etl_database"
)

# Verbindung aufbauen
key, engine = handler.connect(url=url)

# Datenbank-Operationen
with handler.lease() as conn:
    # Einfache Abfrage
    result = conn.execute("SELECT COUNT(*) FROM users")
    count = result.fetchone()[0]
    
    # Parameterisierte Abfrage
    result = conn.execute(
        "SELECT * FROM users WHERE age > %s", 
        (18,)
    )
    users = result.fetchall()

# Pool schließen (optional)
handler.close_pool()
```

### **Bulk-Operationen mit PostgreSQL (psycopg2)**
```python
with handler.lease() as conn:
    # psycopg2-spezifische Bulk-Operationen
    from psycopg2.extras import RealDictCursor, execute_batch
    
    # Schnelle Bulk-Inserts
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
    
    # COPY FROM für extrem schnelle Bulk-Inserts
    with conn.cursor() as cur:
        cur.execute("COPY users (name, email, age) FROM STDIN")
        
        # Daten direkt in den COPY-Stream schreiben
        for user in users_data:
            cur.write_row(user)
        
        cur.close()
```

### **Fehlerbehandlung**
```python
try:
    with handler.lease() as conn:
        result = conn.execute("SELECT * FROM non_existent_table")
except Exception as e:
    print(f"Datenbankfehler: {e}")
    # Handler automatisch aufräumen
finally:
    # Pool-Statistiken anzeigen
    stats = handler.stats()
    print(f"Aktive Verbindungen: {stats}")
```

## 🔒 **Sicherheitsaspekte**

### **1. Credential Management**
- Passwörter werden nicht im Code gespeichert
- Nutzung des `SecureContextAdapter` für sichere Parameter-Verwaltung
- Unterstützung für Umgebungsvariablen und Secret Stores

### **2. Connection Pooling**
- Automatische Verbindungsverwaltung
- Verhindert Connection Leaks
- Thread-sichere Implementierung

### **3. SQL Injection Protection**
```python
# ✅ Sicher - Parameterisierte Abfragen
with handler.lease() as conn:
    result = conn.execute(
        "SELECT * FROM users WHERE name = %s AND age > %s",
        (user_name, min_age)
    )

# ❌ Unsicher - String-Formatting
query = f"SELECT * FROM users WHERE name = '{user_name}'"
```

## 📊 **Performance-Optimierungen**

### **1. Connection Pooling**
```python
# Optimierte Pool-Einstellungen
engine_kwargs = {
    "pool_size": 20,           # Basis-Pool-Größe
    "max_overflow": 30,        # Maximale zusätzliche Verbindungen
    "pool_timeout": 30,        # Timeout für Pool-Verbindungen
    "pool_recycle": 3600,      # Verbindungen nach 1 Stunde erneuern
    "pool_pre_ping": True      # Verbindungen vor Nutzung testen
}

key, engine = handler.connect(url=url, engine_kwargs=engine_kwargs)
```

### **2. Bulk-Operationen**
```python
# Für große Datenmengen
with handler.lease() as conn:
    # Transaktion für Bulk-Inserts
    with conn.begin():
        for batch in data_batches:
            conn.execute(
                "INSERT INTO large_table (col1, col2) VALUES (%s, %s)",
                batch
            )
```

### **3. Query-Optimierung**
```python
with handler.lease() as conn:
    # Explizite Transaktionen für komplexe Operationen
    with conn.begin():
        # Mehrere Operationen in einer Transaktion
        conn.execute("UPDATE table1 SET status = 'processing'")
        conn.execute("INSERT INTO table2 SELECT * FROM table1 WHERE status = 'processing'")
        conn.execute("UPDATE table1 SET status = 'completed'")
```

## 🔄 **Abwärtskompatibilität**

### **1. SQLAlchemy Versionen**
- **SQLAlchemy 1.4+**: Vollständig unterstützt
- **SQLAlchemy 2.0**: Kompatibel mit Legacy-Syntax
- **Zukünftige Versionen**: Automatische Kompatibilität durch SQLAlchemy

### **2. Datenbank-Versionen**
- **PostgreSQL**: 9.6+ (durch psycopg2)
- **MySQL**: 5.7+ (durch mysql-connector-python)
- **MariaDB**: 10.2+ (durch mysql-connector-python)
- **SQLite**: 3.0+ (built-in)

### **3. Framework-Integration**
```python
# Funktioniert mit verschiedenen ORMs
from sqlalchemy.orm import sessionmaker

# Session erstellen
Session = sessionmaker(bind=engine)
session = Session()

# ORM-Operationen
users = session.query(User).filter(User.age > 18).all()
```

## 🧪 **Testing und Debugging**

### **1. Pool-Statistiken**
```python
# Aktuelle Pool-Status anzeigen
stats = handler.stats()
print(f"SQL Pools: {stats['sql']}")
print(f"MongoDB Pools: {stats['mongo']}")
```

### **2. Connection-Monitoring**
```python
# Verbindungsstatus überwachen
with handler.lease() as conn:
    # PostgreSQL-spezifische Informationen
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
    # Spezifische Fehlerbehandlung
    if "relation" in str(e).lower():
        print("Tabelle existiert nicht")
    elif "connection" in str(e).lower():
        print("Verbindungsfehler")
    else:
        print(f"Unbekannter Fehler: {e}")
```

## 📝 **Best Practices**

### **1. Resource Management**
```python
# ✅ Korrekt - Context Manager nutzen
with handler.lease() as conn:
    result = conn.execute("SELECT * FROM table")
    data = result.fetchall()

# ❌ Falsch - Manuelle Verbindungsverwaltung
conn = handler._engine.connect()
try:
    result = conn.execute("SELECT * FROM table")
finally:
    conn.close()
```

### **2. Transaktionsmanagement**
```python
with handler.lease() as conn:
    # Explizite Transaktionen für kritische Operationen
    with conn.begin():
        conn.execute("UPDATE accounts SET balance = balance - %s", (amount,))
        conn.execute("UPDATE accounts SET balance = balance + %s", (amount,))
```

### **3. Connection Pooling**
```python
# Pool-Einstellungen an die Anwendung anpassen
if is_production:
    engine_kwargs = {"pool_size": 50, "max_overflow": 100}
else:
    engine_kwargs = {"pool_size": 5, "max_overflow": 10}

key, engine = handler.connect(url=url, engine_kwargs=engine_kwargs)
```

## 🔮 **Zukünftige Erweiterungen**

### **1. Neue Datenbank-Support**
- **Oracle**: `oracle+cx_oracle`
- **SQL Server**: `mssql+pyodbc`
- **DB2**: `ibm_db_sa`

### **2. Erweiterte Features**
- Automatische Failover
- Read-Replica-Support
- Connection-Health-Checks
- Metriken und Monitoring

### **3. Cloud-Integration**
- AWS RDS Support
- Azure SQL Database
- Google Cloud SQL
- Kubernetes-native Verbindungsverwaltung

## 📚 **Weitere Ressourcen**

- [SQLAlchemy Documentation](https://docs.sqlalchemy.org/)
- [psycopg2 Documentation](https://www.psycopg.org/docs/)
- [MySQL Connector/Python](https://dev.mysql.com/doc/connector-python/en/)
- [Connection Pooling Best Practices](https://docs.sqlalchemy.org/en/14/core/pooling.html)

---

**Hinweis**: Der `SQLConnectionHandler` ist so konzipiert, dass du dir **keine Gedanken** über spezifische Datenbank-Drivers machen musst. Wähle einfach den `db_type` und der Rest wird automatisch gehandhabt! 🎯
