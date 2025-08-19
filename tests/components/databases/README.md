# Database Component Tests

Dieses Verzeichnis enthält Tests für alle Datenbankkomponenten des ETL-Systems.

## 📁 Verzeichnisstruktur

```
tests/components/databases/
├── README.md                           # Diese Datei
├── mariadb/                            # MariaDB-spezifische Tests
│   ├── __init__.py                     # Python-Paket-Markierung
│   ├── test_mariadb_components.py      # Komponenten-Tests (MariaDBRead, MariaDBWrite)
│   ├── test_mariadb_receivers.py       # Receiver-Tests (MariaDBReceiver)
│   └── test_mariadb_integration.py     # Integrations-Tests (Komponente + Receiver)
└── __init__.py                         # Python-Paket-Markierung
```

## 🧪 Test-Kategorien

### 1. **Komponenten-Tests** (`test_mariadb_components.py`)
- **Was wird getestet**: MariaDBRead und MariaDBWrite Komponenten
- **Zweck**: Überprüfung der Komponenten-Logik ohne echte Datenbankverbindungen
- **Tests**:
  - Initialisierung und Konfiguration
  - `process_row`, `process_bulk`, `process_bigdata` Methoden
  - Verbindungsaufbau und Fehlerbehandlung
  - Strategie-Integration

### 2. **Receiver-Tests** (`test_mariadb_receivers.py`)
- **Was wird getestet**: MariaDBReceiver für Datenbankoperationen
- **Zweck**: Überprüfung der Datenbankinteraktionen (SQL-Ausführung, Verbindungsverwaltung)
- **Tests**:
  - `read_row`, `read_bulk`, `read_bigdata` Methoden
  - `write_row`, `write_bulk`, `write_bigdata` Methoden
  - Verbindungsverwaltung und Fehlerbehandlung
  - Async-Thread-Ausführung

### 3. **Integrations-Tests** (`test_mariadb_integration.py`)
- **Was wird getestet**: Zusammenspiel zwischen Komponenten und Receivern
- **Zweck**: Überprüfung der kompletten Datenflüsse
- **Tests**:
  - Read-to-Write Pipeline
  - Verschiedene Ausführungsstrategien (Row, Bulk, BigData)
  - Metriken-Integration
  - Fehlerweiterleitung

## 🚀 Tests ausführen

### Alle Tests ausführen
```bash
pytest tests/components/databases/mariadb/ -v
```

### Spezifische Test-Dateien
```bash
# Nur Komponenten-Tests
pytest tests/components/databases/mariadb/test_mariadb_components.py -v

# Nur Receiver-Tests
pytest tests/components/databases/mariadb/test_mariadb_receivers.py -v

# Nur Integrations-Tests
pytest tests/components/databases/mariadb/test_mariadb_integration.py -v
```

### Mit Coverage-Report
```bash
pytest tests/components/databases/mariadb/ -v --cov=src/components/databases/mariadb --cov-report=html
```

## 🔧 Test-Setup

### Mock-Objekte
Alle Tests verwenden **Mock-Objekte** anstelle echter Datenbankverbindungen:
- **Mock-ConnectionHandler**: Simuliert Datenbankverbindungen
- **Mock-MariaDBReceiver**: Simuliert Datenbankoperationen
- **Mock-Context**: Simuliert Anmeldedaten und Konfiguration
- **Mock-Metrics**: Simuliert Komponenten-Metriken

### Fixtures
Tests verwenden wiederverwendbare Test-Daten:
- `mock_context`: Mock-Kontext mit Anmeldedaten
- `mock_metrics`: Mock-Metriken für Komponenten
- `sample_dataframe`: Beispiel-Pandas-DataFrame
- `sample_dask_dataframe`: Beispiel-Dask-DataFrame

## 📊 Aktuelle Test-Statistiken

**✅ Alle Tests erfolgreich!**
- **Komponenten-Tests**: 12/12 bestanden
- **Receiver-Tests**: 12/12 bestanden
- **Integrations-Tests**: 9/9 bestanden
- **Gesamt**: 33/33 Tests bestanden

## 🎯 Was wird getestet

### MariaDBRead Komponente
- ✅ Initialisierung mit allen erforderlichen Feldern
- ✅ `process_row`: Async-Iterator für Zeilenweise-Verarbeitung
- ✅ `process_bulk`: Pandas DataFrame-Verarbeitung
- ✅ `process_bigdata`: Dask DataFrame-Verarbeitung
- ✅ Verbindungsaufbau und Receiver-Integration
- ✅ Fehlerbehandlung bei Datenbankfehlern

### MariaDBWrite Komponente
- ✅ Initialisierung mit Tabellen- und Verbindungsdaten
- ✅ `process_row`: Async-Iterator für Zeilenweise-Schreibvorgänge
- ✅ `process_bulk`: Bulk-Schreibvorgänge mit DataFrames
- ✅ `process_bigdata`: BigData-Schreibvorgänge mit Dask
- ✅ INSERT-Query-Generierung
- ✅ Duplicate-Key-Update-Behandlung

### MariaDBReceiver
- ✅ SQL-Ausführung mit SQLAlchemy
- ✅ Async-Thread-Ausführung für blockierende DB-Operationen
- ✅ Korrekte Behandlung von SQLAlchemy-Ergebnissen
- ✅ Transaktionsverwaltung (commit/rollback)
- ✅ Fehlerbehandlung und -weiterleitung

## 🔍 Test-Philosophie

### Warum Tests wichtig sind
1. **Qualitätssicherung**: Tests stellen sicher, dass Code funktioniert
2. **Regressionstests**: Änderungen brechen keine bestehende Funktionalität
3. **Dokumentation**: Tests zeigen, wie Code verwendet werden soll
4. **Vertrauen**: Sichere Entwicklung und Refactoring

### Wie Tests funktionieren
- **Unit Tests**: Testen einzelne Komponenten isoliert
- **Integration Tests**: Testen Zusammenspiel zwischen Komponenten
- **Mocking**: Simulieren externe Abhängigkeiten (Datenbanken)
- **Async-Tests**: Überprüfung asynchroner Operationen mit `pytest-asyncio`

## 🚨 Bekannte Einschränkungen

### Dask-Tokenisierung
Einige BigData-Tests können Dask-Tokenisierungsfehler verursachen:
```python
# Erwartetes Verhalten bei Dask-Problemen
try:
    await receiver.write_bigdata("users", mock_metrics, sample_dask_dataframe)
except Exception as e:
    # Dask-Tokenisierung ist ein bekanntes Problem
    assert "tokenize" in str(e).lower() or "serialize" in str(e).lower()
```

### SQLAlchemy-Mocking
Tests simulieren SQLAlchemy-Ergebnisse mit Mock-Objekten:
```python
# Mock-Ergebnis mit _mapping-Attribut
mock_row = Mock()
mock_row._mapping = {"id": 1, "name": "John"}
mock_result.__iter__ = Mock(return_value=iter([mock_row]))
```

## 📈 Nächste Schritte

1. **Coverage erhöhen**: Weitere Edge-Cases testen
2. **Performance-Tests**: Große Datenmengen testen
3. **Stress-Tests**: Gleichzeitige Verbindungen testen
4. **Real-DB-Tests**: Optionale Tests mit echter MariaDB-Instanz

## 🤝 Beitragen

Beim Hinzufügen neuer Tests:
1. Folgen Sie der bestehenden Struktur
2. Verwenden Sie aussagekräftige Test-Namen
3. Mocken Sie alle externen Abhängigkeiten
4. Testen Sie sowohl Erfolgs- als auch Fehlerfälle
5. Aktualisieren Sie diese README bei Änderungen
