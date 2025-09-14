# ETL Core Engine

The ETL Core Engine is a modular Python-based tool that interprets JSON configurations to execute ETL (Extract, Transform, Load) workflows. Built on the **Command Pattern**, it allows for clear orchestration of ETL jobs with support for component-based execution, metrics tracking, and extensibility for various data backends and operations.

---

## Table of Contents

- [Overview](#overview)
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Development Setup](#development-setup)
- [Contributions](#contributions)
- [License](#license)

---

## Overview

The ETL Core Engine is responsible for interpreting a structured configuration (in JSON format) and executing the described job directly, without generating Python code.

Key features include:

- Command-based execution of ETL steps
- Support for various sources and sinks (CSV, SQL, etc.)
- Component-level metrics and structured job tracking
- Designed for integration with external UIs via JSON configuration

---

## Project Structure

- **src/etl_core**
  Contains the main interpreter logic, command implementations, component classes, and orchestration logic.

- **tests/**
  Unit and integration tests covering all key engine behaviors.

- **docs/**
  Documentation and configuration examples.

---

## Getting Started

### Prerequisites

- **Python 3.8+**
- Install all required dependencies via [`requirements.txt`](requirements.txt)

### Development Setup

1. **Clone the Repository:**

    ```bash
    git clone https://github.com/bidiac-de/etl-core.git
    cd etl-core
    ```

2. **Set Up Virtual Environment:**

    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```

3. **Install Dependencies:**

    ```bash
    pip install -r requirements.txt
    ```

4. **Install Python Module:**

    ```bash
    Pip install -e.
    ```

5. **Create .env**

    ```plaintext
    Create a `.env` file in the root, filling in values for the placeholder values found in:
    etl-core/.env_example
   ETL_COMPONENT_MODE excluding certain Components only used for testing
   EXECUTION_ENV setting a default Environment for resolving the Components Context configurations
    ```

### Starting the Core
- **Run the ETL Core Engine:**

    The ETL Core Engine can be run using Uvicorn, which serves the FastAPI application.

    ```bash
    uvicorn src.etl_core.main:app --reload
    ```

    This command starts the ETL Core Engine in development mode, allowing for hot-reloading of code changes.


## Contributions

Contributions are welcome! Please review our [Contributing Guidelines](CONTRIBUTING.md) and [Code of Conduct](CODE_OF_CONDUCT.md) before submitting pull requests. Your input is highly appreciated as we continuously work to enhance the ETL Core Engine.

---

## License

This project is licensed under the [AGPL](LICENSE).

---

Happy ETLing!
