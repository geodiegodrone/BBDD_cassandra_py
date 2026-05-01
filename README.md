# Cassandra Banking Model Practice

**Educational Cassandra/Python exercise for denormalized banking queries**

[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](requirements.txt)
[![Cassandra](https://img.shields.io/badge/database-Cassandra-purple)](actividad2_diegopulido.py)

This repository contains an academic Cassandra exercise that models banking entities and query-oriented tables.

## Important Note

The original script is interactive and expects a local Cassandra keyspace named `diegopulido`. It is preserved as coursework evidence. Future work should separate domain models, database access, and CLI interaction.

## Quickstart

```bash
python -m pip install -r requirements.txt
python -m py_compile actividad2_diegopulido.py
```

## Portfolio Upgrade Path

- Add Docker Compose with Cassandra.
- Add CQL schema files.
- Remove duplicated constructors.
- Add repository classes.
- Add tests with mocked Cassandra sessions.
