# Snowflake Analytics Engineering Validation Toolkit

## Purpose
This repository contains a set of Python scripts designed to perform **data validation, monitoring, and operational checks** in Snowflake. It demonstrates **Analytics Engineering best practices** including:

- Data quality checks (duplicates, nulls, referential integrity)
- Pipeline validation (row counts, outlier detection)
- Task monitoring (detect failed Snowflake tasks)
- Reusable, modular Python code for production use

---

## Structure
snowflake_validation_toolkit/
├── config.py # Snowflake connection setup
├── validation.py # Core validation functions
├── run_validations.py# Script to run all checks
├── logs/ # Optional logs
└── README.md

