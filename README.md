# Bitemporal HR Demo

A demonstration HR web application showcasing bitemporal data storage patterns using Go and SQLite.

This library provides an easy-to-use bitemporal data system built on ANSI SQL, designed for compatibility across
SQL-based relational database management systems.
The goal is to abstract away the complexities of bitemporal queries, allowing developers to work with temporal data as
naturally as they would with traditional tables.

## Overview

This application demonstrates how to implement bitemporal data models for HR systems, allowing you to track both:

- **Valid Time**: When the data was actually true in the real world
- **Transaction Time**: When the data was recorded in the database

This enables powerful capabilities like:

- Historical reporting as of any point in time
- Audit trails showing what was known when
- Correction of historical data without losing the audit trail
- Time travel queries to see the state of employee data at any past moment

## Features

- Employee management with bitemporal tracking
- SQLite database with sqlc for type-safe queries
- Temporal queries for historical data access
- Clean separation of valid time and transaction time

## Database Schema

The database is derived from https://github.com/datacharmer/test_db

The `employees` table includes:

- Employee personal information (first_name, last_name, birth_date, gender)
- Employment data (hire_date)
- Bitemporal columns (valid_from, valid_to, transaction_time)

## Getting Started

1. Install dependencies:
   ```bash
   go mod tidy
   ```

2. Download the database zip and unzip it into the project
   directory https://github.com/datacharmer/test_db/archive/refs/tags/v1.0.7.zip

3. Generate database code:
   ```bash
   go run cmd/import/main.go
   ```

4. Run the tests:
   ```bash
   go test ./...
   ```

## Technology Stack

- **Go 1.24**: Core application language
- **SQLite**: Lightweight database for demo purposes
- **Bitemporal patterns**: For historical data tracking

## State

- Very early, most work being done in tests at the moment.