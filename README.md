# Bitemporal HR Demo

A demonstration HR web application showcasing bitemporal data storage patterns using Go and SQLite.

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

2. Generate database code:
   ```bash
   sqlc generate
   ```

3. Run the application:
   ```bash
   go run cmd/main.go
   ```

## Technology Stack

- **Go 1.24**: Core application language
- **SQLite**: Lightweight database for demo purposes
- **sqlc**: Type-safe SQL code generation
- **Bitemporal patterns**: For historical data tracking