#!/bin/bash
psql -U postgres -d postgres -c "CREATE DATABASE wtc_prod;";
psql -U postgres -d wtc_prod -c "CREATE SCHEMA crm_system;";
psql -U postgres -d wtc_prod -c "CREATE TABLE IF NOT EXISTS crm_system.accounts (account_id INTEGER PRIMARY KEY, owner_name VARCHAR(100), email VARCHAR(100), phone_number VARCHAR(100), modified_ts TIMESTAMP);";
psql -U postgres -d wtc_prod -c "CREATE TABLE IF NOT EXISTS crm_system.addresses (account_id INTEGER PRIMARY KEY, street_address VARCHAR(100), city VARCHAR(100), state VARCHAR(100), postal_code VARCHAR(100), country VARCHAR(100), modified_ts TIMESTAMP );";
psql -U postgres -d wtc_prod -c "CREATE TABLE IF NOT EXISTS crm_system.devices ( device_id INTEGER PRIMARY KEY, account_id INTEGER, device_name VARCHAR(100), device_type VARCHAR(100), device_os VARCHAR(100), modified_ts TIMESTAMP );";
psql -U postgres -d postgres -c "CREATE DATABASE wtc_analytics;";
psql -U postgres -d postgres -c "CREATE DATABASE airflow;";
psql -U postgres -d wtc_analytics -c "CREATE SCHEMA cdr_data;";
psql -U postgres -d wtc_analytics -c "CREATE SCHEMA crm_data;";
psql -U postgres -d wtc_analytics -c "CREATE SCHEMA forex_data;";
psql -U postgres -d wtc_analytics -c "CREATE SCHEMA prepared_layers;";
psql -U postgres -d postgres -c "CREATE SCHEMA airflow;";
psql -U postgres -d wtc_analytics -c " CREATE TABLE IF NOT EXISTS cdr_data.analytics_data (msisdn VARCHAR(15) NOT NULL,tower_id INTEGER NOT NULL,call_type VARCHAR(20) NOT NULL,dest_nr VARCHAR(15) NOT NULL,call_duration_sec INTEGER NOT NULL,start_time TIMESTAMP NOT NULL);";
psql -U postgres -d wtc_analytics -c "CREATE TABLE IF NOT EXISTS forex_data.analytics_data (timestamp TIMESTAMP NOT NULL,pair_name VARCHAR(50) NOT NULL,bid_price NUMERIC(10, 5) NOT NULL,ask_price NUMERIC(10, 5) NOT NULL,spread NUMERIC(10, 5) NOT NULL);";