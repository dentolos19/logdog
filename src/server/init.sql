SELECT 'CREATE DATABASE main' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'main') \gexec
SELECT 'CREATE DATABASE swarm' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'swarm') \gexec
