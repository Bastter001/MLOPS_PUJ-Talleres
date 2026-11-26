\set ON_ERROR_STOP on

-- Crear el rol propietario sin acceso directo.

SELECT 'CREATE ROLE proyecto3_owner NOLOGIN'
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles
    WHERE rolname = 'proyecto3_owner'
)
\gexec

-- Crear los usuarios de servicio.

SELECT 'CREATE ROLE proyecto3_training LOGIN'
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles
    WHERE rolname = 'proyecto3_training'
)
\gexec

SELECT 'CREATE ROLE proyecto3_inference LOGIN'
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles
    WHERE rolname = 'proyecto3_inference'
)
\gexec

SELECT 'CREATE ROLE proyecto3_reader LOGIN'
WHERE NOT EXISTS (
    SELECT 1 FROM pg_roles
    WHERE rolname = 'proyecto3_reader'
)
\gexec

-- Obtener las contraseñas desde variables de entorno.

\getenv training_password TRAINING_PASSWORD
\getenv inference_password INFERENCE_PASSWORD
\getenv reader_password READER_PASSWORD

ALTER ROLE proyecto3_training
    PASSWORD :'training_password';

ALTER ROLE proyecto3_inference
    PASSWORD :'inference_password';

ALTER ROLE proyecto3_reader
    PASSWORD :'reader_password';

-- Crear la base únicamente si no existe.

SELECT 'CREATE DATABASE proyecto3_data
        OWNER proyecto3_owner'
WHERE NOT EXISTS (
    SELECT 1 FROM pg_database
    WHERE datname = 'proyecto3_data'
)
\gexec

-- Restringir el acceso a la nueva base de datos.

REVOKE ALL ON DATABASE proyecto3_data
FROM PUBLIC;

GRANT CONNECT ON DATABASE proyecto3_data
TO proyecto3_training,
   proyecto3_inference,
   proyecto3_reader;

