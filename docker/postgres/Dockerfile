FROM postgres:15

# Set environment variables
ENV POSTGRES_USER=iot_user
ENV POSTGRES_PASSWORD=iot_password
ENV POSTGRES_DB=postgres

# Copy initialization script
COPY ./init/01-init.sql /docker-entrypoint-initdb.d/