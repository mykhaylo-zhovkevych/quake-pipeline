FROM postgres:latest
LABEL authors="mykhaylo zhovkevych"

ENV POSTGRES_DB=events \
    POSTGRES_USER=app \
    POSTGRES_PASSWORD=app

COPY init/ /docker-entrypoint-initdb.d/

EXPOSE 5432

HEALTHCHECK --interval=5s --timeout=5s --retries=10 \
    CMD pg_isready -U "$POSTGRES_USER" -d "$POSTGRES_DB" || exit 1
