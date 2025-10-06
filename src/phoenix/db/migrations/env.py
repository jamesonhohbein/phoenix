import asyncio
import os

from alembic import context
from sqlalchemy import Connection, engine_from_config, pool
from sqlalchemy.ext.asyncio import AsyncEngine,create_async_engine
from sqlalchemy import make_url

from phoenix.config import get_env_database_connection_str, get_env_postgres_auth_mode
from phoenix.db.engines import get_async_db_url
from phoenix.db.models import Base
from phoenix.settings import Settings
from phoenix.db.pg_config import get_pg_config
from phoenix.db import pg_token as pg_token_mod

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# add your model's MetaData object here
# for 'autogenerate' support
target_metadata = Base.metadata

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        transaction_per_migration=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    connectable = context.config.attributes.get("connection", None)
    if connectable is None:
        config = context.config.get_section(context.config.config_ini_section) or {}
        if "sqlalchemy.url" not in config:
            connection_str = get_env_database_connection_str()
            async_url = get_async_db_url(connection_str)
            # If using Postgres, optionally build an Azure token-aware async engine
            if async_url.drivername.startswith("postgresql+"):
                asyncpg_url, asyncpg_args = get_pg_config(make_url(connection_str), "asyncpg")

                mode = get_env_postgres_auth_mode()
                if mode == "azure":
                    async def _asyncpg_creator():
                        import asyncpg  # type: ignore

                        user = asyncpg_url.username or asyncpg_url.query.get("user")
                        # Inject Azure token as password
                        password = pg_token_mod.get_token_value()

                        connect_kwargs = {}
                        if ssl := asyncpg_args.get("ssl"):
                            connect_kwargs["ssl"] = ssl
                        return await asyncpg.connect(
                            host=asyncpg_url.host,
                            port=asyncpg_url.port,
                            database=asyncpg_url.database,
                            user=user,
                            password=password,
                            **connect_kwargs,
                        )

                    connectable = create_async_engine(
                        url=asyncpg_url,
                        future=True,
                        echo=Settings.log_migrations,
                        poolclass=pool.NullPool,
                        async_creator=_asyncpg_creator,
                    )
                else:
                    # Non-Azure: preserve original connect_args-based construction
                    connectable = create_async_engine(
                        url=asyncpg_url,
                        future=True,
                        echo=Settings.log_migrations,
                        poolclass=pool.NullPool,
                        connect_args=asyncpg_args,
                    )
            else:
                # Fallback to engine_from_config for non-Postgres
                config["sqlalchemy.url"] = async_url.render_as_string(hide_password=False)
                connectable = AsyncEngine(
                    engine_from_config(
                        config,
                        prefix="sqlalchemy.",
                        poolclass=pool.NullPool,
                        future=True,
                        echo=Settings.log_migrations,
                    )
                )
        else:
            connectable = AsyncEngine(
                engine_from_config(
                    config,
                    prefix="sqlalchemy.",
                    poolclass=pool.NullPool,
                    future=True,
                    echo=Settings.log_migrations,
                )
            )

    if isinstance(connectable, AsyncEngine):
        try:
            asyncio.get_running_loop()
        except RuntimeError:
            asyncio.run(run_async_migrations(connectable))
        else:
            asyncio.create_task(run_async_migrations(connectable))
    else:
        run_migrations(connectable)


async def run_async_migrations(connectable: AsyncEngine) -> None:
    async with connectable.connect() as connection:
        await connection.run_sync(run_migrations)


def run_migrations(connection: Connection) -> None:
    transaction = connection.begin()
    try:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            transactional_ddl=True,
            transaction_per_migration=True,
        )
        context.run_migrations()
        transaction.commit()
    except Exception:
        transaction.rollback()
        raise
    finally:
        connection.close()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
