import logging
import os
import shutil
import subprocess
import psycopg2

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
PG_CONFIG = {
    "host": "127.0.0.1",
    "port": 5433,
    "database": "feature_store",
    "user": "postgres",
    "password": "postgres"
}

USE_DOCKER_EXEC = (
    os.name == "nt"
    and shutil.which("docker") is not None
)

def execute_sql_via_docker(sql_command):
    try:
        cmd = [
            "docker",
            "exec",
            "-e",
            "PGPASSWORD=postgres",
            "feature_postgres",
            "psql",
            "-U",
            "postgres",
            "-d",
            "feature_store",
            "-c",
            sql_command
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=10
        )

        if result.returncode != 0:
            logger.error(
                f"Docker exec error: {result.stderr}"
            )
            return False
        return True

    except Exception as e:
        logger.error(
            f"Docker exec failed: {e}"
        )
        return False

def get_connection():
    if USE_DOCKER_EXEC:
        return None
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        return conn
    except psycopg2.Error as e:
        logger.error(
            f"PostgreSQL connection failed: {e}"
        )
        return None


def close_connection(conn):
    if conn:
        try:
            conn.close()
        except Exception as e:
            logger.warning(
                f"Error closing connection: {e}"
            )

def postgres_health():
    conn = get_connection()
    if conn:
        close_connection(conn)
        return True
    return False

def write_user_features(
    feature_name,
    feature_map,
    feature_date
):
    if not feature_map:
        logger.warning(
            f"Empty feature map for {feature_name}"
        )

        return True

    if USE_DOCKER_EXEC:
        return _write_user_features_docker(
            feature_name,
            feature_map,
            feature_date
        )

    conn = get_connection()
    if not conn:
        logger.error(
            "No PostgreSQL connection"
        )
        return False

    try:
        batch_data = [
            (
                str(user_id),
                feature_name,
                float(value),
                feature_date
            )
            for user_id, value in feature_map.items()
        ]

        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM user_features
                WHERE feature_name = %s
                AND feature_date = %s
                """,
                (
                    feature_name,
                    feature_date
                )
            )

            cur.executemany(
                """
                INSERT INTO user_features (
                    user_id,
                    feature_name,
                    feature_value,
                    feature_date
                )
                VALUES (%s, %s, %s, %s)
                """,
                batch_data
            )

        conn.commit()
        logger.info(
            f"Wrote {len(batch_data)} "
            f"user feature records"
        )
        return True

    except psycopg2.Error as e:
        logger.error(
            f"Error writing user features: {e}"
        )
        conn.rollback()
        return False

    finally:
        close_connection(conn)


def write_product_features(
    feature_name,
    feature_map,
    feature_date
):
    if not feature_map:
        logger.warning(
            f"Empty feature map for {feature_name}"
        )
        return True

    conn = get_connection()
    if not conn:
        logger.error(
            "No PostgreSQL connection"
        )
        return False

    try:
        batch_data = [
            (
                str(product_id),
                feature_name,
                float(value),
                feature_date
            )
            for product_id, value in feature_map.items()
        ]
        with conn.cursor() as cur:
            cur.execute(
                """
                DELETE FROM product_features
                WHERE feature_name = %s
                AND feature_date = %s
                """,
                (
                    feature_name,
                    feature_date
                )
            )
            cur.executemany(
                """
                INSERT INTO product_features (
                    product_id,
                    feature_name,
                    feature_value,
                    feature_date
                )
                VALUES (%s, %s, %s, %s)
                """,
                batch_data
            )
        conn.commit()
        logger.info(
            f"Wrote {len(batch_data)} "
            f"product feature records"
        )
        return True

    except psycopg2.Error as e:
        logger.error(
            f"Error writing product features: {e}"
        )
        conn.rollback()
        return False

    finally:
        close_connection(conn)

def read_user_features(
    user_id,
    feature_date=None
):
    conn = get_connection()
    if not conn:
        return []
    try:
        with conn.cursor() as cur:
            if feature_date:
                cur.execute(
                    """
                    SELECT
                        feature_name,
                        feature_value,
                        feature_date
                    FROM user_features
                    WHERE user_id = %s
                    AND feature_date = %s
                    ORDER BY feature_date DESC
                    """,
                    (
                        str(user_id),
                        feature_date
                    )
                )

            else:
                cur.execute(
                    """
                    SELECT
                        feature_name,
                        feature_value,
                        feature_date
                    FROM user_features
                    WHERE user_id = %s
                    ORDER BY feature_date DESC
                    """,
                    (str(user_id),)
                )
            return cur.fetchall()

    except psycopg2.Error as e:
        logger.error(
            f"Error reading user features: {e}"
        )
        return []

    finally:
        close_connection(conn)

def read_product_features(
    product_id,
    feature_date=None
):
    conn = get_connection()
    if not conn:
        return []
    try:
        with conn.cursor() as cur:
            if feature_date:
                cur.execute(
                    """
                    SELECT
                        feature_name,
                        feature_value,
                        feature_date
                    FROM product_features
                    WHERE product_id = %s
                    AND feature_date = %s
                    ORDER BY feature_date DESC
                    """,
                    (
                        str(product_id),
                        feature_date
                    )
                )
            else:
                cur.execute(
                    """
                    SELECT
                        feature_name,
                        feature_value,
                        feature_date
                    FROM product_features
                    WHERE product_id = %s
                    ORDER BY feature_date DESC
                    """,
                    (str(product_id),)
                )
            return cur.fetchall()

    except psycopg2.Error as e:
        logger.error(
            f"Error reading product features: {e}"
        )
        return []

    finally:
        close_connection(conn)