from config import conn


def create_table() -> None:
    start_cursor = conn.cursor()
    start_cursor.execute("""
        CREATE TABLE IF NOT EXISTS pre_groups (
            group_id INTEGER PRIMARY KEY AUTOINCREMENT, 
            group_username TEXT UNIQUE)""")

    start_cursor.execute("""
        CREATE TABLE IF NOT EXISTS groups (
            group_id INTEGER,
            group_username TEXT,
            user_id INTEGER)""")

    start_cursor.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            user_id INTEGER PRIMARY KEY,
            session_string TEXT)""")

    start_cursor.execute("""
        CREATE TABLE IF NOT EXISTS discovered_groups (
            user_id INTEGER NOT NULL,
            group_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            username TEXT,
            access_hash INTEGER,
            peer_type TEXT NOT NULL,
            is_admin INTEGER DEFAULT 0,
            is_creator INTEGER DEFAULT 0,
            is_available INTEGER DEFAULT 1,
            last_seen_at TEXT,
            PRIMARY KEY (user_id, group_id))""")

    # Удаляем возможные старые дубли перед созданием уникального индекса.
    start_cursor.execute("""
        DELETE FROM groups
        WHERE rowid NOT IN (
            SELECT MIN(rowid) FROM groups GROUP BY user_id, group_id
        )
    """)
    start_cursor.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_groups_user_group
        ON groups(user_id, group_id)
    """)
    start_cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_discovered_groups_user_available
        ON discovered_groups(user_id, is_available)
    """)

    start_cursor.execute("""
        CREATE TABLE IF NOT EXISTS broadcasts ( 
            user_id INTEGER, 
            group_id INTEGER, 
            session_string TEXT, 
            broadcast_text TEXT, 
            interval_minutes INTEGER,
            is_active BOOLEAN,
            error_reason TEXT,
            photo_url TEXT)""")

    start_cursor.execute("""
        CREATE TABLE IF NOT EXISTS send_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            group_id INTEGER,
            group_name TEXT,
            sent_at TEXT,
            message_text TEXT);""")

    try:
        start_cursor.execute("ALTER TABLE broadcasts ADD COLUMN error_reason TEXT")
        conn.commit()
    except:
        pass

    try:
        start_cursor.execute("ALTER TABLE broadcasts ADD COLUMN photo_url TEXT")
        conn.commit()
    except:
        pass

    conn.commit()
    start_cursor.close()


def delete_table() -> None:
    end_cursor = conn.cursor()
    end_cursor.execute("""UPDATE broadcasts SET is_active = ? WHERE is_active = ?""", (False, True))
    conn.commit()
    end_cursor.close()


def create_dm_tables() -> None:
    """Создает таблицы для DM-автопостера"""
    cursor = conn.cursor()

    # Задачи
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dm_tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            session_string TEXT NOT NULL,
            post_text TEXT NOT NULL,
            photo_url TEXT,
            interval_minutes INTEGER NOT NULL,
            is_active BOOLEAN DEFAULT 1,
            created_at TEXT,
            delay_min INTEGER DEFAULT 30,
            delay_max INTEGER DEFAULT 90
        )
    """)

    # Миграции для существующих БД
    for col, default in [("delay_min", 30), ("delay_max", 90)]:
        try:
            cursor.execute(f"ALTER TABLE dm_tasks ADD COLUMN {col} INTEGER DEFAULT {default}")
            conn.commit()
        except Exception:
            pass

    # Лог отправок
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dm_sent_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dm_task_id INTEGER NOT NULL,
            target_user_id INTEGER NOT NULL,
            sent_at TEXT NOT NULL,
            status TEXT DEFAULT 'sent'
        )
    """)

    try:
        cursor.execute("ALTER TABLE dm_sent_log ADD COLUMN status TEXT DEFAULT 'sent'")
        conn.commit()
    except Exception:
        pass

    # Мониторируемые чаты
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dm_watched_chats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            dm_task_id INTEGER NOT NULL,
            chat_id INTEGER NOT NULL
        )
    """)

    conn.commit()
    cursor.close()
