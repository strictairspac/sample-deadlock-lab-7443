"""Idempotent lightweight schema migration.

create_all covers new deployments; existing volumes keep the old table shapes,
so ALTER statements below add columns/constraints only when missing.
The original samples ledger table is never altered.
"""
from sqlalchemy import inspect,text
from src.config.database import engine
from src.models import entities
def _columns(conn,table): return {c['name'] for c in inspect(conn).get_columns(table)}
def migrate():
    entities.Base.metadata.create_all(engine)
    with engine.begin() as conn:
        if inspect(conn).has_table('trials'):
            cols=_columns(conn,'trials')
            for ddl in (
                "ALTER TABLE trials ADD COLUMN second_transfer_id INTEGER REFERENCES transfers(id)",
                "ALTER TABLE trials ADD COLUMN kind VARCHAR(20) NOT NULL DEFAULT 'single'",
                "ALTER TABLE trials ADD COLUMN run_token VARCHAR(64)",
                "ALTER TABLE trials ADD COLUMN worker_id VARCHAR(120)",
                "ALTER TABLE trials ADD COLUMN locked_until TIMESTAMP WITH TIME ZONE",
                "ALTER TABLE trials ADD COLUMN attempts INTEGER NOT NULL DEFAULT 0",
            ):
                col=ddl.split('ADD COLUMN ')[1].split(' ')[0]
                if col not in cols: conn.execute(text(ddl))
        if inspect(conn).has_table('trial_balances'):
            cols=_columns(conn,'trial_balances')
            if 'sample_id' not in cols:
                # 旧库：单行镜像账复合主键只有 trial_id，回填为对应演练申请的样本后改键
                conn.execute(text("ALTER TABLE trial_balances ADD COLUMN sample_id INTEGER"))
                conn.execute(text("UPDATE trial_balances b SET sample_id=t.sample_id "
                                  "FROM trials tr JOIN transfers t ON t.id=tr.transfer_id WHERE b.trial_id=tr.id"))
                conn.execute(text("ALTER TABLE trial_balances ALTER COLUMN sample_id SET NOT NULL"))
                conn.execute(text("ALTER TABLE trial_balances DROP CONSTRAINT IF EXISTS trial_balances_pkey"))
                conn.execute(text("ALTER TABLE trial_balances ADD PRIMARY KEY (trial_id,sample_id)"))
