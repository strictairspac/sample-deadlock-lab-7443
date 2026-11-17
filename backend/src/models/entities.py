from sqlalchemy import Column, Integer, String, JSON, DateTime, ForeignKey, Text, UniqueConstraint
from datetime import datetime, timezone
from src.config.database import Base
class User(Base):
    __tablename__='users'
    id=Column(Integer,primary_key=True);email=Column(String(120),unique=True,nullable=False)
    password=Column(String(300),nullable=False);role=Column(String(20),nullable=False)
class Sample(Base):
    __tablename__='samples'
    id=Column(Integer,primary_key=True);code=Column(String(80),unique=True,nullable=False)
    title=Column(String(160),nullable=False);quantity=Column(Integer,nullable=False);location=Column(String(120),nullable=False)
class Transfer(Base):
    __tablename__='transfers'
    id=Column(Integer,primary_key=True);code=Column(String(80),unique=True,nullable=False)
    sample_id=Column(Integer,ForeignKey('samples.id'),nullable=False);destination=Column(String(120),nullable=False)
    quantity=Column(Integer,nullable=False);note=Column(String(300),default='')
class Trial(Base):
    __tablename__='trials'
    id=Column(Integer,primary_key=True);name=Column(String(120),nullable=False)
    transfer_id=Column(Integer,ForeignKey('transfers.id'),nullable=False)
    # 交叉演练第二笔事务引用的流转申请；single 演练为空
    second_transfer_id=Column(Integer,ForeignKey('transfers.id'),nullable=True)
    kind=Column(String(20),default='single',nullable=False);timeout_ms=Column(Integer,nullable=False)
    status=Column(String(20),default='queued',nullable=False);result=Column(JSON,nullable=True)
    created_at=Column(DateTime(timezone=True),default=lambda:datetime.now(timezone.utc));error=Column(Text,default='')
    # 可恢复执行：领取令牌（CAS 防并发重复执行）、执行者标识、租约心跳
    run_token=Column(String(64),nullable=True);worker_id=Column(String(120),nullable=True)
    locked_until=Column(DateTime(timezone=True),nullable=True);attempts=Column(Integer,default=0,nullable=False)
class TrialBalance(Base):
    __tablename__='trial_balances'
    # single：一行（sample_id 为镜像样本）；cross：每个镜像样本一行
    trial_id=Column(Integer,ForeignKey('trials.id'),primary_key=True)
    sample_id=Column(Integer,ForeignKey('samples.id'),primary_key=True)
    remaining=Column(Integer,nullable=False);received=Column(Integer,nullable=False,default=0)
class Receipt(Base):
    __tablename__='receipts'
    id=Column(Integer,primary_key=True);trial_id=Column(Integer,ForeignKey('trials.id'),nullable=False)
    # single 使用 A/B；cross 使用事务编号 T1/T2（重试沿用原 actor，天然不重复扣量）
    actor=Column(String(20),nullable=False);quantity=Column(Integer,nullable=False)
    __table_args__=(UniqueConstraint('trial_id','actor'),)
class TrialAttempt(Base):
    """交叉演练阶段账本：死锁/重试每一步的真实数据库结果，支持中断后按阶段恢复。"""
    __tablename__='trial_attempts'
    id=Column(Integer,primary_key=True);trial_id=Column(Integer,ForeignKey('trials.id'),nullable=False)
    actor=Column(String(20),nullable=False);phase=Column(String(20),nullable=False)
    pid=Column(Integer,nullable=True);sqlstate=Column(String(10),nullable=True);error=Column(Text,default='')
    wait_ms=Column(Integer,nullable=True);rolled_back=Column(Integer,default=0,nullable=False);committed=Column(Integer,default=0,nullable=False)
    created_at=Column(DateTime(timezone=True),default=lambda:datetime.now(timezone.utc))
    __table_args__=(UniqueConstraint('trial_id','actor','phase'),)
class Audit(Base):
    __tablename__='audit_logs'
    id=Column(Integer,primary_key=True);actor=Column(String(120),nullable=False);action=Column(String(160),nullable=False)
    created_at=Column(DateTime(timezone=True),default=lambda:datetime.now(timezone.utc))
