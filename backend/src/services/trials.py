import uuid
from datetime import datetime,timedelta,timezone
from sqlalchemy import select,update
from redis import Redis
from fastapi import HTTPException
from src.config.settings import REDIS_URL
from src.config.database import Session
from src.models.entities import Trial,Transfer,Sample,TrialBalance
from src.utils.serialization import record
from .common import save
from src.constants.enums import RUN_STATES,LEASE_SECONDS
queue=Redis.from_url(REDIS_URL,decode_responses=True)
QUEUE_KEY='locklab:queue'
def listing(db): return [record(x) for x in db.scalars(select(Trial).order_by(Trial.id.desc()))]
def detail(db,ident):
    obj=db.get(Trial,ident)
    if not obj: raise HTTPException(404,'演练不存在')
    return record(obj)
def _enqueue(ident):
    try: queue.rpush(QUEUE_KEY,str(ident))
    except Exception:
        with Session() as db:
            obj=db.get(Trial,ident)
            if obj and obj.status in RUN_STATES[:2]: obj.status='failed';obj.error='队列不可用，请稍后重新创建';db.commit()
        raise HTTPException(503,'队列不可用，请稍后重新创建')
def create(db,data,user):
    transfer=db.get(Transfer,data.transfer_id)
    if not transfer: raise HTTPException(404,'流转申请不存在')
    sample=db.get(Sample,transfer.sample_id)
    if transfer.quantity>sample.quantity: raise HTTPException(422,'当前样本量不足')
    cross=data.kind=='cross'
    second=None
    if cross:
        if not data.second_transfer_id: raise HTTPException(422,'交叉演练必须选择第二笔流转申请')
        second=db.get(Transfer,data.second_transfer_id)
        if not second: raise HTTPException(404,'第二笔流转申请不存在')
        if second.id==transfer.id: raise HTTPException(422,'交叉演练需要两笔不同的流转申请')
        if second.sample_id==transfer.sample_id: raise HTTPException(422,'交叉演练必须指向两份不同样本')
        sample2=db.get(Sample,second.sample_id)
        if second.quantity>sample2.quantity: raise HTTPException(422,'第二笔申请量超过对应样本可用量')
    payload=data.model_dump()
    if not cross: payload['second_transfer_id']=None
    obj=Trial(**payload,status=RUN_STATES[0],attempts=0)
    result=save(db,obj,user,('双样本交叉死锁演练入队 ' if cross else '并发演练入队 ')+data.name)
    # 单样本一行镜像账；双样本各建一行镜像账，原台账始终不变
    db.add(TrialBalance(trial_id=obj.id,sample_id=sample.id,remaining=sample.quantity,received=0))
    if cross:
        db.add(TrialBalance(trial_id=obj.id,sample_id=sample2.id,remaining=sample2.quantity,received=0))
    db.commit()
    _enqueue(obj.id)
    return result
def claim(ident,worker_id):
    """原子领取：只有 queued，或租约已过期的 running（中断恢复）才能被领取。
    并发领取时数据库行锁保证只有一个执行者拿到新令牌，旧执行者的写回随后被令牌校验拒绝。"""
    now=datetime.now(timezone.utc);deadline=now+timedelta(seconds=LEASE_SECONDS)
    token=uuid.uuid4().hex
    with Session() as db:
        row=db.execute(
            update(Trial).where(Trial.id==ident)
            .where((Trial.status=='queued')|((Trial.status=='running')&(Trial.locked_until<now)))
            .values(status='running',run_token=token,worker_id=worker_id,locked_until=deadline,attempts=Trial.attempts+1),
        ).rowcount
        db.commit()
        if not row: return None
        return token
def heartbeat(ident,token):
    """续租；仅当前令牌持有者有效。旧执行者恢复运行时无法覆盖新执行者。"""
    now=datetime.now(timezone.utc)
    with Session() as db:
        row=db.execute(update(Trial).where(Trial.id==ident,Trial.run_token==token)
                       .values(locked_until=now+timedelta(seconds=LEASE_SECONDS))).rowcount
        db.commit();return bool(row)
def finish(ident,token,status,result=None,error=''):
    """带令牌的终态写回：旧执行者不覆盖新结果。"""
    with Session() as db:
        row=db.execute(update(Trial).where(Trial.id==ident,Trial.run_token==token,Trial.status=='running')
                       .values(status=status,result=result,error=error,locked_until=None)).rowcount
        db.commit();return bool(row)
def requeue_orphans():
    """启动恢复：把租约过期的 running 任务重新入队（队列可能已丢失消息）。"""
    now=datetime.now(timezone.utc);requeued=[]
    with Session() as db:
        ids=list(db.scalars(select(Trial.id).where(Trial.status=='running',Trial.locked_until<now)))
        for ident in ids:
            try: queue.rpush(QUEUE_KEY,str(ident));requeued.append(ident)
            except Exception: pass
    return requeued
