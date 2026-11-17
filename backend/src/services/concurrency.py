"""Real PostgreSQL lock experiments.

single：原单样本双连接 lock_timeout 演练（保持原算法与证据口径）。
cross：双样本交叉移交，两个事务各先锁自己的镜像账本行，再争对方那一行，
由 PostgreSQL 检出真实死锁（SQLSTATE 40P01）并选出牺牲者；牺牲者整笔回滚
（回执与扣量一起回滚），胜者提交后以发起时的原申请量重试牺牲者，重试不重复扣量。

所有阶段写入 trial_attempts 阶段账本（独立事务提交），中断恢复时按阶段跳过已完成步骤；
终态写回带领取令牌，旧执行者无法覆盖新执行者的结果。
"""
import os,threading,time,hashlib,json
from sqlalchemy import text,select
from src.config.database import engine,Session
from src.models.entities import Trial,Transfer,TrialBalance,Receipt,Sample,TrialAttempt

def _crashpoint(name,trial_id):
    """测试钩子：LOCKLAB_CRASH=trial:<id>:after:<name> 命中时硬退出，模拟执行中断。"""
    rule=os.getenv('LOCKLAB_CRASH','')
    if rule and rule==f'trial:{trial_id}:after:{name}':
        os._exit(1)

def _load(trial_id):
    with Session() as db:
        trial=db.get(Trial,trial_id)
        t1=db.get(Transfer,trial.transfer_id)
        s1=db.get(Sample,t1.sample_id)
        t2=s2=None
        if trial.kind=='cross':
            t2=db.get(Transfer,trial.second_transfer_id)
            s2=db.get(Sample,t2.sample_id)
        return trial,t1,s1,t2,s2

def _record_attempt(trial_id,actor,phase,**kw):
    """阶段账本独立事务提交：即使随后进程中断，已发生的真实结果也不丢。"""
    with Session() as db:
        exists=db.scalar(select(TrialAttempt).where(TrialAttempt.trial_id==trial_id,
            TrialAttempt.actor==actor,TrialAttempt.phase==phase))
        if not exists:
            db.add(TrialAttempt(trial_id=trial_id,actor=actor,phase=phase,**kw));db.commit()

def _done_attempts(trial_id):
    with Session() as db:
        return {(a.actor,a.phase):a for a in db.scalars(select(TrialAttempt).where(TrialAttempt.trial_id==trial_id))}

def execute(trial_id,token=None):
    trial,t1,s1,t2,s2=_load(trial_id)
    if trial.kind=='cross':
        return _execute_cross(trial_id,t1,s1,t2,s2)
    return _execute_single(trial_id,t1,s1)

# ---------- 原单样本演练（算法、口径、证据结构保持不变，仅增加 kind 与恢复短路） ----------
def _execute_single(trial_id,transfer,sample):
    with Session() as db:
        timeout=db.get(Trial,trial_id).timeout_ms
        qty=transfer.quantity
    # 恢复短路：上一轮 A 已提交但进程在终态写回前中断，直接复核而不重复执行
    with Session() as db:
        prior=list(db.scalars(select(Receipt).where(Receipt.trial_id==trial_id)))
        balance=db.get(TrialBalance,(trial_id,sample.id))
    if prior and balance.received==qty:
        return _finalize_single(trial_id,transfer,sample,[
            {'actor':'A','pid':None,'event':'committed','quantity':qty,'recovered':True}])
    acquired=threading.Event(); contender_done=threading.Event(); traces=[]; errors=[]
    def holder():
        try:
            with engine.connect() as c:
                tx=c.begin();pid=c.execute(text('SELECT pg_backend_pid()')).scalar()
                c.execute(text('SELECT remaining FROM trial_balances WHERE trial_id=:id AND sample_id=:sid FOR UPDATE'),
                          {'id':trial_id,'sid':sample.id}).one()
                traces.append({'actor':'A','pid':pid,'event':'row_lock_acquired'})
                # A receipt made before B starts is uncommitted, and B cannot read it.
                c.execute(text("INSERT INTO receipts(trial_id,actor,quantity) VALUES (:id,'A',:qty)"),{'id':trial_id,'qty':qty})
                acquired.set()
                if not contender_done.wait(12): raise RuntimeError('竞争事务未在保护期限内结束')
                changed=c.execute(text('UPDATE trial_balances SET remaining=remaining-:qty,received=received+:qty WHERE trial_id=:id AND sample_id=:sid AND remaining>=:qty'),
                                  {'id':trial_id,'sid':sample.id,'qty':qty})
                if changed.rowcount!=1: raise RuntimeError('可用量校验失败')
                tx.commit();traces.append({'actor':'A','pid':pid,'event':'committed','quantity':qty})
        except Exception as e: errors.append(str(e));acquired.set();contender_done.set()
    def contender():
        try:
            if not acquired.wait(5): raise RuntimeError('持锁事务未就绪')
            with engine.connect() as c:
                tx=c.begin();pid=c.execute(text('SELECT pg_backend_pid()')).scalar()
                c.execute(text(f"SET LOCAL lock_timeout = '{int(timeout)}ms'"))
                c.execute(text("INSERT INTO receipts(trial_id,actor,quantity) VALUES (:id,'B',:qty)"),{'id':trial_id,'qty':qty})
                t=time.perf_counter()
                try:
                    c.execute(text('SELECT remaining FROM trial_balances WHERE trial_id=:id AND sample_id=:sid FOR UPDATE'),
                              {'id':trial_id,'sid':sample.id}).one()
                    raise RuntimeError('竞争连接意外获得锁')
                except Exception as e:
                    state=getattr(getattr(e,'orig',None),'sqlstate',None)
                    tx.rollback()
                    if state!='55P03': raise
                    traces.append({'actor':'B','pid':pid,'event':'rolled_back','sqlstate':state,
                                   'wait_ms':round((time.perf_counter()-t)*1000,2),'receipt_insert_rolled_back':True})
        except Exception as e: errors.append(str(e))
        finally: contender_done.set()
    a=threading.Thread(target=holder);b=threading.Thread(target=contender)
    a.start();b.start();a.join(15);b.join(15)
    if a.is_alive() or b.is_alive(): raise RuntimeError('并发试验未及时结束')
    if errors: raise RuntimeError(';'.join(errors))
    return _finalize_single(trial_id,transfer,sample,traces)

def _finalize_single(trial_id,transfer,sample,traces):
    with Session() as db:
        balance=db.get(TrialBalance,(trial_id,sample.id))
        receipts=list(db.scalars(select(Receipt).where(Receipt.trial_id==trial_id)))
        initial=sample.quantity;qty=transfer.quantity
        invariant=balance.remaining+balance.received==initial and balance.remaining>=0 and len(receipts)==1 and receipts[0].actor=='A'
        result={'kind':'single','mode':'真实 PostgreSQL / 隔离样本镜像账本','sample':sample.code,'initial':initial,
                'transfer_quantity':qty,'remaining':balance.remaining,'received':balance.received,
                'receipt_count':len(receipts),'invariant_passed':invariant,'traces':traces,
                'sql':['SELECT ... FOR UPDATE','SET LOCAL lock_timeout','INSERT INTO receipts','UPDATE trial_balances','ROLLBACK / COMMIT'],
                'formula':'remaining + received = initial；A 回执 1 条，B 回执回滚为 0 条'}
        result['checksum']=hashlib.sha256(json.dumps(result,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
        if not invariant: raise RuntimeError('单样本演练不变量校验未通过')
        return result

# ---------- 双样本交叉移交死锁演练 ----------
def _settings(c,timeout_ms):
    # 交叉演练：死锁检测必须先于锁等待上限触发，才能保留数据库自行选出的 40P01 牺牲者；
    # lock_timeout 仅作兜底，防止非死锁型等待无限挂起。
    c.execute(text(f"SET LOCAL deadlock_timeout = '{int(timeout_ms)}ms'"))
    c.execute(text(f"SET LOCAL lock_timeout = '{int(timeout_ms)+2000}ms'"))

def _deduct(c,trial_id,sample_id,qty):
    changed=c.execute(text('UPDATE trial_balances SET remaining=remaining-:qty,received=received+:qty '
                           'WHERE trial_id=:id AND sample_id=:sid AND remaining>=:qty'),
                      {'id':trial_id,'sid':sample_id,'qty':qty})
    if changed.rowcount!=1: raise RuntimeError(f'样本 {sample_id} 可用量校验失败')

def _execute_cross(trial_id,t1,s1,t2,s2):
    with Session() as db: timeout=db.get(Trial,trial_id).timeout_ms
    q1,q2=t1.quantity,t2.quantity
    # actor → (自己的镜像样本, 申请量)；T1 先锁 sample1 再争 sample2，T2 反向
    actors={'T1':{'sample':s1,'qty':q1,'transfer':t1.code},'T2':{'sample':s2,'qty':q2,'transfer':t2.code}}
    done=_done_attempts(trial_id)
    traces=[];victim=winner=None
    # ---- 阶段一：真实死锁，数据库选出牺牲者 ----
    if not any(k[1]=='deadlock' for k in done):
        # 极窄中断窗口：胜者已 COMMIT、阶段账尚未落库。按持久化回执重建阶段，绝不重放死锁。
        with Session() as db:
            committed=sorted(r.actor for r in db.scalars(select(Receipt).where(Receipt.trial_id==trial_id)))
        if committed:
            winner=committed[0];victim='T2' if winner=='T1' else 'T1'
            _record_attempt(trial_id,victim,'deadlock',sqlstate='40P01',
                            error='中断恢复：牺牲者回滚结果由持久化状态重建（原始错误文本未落库）',rolled_back=1)
            _record_attempt(trial_id,winner,'deadlock',committed=1)
            done=_done_attempts(trial_id)
            traces.append({'actor':'recovery','pid':None,'event':'deadlock_phase_rebuilt_from_durable_receipts'})
        else:
            first_locked={'T1':threading.Event(),'T2':threading.Event()}
            victim_ledger_durable=threading.Event()
            outcome={};errors=[]
            def txn(actor,own_sid,other_sid,qty):
                try:
                    with engine.connect() as c:
                        tx=c.begin();pid=c.execute(text('SELECT pg_backend_pid()')).scalar()
                        _settings(c,timeout)
                        # 1) 先锁自己那份镜像账，并在同一事务内写回执、扣量（均未提交）
                        c.execute(text('SELECT remaining FROM trial_balances WHERE trial_id=:id AND sample_id=:sid FOR UPDATE'),
                                  {'id':trial_id,'sid':own_sid}).one()
                        c.execute(text('INSERT INTO receipts(trial_id,actor,quantity) VALUES (:id,:actor,:qty)'),
                                  {'id':trial_id,'actor':actor,'qty':qty})
                        _deduct(c,trial_id,own_sid,qty)
                        traces.append({'actor':actor,'pid':pid,'event':'own_row_locked','sample_id':own_sid})
                        first_locked[actor].set()
                        other='T2' if actor=='T1' else 'T1'
                        if not first_locked[other].wait(8): raise RuntimeError('另一事务未在保护期限内锁住首行')
                        # 2) 再争另一份样本：两边同时等待对方持有的行，构成真实死锁环
                        t=time.perf_counter()
                        try:
                            c.execute(text('SELECT remaining FROM trial_balances WHERE trial_id=:id AND sample_id=:sid FOR UPDATE'),
                                      {'id':trial_id,'sid':other_sid}).one()
                        except Exception as e:
                            state=getattr(getattr(e,'orig',None),'sqlstate',None)
                            wait=round((time.perf_counter()-t)*1000,2)
                            if state!='40P01': raise
                            # 数据库选定的牺牲者：整笔 ROLLBACK，回执与扣量全部回滚
                            tx.rollback()
                            msg=str(getattr(e,'orig',e)).strip()
                            # 回滚已持久化，立即把真实 PID/错误写入阶段账（独立事务），缩小中断窗口
                            _record_attempt(trial_id,actor,'deadlock',pid=pid,sqlstate='40P01',
                                            error=msg,wait_ms=int(wait),rolled_back=1)
                            victim_ledger_durable.set()
                            outcome[actor]={'victim':True,'pid':pid,'wait_ms':wait,'sqlstate':state,'error':msg}
                            traces.append({'actor':actor,'pid':pid,'event':'deadlock_victim_rolled_back',
                                           'sqlstate':state,'wait_ms':wait,'error':msg})
                            return
                        # 未被选为牺牲者的事务在对方回滚后拿到第二把锁；
                        # 等待牺牲者阶段账落库后再提交，保证“已提交必有回滚证据”
                        if not victim_ledger_durable.wait(8): raise RuntimeError('牺牲者阶段账未及时落库')
                        _crashpoint('winner_lock',trial_id)
                        tx.commit()
                        # 模拟“已提交未记账胜者行”的极窄中断窗口（随后由持久化回执重建）
                        _crashpoint('winner_commit_pre_ledger',trial_id)
                        outcome[actor]={'victim':False,'pid':pid}
                        traces.append({'actor':actor,'pid':pid,'event':'winner_committed'})
                except Exception as e:
                    errors.append(f'{actor}: {e}')
                    first_locked[actor].set();first_locked['T1' if actor=='T2' else 'T1'].set()
            th=[threading.Thread(target=txn,args=('T1',s1.id,s2.id,q1)),
                threading.Thread(target=txn,args=('T2',s2.id,s1.id,q2))]
            for x in th: x.start()
            for x in th: x.join(20)
            if any(x.is_alive() for x in th): raise RuntimeError('交叉死锁演练未及时结束')
            if errors: raise RuntimeError(';'.join(errors))
            victims=[a for a,v in outcome.items() if v.get('victim')]
            if len(victims)!=1: raise RuntimeError(f'预期恰好一个死锁牺牲者，实际 {victims}')
            victim=victims[0];winner='T2' if victim=='T1' else 'T1'
            _record_attempt(trial_id,winner,'deadlock',pid=outcome[winner]['pid'],committed=1)
            _crashpoint('winner_commit',trial_id)
            done=_done_attempts(trial_id)
    if victim is None:
        victim=next(a for a in ('T1','T2') if done.get((a,'deadlock')) and done[(a,'deadlock')].rolled_back)
        winner='T2' if victim=='T1' else 'T1'
        # 胜者回执已持久化但阶段行缺失时幂等补记，证据链保持 2 条 deadlock + 1 条 retry
        if (winner,'deadlock') not in done:
            _record_attempt(trial_id,winner,'deadlock',committed=1)
            done=_done_attempts(trial_id)
        if not traces: traces.append({'actor':'recovery','pid':None,'event':'deadlock_phase_restored_from_ledger'})
    # ---- 阶段二：胜者完成后，重试牺牲者，沿用发起时申请量，回执唯一约束防重复扣量 ----
    if (victim,'retry') not in done:
        info=actors[victim]
        with engine.connect() as c:
            tx=c.begin();pid=c.execute(text('SELECT pg_backend_pid()')).scalar()
            _settings(c,timeout)
            c.execute(text('SELECT remaining FROM trial_balances WHERE trial_id=:id AND sample_id=:sid FOR UPDATE'),
                      {'id':trial_id,'sid':info['sample'].id}).one()
            # 牺牲者上一笔已整笔回滚，此处以原申请量重新插入；若回执已存在则绝不重复扣量
            existing=c.execute(text('SELECT 1 FROM receipts WHERE trial_id=:id AND actor=:actor'),
                               {'id':trial_id,'actor':victim}).scalar()
            if not existing:
                c.execute(text('INSERT INTO receipts(trial_id,actor,quantity) VALUES (:id,:actor,:qty)'),
                          {'id':trial_id,'actor':victim,'qty':info['qty']})
                _deduct(c,trial_id,info['sample'].id,info['qty'])
            tx.commit()
        _record_attempt(trial_id,victim,'retry',pid=pid,committed=1)
        traces.append({'actor':victim,'pid':pid,'event':'retried_with_original_quantity',
                       'quantity':info['qty'],'duplicate_deduction':False})
        _crashpoint('victim_retry_commit',trial_id)
    return _finalize_cross(trial_id,t1,s1,t2,s2,actors,victim)

def _finalize_cross(trial_id,t1,s1,t2,s2,actors,victim):
    with Session() as db:
        balances={b.sample_id:b for b in db.scalars(select(TrialBalance).where(TrialBalance.trial_id==trial_id))}
        receipts={r.actor:r for r in db.scalars(select(Receipt).where(Receipt.trial_id==trial_id))}
        attempts=[{k:getattr(a,k) for k in ('actor','phase','pid','sqlstate','error','wait_ms','rolled_back','committed')}
                  for a in db.scalars(select(TrialAttempt).where(TrialAttempt.trial_id==trial_id).order_by(TrialAttempt.id))]
        samples=[];invariant=True
        for s,t in ((s1,t1),(s2,t2)):
            b=balances[s.id]
            ok=b.remaining+b.received==s.quantity and b.remaining>=0 and b.received==t.quantity
            invariant=invariant and ok
            samples.append({'sample':s.code,'initial':s.quantity,'remaining':b.remaining,
                            'received':b.received,'transfer':t.code,'requested_quantity':t.quantity,'conservation_passed':ok})
        receipt_ok=set(receipts)=={'T1','T2'} and receipts['T1'].quantity==actors['T1']['qty'] and receipts['T2'].quantity==actors['T2']['qty']
        deadlock_row=next(a for a in attempts if a['phase']=='deadlock' and a['rolled_back'])
        invariant=invariant and receipt_ok and deadlock_row['sqlstate']=='40P01'
        result={'kind':'cross','mode':'真实 PostgreSQL / 双样本镜像账本交叉死锁',
                'samples':samples,'receipt_count':len(receipts),'receipts':[{'actor':a,'quantity':r.quantity} for a,r in sorted(receipts.items())],
                'deadlock_victim':victim,'victim_sqlstate':'40P01','victim_error':deadlock_row['error'],
                'retry_quantity':actors[victim]['qty'],'retry_duplicate_deduction':False,
                'attempts':attempts,'invariant_passed':invariant,
                'traces':[{'actor':'T1/T2','pid':None,'event':'cross_handshake','event_detail':'各先锁自身镜像行，再 FOR UPDATE 对方行'}],
                'sql':['SET LOCAL deadlock_timeout / lock_timeout','SELECT ... FOR UPDATE（自身行→对方行）',
                       'INSERT INTO receipts（未提交）','UPDATE trial_balances（未提交）',
                       '死锁 40P01 → 牺牲者 ROLLBACK','胜者 COMMIT','牺牲者按原申请量重试 COMMIT'],
                'formula':'两份镜像账各自 remaining + received = initial；牺牲者回执与扣量随死锁整笔回滚，重试仅扣一次'}
        result['checksum']=hashlib.sha256(json.dumps(result,sort_keys=True,ensure_ascii=False).encode()).hexdigest()
        if not invariant: raise RuntimeError('交叉演练守恒或回执校验未通过')
        return result
