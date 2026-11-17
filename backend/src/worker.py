"""单进程顺序 worker：原子领取（CAS 令牌）、租约心跳、中断恢复。

- 队列消息可重复投递，claim 依靠数据库行锁保证同一任务同一时刻只有一个执行者；
- 心跳为 running 任务续租，租约过期的任务在启动时重新入队并可被重新领取；
- 终态写回校验 run_token，旧执行者即使恢复运行也不能覆盖新执行者的结果。
"""
import os,time,traceback,json,socket,threading
from pathlib import Path
from src.config.migrate import migrate
from src.services.trials import queue,claim,heartbeat,finish,requeue_orphans,QUEUE_KEY
from src.services.concurrency import execute
WORKER_ID=f"{socket.gethostname()}-{os.getpid()}"
ARTIFACTS=Path(os.getenv('LOCKLAB_ARTIFACTS','/artifacts'))
def run_item(ident):
    token=claim(ident,WORKER_ID)
    if not token:
        print(f'[worker] trial {ident} 已被其他执行者领取或已结束，跳过')
        return
    stop=threading.Event()
    def beat():
        while not stop.wait(5):
            if not heartbeat(ident,token): return
    t=threading.Thread(target=beat,daemon=True);t.start()
    try:
        result=execute(ident,token=token)
        ok=finish(ident,token,'completed',result=result)
        if ok:
            ARTIFACTS.mkdir(parents=True,exist_ok=True)
            (ARTIFACTS/f'trial-{ident}.json').write_text(json.dumps(result,ensure_ascii=False,indent=2))
        else:
            print(f'[worker] trial {ident} 终态写回被拒（令牌已更替），保留新执行者结果')
    except Exception as e:
        traceback.print_exc()
        finish(ident,token,'failed',error=str(e))
    finally: stop.set()
def main():
    migrate()
    recovered=requeue_orphans()
    if recovered: print(f'[worker] 恢复中断任务入队：{recovered}')
    last_sweep=time.time()
    while True:
        Path('/tmp/worker-heartbeat').touch()
        item=queue.blpop(QUEUE_KEY,timeout=2)
        if item: run_item(int(item[1]))
        # 空闲时定期回收租约过期的中断任务（队列消息可能已随旧进程丢失）
        elif time.time()-last_sweep>10:
            for ident in requeue_orphans(): print(f'[worker] 租约过期任务重新入队：{ident}')
            last_sweep=time.time()
if __name__=='__main__': main()
