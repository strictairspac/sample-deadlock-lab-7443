ROLES = ('admin', 'operator', 'viewer')
RUN_STATES = ('queued', 'running', 'completed', 'failed')
# single：原单样本双连接锁等待演练；cross：双样本交叉移交死锁演练
TRIAL_KINDS = ('single', 'cross')
# 任务租约：超过此时长没有心跳的 running 任务视为中断，可被重新领取
LEASE_SECONDS = 30
