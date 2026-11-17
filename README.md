# 实验样本流转并发台

青屿检测样本中心（24 人）的样本移交复核工作台。原生 HTML/CSS/JavaScript 前端，Python FastAPI 后端，真实 PostgreSQL 两连接事务与行锁演练。

## 场景和模块

团队原先通过纸质交接单、SQL 截图和 Excel 交接。三处痛点：样本版本难统一、并发移交容易重复、超时后无法确认回执是否回滚。上游为样本接收记录，本站承接样本登记、移交申请、并发复核和证据导出，下游为理化检测室与复核员。

- 样本台账：批次、可用份数、保管位置，可录入。
- 流转申请：关联样本、指定数量和接收实验室。
- 并发执行：Redis 队列、独立 Python worker、可设锁等待上限；支持单样本锁等待与双样本交叉死锁两类演练。
- 冲突证据：Backend PID、SQLSTATE、死锁牺牲者、回执回滚、牺牲者重试、双账数量守恒与 JSON 导出。
- 可靠执行：数据库原子领取（CAS 令牌）、租约心跳、中断按阶段恢复、旧执行者不覆盖新结果。
- 管理员审计：登录、写入和执行动作。

## 启动

```sh
cp .env.example .env
docker compose up -d --build
docker compose exec -T backend python -m scripts.seed_demo
```

前端 http://localhost:18242 ，后端 http://localhost:19242/docs ，数据库 localhost:17742。

|角色|账号|密码|权限|
|---|---|---|---|
|admin|admin@qingyu.test|Demo2026!|全业务写入、执行、审计|
|operator|operator@qingyu.test|Demo2026!|创建样本、申请、执行、导出|
|viewer|viewer@qingyu.test|Demo2026!|只读、导出；写 API 返回403|

## 一天剧本

09:00 operator 登录；09:05 复核 `QY-261117-A` 近海水质留样 120 份，位置冷藏间 A-03；09:10 查看申请 `MOVE-1117-01`，接收理化检测室 30 份；09:15 新建 `shift-20261117` 流转申请；09:20 以该申请发起 250ms 单样本并发演练；09:25 查看 A 提交 30、B SQLSTATE 55P03 回滚，90+30=120；09:30 选择 `MOVE-1117-01` 与 `MOVE-1117-02` 发起双样本交叉死锁演练，查看数据库选出 T2 为 40P01 牺牲者、回执与扣量整笔回滚，胜者提交后 T2 按原 20 份重试且不重复扣量，两份镜像账各自守恒；09:35 导出 `trial-2.json`；09:40 viewer 复核证据；10:00 admin 查看审计。

## 算法与业务边界

这是**真实数据库行锁的受控演练**，两类演练都只在隔离的 `trial_balances` 镜像账上执行，原样本台账始终不变。

### 单样本锁等待（single，原演练保留）

每次以样本可用量建立独立 `trial_balances` 镜像行，不扣减原台账。A 使用 `SELECT FOR UPDATE` 持锁并写未提交回执；Python threading.Event 确保 B 在 A 持锁期间运行。B 先写自己的回执，再以指定 `lock_timeout` 请求同一行锁；PostgreSQL 返回 SQLSTATE 55P03 后完整 ROLLBACK。B 结束事件释放 A，A 扣减 remaining、增加 received 并 COMMIT。没有随机结果、sleep 假等待或假 SQL 计划。

### 双样本交叉移交死锁（cross，新增）

输入两笔指向**不同样本**的流转申请。两个独立连接 T1/T2 在同一时刻各自 `FOR UPDATE` 锁住自己那份镜像账行，并在同一事务内写未提交回执、做未提交扣量；随后双方再去争对方持有的那一行，构成真实死锁环。`deadlock_timeout` 先于兜底 `lock_timeout` 触发，由 PostgreSQL 死锁检测自行选出唯一牺牲者并返回 SQLSTATE 40P01，页面与导出保留数据库给出的真实错误文本（含两个 Backend PID 的等待环）。

- 牺牲者整笔 ROLLBACK：未提交回执与镜像账扣量同时回滚，不留残量。
- 胜者在对方回滚后拿到第二把锁并 COMMIT；随后**以发起时的原申请量重试牺牲者**，同一 actor 的回执唯一约束保证重试不产生第二条回执、不重复扣量。
- 任务只有走到 `completed` 才算发布成功；终态判据是两份镜像账各自 `remaining + received == initial`、各 received 等于对应申请量、回执恰为 T1/T2 各一条、阶段账含一次 40P01 回滚与两次提交。

### 可靠执行与中断恢复

- 领取走数据库条件 UPDATE（queued，或租约已过期的 running）并换发 `run_token`：并发领取只有一个执行者成功，队列消息重复投递不会重复执行。
- worker 每 5 秒按令牌续租（30 秒租约）；启动与空闲巡检会把租约过期的中断任务重新入队。
- 每个真实阶段独立事务写入 `trial_attempts` 阶段账本（deadlock / retry）；恢复时按阶段跳过已完成步骤，已提交的扣量与回执不会重做。
- 终态写回校验令牌与 running 状态：旧执行者即使恢复运行，其 finish 也会被拒绝，不能覆盖新执行者的结果。

输入：流转申请（cross 需两笔不同样本的申请）、50–2000ms 锁等待/死锁检测上限。输出：实际 PID、错误码、等待耗时、剩余量、接收量、回执数、阶段账和 SHA256。等待耗时受主机负载影响，SHA 包含 PID 和实际耗时因此每次不同；算法输入、死锁牺牲者选择口径与最终不变量可复现。失败态包括申请量超限、两申请指向同样本、数据库错误、线程协调超时、队列不可用；页面显示错误并允许创建新演练。

演练仅操作隔离镜像账本，不能用作生产样本库存扣减；单 worker 顺序运行，崩溃后的恢复依赖租约与阶段账本自动完成，无需人工重建任务。

## 工程说明

|服务|技术|职责|
|---|---|---|
|frontend|原生HTML/CSS/JS + Nginx|4业务页+登录，反向代理 /api/|
|backend|Python3.11/FastAPI/SQLAlchemy/PyJWT|校验、权限、审计、持久化|
|worker|Python3.11/psycopg|真实双连接锁竞争与交叉死锁；令牌领取、租约心跳、阶段恢复|
|db|PostgreSQL16|业务数据、隔离镜像账本|
|redis|Redis7|持久队列|

目录：`frontend/src/{api,stores,types,components/common,hooks,pages,router,utils,constants}`；`backend/src/{routes,controllers,services,models,schemas,middlewares,utils,constants,config}`；`backend/scripts/seed_demo.py`；`backend/migrations/001_initial.sql`。
核心实体 Sample、Transfer、Trial 各有模型、schema、服务、路由、API、store与页面；共享 table/badge/frame 组件。共享枚举 roles/run states/trial kinds 位于后端 `src/constants/enums.py`、模型/权限引用和前端 `src/constants/enums.js`、frame/badge；状态枚举 queued/running/completed/failed，演练类型 single/cross。`trial_balances` 以 (trial_id, sample_id) 复合主键支持双样本镜像账，`trial_attempts` 持久化每次尝试与回滚。两个后端中间件：request_id/errors。旧库启动时由 `src/config/migrate.py` 幂等升级，不丢失既有演练数据。

环境变量见 `.env.example`：COMPOSE_PROJECT_NAME、三端口、DB_PASSWORD、JWT_SECRET、SEED_DEMO。默认密码仅用于本地演示；JWT8小时有效。seed按自然键仅插入缺失行，不覆盖修改。所有5服务有健康检查，backend/worker依赖健康数据库/Redis，frontend依赖backend；db/redis/artifacts使用命名卷。停止及清空本项目数据：`docker compose down -v --remove-orphans`。

本地开发：Python3.11安装 backend/requirements.txt；设置 DATABASE_URL/REDIS_URL 后 `uvicorn src.main:app --reload`（backend目录）；worker `python -m src.worker`；前端用Nginx同源代理后端。前端双字体经Google Fonts加载，离线使用浏览器字体回退；业务不依赖外部字体。

## License
MIT，见 LICENSE。测试脚本、截图与实际结果位于源码外 `../../test-artifacts/gb-4103/`。
