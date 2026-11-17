import {table} from '../components/common/table.js';
import {badge} from '../components/common/badge.js';
import {esc,date} from '../utils/html.js';
import {TRIAL_KINDS} from '../constants/enums.js';
function conservation(r){
  if(r.kind==='cross'){
    return `<div class="verdict"><div class="verdict-icon">${r.invariant_passed?'✓':'!'}</div><div><span class="eyebrow">CONSERVATION CHECK · 双样本</span><h2>${r.invariant_passed?'两份镜像账守恒，牺牲者已按原量重试':'守恒或回执校验未通过'}</h2><p>死锁牺牲者 事务 ${esc(r.deadlock_victim)}（SQLSTATE ${esc(r.victim_sqlstate)}）回执与扣量已整笔回滚；重试沿用申请量 ${r.retry_quantity} 份，重复扣量：${r.retry_duplicate_deduction?'是':'否'}</p></div></div>
    <div class="section-top"><h2>两份演练账守恒核对</h2><span>原台账不参与扣减</span></div>
    ${table(['镜像样本 / 申请','初始','剩余','已接收','守恒'],r.samples.map(s=>[`<strong>${esc(s.sample)}</strong><small>${esc(s.transfer)} · 申请 ${s.requested_quantity} 份</small>`,s.initial,s.remaining,s.received,s.conservation_passed?'<b class="ok">✓ 守恒</b>':'<b class="bad">异常</b>']))}
    <div class="equations">${r.samples.map(s=>`<div class="equation">${s.remaining}<span> + </span>${s.received}<span> = </span>${s.initial}</div>`).join('')}</div>`;
  }
  return `<div class="verdict"><div class="verdict-icon">${r.invariant_passed?'✓':'!'}</div><div><span class="eyebrow">CONSERVATION CHECK</span><h2>${r.invariant_passed?'回滚完整，数量守恒':'不变量检查未通过'}</h2><p>${r.initial} 份 = 剩余 ${r.remaining} 份 + 已接收 ${r.received} 份 · 提交回执 ${r.receipt_count} 条</p></div><div class="equation">${r.remaining}<span> + </span>${r.received}<span> = </span>${r.initial}</div></div>`;
}
function tracesBlock(r){
  if(r.kind==='cross'){
    return `<div class="section-top"><h2>尝试与回滚阶段账</h2><span>每一步独立事务持久化，中断可按阶段恢复</span></div>
    ${table(['事务','阶段','Backend PID','SQLSTATE','结果 / 真实错误'],r.attempts.map(a=>[`<strong>事务 ${esc(a.actor)}</strong>`,a.phase==='deadlock'?'死锁竞争':'牺牲者重试',a.pid??'—',a.sqlstate?esc(a.sqlstate):'—',a.rolled_back?`整笔回滚（回执+扣量）${a.wait_ms!=null?` · 等待 ${a.wait_ms} ms`:''}${a.error?`<small>${esc(a.error)}</small>`:''}`:(a.committed?'提交成功 · 未重复扣量':'—')]))}
    <div class="section-top"><h2>数据库事务轨迹</h2><span>证据来自实际连接与错误码</span></div>
    ${table(['连接','Backend PID','事务事件','等待 / SQLSTATE'],r.traces.map(t=>[`<strong>${esc(t.actor)}</strong>`,t.pid??'—',esc(t.event+(t.event_detail?' / '+t.event_detail:'')),t.sqlstate?`${t.wait_ms} ms / ${esc(t.sqlstate)}`:(t.quantity!=null?`原申请量 ${t.quantity} 份`:'真实死锁环')]))}
    <div class="evidence-notes"><section><h3>数据库选出的牺牲者</h3><p>事务 ${esc(r.deadlock_victim)} 在等待对方行锁时收到 PostgreSQL 死锁检测中止（40P01），完整 ROLLBACK；其未提交回执与镜像账扣量同时消失。</p><p>${esc(r.victim_error||'')}</p></section><section><h3>复现摘要</h3><p>${esc(r.mode)}</p><code>${esc(r.checksum)}</code></section></div>`;
  }
  return `<div class="section-top"><h2>数据库事务轨迹</h2><span>证据来自实际连接与错误码</span></div>${table(['连接','Backend PID','事务事件','等待 / SQLSTATE'],r.traces.map(t=>[`<strong>事务 ${esc(t.actor)}</strong>`,t.pid??'—',esc(t.event),t.sqlstate?`${t.wait_ms} ms / ${esc(t.sqlstate)}`:(t.event==='row_lock_acquired'?'已获取行锁':t.recovered?'恢复后复核确认':'提交成功')]))}<div class="evidence-notes"><section><h3>判定口径</h3><p>${esc(r.formula)}</p><p>B 事务先插入的回执，已随 lock_timeout 事务回滚。A 持锁等待由线程事件协调，未伪造等待结果。</p></section><section><h3>复现摘要</h3><p>${esc(r.mode)}</p><code>${esc(r.checksum)}</code></section></div>`;
}
export function evidencePage(trial){if(!trial)return '<div class="empty">尚无冲突证据。请到「并发执行」发起演练，完成后在此复核事务回滚结果。</div>';const r=trial.result;const kind=trial.kind||'single';return `<div class="evidence-top"><div><span class="eyebrow">EVIDENCE / #${trial.id} · ${esc(TRIAL_KINDS[kind]||kind)}</span><h2>${esc(trial.name)} ${badge(trial.status)}</h2><p>${date(trial.created_at)} · PostgreSQL 真实连接 · 领取 ${trial.attempts||0} 次${trial.worker_id?' · '+esc(trial.worker_id):''}</p></div><button id="export" data-id="${trial.id}">导出 JSON ↓</button></div>${trial.error?`<div class="error">${esc(trial.error)}。请检查配置后重新发起演练。</div>`:''}${r?`${conservation(r)}${kind==='cross'?`<div class="notice">最终回执 ${r.receipt_count} 条：${r.receipts.map(x=>`事务 ${esc(x.actor)} ${x.quantity} 份`).join('；')}。牺牲者仅在重试阶段产生一条回执，死锁阶段的回执已随事务回滚，不重复扣量。</div>`:''}${tracesBlock(r)}`:'<div class="notice">任务正在后台执行，页面每 1.5 秒刷新。中断恢复时阶段账保证不重复执行已完成步骤；可保留此页或稍后从运行列表返回。</div>'}`;}
