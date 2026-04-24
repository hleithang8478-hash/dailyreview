import logging
import re
from collections import Counter, defaultdict, deque
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    session,
    send_file,
    jsonify,
    flash,
    g,
    has_request_context,
)
import sqlite3
import os
import time
import io
from openpyxl import Workbook
import requests
from datetime import datetime, timedelta
import tempfile
import hashlib
import secrets
from functools import wraps
import calendar as cal_lib
import pandas as pd
import json
import threading


# 配置日志记录 - 优化为简洁且完整的日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# 创建专用日志器，用于不同场景
app_logger = logging.getLogger('app')
query_logger = logging.getLogger('query')  # 查询相关日志（简化输出）
error_logger = logging.getLogger('error')  # 错误日志（详细输出）

# 设置查询日志器为WARNING级别，减少正常查询的日志输出
query_logger.setLevel(logging.WARNING)

# DeepSeek API 配置（优先环境变量 DEEPSEEK_API_KEY）
DEEPSEEK_API_URL = "https://api.deepseek.com/v1/chat/completions"
DEEPSEEK_API_KEY = os.environ.get("DEEPSEEK_API_KEY", "sk-ccde4a29c7a44f6486854d96a80c8f26")

def generate_prompt_for_day(row):
    """
    为某一天的数据生成提示词
    """
    date_str = row["date"].strftime("%Y-%m-%d")

    prompt = f"""
    以下是 {date_str} 的市场分析和工作总结：
    - 整体行情：{row['overall_market']}
    - 事件&政策&热点&异常：{row['events']}
    - 情绪周期：{row['emotion_cycle']}
    - 市场风格：{row['market_style']}
    - 基本面&资金面：{row['fundamentals']}
    - 国际热点：{row['international_hotspots']}
    - 前瞻点&思考总结：{row['insights']}
    - 每日金股：{row['golden_stocks']}

    请根据以上内容：
    1. 发表你的看法，如果提到了行业、个股，尽量按照产业链和事件驱动机会来进行联想。
    2. 分析我的思考总结，指出其中的亮点和不足，重点是你对我思考总结的看法，如果我的观点和看法有逻辑错误，请指出并给出答案。
    3. 提供你的拓展思考（请特别标注“【DeepSeek 拓展】”），多联想一些，给我更多的方向，有一些关于财经、股票、政策的知识点，每次生成都给我三个。
    4. 如果我分析了多天的数据，请将多天的数据上下文联系起来，分析多天的变化趋势，或者有无热点延续。
    5. 对于每日金股，指出其中个股的行业，主要合作伙伴，主营业务。
    6. 今天发生的一些你觉得重要的事情，要你自己去搜索思考，而不是从我的原文中去摘录。
    7. 要求所有回答都联网。、
    8. 不需要复述我的原文，直接开始你的回答就可以了，避免我忘记说什么去翻阅原文，你可以稍加提醒。
    """
    # 正常操作不记录详细内容
    # logging.debug(f"Generated prompt for {date_str}")
    return prompt

def call_deepseek_api(prompt):
    """
    调用 DeepSeek API 获取总结和拓展
    """
    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type": "application/json"
    }
    payload = {
        "model": "deepseek-reasoner",  # 使用 deepseek-reasoner 模型
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.7,
        "max_tokens": 5000,
        "web_search": True  # 启用联网搜索
    }
    max_retries = 3  # 最大重试次数（减少到3次，避免过长等待）
    retries = 0
    
    while retries < max_retries:
        try:
            response = requests.post(DEEPSEEK_API_URL, headers=headers, json=payload, timeout=120)
            # 只在错误时记录状态码
            # logging.debug(f"API response status: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                content = result.get("choices", [{}])[0].get("message", {}).get("content", "No response returned.")
                if content and content != "No response returned.":
                    return content
                else:
                    # 重试是正常情况，不记录每次重试
                    # logging.debug("API 返回空内容，重试...")
                    retries += 1
                    time.sleep(2)  # 等待2秒后重试
                    continue
            else:
                logging.error(f"API 返回错误状态码: {response.status_code}, 响应: {response.text}")
                if response.status_code == 401:
                    raise Exception("API 密钥无效，请检查配置")
                elif response.status_code == 429:
                    logging.warning("API 请求频率过高，等待后重试")
                    retries += 1
                    time.sleep(5)  # 等待5秒后重试
                    continue
                else:
                    retries += 1
                    time.sleep(2)
                    continue
                    
        except requests.exceptions.Timeout:
            # 只在最后一次重试失败时记录
            if retries == max_retries - 1:
                logging.warning(f"API 请求超时，已重试 {max_retries} 次")
            # logging.debug(f"API 请求超时，重试第 {retries + 1} 次...")
            retries += 1
            if retries < max_retries:
                time.sleep(3)
                continue
            else:
                raise Exception("API 请求超时，请稍后重试")
                
        except requests.exceptions.RequestException as req_err:
            logging.error(f"请求异常: {req_err}")
            retries += 1
            if retries < max_retries:
                time.sleep(2)
                continue
            else:
                raise Exception(f"API 请求失败: {str(req_err)}")
                
        except ValueError as value_err:
            if "Expecting value" in str(value_err):
                # 只在最后一次重试失败时记录
                if retries == max_retries - 1:
                    logging.warning(f"JSON 解析错误，已重试 {max_retries} 次")
                # logging.debug(f"JSON 解析错误，重试第 {retries + 1} 次...")
                retries += 1
                if retries < max_retries:
                    time.sleep(2)
                    continue
                else:
                    raise Exception("API 响应格式错误，无法解析")
            else:
                raise Exception(f"数据解析错误: {str(value_err)}")
                
        except Exception as e:
            logging.error(f"API 请求失败: {e}")
            if retries < max_retries - 1:
                retries += 1
                time.sleep(2)
                continue
            else:
                raise Exception(f"API 请求失败: {str(e)}")
    
    raise Exception("API 请求失败，已达到最大重试次数")


def _extract_json_object_string(text):
    """从模型输出中取出 JSON 字符串（兼容 ```json 包裹）。"""
    if not text or not str(text).strip():
        raise ValueError("模型返回为空")
    s = str(text).strip()
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.IGNORECASE)
        s = re.sub(r"\s*```$", "", s)
    return s.strip()


def call_deepseek_json_chat(messages, temperature=0.2, max_tokens=2500, timeout=90):
    """
    调用 DeepSeek Chat，要求返回 JSON 对象（response_format=json_object）。
    返回已解析的 dict。
    """
    headers = {
        "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": "deepseek-chat",
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "response_format": {"type": "json_object"},
    }
    last_err = None
    for attempt in range(3):
        try:
            response = requests.post(
                DEEPSEEK_API_URL, headers=headers, json=payload, timeout=timeout
            )
            if response.status_code == 200:
                result = response.json()
                content = (
                    result.get("choices", [{}])[0]
                    .get("message", {})
                    .get("content", "")
                )
                blob = _extract_json_object_string(content)
                return json.loads(blob)
            if response.status_code == 401:
                raise ValueError("DeepSeek API 密钥无效，请检查 DEEPSEEK_API_KEY")
            last_err = f"HTTP {response.status_code}: {response.text[:500]}"
        except json.JSONDecodeError as e:
            last_err = f"JSON 解析失败: {e}"
        except requests.exceptions.Timeout:
            last_err = "请求超时"
        except requests.exceptions.RequestException as e:
            last_err = str(e)
        time.sleep(2 ** attempt)
    raise ValueError(last_err or "DeepSeek 调用失败")


_INVESTMENT_PLAN_AI_SYSTEM = """你是投研笔记结构化助手。用户会给出一段自然语言投研笔记。
你必须只输出一个 JSON 对象（不要 markdown、不要解释）。用户消息里会包含「今天是 YYYY-MM-DD（公历）」，用于换算相对日期。

【必须先识别的三项】
1) instruments (string): 投资标的（股票/ETF/指数/可转债/期货主力等）。中文简称、全名、代码（如 宁德时代、300750）。多项英文逗号分隔。也可用 investment_targets 字段名（与 instruments 二选一，内容相同）。无则 ""。
2) tracking_items (array): 跟踪事项。每条为 {"title":"简短标题","date":"YYYY-MM-DD 或 null","detail":"需关注的变化、催化剂、风险、验证点等"}。凡原文提到的关键事件（财报、会议、数据发布、政策窗口、技术验证等）尽量拆成多条；有明确日期则填 date，否则 null。至少一条时 title 不可为空。无任何可拆事项则 []。
3) target_date (string 或 null): 本计划层面的目标/复盘/观察截止日 YYYY-MM-DD（与单条 tracking_items 可并存）。相对日期结合「今天是…」换算；2026Q1末等季度末按：Q1→03-31，Q2→06-30，Q3→09-30，Q4→12-31；H1末→06-30；H2末→12-31。

【其余字段】
- title (string, 必填): 计划标题，点出核心标的或主题。
- description (string): 完整说明；逻辑与观察点写清楚。
- status (string): todo、in_progress、done、cancelled；未提及则 todo
- priority (string): low、medium、high；默认 medium
- category (string): 如 股票投资；无则 ""
- tags (string): 英文逗号分隔；无则 ""
- keywords (string): 资讯爬虫用补充词（题材、行业关键词等）；系统会把 instruments 与 keywords 合并去重，缺省可 ""。
- progress (number): 0-100；未提及则 0
- color (string 或 null): #RRGGBB；无法判断则 null
- is_profitable (number 或 null): 仅已了结且明确盈/亏时为 1 或 0

除 target_date、color、is_profitable、tracking_items 内外层 date 外，字符串缺省用 ""；tracking_items 无事项时用 []。"""


def _coerce_comma_separated_text(val):
    if val is None:
        return ""
    if isinstance(val, list):
        parts = []
        for x in val:
            s = str(x).strip()
            if s:
                parts.append(s)
        return ",".join(parts)
    return str(val).strip()


def _split_and_merge_keywords(*chunks):
    """按中英文逗号/顿号拆分，去重（大小写不敏感），保持先出现的顺序。"""
    parts = []
    for ch in chunks:
        if ch is None:
            continue
        if isinstance(ch, list):
            for x in ch:
                s = str(x).strip()
                if s:
                    parts.extend(re.split(r"[,，、;；]+", s))
        else:
            s = str(ch).strip()
            if s:
                parts.extend(re.split(r"[,，、;；]+", s))
    seen = set()
    out = []
    for raw in parts:
        t = (raw or "").strip()
        if not t:
            continue
        k = t.casefold()
        if k in seen:
            continue
        seen.add(k)
        out.append(t)
    return ",".join(out)


def _quarter_end_iso(year, quarter):
    """公历季度末日 YYYY-MM-DD。"""
    last_m = {1: 3, 2: 6, 3: 9, 4: 12}[quarter]
    last_d = cal_lib.monthrange(year, last_m)[1]
    return f"{year:04d}-{last_m:02d}-{last_d:02d}"


def _month_end_iso(year, month):
    if not (1 <= month <= 12):
        return ""
    last_d = cal_lib.monthrange(year, month)[1]
    return f"{year:04d}-{month:02d}-{last_d:02d}"


def _infer_target_date_from_notes(text, ref_date=None):
    """
    从笔记中启发式提取目标日期（不调用模型）。用于「2026Q1末」等模型常返回 null 的情况。
    命中则返回 YYYY-MM-DD，否则 ""。
    """
    if not text or not str(text).strip():
        return ""
    ref = ref_date or datetime.now().date()
    ref_y = ref.year
    s = str(text)
    cn_q = r"[一二三四1-4]"

    def _year_from_context():
        y_m = re.search(r"(20\d{2}|19\d{2})", s)
        return int(y_m.group(1)) if y_m else ref_y

    # 2026Q1末 / (2026Q1末) / 2026年Q1末 / 2026-Q1末
    m = re.search(
        r"(?P<y>20\d{2}|19\d{2})\s*年?\s*[-]?\s*[Qq](?P<q>[1-4])\s*末?",
        s,
    )
    if m:
        return _quarter_end_iso(int(m.group("y")), int(m.group("q")))

    # 2026年第1季度末 / 2026年一季度末
    qmap = {"一": 1, "二": 2, "三": 3, "四": 4, "1": 1, "2": 2, "3": 3, "4": 4}
    m = re.search(
        rf"(?P<y>20\d{{2}}|19\d{{2}})\s*年\s*第?\s*(?P<q>{cn_q})\s*季度\s*末",
        s,
    )
    if m:
        q = qmap.get(m.group("q"))
        if q:
            return _quarter_end_iso(int(m.group("y")), q)

    # 2026H1末 / 2026h2末
    m = re.search(r"(?P<y>20\d{2}|19\d{2})\s*[Hh](?P<h>[12])\s*末?", s)
    if m:
        y = int(m.group("y"))
        return f"{y:04d}-06-30" if m.group("h") == "1" else f"{y:04d}-12-31"

    # 上半年末 / 下半年末（优先取文中四位年份，否则当年）
    if re.search(r"上半年\s*末", s):
        return f"{_year_from_context():04d}-06-30"
    if re.search(r"下半年\s*末", s):
        return f"{_year_from_context():04d}-12-31"

    # 一季度末（无「年」时：取文中年份，否则当年）
    m = re.search(rf"第?\s*(?P<q>{cn_q})\s*季度\s*末", s)
    if m:
        q = qmap.get(m.group("q"))
        if q:
            y = _year_from_context()
            return _quarter_end_iso(y, q)

    # 2026年3月底 / 2026年03月末
    m = re.search(
        r"(?P<y>20\d{2}|19\d{2})\s*年\s*(?P<mo>\d{1,2})\s*月\s*(底|末)",
        s,
    )
    if m:
        y, mo = int(m.group("y")), int(m.group("mo"))
        iso = _month_end_iso(y, mo)
        if iso:
            return iso

    return ""


def _normalize_tracking_items_list(raw):
    """统一为 [{title, date, detail}, ...]，date 为 YYYY-MM-DD 或 None。"""
    if raw is None:
        return []
    if isinstance(raw, str):
        s = raw.strip()
        if not s:
            return []
        try:
            raw = json.loads(s)
        except json.JSONDecodeError:
            return []
    if not isinstance(raw, list):
        return []
    out = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        title = (item.get("title") or item.get("name") or "").strip()
        if not title:
            continue
        date = item.get("date") or item.get("event_date")
        ds = None
        if date is not None and str(date).strip():
            tds = str(date).strip()[:32]
            if re.match(r"^\d{4}-\d{2}-\d{2}$", tds):
                ds = tds
        detail = (item.get("detail") or item.get("note") or item.get("description") or "").strip()
        out.append({"title": title, "date": ds, "detail": detail})
    return out


def _tracking_items_form_to_json(text):
    """表单文本 → tracking_items JSON 字符串入库。支持 JSON 数组，或「YYYY-MM-DD 事项」逐行。"""
    s = (text or "").strip()
    if not s:
        return "[]"
    try:
        data = json.loads(s)
        normalized = _normalize_tracking_items_list(data)
        return json.dumps(normalized, ensure_ascii=False)
    except json.JSONDecodeError:
        pass
    items = []
    for line in s.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        m = re.match(r"^(\d{4}-\d{2}-\d{2})[：:\s]+\s*(.+)$", line)
        if m:
            items.append({"title": m.group(2).strip(), "date": m.group(1), "detail": ""})
            continue
        m2 = re.match(r"^(\d{4}-\d{2}-\d{2})\s+(.+)$", line)
        if m2:
            items.append({"title": m2.group(2).strip(), "date": m2.group(1), "detail": ""})
            continue
        items.append({"title": line, "date": None, "detail": ""})
    return json.dumps(items, ensure_ascii=False)


def _tracking_items_json_to_list(stored):
    if not stored or not str(stored).strip():
        return []
    try:
        return _normalize_tracking_items_list(json.loads(stored))
    except json.JSONDecodeError:
        return []


def _plan_tuple_to_dict(plan):
    """SELECT * FROM investment_plans 一行 → dict（兼容列数）。"""
    if not plan:
        return None
    return {
        "id": plan[0],
        "title": plan[1],
        "description": plan[2] or "",
        "status": plan[3] or "todo",
        "priority": plan[4] or "medium",
        "category": plan[5],
        "tags": plan[6],
        "target_date": plan[7],
        "progress": plan[8] or 0,
        "color": plan[9] or "#f59e0b",
        "is_profitable": plan[10] if len(plan) > 10 else None,
        "created_at": plan[11] if len(plan) > 11 else None,
        "updated_at": plan[12] if len(plan) > 12 else None,
        "keywords": (plan[13] or "") if len(plan) > 13 else "",
        "instruments": (plan[14] or "") if len(plan) > 14 else "",
        "tracking_items": (plan[15] or "") if len(plan) > 15 else "",
    }


def _insert_plan_calendar_events(plan_id, row, sync_calendar=True):
    """
    为计划写入日历：计划 target_date 一条 + 每条带日期的 tracking_item 一条。
    row 须含 target_date、title、description、color、tracking_items（list[dict]）。
    返回新建事件 id 列表。
    """
    if not sync_calendar or not plan_id:
        return []
    ids = []
    color = (row.get("color") or "#6366f1")[:32]
    title_base = (row.get("title") or "投资计划")[:180]
    td = (row.get("target_date") or "").strip()
    if td and re.match(r"^\d{4}-\d{2}-\d{2}$", td):
        try:
            ev_title = ("计划节点 · " + title_base)[:200]
            snippet = (row.get("description") or "")[:2000]
            ids.append(
                insert_db_return_id(
                    """INSERT INTO investment_calendar
                    (title, content, start_date, end_date, reminder_type,
                     reminder_time, color, event_type, event_category, related_stock, related_plan_id)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        ev_title,
                        snippet,
                        td,
                        td,
                        "notification",
                        0,
                        color,
                        "single",
                        "other",
                        "",
                        plan_id,
                    ),
                )
            )
        except Exception as e:
            logging.warning("计划 target_date 同步日历失败 plan_id=%s: %s", plan_id, e)

    for it in row.get("tracking_items") or []:
        if not isinstance(it, dict):
            continue
        d = (it.get("date") or "") or ""
        d = str(d).strip() if d else ""
        tit = (it.get("title") or "").strip()
        det = (it.get("detail") or "").strip()
        if not tit or not d or not re.match(r"^\d{4}-\d{2}-\d{2}$", d):
            continue
        try:
            ev_title = ("跟踪 · " + tit)[:200]
            content = (det or tit)[:2000]
            ids.append(
                insert_db_return_id(
                    """INSERT INTO investment_calendar
                    (title, content, start_date, end_date, reminder_type,
                     reminder_time, color, event_type, event_category, related_stock, related_plan_id)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                    (
                        ev_title,
                        content,
                        d,
                        d,
                        "notification",
                        0,
                        color,
                        "single",
                        "other",
                        "",
                        plan_id,
                    ),
                )
            )
        except Exception as e:
            logging.warning("跟踪事项同步日历失败 plan_id=%s title=%s: %s", plan_id, tit, e)
    return ids


def _normalize_ai_investment_plan(d):
    """将模型 JSON 规范为 investment_plans 一行字段（dict，便于 API 与前端）。"""
    if not isinstance(d, dict):
        raise ValueError("模型返回不是 JSON 对象")
    title = (d.get("title") or "").strip()
    if not title:
        raise ValueError("解析结果缺少标题 title")
    description = (d.get("description") or "").strip()
    category = (d.get("category") or "").strip()
    tags = _coerce_comma_separated_text(d.get("tags"))
    inst_raw = d.get("instruments") if d.get("instruments") not in (None, "") else d.get("investment_targets")
    instruments = _coerce_comma_separated_text(inst_raw)
    keywords = _split_and_merge_keywords(instruments, d.get("keywords"))
    if not keywords.strip():
        keywords = instruments
    tracking_items = _normalize_tracking_items_list(d.get("tracking_items"))

    praw = (d.get("priority") or "medium").strip().lower()
    if praw not in ("low", "medium", "high"):
        praw = "medium"

    raw_status = (d.get("status") or "todo").strip().lower()
    if raw_status not in ("todo", "in_progress", "done", "cancelled"):
        raw_status = "todo"

    prog = d.get("progress")
    try:
        progress = int(prog)
    except (TypeError, ValueError):
        progress = 0
    progress = max(0, min(100, progress))
    if raw_status == "done":
        progress = max(progress, 100)

    if raw_status == "cancelled":
        st = "cancelled"
    elif progress >= 100:
        st = "done"
        progress = 100
    elif progress == 0:
        st = "todo"
    else:
        st = "in_progress"

    td = d.get("target_date")
    target_date = ""
    if td is not None and str(td).strip():
        tds = str(td).strip()[:32]
        if re.match(r"^\d{4}-\d{2}-\d{2}$", tds):
            target_date = tds

    color = (d.get("color") or "").strip()
    if not re.match(r"^#[0-9A-Fa-f]{6}$", color):
        color = "#f59e0b"

    is_profitable = d.get("is_profitable")
    ip = None
    if is_profitable is not None and is_profitable != "":
        try:
            ip = int(is_profitable)
        except (TypeError, ValueError):
            ip = None
    if ip is not None and ip not in (0, 1):
        ip = None
    if st != "done":
        ip = None

    return {
        "title": title,
        "description": description,
        "status": st,
        "priority": praw,
        "category": category,
        "tags": tags,
        "instruments": instruments,
        "keywords": keywords,
        "tracking_items": tracking_items,
        "target_date": target_date,
        "progress": progress,
        "color": color,
        "is_profitable": ip,
    }


def ai_parse_investment_plan_from_text(user_notes):
    """调用 DeepSeek 将自然语言转为投资计划字段 dict（已规范化）。"""
    notes = (user_notes or "").strip()
    if len(notes) < 8:
        raise ValueError("描述过短，请补充时间、标的或逻辑等信息")
    today_str = datetime.now().strftime("%Y-%m-%d")
    messages = [
        {"role": "system", "content": _INVESTMENT_PLAN_AI_SYSTEM},
        {
            "role": "user",
            "content": f"今天是 {today_str}（公历）。\n\n用户笔记：\n{notes}",
        },
    ]
    raw = call_deepseek_json_chat(messages)
    row = _normalize_ai_investment_plan(raw)
    if not (row.get("target_date") or "").strip():
        inferred = _infer_target_date_from_notes(
            notes, ref_date=datetime.strptime(today_str, "%Y-%m-%d").date()
        )
        if inferred:
            row["target_date"] = inferred
    return row


app = Flask(__name__)
# 数据库与密钥相对 app.py 目录，避免从不同工作目录启动时读写「另一个」database.db（典型症状：登录成功立刻回登录页）
_APP_ROOT = os.path.dirname(os.path.abspath(__file__))
DATABASE_FILE = os.path.join(_APP_ROOT, 'database.db')
# 生成安全的密钥（如果不存在则生成新的）
SECRET_KEY_FILE = os.path.join(_APP_ROOT, 'secret_key.txt')
if os.path.exists(SECRET_KEY_FILE):
    with open(SECRET_KEY_FILE, 'r') as f:
        app.secret_key = f.read().strip()
else:
    app.secret_key = secrets.token_hex(32)
    with open(SECRET_KEY_FILE, 'w') as f:
        f.write(app.secret_key)
    # 正常操作不记录
    # logging.debug("已生成新的安全密钥")

SESSION_TIMEOUT = 30 * 60  # 30分钟超时（更安全）
MAX_LOGIN_ATTEMPTS = 5  # 最大登录尝试次数
LOGIN_LOCKOUT_TIME = 15 * 60  # 锁定15分钟

# 登录/RBAC 流水日志：默认开启；关闭可设环境变量 AUTH_FLOW_LOG=0
AUTH_FLOW_LOG = os.environ.get('AUTH_FLOW_LOG', '1').lower() not in ('0', 'false', 'no')


def _auth_log(event, **fields):
    """统一前缀 [auth]，便于 grep / 接入日志系统。"""
    if not AUTH_FLOW_LOG:
        return
    try:
        tail = ' '.join('%s=%r' % (k, v) for k, v in sorted(fields.items()))
    except Exception:
        tail = ''
    logging.info('[auth] %s | %s', event, tail)


def _auth_permission_codes_preview(username, limit=40):
    try:
        codes = sorted(_rbac_get_user_permission_codes(username))
        return {'count': len(codes), 'codes': codes[:limit]}
    except Exception as e:
        return {'error': str(e)}


def _login_page_diag(**extra):
    """登录页下发给浏览器控制台与 JSON-LD 的诊断字段（勿在生产对公网暴露敏感信息时可关 AUTH_FLOW_LOG）。"""
    d = {
        'step': 'login_page',
        'time': datetime.now().isoformat(timespec='seconds'),
        'path': getattr(request, 'path', '') or '',
        'endpoint': getattr(request, 'endpoint', None),
        'method': getattr(request, 'method', '') or '',
        'remote_addr': getattr(request, 'remote_addr', None),
        'session_username': session.get('username'),
        'database_file': DATABASE_FILE,
    }
    if extra:
        d.update(extra)
    return d


@app.context_processor
def inject_rbac_template_helpers():
    """模板中 has_perm('code')：只认角色-权限表里实际挂上的 code。

    刻意不用 user_has_permission：后者对配置超管用户名、以及部分兜底逻辑会「恒为真」，
    导致首页日记区、灰显按钮等 UI 权限开关看起来「怎么改都不生效」。
    """
    def has_perm(code):
        u = session.get('username')
        if not u or not code:
            return False
        try:
            owned = _rbac_get_user_permission_codes(u)
            if not owned and has_request_context() and not getattr(g, '_rbac_has_perm_repair', False):
                g._rbac_has_perm_repair = True
                _rbac_repair_login_access(u)
                owned = _rbac_get_user_permission_codes(u)
            return code in owned
        except Exception:
            return False

    return dict(has_perm=has_perm)


# ==================== 动量评分日志收集器 ====================
# 用于在前端显示执行进度
momentum_logs = {}  # {task_id: deque([log1, log2, ...])}
momentum_logs_lock = threading.Lock()
MOMENTUM_LOG_MAX_SIZE = 1000  # 每个任务最多保存1000条日志


class MomentumLogHandler(logging.Handler):
    """自定义日志处理器，将日志收集到内存中"""
    def __init__(self, task_id):
        super().__init__()
        self.task_id = task_id
    
    def emit(self, record):
        try:
            log_msg = self.format(record)
            # 过滤掉一些不重要的日志
            if any(skip in log_msg for skip in ['GET /', 'POST /', 'HTTP/1.1', '172.16.', '127.0.0.1']):
                return
            
            timestamp = datetime.now().strftime('%H:%M:%S')
            log_entry = f"[{timestamp}] {log_msg}"
            
            with momentum_logs_lock:
                if self.task_id not in momentum_logs:
                    momentum_logs[self.task_id] = deque(maxlen=MOMENTUM_LOG_MAX_SIZE)
                momentum_logs[self.task_id].append(log_entry)
        except Exception:
            pass  # 忽略日志收集错误，避免影响主流程


def get_momentum_logs(task_id, last_index=0):
    """获取任务日志（从指定索引开始）"""
    with momentum_logs_lock:
        if task_id not in momentum_logs:
            return [], 0
        
        logs = list(momentum_logs[task_id])
        if last_index >= len(logs):
            return [], len(logs)
        
        return logs[last_index:], len(logs)


def clear_momentum_logs(task_id):
    """清理任务日志"""
    with momentum_logs_lock:
        if task_id in momentum_logs:
            del momentum_logs[task_id]

# 访问日志表
def init_security_tables(conn, c):
    """初始化安全相关的表"""
    # 登录尝试记录表
    c.execute('''CREATE TABLE IF NOT EXISTS login_attempts
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                 ip_address TEXT NOT NULL,
                 username TEXT,
                 attempt_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                 success INTEGER DEFAULT 0)''')
    
    # 访问日志表
    c.execute('''CREATE TABLE IF NOT EXISTS access_logs
                 (id INTEGER PRIMARY KEY AUTOINCREMENT,
                 username TEXT,
                 ip_address TEXT,
                 endpoint TEXT,
                 method TEXT,
                 access_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    
    conn.commit()

# 密码哈希函数
def hash_password(password):
    """使用SHA256哈希密码（加盐）"""
    salt = secrets.token_hex(16)
    password_hash = hashlib.sha256((password + salt).encode()).hexdigest()
    return f"{salt}:{password_hash}"

def verify_password(password, stored_hash):
    """验证密码"""
    try:
        salt, password_hash = stored_hash.split(':')
        return hashlib.sha256((password + salt).encode()).hexdigest() == password_hash
    except:
        return False

# 检查登录尝试次数
def check_login_attempts(ip_address, username=None):
    """检查登录尝试次数是否超限"""
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    
    # 检查最近15分钟内的失败尝试
    c.execute("""SELECT COUNT(*) FROM login_attempts 
                 WHERE ip_address = ? AND success = 0 
                 AND attempt_time > datetime('now', '-15 minutes')""", 
              (ip_address,))
    failed_attempts = c.fetchone()[0]
    
    conn.close()
    return failed_attempts < MAX_LOGIN_ATTEMPTS

# 记录登录尝试
def log_login_attempt(ip_address, username, success):
    """记录登录尝试"""
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    c.execute("INSERT INTO login_attempts (ip_address, username, success) VALUES (?, ?, ?)",
              (ip_address, username, 1 if success else 0))
    conn.commit()
    conn.close()

# 记录访问日志
def log_access(username, endpoint, method):
    """记录访问日志"""
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        ip_address = request.remote_addr
        c.execute("INSERT INTO access_logs (username, ip_address, endpoint, method) VALUES (?, ?, ?, ?)",
                  (username, ip_address, endpoint, method))
        conn.commit()
        conn.close()
    except:
        pass  # 日志记录失败不影响主功能

# 登录验证装饰器
def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'username' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function


# ==================== RBAC 权限模型（用户-角色-权限）====================

def _init_rbac_tables(c):
    """创建角色、权限及关联表。"""
    c.execute('''CREATE TABLE IF NOT EXISTS permissions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        code TEXT NOT NULL UNIQUE,
        name TEXT NOT NULL,
        module TEXT,
        description TEXT)''')
    c.execute('''CREATE TABLE IF NOT EXISTS roles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        slug TEXT NOT NULL UNIQUE,
        description TEXT,
        is_system INTEGER NOT NULL DEFAULT 0)''')
    c.execute('''CREATE TABLE IF NOT EXISTS role_permissions (
        role_id INTEGER NOT NULL,
        permission_id INTEGER NOT NULL,
        PRIMARY KEY (role_id, permission_id))''')
    c.execute('''CREATE TABLE IF NOT EXISTS user_roles (
        user_id INTEGER NOT NULL,
        role_id INTEGER NOT NULL,
        PRIMARY KEY (user_id, role_id))''')
    c.execute('''CREATE TABLE IF NOT EXISTS rbac_migrations (
        name TEXT PRIMARY KEY
    )''')


# 权限字典：代码中新增权限码时由 init_db 写入 permissions；admin 始终拥有全部。member 的默认授权由迁移/管理员显式配置决定，不再自动「全家桶」。
_RBAC_ALL_PERMS = [
    ('app:access', '基础访问', 'app', '写随笔/复盘、AI 分析、导出、市场基调与指数设置、首页内联编辑等'),
    ('app:index', '首页入口', 'app', '登录后可进入首页与统计壳；不含写随笔/复盘/AI/导出'),
    ('diary:content', '首页日记区', 'diary', '首页「今日市场数据」与「最近记录」等日记相关展示'),
    ('rbac:admin', '系统权限管理', 'rbac', '用户管理、角色管理、权限分配（全局）'),
    ('crawler:dashboard', '爬虫看板', 'crawler', '爬虫数据看板与词云'),
    ('calendar:module', '投资日历', 'calendar', '日历视图与提醒'),
    ('plans:module', '投资计划', 'plans', '投资计划与进度'),
    ('reports:module', '深度报告', 'reports', '深度报告管理'),
    ('concept_stocks:module', '概念板块', 'concept', '概念板块与行业树'),
    ('market_sentiment:module', '市场情绪', 'sentiment', '市场情绪看板与相关接口'),
    ('topics:module', '主题研究', 'topics', '主题与卡片'),
    ('industry_inventory:module', '行业库存', 'industry', '行业库存分析'),
    ('data_query:module', '数据查询', 'data_query', '数据查询构建器'),
    ('screening_tasks:module', '筛选任务', 'screening', '筛选任务与历史'),
    ('momentum_scores:module', '动量评分', 'momentum', '动量评分'),
    ('portfolio:module', '组合分析', 'portfolio', '组合导入与分析'),
]


def _index_entry_permission_codes():
    """进入首页 `/`：持有任一已登记权限即可（OR），便于「仅有模块权」用户进入壳页。"""
    return tuple(p[0] for p in _RBAC_ALL_PERMS)


# 该用户名始终绑定「系统管理员」角色（即使库中曾缺失，也会在 init_db / 登录时补全）。匹配时不区分大小写、忽略首尾空格。
RBAC_SUPERUSER_USERNAME = 'tanjiarong'

# ---------- users 主键约定（全链路一致）----------
# SQLite 下 API 返回的「用户 id」= COALESCE(users.id, users.rowid)。user_roles.user_id 存同一数值。
# 任意「按用户查 / 改 / 删」必须用 WHERE COALESCE(id,rowid)=?；与 user_roles 连接必须用 COALESCE(u.id,u.rowid)=ur.user_id。
# 避免出现列表里是 1、PUT 却 WHERE id=1 查不到（id 为 NULL 时）→「用户不存在」。


def _rbac_is_configured_superuser_login_name(name):
    su = (RBAC_SUPERUSER_USERNAME or '').strip().lower()
    if not su:
        return False
    return (name or '').strip().lower() == su


def _rbac_ensure_superuser_admin_for_user_id(user_id):
    """为指定用户 id 绑定 admin 角色（INSERT OR IGNORE）。"""
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        _ensure_core_roles(c)
        _sync_permission_definitions_and_grants(c)
        c.execute('SELECT id FROM roles WHERE slug=?', ('admin',))
        ra = c.fetchone()
        if ra:
            c.execute(
                'INSERT OR IGNORE INTO user_roles (user_id, role_id) VALUES (?,?)',
                (user_id, ra[0]),
            )
        conn.commit()
        conn.close()
    except Exception as e:
        logging.error('_rbac_ensure_superuser_admin_for_user_id: %s', e, exc_info=True)


def _ensure_core_roles(c):
    c.execute('SELECT id FROM roles WHERE slug=?', ('admin',))
    if c.fetchone() is None:
        c.execute(
            "INSERT INTO roles (name, slug, description, is_system) VALUES (?,?,?,?)",
            ('系统管理员', 'admin', '拥有 rbac:admin，可管理用户与权限', 1),
        )
    c.execute('SELECT id FROM roles WHERE slug=?', ('member',))
    if c.fetchone() is None:
        c.execute(
            "INSERT INTO roles (name, slug, description, is_system) VALUES (?,?,?,?)",
            ('普通成员', 'member', '默认业务访问', 1),
        )


def _rbac_migrate_scrub_member_overgrants_once(c):
    """一次性迁移：历史上误把大量权限同步到 member，先清空仅保留 app:access，由管理员在后台再精细分配。"""
    c.execute('SELECT 1 FROM rbac_migrations WHERE name=? LIMIT 1', ('scrub_member_overgrants_v1',))
    if c.fetchone():
        return
    c.execute('SELECT id FROM roles WHERE slug=?', ('member',))
    mr = c.fetchone()
    if not mr:
        return
    member_id = mr[0]
    c.execute('DELETE FROM role_permissions WHERE role_id=?', (member_id,))
    c.execute('SELECT id FROM permissions WHERE code=?', ('app:access',))
    row_ac = c.fetchone()
    if row_ac:
        c.execute(
            'INSERT OR IGNORE INTO role_permissions (role_id, permission_id) VALUES (?,?)',
            (member_id, row_ac[0]),
        )
    c.execute('INSERT OR IGNORE INTO rbac_migrations (name) VALUES (?)', ('scrub_member_overgrants_v1',))
    logging.info(
        '[rbac_sync] scrub_member_overgrants_v1: reset member role_permissions to app:access only (member_id=%s)',
        member_id,
    )


def _rbac_migrate_member_shell_index_v2(c):
    """一次性：member 默认不应持有 app:access（否则首页写随笔/复盘/AI 始终可点）。改为仅 app:index，写操作需显式授予 app:access。"""
    c.execute('SELECT 1 FROM rbac_migrations WHERE name=? LIMIT 1', ('member_shell_app_index_v2',))
    if c.fetchone():
        return
    c.execute('SELECT id FROM roles WHERE slug=?', ('member',))
    mr = c.fetchone()
    if not mr:
        return
    member_id = mr[0]
    c.execute(
        'DELETE FROM role_permissions WHERE role_id=? AND permission_id IN (SELECT id FROM permissions WHERE code=?)',
        (member_id, 'app:access'),
    )
    c.execute('SELECT id FROM permissions WHERE code=?', ('app:index',))
    row_ix = c.fetchone()
    if row_ix:
        c.execute(
            'INSERT OR IGNORE INTO role_permissions (role_id, permission_id) VALUES (?,?)',
            (member_id, row_ix[0]),
        )
    c.execute('INSERT OR IGNORE INTO rbac_migrations (name) VALUES (?)', ('member_shell_app_index_v2',))
    logging.info(
        '[rbac_sync] member_shell_app_index_v2: member role no longer defaults to app:access; ensured app:index (member_id=%s)',
        member_id,
    )


def _sync_permission_definitions_and_grants(c):
    """确保 permissions 表与代码一致；admin 拥有全部权限；member 不再因「新权限码」自动获权。"""
    c.execute('SELECT id FROM roles WHERE slug=?', ('admin',))
    ra = c.fetchone()
    c.execute('SELECT id FROM roles WHERE slug=?', ('member',))
    rm = c.fetchone()
    if not ra or not rm:
        return False
    admin_id, member_id = ra[0], rm[0]
    for code, name, module, desc in _RBAC_ALL_PERMS:
        c.execute('SELECT id FROM permissions WHERE code=?', (code,))
        row = c.fetchone()
        if row:
            pid = row[0]
        else:
            c.execute(
                'INSERT INTO permissions (code, name, module, description) VALUES (?,?,?,?)',
                (code, name, module, desc),
            )
            pid = c.lastrowid
        c.execute(
            'INSERT OR IGNORE INTO role_permissions (role_id, permission_id) VALUES (?,?)',
            (admin_id, pid),
        )
    _rbac_migrate_scrub_member_overgrants_once(c)
    _rbac_migrate_member_shell_index_v2(c)
    # 仅当 member 角色尚未关联任何权限时补挂 app:index（旧库空白兜底），不自动给 app:access。
    # 禁止在每次 sync 时无条件 INSERT：否则管理员从 member 上摘掉 app:access / diary:content 后，
    # 会在下一次 _rbac_repair_login_access / bootstrap 时又被塞回去，首页权限开关「永远关不掉」。
    c.execute('SELECT COUNT(*) FROM role_permissions WHERE role_id = ?', (member_id,))
    _mc = c.fetchone()
    if _mc and int(_mc[0]) == 0:
        c.execute('SELECT id FROM permissions WHERE code=?', ('app:index',))
        row_ix = c.fetchone()
        if row_ix:
            c.execute(
                'INSERT OR IGNORE INTO role_permissions (role_id, permission_id) VALUES (?,?)',
                (member_id, row_ix[0]),
            )
            logging.info(
                '[rbac_sync] member_role had zero permissions; auto_granted app:index only '
                '(member_id=%s permission_id=%s)',
                member_id,
                row_ix[0],
            )
    return True


def _rbac_assign_superuser_admin(c):
    c.execute(
        'SELECT COALESCE(id, rowid) FROM users WHERE lower(trim(username)) = lower(trim(?))',
        ((RBAC_SUPERUSER_USERNAME or '').strip(),),
    )
    tu = c.fetchone()
    c.execute('SELECT id FROM roles WHERE slug=?', ('admin',))
    ra = c.fetchone()
    if tu and ra:
        c.execute(
            'INSERT OR IGNORE INTO user_roles (user_id, role_id) VALUES (?,?)',
            (tu[0], ra[0]),
        )


def _merge_rbac_endpoint_permission_map():
    """Flask view 函数名 → 所需权限（未列出则默认 app:access）。集中与 before_request 一致。"""
    m = {}
    adm = ('rbac:admin',)
    cr = ('crawler:dashboard',)
    cal = ('calendar:module',)
    pl = ('plans:module',)
    rep = ('reports:module',)
    con = ('concept_stocks:module',)
    sen = ('market_sentiment:module',)
    top = ('topics:module',)
    ind = ('industry_inventory:module',)
    dq = ('data_query:module',)
    scr = ('screening_tasks:module',)
    mom = ('momentum_scores:module',)
    prt = ('portfolio:module',)
    for name in (
        'migrate_data', 'clean_duplicate_migrations', 'admin_rbac_page',
        'api_rbac_permissions_list', 'api_rbac_roles_list', 'api_rbac_roles_create', 'api_rbac_roles_delete',
        'api_rbac_role_permissions_update', 'api_rbac_users_list', 'api_rbac_users_create', 'api_rbac_users_update',
        'api_rbac_users_delete',
    ):
        m[name] = adm
    for name in (
        'crawler_dashboard', 'crawler_dashboard_wordcloud', 'api_crawler_dashboard_overview',
        'api_crawler_dashboard_channel_summary', 'api_crawler_dashboard_wordcloud',
        'api_crawler_wordcloud_blacklist_get', 'api_crawler_wordcloud_blacklist_post', 'api_crawler_wordcloud_blacklist_delete',
        'api_crawler_dashboard_latest', 'api_crawler_dashboard_export',
        'api_crawler_dashboard_feed_state',
        'api_crawler_search',
        'timeline_page', 'api_timeline_add', 'api_timeline_list', 'api_timeline_delete',
    ):
        m[name] = cr
    for name in (
        'calendar', 'add_calendar_event', 'delete_calendar_event', 'save_calendar_event',
        'get_calendar_events', 'check_reminders', 'get_lunar_date',
    ):
        m[name] = cal
    for name in (
        'plans', 'add_plan', 'delete_plan', 'update_plan_status', 'update_plan_progress', 'update_plan_profitable',
    ):
        m[name] = pl
    for name in ('reports', 'add_report', 'delete_report', 'view_report'):
        m[name] = rep
    for name in (
        'concept_stocks', 'add_concept_stock', 'delete_concept_stock', 'get_concept_stocks_list', 'get_concept_stocks',
        'recognize_stocks_from_images', 'get_industries_hierarchy', 'get_industry_stocks',
    ):
        m[name] = con
    for name in (
        'market_sentiment',
        'api_market_sentiment_cached', 'api_market_sentiment_sparkline', 'api_market_sentiment_metric_history',
        'api_market_sentiment_report', 'api_market_sentiment_ai_report', 'api_market_sentiment_score',
        'api_market_sentiment_range_score', 'api_market_sentiment_range_score_stream', 'api_market_sentiment_range_export',
    ):
        m[name] = sen
    for name in (
        'topics', 'add_topic', 'delete_topic', 'edit_topic', 'save_topic_info', 'save_card', 'delete_card',
        'save_connection', 'delete_connection',
    ):
        m[name] = top
    for name in ('industry_inventory', 'search_stock', 'get_industries', 'analyze_stock', 'analyze_industry'):
        m[name] = ind
    for name in (
        'data_query_config', 'data_query_builder', 'get_table_configs', 'delete_table_config', 'get_table_fields',
        'save_field_config', 'delete_field_config', 'validate_fields', 'get_all_stocks', 'search_stock_for_query',
        'get_sql_templates', 'save_sql_template', 'delete_sql_template', 'execute_template_query',
        'execute_multi_table_query', 'execute_data_query', 'recognize_table_from_text', 'recognize_table_from_image',
        'save_table_config',
    ):
        m[name] = dq
    for name in (
        'screening_tasks', 'get_task_types', 'execute_screening_task', 'screening_task_history',
        'get_task_result', 'download_task_result',
    ):
        m[name] = scr
    for name in (
        'momentum_scores_page', 'api_run_momentum_scores', 'api_get_momentum_logs', 'api_get_momentum_scores',
        'api_get_momentum_config', 'api_get_latest_trading_date', 'api_get_momentum_dates',
    ):
        m[name] = mom
    for name in (
        'portfolio_analysis', 'parse_excel_headers', 'save_field_mapping', 'import_portfolio_data', 'get_field_mappings',
        'preprocess_portfolio_data', 'get_market_value_analysis', 'get_stock_changes_analysis',
        'get_industry_changes_analysis', 'get_buy_performance_analysis', 'get_portfolio_list',
        'get_portfolio_date_range', 'get_trading_summary', 'get_stock_metrics',
    ):
        m[name] = prt
    m['index'] = _index_entry_permission_codes()
    return m


RBAC_ENDPOINT_PERMISSIONS = _merge_rbac_endpoint_permission_map()


def _rbac_required_permission_codes(endpoint):
    """未出现在映射表中的已登录视图默认需要 app:access。"""
    if not endpoint:
        return ('app:access',)
    return RBAC_ENDPOINT_PERMISSIONS.get(endpoint, ('app:access',))


def _seed_rbac_data(conn, c):
    """确保内置角色与权限目录；无角色用户挂 member；最小 id 用户与指定超管挂 admin。"""
    _ensure_core_roles(c)
    if not _sync_permission_definitions_and_grants(c):
        return
    c.execute('SELECT id FROM roles WHERE slug=?', ('member',))
    row_m = c.fetchone()
    if not row_m:
        return
    member_id = row_m[0]
    c.execute('SELECT id FROM roles WHERE slug=?', ('admin',))
    row_a = c.fetchone()
    admin_id = row_a[0] if row_a else None
    # 部分旧库 users.id 可能为 NULL，min(None,...) 会报错；用 rowid 兜底并过滤无效行
    c.execute('SELECT COALESCE(id, rowid) FROM users')
    user_ids = []
    for r in c.fetchall():
        if not r or r[0] is None:
            continue
        try:
            user_ids.append(int(r[0]))
        except (TypeError, ValueError):
            continue
    for uid in user_ids:
        c.execute('SELECT 1 FROM user_roles WHERE user_id=?', (uid,))
        if c.fetchone() is None:
            c.execute(
                'INSERT OR IGNORE INTO user_roles (user_id, role_id) VALUES (?,?)',
                (uid, member_id),
            )
    if admin_id and user_ids:
        first_uid = min(user_ids)
        c.execute(
            'INSERT OR IGNORE INTO user_roles (user_id, role_id) VALUES (?,?)',
            (first_uid, admin_id),
        )
    _rbac_assign_superuser_admin(c)


def _rbac_get_user_permission_codes(username):
    """返回用户通过角色继承到的权限 code 集合。"""
    if not username:
        return frozenset()
    uname = (username or '').strip()
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute(
            'SELECT COALESCE(id, rowid) FROM users WHERE lower(trim(username)) = lower(trim(?)) LIMIT 1',
            (uname,),
        )
        uid_row = c.fetchone()
        if not uid_row:
            conn.close()
            return frozenset()
        user_pk = int(uid_row[0])
        c.execute('PRAGMA table_info(users)')
        user_cols = {row[1] for row in c.fetchall()}
        has_active = 'is_active' in user_cols
        if has_active:
            c.execute(
                '''SELECT DISTINCT p.code FROM permissions p
                   INNER JOIN role_permissions rp ON rp.permission_id = p.id
                   INNER JOIN user_roles ur ON ur.role_id = rp.role_id
                   INNER JOIN users u ON COALESCE(u.id, u.rowid) = ur.user_id
                   WHERE COALESCE(u.id, u.rowid) = ? AND COALESCE(u.is_active, 1) = 1''',
                (user_pk,),
            )
        else:
            c.execute(
                '''SELECT DISTINCT p.code FROM permissions p
                   INNER JOIN role_permissions rp ON rp.permission_id = p.id
                   INNER JOIN user_roles ur ON ur.role_id = rp.role_id
                   INNER JOIN users u ON COALESCE(u.id, u.rowid) = ur.user_id
                   WHERE COALESCE(u.id, u.rowid) = ?''',
                (user_pk,),
            )
        codes = frozenset(row[0] for row in c.fetchall() if row and row[0])
        conn.close()
        return codes
    except Exception as e:
        logging.error('读取用户权限失败 username=%r: %s', username, e, exc_info=True)
        return frozenset()


def _rbac_log_home_permission_snapshot(username, owned_codes, did_repair, show_diary, can_write):
    """首页权限诊断：控制台/文件里 grep `[rbac_home]`。"""
    u = (username or '').strip()
    if not u:
        logging.warning(
            '[rbac_home] index_perm session_username_empty db=%s show_diary_zone=%s can_app_write=%s',
            DATABASE_FILE,
            show_diary,
            can_write,
        )
        return
    role_slugs = []
    user_pk = None
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute(
            'SELECT COALESCE(id, rowid) FROM users WHERE lower(trim(username)) = lower(trim(?)) LIMIT 1',
            (u,),
        )
        row = c.fetchone()
        if not row:
            logging.warning(
                '[rbac_home] index_perm user_row_missing username=%r db=%s owned_sorted=%s did_repair=%s '
                'show_diary_zone=%s can_app_write=%s',
                u,
                DATABASE_FILE,
                sorted(owned_codes),
                did_repair,
                show_diary,
                can_write,
            )
            conn.close()
            return
        user_pk = int(row[0])
        c.execute(
            '''SELECT r.slug FROM roles r
               INNER JOIN user_roles ur ON ur.role_id = r.id
               WHERE ur.user_id = ?
               ORDER BY r.slug''',
            (user_pk,),
        )
        role_slugs = [x[0] for x in c.fetchall() if x and x[0]]
        conn.close()
    except Exception as e:
        logging.exception('[rbac_home] index_perm role_query_failed username=%r err=%s', u, e)

    logging.info(
        '[rbac_home] index_perm username=%r user_pk=%s db=%s roles=%s perm_count=%s '
        'diary_in_owned=%s app_in_owned=%s show_diary_zone=%s can_app_write=%s did_repair=%s codes=%s',
        u,
        user_pk,
        DATABASE_FILE,
        role_slugs,
        len(owned_codes),
        'diary:content' in owned_codes,
        'app:access' in owned_codes,
        show_diary,
        can_write,
        did_repair,
        sorted(owned_codes),
    )


def _rbac_repair_login_access(username):
    """为已存在用户补全 RBAC 表、角色与 member/超管绑定，解决无 user_roles 或库路径不一致导致的「权限恒为空」。"""
    if not username:
        return
    _auth_log('rbac_repair_begin', username=username, database_file=DATABASE_FILE)
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        try:
            c.execute('ALTER TABLE users ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1')
        except sqlite3.OperationalError:
            pass
        _init_rbac_tables(c)
        _ensure_core_roles(c)
        _sync_permission_definitions_and_grants(c)
        c.execute(
            'SELECT COALESCE(id, rowid) FROM users WHERE lower(trim(username)) = lower(trim(?)) LIMIT 1',
            ((username or '').strip(),),
        )
        ur = c.fetchone()
        if not ur:
            conn.close()
            _auth_log('rbac_repair_skip_no_user', username=username)
            return
        uid = int(ur[0])
        c.execute('SELECT id FROM roles WHERE slug=?', ('member',))
        mr = c.fetchone()
        if mr:
            c.execute(
                'INSERT OR IGNORE INTO user_roles (user_id, role_id) VALUES (?,?)',
                (uid, mr[0]),
            )
        _rbac_assign_superuser_admin(c)
        conn.commit()
        conn.close()
        snap = _auth_permission_codes_preview(username)
        _auth_log('rbac_repair_ok', username=username, **snap)
    except Exception as e:
        logging.error('_rbac_repair_login_access(%r): %s', username, e, exc_info=True)
        _auth_log('rbac_repair_fail', username=username, error=str(e))


def user_has_permission(username, *codes, require_all=False):
    """若持有 rbac:admin 则视为拥有全部已定义能力；否则按 codes 校验。"""
    if not username:
        return False
    uname = (username or '').strip()
    # 配置的超管：只要库中存在该账号（不区分大小写），一律放行并强制挂上 admin，避免 RBAC 数据异常时把自己锁在登录页外
    if _rbac_is_configured_superuser_login_name(uname):
        uid_row = query_db(
            'SELECT COALESCE(id, rowid) FROM users WHERE lower(trim(username)) = lower(trim(?)) LIMIT 1',
            (uname,),
            one=True,
        )
        if uid_row and uid_row[0] is not None:
            _rbac_ensure_superuser_admin_for_user_id(int(uid_row[0]))
            return True
    owned = _rbac_get_user_permission_codes(uname)
    if not owned:
        skip = has_request_context() and getattr(g, '_rbac_repair_tried', False)
        if not skip:
            if has_request_context():
                g._rbac_repair_tried = True
            _rbac_repair_login_access(username)
            owned = _rbac_get_user_permission_codes(username)
    if 'rbac:admin' in owned:
        return True
    if not codes:
        return 'app:access' in owned
    if require_all:
        return all(c in owned for c in codes)
    return any(c in owned for c in codes)


def permission_required(*codes, require_all=False):
    """要求已登录且具备权限（与全局 before_request 策略一致：API 403，页面清空会话后跳转登录）。"""
    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            if 'username' not in session:
                return redirect(url_for('login'))
            uname = session.get('username')
            if not user_has_permission(uname, *codes, require_all=require_all):
                _auth_log(
                    'decorator_permission_denied',
                    username=uname,
                    endpoint=getattr(request, 'endpoint', None),
                    path=getattr(request, 'path', ''),
                    required=list(codes),
                    preview=_auth_permission_codes_preview(uname),
                )
                if request.path.startswith('/api/') or request.is_json or (
                    request.accept_mimetypes and request.accept_mimetypes.best == 'application/json'
                ):
                    return jsonify({'success': False, 'error': '没有权限执行此操作'}), 403
                session.pop('username', None)
                session.pop('last_activity', None)
                return redirect(url_for('login'))
            return f(*args, **kwargs)
        return wrapped
    return decorator


_rbac_bootstrap_done = False


def _rbac_bootstrap_once():
    """进程内首次请求时同步 RBAC（与 init_db 中 RBAC 段一致），避免未跑 init_db 或旧库缺列时权限恒为空、登录后立刻被踢回登录页。"""
    global _rbac_bootstrap_done
    if _rbac_bootstrap_done:
        return
    _auth_log('rbac_bootstrap_begin', database_file=DATABASE_FILE)
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        try:
            c.execute('ALTER TABLE users ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1')
        except sqlite3.OperationalError:
            pass
        _init_rbac_tables(c)
        _seed_rbac_data(conn, c)
        conn.commit()
        conn.close()
        _rbac_bootstrap_done = True
        _auth_log('rbac_bootstrap_ok', database_file=DATABASE_FILE)
    except Exception as e:
        logging.exception('RBAC 引导失败（请检查 %s）: %s', DATABASE_FILE, e)
        _auth_log('rbac_bootstrap_fail', database_file=DATABASE_FILE, error=str(e))


def _rbac_assign_role_slugs(user_id, conn, c, role_slugs):
    """用角色 slug 列表覆盖该用户的角色（事务外需 commit）。"""
    c.execute('DELETE FROM user_roles WHERE user_id=?', (user_id,))
    for slug in role_slugs or []:
        c.execute('SELECT id FROM roles WHERE slug=?', (slug,))
        r = c.fetchone()
        if r:
            c.execute(
                'INSERT OR IGNORE INTO user_roles (user_id, role_id) VALUES (?,?)',
                (user_id, r[0]),
            )


def _rbac_user_id_by_username(username):
    """返回与 /api/rbac/users 列表中 id 相同的用户主键（COALESCE(id,rowid)）。"""
    row = query_db(
        'SELECT COALESCE(id, rowid) FROM users WHERE lower(trim(username)) = lower(trim(?)) LIMIT 1',
        ((username or '').strip(),),
        one=True,
    )
    if not row or row[0] is None:
        return None
    try:
        return int(row[0])
    except (TypeError, ValueError):
        return None


# 初始化数据库
def init_db():
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        
        # 用户表
        c.execute('''CREATE TABLE IF NOT EXISTS users
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                     username TEXT NOT NULL UNIQUE,
                     password TEXT NOT NULL)''')

        # 爬虫标题词云 · 用户自定义黑名单（点击词云加入，仅当前登录用户）
        c.execute('''CREATE TABLE IF NOT EXISTS crawler_wordcloud_blacklist (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     username TEXT NOT NULL,
                     word TEXT NOT NULL,
                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                     UNIQUE(username, word))''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_cwb_username ON crawler_wordcloud_blacklist(username)')

        # 盘中异动 / 历史复盘时间轴（SQLite，与爬虫看板联动）
        c.execute('''CREATE TABLE IF NOT EXISTS market_timeline (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     date TEXT NOT NULL,
                     time TEXT NOT NULL,
                     title TEXT NOT NULL,
                     url TEXT,
                     remark TEXT,
                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
        c.execute('CREATE INDEX IF NOT EXISTS idx_market_timeline_date ON market_timeline(date)')

        try:
            c.execute('ALTER TABLE users ADD COLUMN is_active INTEGER NOT NULL DEFAULT 1')
        except sqlite3.OperationalError:
            pass

        _init_rbac_tables(c)
        
        # 随笔表 - 一天可以有多个随笔卡片
        c.execute('''CREATE TABLE IF NOT EXISTS essays
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                     date TEXT NOT NULL,
                     content TEXT,
                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
        
        # 复盘表 - 固定模式的复盘，一天一条
        c.execute('''CREATE TABLE IF NOT EXISTS reviews
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                     date TEXT NOT NULL UNIQUE,
                     market_emotion TEXT,
                     sectors TEXT,
                     themes TEXT,
                     market_cap_performance TEXT,
                     investment_style TEXT,
                     major_events TEXT,
                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
        
        # 投资日历表 - 支持单日、多日、周期事件
        c.execute('''CREATE TABLE IF NOT EXISTS investment_calendar
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                     title TEXT NOT NULL,
                     content TEXT,
                     start_date TEXT NOT NULL,
                     end_date TEXT,
                     reminder_type TEXT DEFAULT 'notification',
                     reminder_time INTEGER DEFAULT 0,
                     reminder_sent INTEGER DEFAULT 0,
                     color TEXT DEFAULT '#6366f1',
                     event_type TEXT DEFAULT 'single',
                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
        
        # 投资日历：投研分类、关联股票、关联交易计划
        for _mig in (
            "ALTER TABLE investment_calendar ADD COLUMN event_category TEXT DEFAULT 'other'",
            "ALTER TABLE investment_calendar ADD COLUMN related_stock TEXT",
            "ALTER TABLE investment_calendar ADD COLUMN related_plan_id INTEGER",
        ):
            try:
                c.execute(_mig)
            except sqlite3.OperationalError as e:
                if "duplicate column" not in str(e).lower():
                    logging.debug("投资日历表迁移: %s", e)
        
        # 投资计划表 - 参考Notion/Linear的设计
        c.execute('''CREATE TABLE IF NOT EXISTS investment_plans
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                     title TEXT NOT NULL,
                     description TEXT,
                     status TEXT DEFAULT 'todo',
                     priority TEXT DEFAULT 'medium',
                     category TEXT,
                     tags TEXT,
                     target_date TEXT,
                     progress INTEGER DEFAULT 0,
                     color TEXT DEFAULT '#f59e0b',
                     is_profitable INTEGER,
                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
        
        # 检查并添加 is_profitable 字段（如果不存在）
        try:
            c.execute("ALTER TABLE investment_plans ADD COLUMN is_profitable INTEGER")
            conn.commit()
            # 数据库迁移操作，只在首次执行时记录
            logging.info("数据库迁移: 已添加 is_profitable 字段")
        except sqlite3.OperationalError:
            # 字段已存在，忽略错误
            pass
        # 投研计划：资讯追踪关键词（逗号分隔，软关联爬虫）
        try:
            c.execute("ALTER TABLE investment_plans ADD COLUMN keywords TEXT")
            conn.commit()
            logging.info("数据库迁移: 已添加 investment_plans.keywords 字段")
        except sqlite3.OperationalError:
            pass
        try:
            c.execute("ALTER TABLE investment_plans ADD COLUMN instruments TEXT")
            conn.commit()
            logging.info("数据库迁移: 已添加 investment_plans.instruments 字段")
        except sqlite3.OperationalError:
            pass
        try:
            c.execute("ALTER TABLE investment_plans ADD COLUMN tracking_items TEXT")
            conn.commit()
            logging.info("数据库迁移: 已添加 investment_plans.tracking_items 字段")
        except sqlite3.OperationalError:
            pass
        
        # 市场基调表 - 每日市场基调设置
        c.execute('''CREATE TABLE IF NOT EXISTS market_tone
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                     date TEXT NOT NULL UNIQUE,
                     volume_status TEXT,
                     emotion_status TEXT,
                     divergence_status TEXT,
                     investment_strategies TEXT,
                     investment_styles TEXT,
                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
        
        # 指数走势表 - 每日三大指数走势
        c.execute('''CREATE TABLE IF NOT EXISTS index_trend
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                     date TEXT NOT NULL UNIQUE,
                     shanghai_trend TEXT,
                     shenzhen_trend TEXT,
                     chinext_trend TEXT,
                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')

        # 每日市场情绪结果表（保存最近计算结果，页面可直接展示）
        c.execute('''CREATE TABLE IF NOT EXISTS market_sentiment_results
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                     trading_day TEXT NOT NULL UNIQUE,
                     result_json TEXT NOT NULL,
                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
        try:
            c.execute('DELETE FROM market_sentiment_results WHERE rowid NOT IN (SELECT MAX(rowid) FROM market_sentiment_results GROUP BY trading_day)')
            c.execute('CREATE UNIQUE INDEX IF NOT EXISTS idx_msr_trading_day ON market_sentiment_results(trading_day)')
        except Exception:
            pass

        # 算法版本追踪 — 版本变更时清空旧缓存
        c.execute('''CREATE TABLE IF NOT EXISTS sentiment_meta
                     (key TEXT PRIMARY KEY, value TEXT)''')
        from market_sentiment import SENTIMENT_VERSION as _SV
        row = c.execute("SELECT value FROM sentiment_meta WHERE key='version'").fetchone()
        if row is None or row[0] != _SV:
            c.execute("DELETE FROM market_sentiment_results")
            c.execute("INSERT OR REPLACE INTO sentiment_meta (key, value) VALUES ('version', ?)", (_SV,))
            app_logger.info(f"情绪算法版本更新为 {_SV}，已清空旧缓存")
        
        # 深度报告和总结表
        c.execute('''CREATE TABLE IF NOT EXISTS deep_reports
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                     title TEXT NOT NULL,
                     content TEXT,
                     summary TEXT,
                     category TEXT,
                     tags TEXT,
                     related_plan_id INTEGER,
                     date TEXT,
                     sheet_types TEXT,
                     stock_codes TEXT,
                     concept_ids TEXT,
                     industry_names TEXT,
                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
        
        # 检查并添加新字段（向后兼容）
        try:
            c.execute("ALTER TABLE deep_reports ADD COLUMN sheet_types TEXT")
            conn.commit()
        except sqlite3.OperationalError:
            pass
        
        try:
            c.execute("ALTER TABLE deep_reports ADD COLUMN stock_codes TEXT")
            conn.commit()
        except sqlite3.OperationalError:
            pass
        
        try:
            c.execute("ALTER TABLE deep_reports ADD COLUMN concept_ids TEXT")
            conn.commit()
        except sqlite3.OperationalError:
            pass
        
        try:
            c.execute("ALTER TABLE deep_reports ADD COLUMN industry_names TEXT")
            conn.commit()
        except sqlite3.OperationalError:
            pass
        
        # 专题表
        c.execute('''CREATE TABLE IF NOT EXISTS topics
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                     title TEXT NOT NULL,
                     description TEXT,
                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
        
        # 专题卡片表（行业分析）
        c.execute('''CREATE TABLE IF NOT EXISTS topic_cards
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                     topic_id INTEGER NOT NULL,
                     title TEXT NOT NULL,
                     content TEXT,
                     card_type TEXT DEFAULT 'company',
                     x INTEGER DEFAULT 100,
                     y INTEGER DEFAULT 100,
                     width INTEGER DEFAULT 220,
                     height INTEGER DEFAULT 160,
                     color TEXT DEFAULT '#6366f1',
                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                     FOREIGN KEY (topic_id) REFERENCES topics(id) ON DELETE CASCADE)''')
        
        # 检查并添加card_type字段
        try:
            c.execute("ALTER TABLE topic_cards ADD COLUMN card_type TEXT DEFAULT 'company'")
            conn.commit()
            logging.info("数据库迁移: 已添加 card_type 字段")
        except sqlite3.OperationalError:
            pass
        
        # 卡片关系表（连线）- 行业关系
        c.execute('''CREATE TABLE IF NOT EXISTS card_connections
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                     topic_id INTEGER NOT NULL,
                     from_card_id INTEGER NOT NULL,
                     to_card_id INTEGER NOT NULL,
                     relation_type TEXT DEFAULT 'downstream',
                     connection_type TEXT DEFAULT 'bezier',
                     color TEXT DEFAULT '#999999',
                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                     FOREIGN KEY (topic_id) REFERENCES topics(id) ON DELETE CASCADE,
                     FOREIGN KEY (from_card_id) REFERENCES topic_cards(id) ON DELETE CASCADE,
                     FOREIGN KEY (to_card_id) REFERENCES topic_cards(id) ON DELETE CASCADE)''')
        
        # 检查并添加relation_type字段
        try:
            c.execute("ALTER TABLE card_connections ADD COLUMN relation_type TEXT DEFAULT 'downstream'")
            conn.commit()
            logging.info("数据库迁移: 已添加 relation_type 字段")
        except sqlite3.OperationalError:
            pass
        
        # 数据表配置表
        c.execute('''CREATE TABLE IF NOT EXISTS data_table_configs
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                     table_name TEXT NOT NULL UNIQUE,
                     table_display_name TEXT NOT NULL,
                     description TEXT,
                     primary_key_field TEXT,
                     date_field TEXT,
                     code_field TEXT,
                     name_field TEXT,
                     join_type TEXT DEFAULT 'CompanyCode',
                     join_field TEXT,
                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
        
        # 检查并添加join_type和join_field字段（向后兼容）
        try:
            c.execute("ALTER TABLE data_table_configs ADD COLUMN join_type TEXT DEFAULT 'CompanyCode'")
            conn.commit()
        except sqlite3.OperationalError:
            pass
        
        try:
            c.execute("ALTER TABLE data_table_configs ADD COLUMN join_field TEXT")
            conn.commit()
        except sqlite3.OperationalError:
            pass
        
        # 概念股表
        c.execute('''CREATE TABLE IF NOT EXISTS concept_stocks
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                     concept_name TEXT NOT NULL UNIQUE,
                     description TEXT,
                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
        
        # 概念股股票关联表
        c.execute('''CREATE TABLE IF NOT EXISTS concept_stock_items
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                     concept_id INTEGER NOT NULL,
                     stock_code TEXT NOT NULL,
                     stock_name TEXT,
                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                     FOREIGN KEY (concept_id) REFERENCES concept_stocks(id) ON DELETE CASCADE,
                     UNIQUE(concept_id, stock_code))''')
        
        # 字段配置表
        c.execute('''CREATE TABLE IF NOT EXISTS data_field_configs
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                     table_config_id INTEGER NOT NULL,
                     field_name TEXT NOT NULL,
                     field_display_name TEXT NOT NULL,
                     field_type TEXT DEFAULT 'TEXT',
                     description TEXT,
                     is_sortable INTEGER DEFAULT 1,
                     is_filterable INTEGER DEFAULT 1,
                     order_index INTEGER DEFAULT 0,
                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                     FOREIGN KEY (table_config_id) REFERENCES data_table_configs(id) ON DELETE CASCADE,
                     UNIQUE(table_config_id, field_name))''')
        
        # SQL查询模板表
        c.execute('''CREATE TABLE IF NOT EXISTS sql_query_templates
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                     template_name TEXT NOT NULL,
                     sql_template TEXT NOT NULL,
                     description TEXT,
                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
        
        # 筛选任务执行历史表
        c.execute('''CREATE TABLE IF NOT EXISTS screening_task_executions
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                     task_type TEXT NOT NULL,
                     task_name TEXT NOT NULL,
                     status TEXT NOT NULL DEFAULT 'pending',
                     output_file_path TEXT,
                     execution_params TEXT,
                     start_time TIMESTAMP,
                     end_time TIMESTAMP,
                     duration_seconds INTEGER,
                     error_message TEXT,
                     result_summary TEXT,
                     created_by TEXT,
                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
        
        # 动量评分结果表
        # 模拟仓字段映射配置表
        c.execute('''CREATE TABLE IF NOT EXISTS portfolio_field_mappings
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                     mapping_name TEXT NOT NULL UNIQUE,
                     is_default INTEGER DEFAULT 0,
                     mapping_config TEXT NOT NULL,
                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                     updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
        
        # 模拟仓数据表
        c.execute('''CREATE TABLE IF NOT EXISTS portfolio_data
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                     portfolio_name TEXT NOT NULL,
                     date TEXT NOT NULL,
                     stock_code TEXT NOT NULL,
                     stock_name TEXT,
                     stock_market TEXT,
                     position_quantity REAL,
                     position_value REAL,
                     buy_quantity REAL DEFAULT 0,
                     sell_quantity REAL DEFAULT 0,
                     avg_price REAL,
                     first_industry_name TEXT,
                     second_industry_name TEXT,
                     third_industry_name TEXT,
                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                     UNIQUE(portfolio_name, date, stock_code))''')
        
        c.execute('''CREATE TABLE IF NOT EXISTS momentum_scores
                     (id INTEGER PRIMARY KEY AUTOINCREMENT,
                      date TEXT NOT NULL,
                      code TEXT NOT NULL,
                      momentum_score REAL NOT NULL,
                      factors_json TEXT,
                      status TEXT,
                      status_start_date TEXT,
                      amplitude REAL,
                      lowest_price REAL,
                      highest_price REAL,
                      amount_std REAL,
                      duration_days INTEGER,
                      trend_return REAL,
                      has_volume_surge INTEGER,
                      five_day_acceleration INTEGER,
                      underlying_data_json TEXT,
                      -- 动量指标字段（核心指标）
                      return_1d REAL, return_5d REAL, return_10d REAL, return_20d REAL,
                      return_60d REAL, return_120d REAL, return_250d REAL,
                      amplitude_5d REAL, amplitude_20d REAL, amplitude_60d REAL,
                      daily_volatility REAL, annualized_volatility REAL,
                      max_gain_20d REAL, max_gain_60d REAL,
                      max_drawdown_20d REAL, max_drawdown_60d REAL,
                      recent_high REAL, recent_low REAL, price_vs_high REAL,
                      ma5_slope REAL, ma10_slope REAL, ma20_slope REAL,
                      price_vs_ma5 REAL, price_vs_ma20 REAL, ma5_vs_ma20 REAL,
                      trend_strength REAL,
                      volume_ratio_5d REAL, volume_ratio_20d REAL,
                      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                      UNIQUE(date, code))''')
        
        # 为现有表添加状态字段（如果不存在）
        try:
            c.execute("ALTER TABLE momentum_scores ADD COLUMN status TEXT")
        except sqlite3.OperationalError:
            pass  # 字段已存在
        try:
            c.execute("ALTER TABLE momentum_scores ADD COLUMN status_start_date TEXT")
        except sqlite3.OperationalError:
            pass
        try:
            c.execute("ALTER TABLE momentum_scores ADD COLUMN amplitude REAL")
        except sqlite3.OperationalError:
            pass
        try:
            c.execute("ALTER TABLE momentum_scores ADD COLUMN lowest_price REAL")
        except sqlite3.OperationalError:
            pass
        try:
            c.execute("ALTER TABLE momentum_scores ADD COLUMN highest_price REAL")
        except sqlite3.OperationalError:
            pass
        try:
            c.execute("ALTER TABLE momentum_scores ADD COLUMN amount_std REAL")
        except sqlite3.OperationalError:
            pass
        try:
            c.execute("ALTER TABLE momentum_scores ADD COLUMN duration_days INTEGER")
        except sqlite3.OperationalError:
            pass
        try:
            c.execute("ALTER TABLE momentum_scores ADD COLUMN trend_return REAL")
        except sqlite3.OperationalError:
            pass
        try:
            c.execute("ALTER TABLE momentum_scores ADD COLUMN has_volume_surge INTEGER")
        except sqlite3.OperationalError:
            pass
        try:
            c.execute("ALTER TABLE momentum_scores ADD COLUMN five_day_acceleration INTEGER")
        except sqlite3.OperationalError:
            pass
        try:
            c.execute("ALTER TABLE momentum_scores ADD COLUMN underlying_data_json TEXT")
        except sqlite3.OperationalError:
            pass
        
        # 为现有表添加动量指标字段（如果不存在）
        momentum_metrics_fields = [
            ('return_1d', 'REAL'), ('return_5d', 'REAL'), ('return_10d', 'REAL'), ('return_20d', 'REAL'),
            ('return_60d', 'REAL'), ('return_120d', 'REAL'), ('return_250d', 'REAL'),
            ('amplitude_5d', 'REAL'), ('amplitude_20d', 'REAL'), ('amplitude_60d', 'REAL'),
            ('daily_volatility', 'REAL'), ('annualized_volatility', 'REAL'),
            ('max_gain_20d', 'REAL'), ('max_gain_60d', 'REAL'),
            ('max_drawdown_20d', 'REAL'), ('max_drawdown_60d', 'REAL'),
            ('recent_high', 'REAL'), ('recent_low', 'REAL'), ('price_vs_high', 'REAL'),
            ('ma5_slope', 'REAL'), ('ma10_slope', 'REAL'), ('ma20_slope', 'REAL'),
            ('price_vs_ma5', 'REAL'), ('price_vs_ma20', 'REAL'), ('ma5_vs_ma20', 'REAL'),
            ('trend_strength', 'REAL'),
            ('volume_ratio_5d', 'REAL'), ('volume_ratio_20d', 'REAL')
        ]
        for field_name, field_type in momentum_metrics_fields:
            try:
                c.execute(f"ALTER TABLE momentum_scores ADD COLUMN {field_name} {field_type}")
            except sqlite3.OperationalError:
                pass  # 字段已存在
        
        # 初始化安全表
        init_security_tables(conn, c)
        
        # 检查是否存在旧表，如果存在则迁移数据
        c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='daily_reviews'")
        if c.fetchone():
            migrate_old_data(conn, c)

        _seed_rbac_data(conn, c)
        
        conn.commit()
        conn.close()
        logging.info("数据库初始化成功")
    except Exception as e:
        logging.error(f"数据库初始化失败: {e}", exc_info=True)

# 封装数据库查询函数
def query_db(query, args=(), one=False):
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    c.execute(query, args)
    rv = c.fetchall()
    conn.close()
    return (rv[0] if rv else None) if one else rv


# ==================== 动量评分存储与查询工具函数 ====================

def save_momentum_scores_to_db(df, date_str):
    """
    将动量评分结果写入 SQLite 的 momentum_scores 表
    df: DataFrame，至少包含列 ['code', 'momentum_score']，其余列打包为 JSON
    """
    # 检测数据库中的日期格式，确保保存时使用相同格式
    db_format = get_db_date_format()
    logging.info(f'[save_momentum_scores_to_db] 数据库日期格式: {db_format}, 输入日期: {date_str}')
    
    # 如果数据库已有数据，使用数据库的格式；否则使用标准格式 YYYY-MM-DD
    if db_format == 'compact':
        date_str = normalize_date(date_str, target_format='compact')
    else:
        date_str = normalize_date(date_str, target_format='standard')
    
    logging.info(f'[save_momentum_scores_to_db] 保存日期（标准化后）: {date_str}')
    
    if df is None or df.empty:
        return 0
    
    # 规范列名
    if 'code' not in df.columns:
        raise ValueError("动量结果缺少 'code' 列")
    if 'momentum_score' not in df.columns:
        raise ValueError("动量结果缺少 'momentum_score' 列")
    
    records = []
    # 状态相关字段
    status_cols = ['status', 'status_start_date', 'amplitude', 'lowest_price', 'highest_price', 
                   'amount_std', 'duration_days', 'trend_return', 'has_volume_surge', 'five_day_acceleration']
    # 动量指标字段
    momentum_metrics_cols = [
        'return_1d', 'return_5d', 'return_10d', 'return_20d', 'return_60d', 'return_120d', 'return_250d',
        'amplitude_5d', 'amplitude_20d', 'amplitude_60d', 'daily_volatility', 'annualized_volatility',
        'max_gain_20d', 'max_gain_60d', 'max_drawdown_20d', 'max_drawdown_60d',
        'recent_high', 'recent_low', 'price_vs_high',
        'ma5_slope', 'ma10_slope', 'ma20_slope', 'price_vs_ma5', 'price_vs_ma20', 'ma5_vs_ma20', 'trend_strength',
        'volume_ratio_5d', 'volume_ratio_20d'
    ]
    # 因子字段（排除状态字段、动量指标字段和基础字段）
    factor_cols = [c for c in df.columns if c not in (['code', 'date', 'momentum_score'] + status_cols + momentum_metrics_cols + ['underlying_data'])]
    
    for _, row in df.iterrows():
        code = str(row['code']).zfill(6)
        score = float(row['momentum_score'])
        factors = {col: row[col] for col in factor_cols if col in row}
        factors_json = json.dumps(factors, ensure_ascii=False, default=float)
        
        # 提取状态字段
        status = row.get('status')
        status_start_date = row.get('status_start_date')
        amplitude = row.get('amplitude')
        lowest_price = row.get('lowest_price')
        highest_price = row.get('highest_price')
        amount_std = row.get('amount_std')
        duration_days = int(row.get('duration_days', 0)) if pd.notna(row.get('duration_days')) else None
        trend_return = row.get('trend_return')
        has_volume_surge = 1 if row.get('has_volume_surge') else 0
        five_day_acceleration = 1 if row.get('five_day_acceleration') else 0
        
        # 提取底层数据
        underlying_data = row.get('underlying_data')
        underlying_data_json = json.dumps(underlying_data, ensure_ascii=False, default=float) if underlying_data else None
        
        # 提取动量指标字段
        momentum_metrics = {}
        for col in momentum_metrics_cols:
            val = row.get(col)
            if pd.notna(val):
                momentum_metrics[col] = float(val)
            else:
                momentum_metrics[col] = None
        
        records.append((
            date_str, code, score, factors_json,
            status, status_start_date, amplitude, lowest_price, highest_price, amount_std,
            duration_days, trend_return, has_volume_surge, five_day_acceleration, underlying_data_json,
            # 动量指标字段
            momentum_metrics['return_1d'], momentum_metrics['return_5d'], momentum_metrics['return_10d'], momentum_metrics['return_20d'],
            momentum_metrics['return_60d'], momentum_metrics['return_120d'], momentum_metrics['return_250d'],
            momentum_metrics['amplitude_5d'], momentum_metrics['amplitude_20d'], momentum_metrics['amplitude_60d'],
            momentum_metrics['daily_volatility'], momentum_metrics['annualized_volatility'],
            momentum_metrics['max_gain_20d'], momentum_metrics['max_gain_60d'],
            momentum_metrics['max_drawdown_20d'], momentum_metrics['max_drawdown_60d'],
            momentum_metrics['recent_high'], momentum_metrics['recent_low'], momentum_metrics['price_vs_high'],
            momentum_metrics['ma5_slope'], momentum_metrics['ma10_slope'], momentum_metrics['ma20_slope'],
            momentum_metrics['price_vs_ma5'], momentum_metrics['price_vs_ma20'], momentum_metrics['ma5_vs_ma20'],
            momentum_metrics['trend_strength'],
            momentum_metrics['volume_ratio_5d'], momentum_metrics['volume_ratio_20d']
        ))
    
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    c.executemany(
        '''INSERT OR REPLACE INTO momentum_scores 
           (date, code, momentum_score, factors_json, status, status_start_date, 
            amplitude, lowest_price, highest_price, amount_std, duration_days, 
            trend_return, has_volume_surge, five_day_acceleration, underlying_data_json,
            return_1d, return_5d, return_10d, return_20d, return_60d, return_120d, return_250d,
            amplitude_5d, amplitude_20d, amplitude_60d, daily_volatility, annualized_volatility,
            max_gain_20d, max_gain_60d, max_drawdown_20d, max_drawdown_60d,
            recent_high, recent_low, price_vs_high,
            ma5_slope, ma10_slope, ma20_slope, price_vs_ma5, price_vs_ma20, ma5_vs_ma20, trend_strength,
            volume_ratio_5d, volume_ratio_20d)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
        records
    )
    conn.commit()
    conn.close()
    return len(records)


def get_latest_momentum_date():
    """获取 momentum_scores 中最新的日期（按日期排序，取最新的）"""
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    
    # 调试：检查所有日期（原始格式）
    c.execute("SELECT DISTINCT date FROM momentum_scores ORDER BY date DESC LIMIT 10")
    all_dates_raw = [r[0] for r in c.fetchall()]
    logging.info(f'[get_latest_momentum_date] 数据库中所有日期（原始格式，前10个）: {all_dates_raw}')
    
    # 使用 ORDER BY date DESC 确保按日期排序取最新
    c.execute("SELECT date FROM momentum_scores ORDER BY date DESC LIMIT 1")
    row = c.fetchone()
    latest_date_raw = row[0] if row and row[0] else None
    logging.info(f'[get_latest_momentum_date] 数据库最新日期（原始格式）: {latest_date_raw}')
    
    # 标准化日期格式为 YYYY-MM-DD（前端使用）
    latest_date = normalize_date(latest_date_raw, target_format='standard') if latest_date_raw else None
    logging.info(f'[get_latest_momentum_date] 数据库最新日期（标准化为YYYY-MM-DD）: {latest_date}')
    
    conn.close()
    return latest_date


def get_db_date_format():
    """检测数据库中实际使用的日期格式"""
    conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    c.execute("SELECT DISTINCT date FROM momentum_scores ORDER BY date DESC LIMIT 1")
    row = c.fetchone()
    conn.close()
    
    if not row or not row[0]:
        return None
    
    date_str = str(row[0])
    # 判断格式：如果是8位数字，说明是 YYYYMMDD 格式
    if len(date_str) == 8 and date_str.isdigit():
        return 'compact'  # YYYYMMDD
    elif len(date_str) == 10 and date_str.count('-') == 2:
        return 'standard'  # YYYY-MM-DD
    else:
        return 'unknown'


def normalize_date(date_str, target_format='standard'):
    """
    标准化日期格式
    - date_str: 输入的日期字符串
    - target_format: 'standard' (YYYY-MM-DD) 或 'compact' (YYYYMMDD)
    """
    if not date_str:
        return None
    date_str = str(date_str).strip()
    
    # 先解析为datetime对象
    dt = None
    try:
        if len(date_str) == 8 and date_str.isdigit():
            dt = datetime.strptime(date_str, '%Y%m%d')
        elif len(date_str) == 10 and date_str.count('-') == 2:
            dt = datetime.strptime(date_str, '%Y-%m-%d')
        else:
            # 尝试其他格式
            for fmt in ['%Y-%m-%d', '%Y%m%d', '%Y/%m/%d']:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    break
                except:
                    continue
    except Exception as e:
        logging.warning(f'无法解析日期格式: {date_str}, 错误: {e}')
        return date_str
    
    if not dt:
        return date_str
    
    # 根据目标格式返回
    if target_format == 'compact':
        return dt.strftime('%Y%m%d')
    else:
        return dt.strftime('%Y-%m-%d')


def get_momentum_scores(date_str, limit=200, offset=0):
    """按日期获取动量分列表"""
    # 检测数据库中的日期格式
    db_format = get_db_date_format()
    
    # 根据数据库格式标准化日期
    if db_format == 'compact':
        date_str = normalize_date(date_str, target_format='compact')
    else:
        date_str = normalize_date(date_str, target_format='standard')
    
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        
        # 查询数据
        c.execute("SELECT COUNT(*) FROM momentum_scores WHERE date = ?", (date_str,))
        total = c.fetchone()[0] or 0
        
        rows = []
        if total > 0:
            c.execute(
                '''SELECT code, momentum_score, factors_json, status, status_start_date, amplitude, lowest_price, 
                          highest_price, amount_std, duration_days, trend_return, has_volume_surge, five_day_acceleration, underlying_data_json,
                          return_1d, return_5d, return_10d, return_20d, return_60d, return_120d, return_250d,
                          amplitude_5d, amplitude_20d, amplitude_60d, daily_volatility, annualized_volatility,
                          max_gain_20d, max_gain_60d, max_drawdown_20d, max_drawdown_60d,
                          recent_high, recent_low, price_vs_high,
                          ma5_slope, ma10_slope, ma20_slope, price_vs_ma5, price_vs_ma20, ma5_vs_ma20, trend_strength,
                          volume_ratio_5d, volume_ratio_20d
                   FROM momentum_scores
                   WHERE date = ?
                   ORDER BY momentum_score DESC
                   LIMIT ? OFFSET ?''',
                (date_str, int(limit), int(offset))
            )
            rows = c.fetchall()
        
        items = []
        for idx, r in enumerate(rows):
            # 首先尝试从单独的列读取状态字段
            # sqlite3.Row 使用字典式访问 r[key]，如果值为NULL会返回None
            status = r['status']
            status_start_date = r['status_start_date']
            amplitude = r['amplitude']
            lowest_price = r['lowest_price']
            highest_price = r['highest_price']
            amount_std = r['amount_std']
            duration_days = r['duration_days']
            trend_return = r['trend_return']
            has_volume_surge = r['has_volume_surge']
            five_day_acceleration = r['five_day_acceleration']
            
            # 解析underlying_data_json
            underlying_data = None
            underlying_data_json_str = r['underlying_data_json']
            if underlying_data_json_str:
                try:
                    underlying_data = json.loads(underlying_data_json_str)
                except Exception as e:
                    logging.warning(f'[get_momentum_scores] 解析underlying_data_json失败: {e}')
            
            # 如果单独的列为NULL，尝试从factors_json中解析
            # 注意：根据用户提供的数据，status等字段存储在factors_json中
            factors_json_str = r['factors_json']
            if factors_json_str and (status is None or status_start_date is None):
                    try:
                        factors_dict = json.loads(factors_json_str)
                        # 从factors_json中提取状态字段（如果单独列为NULL）
                        if status is None:
                            status = factors_dict.get('status')
                        if status_start_date is None:
                            status_start_date = factors_dict.get('status_start_date')
                        if amplitude is None:
                            amplitude = factors_dict.get('amplitude')
                        if lowest_price is None:
                            lowest_price = factors_dict.get('lowest_price')
                        if highest_price is None:
                            highest_price = factors_dict.get('highest_price')
                        if amount_std is None:
                            amount_std = factors_dict.get('amount_std')
                        if duration_days is None:
                            duration_days = factors_dict.get('duration_days')
                        if trend_return is None:
                            trend_return = factors_dict.get('trend_return')
                        if has_volume_surge is None:
                            has_volume_surge = factors_dict.get('has_volume_surge')
                        if five_day_acceleration is None:
                            five_day_acceleration = factors_dict.get('five_day_acceleration')
                        
                        if idx == 0:
                            logging.info(f'[get_momentum_scores] 从factors_json解析状态字段: status={status}, status_start_date={status_start_date}')
                    except Exception as e:
                        logging.warning(f'[get_momentum_scores] 解析factors_json失败: {e}')
            
            # 解析factors_json获取所有因子值
            factors_dict = {}
            if factors_json_str:
                try:
                    factors_dict = json.loads(factors_json_str)
                except Exception as e:
                    logging.warning(f'[get_momentum_scores] 解析factors_json失败: {e}')
            
            # 构建item字典，确保所有字段都包含（即使为None）
            # 将 NaN 值转换为 None，因为 JSON 不支持 NaN
            import math
            
            def clean_value(val):
                if val is None:
                    return None
                if isinstance(val, float):
                    if math.isnan(val) or math.isinf(val):
                        return None
                return val
            
            # 读取动量指标字段（sqlite3.Row 使用下标访问 r['col']）
            momentum_metrics = {
                'return_1d': clean_value(r['return_1d']) if 'return_1d' in r.keys() else None,
                'return_5d': clean_value(r['return_5d']) if 'return_5d' in r.keys() else None,
                'return_10d': clean_value(r['return_10d']) if 'return_10d' in r.keys() else None,
                'return_20d': clean_value(r['return_20d']) if 'return_20d' in r.keys() else None,
                'return_60d': clean_value(r['return_60d']) if 'return_60d' in r.keys() else None,
                'return_120d': clean_value(r['return_120d']) if 'return_120d' in r.keys() else None,
                'return_250d': clean_value(r['return_250d']) if 'return_250d' in r.keys() else None,
                'amplitude_5d': clean_value(r['amplitude_5d']) if 'amplitude_5d' in r.keys() else None,
                'amplitude_20d': clean_value(r['amplitude_20d']) if 'amplitude_20d' in r.keys() else None,
                'amplitude_60d': clean_value(r['amplitude_60d']) if 'amplitude_60d' in r.keys() else None,
                'daily_volatility': clean_value(r['daily_volatility']) if 'daily_volatility' in r.keys() else None,
                'annualized_volatility': clean_value(r['annualized_volatility']) if 'annualized_volatility' in r.keys() else None,
                'max_gain_20d': clean_value(r['max_gain_20d']) if 'max_gain_20d' in r.keys() else None,
                'max_gain_60d': clean_value(r['max_gain_60d']) if 'max_gain_60d' in r.keys() else None,
                'max_drawdown_20d': clean_value(r['max_drawdown_20d']) if 'max_drawdown_20d' in r.keys() else None,
                'max_drawdown_60d': clean_value(r['max_drawdown_60d']) if 'max_drawdown_60d' in r.keys() else None,
                'recent_high': clean_value(r['recent_high']) if 'recent_high' in r.keys() else None,
                'recent_low': clean_value(r['recent_low']) if 'recent_low' in r.keys() else None,
                'price_vs_high': clean_value(r['price_vs_high']) if 'price_vs_high' in r.keys() else None,
                'ma5_slope': clean_value(r['ma5_slope']) if 'ma5_slope' in r.keys() else None,
                'ma10_slope': clean_value(r['ma10_slope']) if 'ma10_slope' in r.keys() else None,
                'ma20_slope': clean_value(r['ma20_slope']) if 'ma20_slope' in r.keys() else None,
                'price_vs_ma5': clean_value(r['price_vs_ma5']) if 'price_vs_ma5' in r.keys() else None,
                'price_vs_ma20': clean_value(r['price_vs_ma20']) if 'price_vs_ma20' in r.keys() else None,
                'ma5_vs_ma20': clean_value(r['ma5_vs_ma20']) if 'ma5_vs_ma20' in r.keys() else None,
                'trend_strength': clean_value(r['trend_strength']) if 'trend_strength' in r.keys() else None,
                'volume_ratio_5d': clean_value(r['volume_ratio_5d']) if 'volume_ratio_5d' in r.keys() else None,
                'volume_ratio_20d': clean_value(r['volume_ratio_20d']) if 'volume_ratio_20d' in r.keys() else None
            }
            
            item = {
                'code': r['code'],
                'momentum_score': r['momentum_score'],
                'status': status,  # 即使为None也要包含
                'status_start_date': status_start_date,
                'amplitude': clean_value(amplitude),
                'lowest_price': clean_value(lowest_price),
                'highest_price': clean_value(highest_price),
                'amount_std': clean_value(amount_std),
                'duration_days': clean_value(duration_days),
                'trend_return': clean_value(trend_return),
                'has_volume_surge': bool(has_volume_surge) if has_volume_surge is not None and has_volume_surge != 0 else False,
                'five_day_acceleration': bool(five_day_acceleration) if five_day_acceleration is not None and five_day_acceleration != 0 else False,
                'factors': factors_dict,  # 包含所有因子值
                'underlying_data': underlying_data,  # 包含底层数据（MASS值、均线斜率等）
                'momentum_metrics': momentum_metrics  # 包含所有动量指标
            }
            
            # 调试：打印第一条记录转换后的数据
            if idx == 0:
                logging.info(f'[get_momentum_scores] 第一条记录转换后所有字段: {list(item.keys())}')
                logging.info(f'[get_momentum_scores] 第一条记录转换后完整内容: {json.dumps(item, ensure_ascii=False, default=str)}')
            
            items.append(item)
        
        logging.info(f'[get_momentum_scores] 返回 {len(items)} 条记录，第一条记录的字段: {list(items[0].keys()) if items else "无数据"}')
    except Exception as e:
        logging.error(f'[get_momentum_scores] 查询失败: {e}', exc_info=True)
        return 0, []
    finally:
        if 'conn' in locals():
            conn.close()
            logging.info(f'[get_momentum_scores] 数据库连接已关闭')
    
    logging.info(f'[get_momentum_scores] ===== 查询完成 =====')
    return total, items

# 封装数据库插入函数
def insert_db(query, args=()):
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute(query, args)
        conn.commit()
        conn.close()
        # 正常操作不记录，只在错误时记录
        # logging.debug(f"Data inserted: {query[:100]}...")
    except Exception as e:
        logging.error(f"数据插入失败: {e}", exc_info=True)


def insert_db_return_id(query, args=()):
    """执行 INSERT 并返回 lastrowid；失败时抛出异常。"""
    conn = sqlite3.connect(DATABASE_FILE)
    try:
        c = conn.cursor()
        c.execute(query, args)
        rid = c.lastrowid
        conn.commit()
        return rid
    except Exception as e:
        logging.error(f"数据插入失败: {e}", exc_info=True)
        raise
    finally:
        conn.close()

# 封装数据库更新函数
def update_db(query, args=()):
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute(query, args)
        conn.commit()
        conn.close()
        # 正常操作不记录，只在错误时记录
        # logging.debug(f"Data updated: {query[:100]}...")
    except Exception as e:
        logging.error(f"数据更新失败: {e}", exc_info=True)

# 封装数据库删除函数
def delete_db(query, args=()):
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute(query, args)
        conn.commit()
        conn.close()
        # 正常操作不记录，只在错误时记录
        # logging.debug(f"Data deleted: {query[:100]}...")
    except Exception as e:
        logging.error(f"数据删除失败: {e}", exc_info=True)

# 数据迁移函数：将旧表 daily_reviews 的数据迁移到新表
def migrate_old_data(conn, c):
    """
    将旧表 daily_reviews 的数据迁移到新表 essays 和 reviews
    旧表字段：id, date, overall_market, events, emotion_cycle, market_style, 
             fundamentals, international_hotspots, insights, golden_stocks, deep_report
    """
    try:
        # 检查是否已经迁移过（通过检查是否有包含"旧数据迁移"的随笔）
        c.execute("SELECT COUNT(*) FROM essays WHERE content LIKE '%旧数据迁移%'")
        already_migrated = c.fetchone()[0] > 0
        
        if already_migrated:
            # 迁移操作，只在首次执行时记录
            # logging.debug("数据已经迁移过，跳过迁移")
            return
        
        # 查询旧表所有数据
        c.execute("SELECT * FROM daily_reviews ORDER BY date")
        old_records = c.fetchall()
        
        if not old_records:
            # 迁移操作，只在首次执行时记录
            # logging.debug("旧表 daily_reviews 没有数据，无需迁移")
            return
        
        migrated_count = 0
        
        for record in old_records:
            old_id = record[0]
            date = record[1]
            overall_market = record[2] or ''
            events = record[3] or ''
            emotion_cycle = record[4] or ''
            market_style = record[5] or ''
            fundamentals = record[6] or ''
            international_hotspots = record[7] or ''
            insights = record[8] or ''
            golden_stocks = record[9] or ''
            
            # 将所有字段内容合并成一个随笔
            essay_content = f"""【旧数据迁移 - 原ID: {old_id}】

整体行情：{overall_market}

事件&政策&热点&异常：{events}

情绪周期：{emotion_cycle}

市场风格：{market_style}

基本面&资金面：{fundamentals}

国际热点：{international_hotspots}

前瞻点&思考总结：{insights}

每日金股：{golden_stocks}
"""
            
            # 插入到随笔表
            c.execute("INSERT INTO essays (date, content) VALUES (?, ?)", 
                     (date, essay_content.strip()))
            
            # 尝试将部分字段映射到复盘表（如果该日期还没有复盘记录）
            c.execute("SELECT id FROM reviews WHERE date = ?", (date,))
            existing_review = c.fetchone()
            
            if not existing_review:
                # 创建复盘记录，将相关字段映射过去
                # 映射关系：
                # emotion_cycle -> market_emotion (市场情绪)
                # market_style -> sectors (板块)
                # events -> themes (题材)
                # fundamentals -> market_cap_performance (市值表现)
                # insights -> investment_style (投资方式)
                # events -> major_events (大事)
                
                c.execute("""INSERT INTO reviews (date, market_emotion, sectors, themes, 
                            market_cap_performance, investment_style, major_events) 
                            VALUES (?, ?, ?, ?, ?, ?, ?)""",
                         (date, 
                          emotion_cycle,  # 情绪周期 -> 市场情绪
                          market_style,   # 市场风格 -> 板块
                          events,         # 事件 -> 题材
                          fundamentals,   # 基本面 -> 市值表现
                          insights,       # 思考总结 -> 投资方式
                          events))        # 事件 -> 大事
                
            migrated_count += 1
        
        if migrated_count > 0:
            logging.info(f"数据迁移完成: 成功迁移 {migrated_count} 条记录")
        # 迁移建议只在首次执行时显示
        
    except Exception as e:
        logging.error(f"数据迁移失败: {e}", exc_info=True)
        # 不抛出异常，让程序继续运行

@app.route('/ai_generate', methods=['POST'])
@login_required
def ai_generate():
    dates = request.form.getlist('date')  # 获取多个日期
    if not dates:
        return jsonify({"error": "请至少选择一个日期"})
    
    combined_prompt = ""
    found_data = False
    
    for date in dates:
        # 从新的数据结构中获取数据
        # 获取随笔（可能有多条）
        essays = query_db("SELECT content FROM essays WHERE date = ? ORDER BY created_at", (date,))
        essay_contents = [essay[0] for essay in essays] if essays else []
        
        # 获取复盘（每个日期只有一条）
        review = query_db("SELECT * FROM reviews WHERE date = ?", (date,), one=True)
        
        # 获取市场基调
        tone = query_db("SELECT * FROM market_tone WHERE date = ?", (date,), one=True)
        
        # 获取指数走势
        trend = query_db("SELECT * FROM index_trend WHERE date = ?", (date,), one=True)
        
        if essay_contents or review or tone or trend:
            found_data = True
            date_str = date
            
            # 构建提示词
            prompt = f"""
以下是 {date_str} 的市场分析和工作总结：

"""
            # 随笔内容
            if essay_contents:
                prompt += f"- 随笔记录（共{len(essay_contents)}条）：\n"
                for i, content in enumerate(essay_contents, 1):
                    prompt += f"  随笔{i}：{content}\n"
            
            # 复盘内容
            if review:
                prompt += f"""
- 复盘记录：
  - 市场情绪：{review[2] or '未填写'}
  - 板块表现：{review[3] or '未填写'}
  - 题材热点：{review[4] or '未填写'}
  - 市值表现：{review[5] or '未填写'}
  - 投资风格：{review[6] or '未填写'}
  - 重大事件：{review[7] or '未填写'}
"""
            
            # 市场基调
            if tone:
                prompt += f"""
- 市场基调：
  - 成交量状态：{tone[2] or '未填写'}
  - 情绪状态：{tone[3] or '未填写'}
  - 分化状态：{tone[4] or '未填写'}
  - 投资策略：{tone[5] or '未填写'}
  - 投资风格：{tone[6] or '未填写'}
"""
            
            # 指数走势
            if trend:
                prompt += f"""
- 指数走势：
  - 上证指数：{trend[2] or '未填写'}
  - 深证成指：{trend[3] or '未填写'}
  - 创业板指：{trend[4] or '未填写'}
"""
            
            prompt += """
请根据以上内容：
1. 发表你的看法，如果提到了行业、个股，尽量按照产业链和事件驱动机会来进行联想。
2. 分析我的思考总结，指出其中的亮点和不足，重点是你对我思考总结的看法，如果我的观点和看法有逻辑错误，请指出并给出答案。
3. 提供你的拓展思考（请特别标注"【DeepSeek 拓展】"），多联想一些，给我更多的方向，有一些关于财经、股票、政策的知识点，每次生成都给我三个。
4. 如果我分析了多天的数据，请将多天的数据上下文联系起来，分析多天的变化趋势，或者有无热点延续。
5. 对于每日金股，指出其中个股的行业，主要合作伙伴，主营业务。
6. 今天发生的一些你觉得重要的事情，要你自己去搜索思考，而不是从我的原文中去摘录。
7. 要求所有回答都联网。
8. 不需要复述我的原文，直接开始你的回答就可以了，避免我忘记说什么去翻阅原文，你可以稍加提醒。

"""
            combined_prompt += prompt
    
    if not found_data:
        return jsonify({"error": "未找到所选日期的记录"})
    
    try:
        analysis = call_deepseek_api(combined_prompt)
        # 创建一个临时文件来保存分析结果
        temp_dir = tempfile.gettempdir()
        file_path = os.path.join(temp_dir, f'analysis_result_{datetime.now().strftime("%Y%m%d_%H%M%S")}.txt')
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(analysis)
        # 返回文件供用户下载
        return send_file(file_path, as_attachment=True, download_name=f'AI分析报告_{datetime.now().strftime("%Y%m%d")}.txt')
    except Exception as e:
        logging.error(f"AI分析失败: {e}", exc_info=True)
        return jsonify({"error": f"AI 分析失败: {str(e)}"})

# 检查会话超时和记录访问日志
@app.before_request
def check_session_timeout():
    _rbac_bootstrap_once()
    # 排除静态文件和登录相关页面
    if request.endpoint in ['static', 'login', 'register', 'logout']:
        return
    # 未匹配到任何路由时交由 Flask 返回 404，避免把匿名访问无效 URL 也重定向到登录页
    if request.endpoint is None:
        return

    # 未登录访问任意业务地址 → 登录页（与各视图上的 @login_required 一致，并防止漏挂装饰器）
    if not session.get('username'):
        if request.endpoint != 'static':
            _auth_log(
                'need_login',
                path=request.path,
                endpoint=request.endpoint,
                method=request.method,
                remote_addr=request.remote_addr,
            )
        return redirect(url_for('login'))

    # 按 endpoint 校验功能权限；无权限时 API 返回 403，浏览器访问清空会话并进入登录页
    req_codes = _rbac_required_permission_codes(request.endpoint)
    uname = session.get('username')
    if req_codes and not user_has_permission(uname, *req_codes):
        snap = _auth_permission_codes_preview(uname)
        _auth_log(
            'permission_denied',
            username=uname,
            path=request.path,
            endpoint=request.endpoint,
            method=request.method,
            required=list(req_codes),
            remote_addr=request.remote_addr,
            **snap,
        )
        if request.path.startswith('/api/'):
            return jsonify({'success': False, 'error': '没有权限执行此操作'}), 403
        # 不清空 session：避免权限数据短暂异常或修复逻辑未命中时陷入「假登录」死循环
        flash('当前账号无权访问该页面，或权限数据未就绪。若刚升级程序，请再试一次登录；仍不行请联系管理员。', 'warning')
        return redirect(url_for('login'))

    # 记录访问日志
    if request.endpoint:
        username = session.get('username', 'anonymous')
        log_access(username, request.endpoint, request.method)

    # 检查会话超时
    if 'username' in session:
        last_activity = session.get('last_activity')
        if last_activity and (time.time() - last_activity) > SESSION_TIMEOUT:
            _auth_log(
                'session_timeout',
                username=session.get('username'),
                path=request.path,
                endpoint=request.endpoint,
                remote_addr=request.remote_addr,
            )
            session.pop('username', None)
            session.pop('last_activity', None)
            return redirect(url_for('login'))
        session['last_activity'] = time.time()

# 注册页面（可选：可以注释掉以禁用注册）
@app.route('/register', methods=['GET', 'POST'])
def register():
    # 可选：禁用注册功能，只允许已有用户登录
    # return redirect(url_for('login'))
    
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        # 验证密码强度
        if len(password) < 8:
            return render_template('register.html', error='密码长度至少8位')
        
        user = query_db("SELECT * FROM users WHERE username =?", (username,), one=True)
        if user:
            return render_template('register.html', error='用户名已存在')
        
        # 使用哈希密码
        password_hash = hash_password(password)
        insert_db("INSERT INTO users (username, password) VALUES (?,?)", (username, password_hash))
        try:
            uid_row = query_db('SELECT id FROM users WHERE username=?', (username,), one=True)
            if uid_row:
                conn_r = sqlite3.connect(DATABASE_FILE)
                cr = conn_r.cursor()
                _init_rbac_tables(cr)
                cr.execute('SELECT id FROM roles WHERE slug=?', ('member',))
                mr = cr.fetchone()
                if mr:
                    cr.execute(
                        'INSERT OR IGNORE INTO user_roles (user_id, role_id) VALUES (?,?)',
                        (uid_row[0], mr[0]),
                    )
                conn_r.commit()
                conn_r.close()
        except Exception as _e:
            logging.warning(f'新用户挂载默认角色失败（可重启后由 init_db 修复）: {_e}')
        logging.info(f"新用户注册: {username} (IP: {request.remote_addr})")
        return redirect(url_for('login'))
    return render_template('register.html')

# 登录页面
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'GET' and session.get('username'):
        u = session.get('username')
        if user_has_permission(u, *_index_entry_permission_codes()):
            _auth_log('login_get_redirect_index', username=u, reason='can_open_index')
            return redirect(url_for('index'))
        _auth_log(
            'login_get_stay_on_form',
            username=u,
            reason='no_index_access',
            preview=_auth_permission_codes_preview(u),
        )
        return render_template(
            'login.html',
            auth_diag=_login_page_diag(
                step='get_session_but_no_index_access',
                session_username=u,
                permission_preview=_auth_permission_codes_preview(u),
            ),
        )
    if request.method == 'POST':
        username = (request.form.get('username') or '').strip()
        password = request.form.get('password')
        ip_address = request.remote_addr
        _auth_log('login_post_begin', username=username, remote_addr=ip_address)

        # 检查登录尝试次数
        if not check_login_attempts(ip_address, username):
            logging.warning(f"登录尝试次数超限: {username} (IP: {ip_address})")
            _auth_log('login_post_blocked_rate_limit', username=username, remote_addr=ip_address)
            return render_template(
                'login.html',
                error=f'登录尝试次数过多，请15分钟后再试',
                auth_diag=_login_page_diag(step='post_rate_limited'),
            )
        
        # 查询用户（不区分大小写，避免库内用户名与输入大小写不一致导致「能登录但 RBAC 查不到人」）
        user = query_db(
            'SELECT * FROM users WHERE lower(trim(username)) = lower(trim(?)) LIMIT 1',
            (username,),
            one=True,
        )
        
        if user:
            # 验证密码（支持旧密码和新哈希密码）
            stored_password = user[2]
            password_valid = False
            
            if ':' in stored_password:
                # 新格式：哈希密码
                password_valid = verify_password(password, stored_password)
            else:
                # 旧格式：明文密码（兼容旧数据，首次登录后自动升级）
                password_valid = (password == stored_password)
                if password_valid:
                    # 升级为哈希密码
                    password_hash = hash_password(password)
                    update_db("UPDATE users SET password =? WHERE id =?", (password_hash, user[0]))
                    # 密码升级是正常操作，不记录
                    # logging.debug(f"用户 {username} 密码已升级为哈希格式")
            
            if password_valid:
                session['username'] = user[1]
                session['last_activity'] = time.time()
                log_login_attempt(ip_address, username, True)
                logging.info(f"用户登录成功: {username} (IP: {ip_address})")
                try:
                    _rbac_repair_login_access(username)
                except Exception as _su_e:
                    logging.warning(f'登录后 RBAC 修复失败（可重启或 init_db）: {_su_e}', exc_info=True)
                prev = _auth_permission_codes_preview(username)
                ok_home = user_has_permission(username, *_index_entry_permission_codes())
                _auth_log(
                    'login_post_success',
                    username=username,
                    remote_addr=ip_address,
                    can_access_home=ok_home,
                    **prev,
                )
                return redirect(url_for('index'))
            else:
                log_login_attempt(ip_address, username, False)
                logging.warning(f"登录失败: {username} (IP: {ip_address}) - 密码错误")
                _auth_log('login_post_fail', username=username, reason='bad_password', remote_addr=ip_address)
                return render_template(
                    'login.html',
                    error='用户名或密码错误',
                    auth_diag=_login_page_diag(step='post_bad_password', attempted_user=username),
                )
        else:
            log_login_attempt(ip_address, username, False)
            logging.warning(f"登录失败: {username} (IP: {ip_address}) - 用户不存在")
            _auth_log('login_post_fail', username=username, reason='user_not_found', remote_addr=ip_address)
            return render_template(
                'login.html',
                error='用户名或密码错误',
                auth_diag=_login_page_diag(step='post_user_not_found', attempted_user=username),
            )

    return render_template('login.html', auth_diag=_login_page_diag(step='get_form'))

# 注销功能
@app.route('/logout')
def logout():
    session.pop('username', None)
    return redirect(url_for('login'))

# 手动触发数据迁移（可选，用于重新迁移）
@app.route('/migrate_data')
@login_required
def migrate_data():
    
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        migrate_old_data(conn, c)
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'message': '数据迁移完成'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# 清理重复的迁移数据（保留每个原ID的第一条）
@app.route('/clean_duplicate_migrations')
@login_required
def clean_duplicate_migrations():
    
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        
        # 查找所有包含"旧数据迁移"的随笔
        c.execute("SELECT id, content FROM essays WHERE content LIKE '%旧数据迁移%'")
        essays = c.fetchall()
        
        # 提取原ID，找出重复的
        original_ids = {}
        duplicates_to_delete = []
        
        for essay_id, content in essays:
            # 从内容中提取原ID
            import re
            match = re.search(r'原ID: (\d+)', content)
            if match:
                old_id = int(match.group(1))
                if old_id not in original_ids:
                    original_ids[old_id] = essay_id  # 保留第一个
                else:
                    duplicates_to_delete.append(essay_id)  # 标记为重复
        
        # 删除重复的随笔
        deleted_count = 0
        for essay_id in duplicates_to_delete:
            c.execute("DELETE FROM essays WHERE id = ?", (essay_id,))
            deleted_count += 1
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True, 
            'message': f'清理完成：删除了 {deleted_count} 条重复的迁移数据',
            'deleted_count': deleted_count
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# 删除随笔
@app.route('/delete_essay/<int:id>')
@login_required
def delete_essay(id):
    delete_db("DELETE FROM essays WHERE id =?", (id,))
    return redirect(url_for('index'))

# 删除复盘
@app.route('/delete_review/<int:id>')
@login_required
def delete_review(id):
    delete_db("DELETE FROM reviews WHERE id =?", (id,))
    return redirect(url_for('index'))

# 主页 - 显示随笔和复盘
@app.route('/', methods=['GET'])
@login_required
def index():
    uname = session.get('username')
    # 仅持有「业务模块」等权限、无 app:access 的用户不应被强制去填基调/指数页
    can_setup_diary_flow = user_has_permission(uname, 'app:access')
    # 检查今日是否已设置市场基调
    today = datetime.now().strftime('%Y-%m-%d')
    today_tone = query_db("SELECT * FROM market_tone WHERE date = ?", (today,), one=True)

    if not today_tone:
        if can_setup_diary_flow:
            return redirect(url_for('set_market_tone', date=today))
        current_tone = None
    else:
        # 获取今日基调数据
        current_tone = {
            'date': today_tone[1],
            'volume_status': today_tone[2],
            'emotion_status': today_tone[3],
            'divergence_status': today_tone[4],
            'investment_strategies': today_tone[5] or '',
            'investment_styles': today_tone[6] or '',
        }

    # 检查今日是否已设置指数走势
    today_trend = query_db("SELECT * FROM index_trend WHERE date = ?", (today,), one=True)

    if not today_trend:
        if can_setup_diary_flow:
            return redirect(url_for('set_index_trend', date=today))
        current_trend = None
    else:
        # 获取今日指数走势数据
        current_trend = {
            'date': today_trend[1],
            'shanghai_trend': today_trend[2],
            'shenzhen_trend': today_trend[3],
            'chinext_trend': today_trend[4],
        }
    
    # 分页参数
    page = int(request.args.get('page', 1))
    per_page = 5  # 每页显示5个日期组
    
    # 获取所有日期，按日期分组
    date_filter = request.args.get('date', '')
    
    # 查询所有随笔，按日期分组
    if date_filter:
        essays = query_db("SELECT * FROM essays WHERE date = ? ORDER BY created_at DESC", (date_filter,))
    else:
        essays = query_db("SELECT * FROM essays ORDER BY date DESC, created_at DESC")
    
    # 查询所有复盘
    if date_filter:
        reviews = query_db("SELECT * FROM reviews WHERE date = ? ORDER BY date DESC", (date_filter,))
    else:
        reviews = query_db("SELECT * FROM reviews ORDER BY date DESC")
    
    # 按日期组织数据
    date_groups = {}
    
    # 添加随笔
    for essay in essays:
        date = essay[1]  # date字段
        if date not in date_groups:
            date_groups[date] = {'essays': [], 'review': None}
        date_groups[date]['essays'].append({
            'id': essay[0],
            'date': essay[1],
            'content': essay[2],
            'created_at': essay[3]
        })
    
    # 添加复盘
    for review in reviews:
        date = review[1]  # date字段
        if date not in date_groups:
            date_groups[date] = {'essays': [], 'review': None}
        date_groups[date]['review'] = {
            'id': review[0],
            'date': review[1],
            'market_emotion': review[2],
            'sectors': review[3],
            'themes': review[4],
            'market_cap_performance': review[5],
            'investment_style': review[6],
            'major_events': review[7]
        }
    
    # 转换为列表，按日期倒序
    sorted_dates = sorted(date_groups.keys(), reverse=True)
    
    # 分页处理
    total_dates = len(sorted_dates)
    total_pages = (total_dates + per_page - 1) // per_page if total_dates > 0 else 1
    
    # 确保页码有效
    if page < 1:
        page = 1
    elif total_pages > 0 and page > total_pages:
        page = total_pages
    
    # 计算当前页的日期范围
    if total_dates > 0:
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_dates = sorted_dates[start_idx:end_idx]
        # 只保留当前页的日期组数据
        paginated_date_groups = {date: date_groups[date] for date in paginated_dates}
    else:
        paginated_dates = []
        paginated_date_groups = {}
    
    # 获取统计数据
    stats = {
        'total_essays': query_db("SELECT COUNT(*) FROM essays", one=True)[0],
        'total_reviews': query_db("SELECT COUNT(*) FROM reviews", one=True)[0],
        'total_plans': query_db("SELECT COUNT(*) FROM investment_plans", one=True)[0],
        'active_plans': query_db("SELECT COUNT(*) FROM investment_plans WHERE status IN ('todo', 'in_progress')", one=True)[0],
        'total_reports': query_db("SELECT COUNT(*) FROM deep_reports", one=True)[0],
        'total_topics': query_db("SELECT COUNT(*) FROM topics", one=True)[0],
        'today_essays': query_db("SELECT COUNT(*) FROM essays WHERE date = ?", (today,), one=True)[0],
        'upcoming_events': query_db("""SELECT COUNT(*) FROM investment_calendar 
                                       WHERE start_date >= ? AND start_date <= date('now', '+7 days')""", 
                                    (today,), one=True)[0]
    }
    
    # 读取最新市场情绪快照（供首页卡片缩略展示）
    sentiment_snapshot = None
    try:
        import json as _js_idx
        _conn_s = sqlite3.connect(DATABASE_FILE)
        _cs = _conn_s.cursor()
        _cs.execute("SELECT trading_day, result_json FROM market_sentiment_results ORDER BY trading_day DESC LIMIT 1")
        _row_s = _cs.fetchone()
        _conn_s.close()
        if _row_s and _row_s[1]:
            _sd = _js_idx.loads(_row_s[1])
            sentiment_snapshot = {
                'day': _row_s[0],
                'score': _sd.get('total_score', '-'),
                'penalties': [(_sd.get(f'module{i}') or {}).get('penalty', 0) for i in range(1, 9)],
            }
    except Exception:
        pass

    # 首页 UI 与「角色-权限表」严格一致（不依赖模板里 has_perm，避免与 sync/上下文不一致）
    _u = (uname or '').strip()
    _owned_ui = _rbac_get_user_permission_codes(_u) if _u else frozenset()
    _perm_repaired = False
    if _u and not _owned_ui:
        _perm_repaired = True
        logging.info(
            '[rbac_home] index_perm owned_empty_trigger_repair username=%r db=%s',
            _u,
            DATABASE_FILE,
        )
        _rbac_repair_login_access(_u)
        _owned_ui = _rbac_get_user_permission_codes(_u)
    show_diary_zone = bool(_owned_ui and 'diary:content' in _owned_ui)
    can_app_write = bool(_owned_ui and 'app:access' in _owned_ui)
    _rbac_log_home_permission_snapshot(_u, _owned_ui, _perm_repaired, show_diary_zone, can_app_write)

    return render_template('index.html', 
                         date_groups=paginated_date_groups, 
                         sorted_dates=paginated_dates, 
                         date_filter=date_filter or '',
                         page=page,
                         total_pages=total_pages,
                         total_dates=total_dates,
                         current_tone=current_tone,
                         current_trend=current_trend,
                         stats=stats,
                         sentiment_snapshot=sentiment_snapshot,
                         show_diary_zone=show_diary_zone,
                         can_app_write=can_app_write)

# 添加随笔
@app.route('/add_essay', methods=['GET', 'POST'])
@login_required
def add_essay():
    if request.method == 'POST':
        date = request.form.get('date')
        content = request.form.get('content', '')
        insert_db("INSERT INTO essays (date, content) VALUES (?,?)", (date, content))
        return redirect(url_for('index'))
    # GET请求时，默认日期为今天
    today = datetime.now().strftime('%Y-%m-%d')
    return render_template('add_essay.html', default_date=today)

# 添加/编辑复盘
@app.route('/add_review', methods=['GET', 'POST'])
@app.route('/edit_review/<int:id>', methods=['GET', 'POST'])
@login_required
def add_review(id=None):
    
    if request.method == 'POST':
        date = request.form.get('date')
        market_emotion = request.form.get('market_emotion', '')
        sectors = request.form.get('sectors', '')
        themes = request.form.get('themes', '')
        market_cap_performance = request.form.get('market_cap_performance', '')
        investment_style = request.form.get('investment_style', '')
        major_events = request.form.get('major_events', '')
        
        if id:
            # 更新
            update_db("""UPDATE reviews SET date=?, market_emotion=?, sectors=?, themes=?, 
                        market_cap_performance=?, investment_style=?, major_events=?, 
                        updated_at=CURRENT_TIMESTAMP WHERE id=?""",
                     (date, market_emotion, sectors, themes, market_cap_performance, 
                      investment_style, major_events, id))
        else:
            # 新增
            insert_db("""INSERT INTO reviews (date, market_emotion, sectors, themes, 
                        market_cap_performance, investment_style, major_events) 
                        VALUES (?,?,?,?,?,?,?)""",
                     (date, market_emotion, sectors, themes, market_cap_performance, 
                      investment_style, major_events))
        return redirect(url_for('index'))
    
    # GET请求
    today = datetime.now().strftime('%Y-%m-%d')
    review = None
    if id:
        review = query_db("SELECT * FROM reviews WHERE id =?", (id,), one=True)
        if review:
            review = {
                'id': review[0],
                'date': review[1],
                'market_emotion': review[2],
                'sectors': review[3],
                'themes': review[4],
                'market_cap_performance': review[5],
                'investment_style': review[6],
                'major_events': review[7]
            }
    
    return render_template('add_review.html', review=review, default_date=today)

# 保存随笔
@app.route('/save_essay', methods=['POST'])
@login_required
def save_essay():
    essay_id = request.form.get('id')
    content = request.form.get('content', '')
    if essay_id:
        update_db("UPDATE essays SET content =? WHERE id =?", (content, essay_id))
        return jsonify({'success': True})
    return jsonify({'success': False, 'error': '缺少ID'})

# 保存复盘字段
@app.route('/save_review_field', methods=['POST'])
@login_required
def save_review_field():
    review_id = request.form.get('id')
    field = request.form.get('field')
    value = request.form.get('value', '')
    
    if review_id and field:
        # 验证字段名，防止SQL注入
        allowed_fields = ['market_emotion', 'sectors', 'themes', 'market_cap_performance', 
                         'investment_style', 'major_events']
        if field in allowed_fields:
            update_db(f"UPDATE reviews SET {field} =?, updated_at=CURRENT_TIMESTAMP WHERE id =?", 
                     (value, review_id))
            return jsonify({'success': True})
    return jsonify({'success': False, 'error': '参数错误'})

# 导出 Excel 路由
@app.route('/export_excel')
@login_required
def export_excel():
    
    reviews = query_db("SELECT * FROM reviews ORDER BY date DESC")
    essays = query_db("SELECT * FROM essays ORDER BY date DESC")

    # 创建 Excel 工作簿
    wb = Workbook()
    
    # 复盘工作表
    ws_reviews = wb.active
    ws_reviews.title = "复盘记录"
    headers_reviews = ['ID', '日期', '市场情绪', '板块', '题材', '市值表现', '投资方式', '大事']
    ws_reviews.append(headers_reviews)
    for review in reviews:
        ws_reviews.append(review)
    
    # 随笔工作表
    ws_essays = wb.create_sheet("随笔记录")
    headers_essays = ['ID', '日期', '内容', '创建时间']
    ws_essays.append(headers_essays)
    for essay in essays:
        ws_essays.append(essay)

    # 保存 Excel 文件到内存
    output = io.BytesIO()
    wb.save(output)
    output.seek(0)

    # 发送 Excel 文件作为响应
    return send_file(
        output,
        as_attachment=True,
        download_name='复盘记录.xlsx',
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

#-------------------------2025年4月15日16:03:02新增功能------------------------------
@app.route('/upload_report/<int:id>', methods=['POST'])
@login_required
def upload_report(id):

    if 'report' not in request.files:
        return '没有选择文件', 400
    file = request.files['report']
    if file.filename == '':
        return '没有选择文件', 400

    if file:
        try:
            # 读取文件内容
            file_content = file.read()

            # 更新数据库
            conn = sqlite3.connect(DATABASE_FILE)
            c = conn.cursor()
            c.execute("UPDATE daily_reviews SET deep_report = ? WHERE id = ?", (file_content, id))
            conn.commit()
            conn.close()

            return redirect(url_for('index'))
        except Exception as e:
            logging.error(f"文件上传失败: {e}")
            return '文件上传失败', 500


@app.route('/download_report/<int:id>')
@login_required
def download_report(id):

    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute("SELECT deep_report FROM daily_reviews WHERE id = ?", (id,))
        report = c.fetchone()
        conn.close()

        if report and report[0]:
            # 创建内存文件
            mem_file = io.BytesIO(report[0])
            mem_file.seek(0)

            # 获取原始文件名
            conn = sqlite3.connect(DATABASE_FILE)
            c = conn.cursor()
            c.execute("SELECT date FROM daily_reviews WHERE id = ?", (id,))
            date = c.fetchone()[0]
            conn.close()

            # 设置文件名
            filename = f"深度报告_{date}.docx"

            return send_file(
                mem_file,
                as_attachment=True,
                download_name=filename,
                mimetype='application/vnd.openxmlformats-officedocument.wordprocessingml.document'
            )
        else:
            return '报告不存在', 404
    except Exception as e:
        logging.error(f"文件下载失败: {e}")
        return '文件下载失败', 500

# ==================== 投资日历功能 ====================

# 投研日历事件分类（色块与前端图例一致）
CALENDAR_EVENT_CATEGORIES = {
    'macro': {'label': '宏观数据', 'color': '#0369a1'},
    'earnings': {'label': '财报发布', 'color': '#15803d'},
    'meeting': {'label': '重要会议', 'color': '#7e22ce'},
    'personal_trade': {'label': '个人交易计划', 'color': '#c2410c'},
    'other': {'label': '其他', 'color': '#4f46e5'},
}


def _cal_date(s):
    if not s:
        return ''
    return str(s).strip()[:10]


def _category_display_color(category, stored_color):
    cat = category or 'other'
    if cat in CALENDAR_EVENT_CATEGORIES:
        return CALENDAR_EVENT_CATEGORIES[cat]['color']
    return stored_color or '#6366f1'


def _fc_end_exclusive_to_db_end_inclusive(start_s, end_exclusive_s):
    start_s = _cal_date(start_s)
    if not end_exclusive_s:
        return start_s
    end_ex = _cal_date(end_exclusive_s)
    if not end_ex or end_ex <= start_s:
        return start_s
    d_end = datetime.strptime(end_ex, '%Y-%m-%d') - timedelta(days=1)
    return d_end.strftime('%Y-%m-%d')


def _db_end_inclusive_to_fc_end_exclusive(start_s, end_inclusive_s):
    start_s = _cal_date(start_s)
    end_inc = _cal_date(end_inclusive_s) or start_s
    if not end_inc or end_inc <= start_s:
        return None
    d = datetime.strptime(end_inc, '%Y-%m-%d') + timedelta(days=1)
    return d.strftime('%Y-%m-%d')


def _calendar_row_to_dict(row):
    """SELECT c.*..., p.title 共 14 列（含 plan_title）。"""
    return {
        'id': row[0],
        'title': row[1],
        'content': row[2] or '',
        'start_date': row[3],
        'end_date': row[4],
        'reminder_type': row[5] or 'notification',
        'reminder_time': row[6] or 0,
        'reminder_sent': row[7] or 0,
        'color': row[8] or '#6366f1',
        'event_type': row[9] or 'single',
        'event_category': (row[10] or 'other') if len(row) > 10 else 'other',
        'related_stock': (row[11] or '').strip() if len(row) > 11 else '',
        'related_plan_id': int(row[12]) if len(row) > 12 and row[12] not in (None, '') else None,
        'plan_title': row[13] if len(row) > 13 else None,
    }


def _event_to_fullcalendar(ev):
    start = _cal_date(ev['start_date'])
    end_fc = _db_end_inclusive_to_fc_end_exclusive(start, ev.get('end_date'))
    cat = ev.get('event_category') or 'other'
    if cat not in CALENDAR_EVENT_CATEGORIES:
        cat = 'other'
    color = _category_display_color(cat, ev.get('color'))
    item = {
        'id': str(ev['id']),
        'title': ev['title'],
        'start': start,
        'allDay': True,
        'backgroundColor': color,
        'borderColor': color,
        'textColor': '#ffffff',
        'extendedProps': {
            'db_id': ev['id'],
            'content': ev.get('content') or '',
            'reminder_type': ev.get('reminder_type'),
            'reminder_time': ev.get('reminder_time'),
            'event_type': ev.get('event_type'),
            'event_category': cat,
            'related_stock': ev.get('related_stock') or '',
            'related_plan_id': ev.get('related_plan_id'),
            'plan_title': ev.get('plan_title'),
        },
    }
    if end_fc:
        item['end'] = end_fc
    return item


CALENDAR_TITLE_MAX = 500
CALENDAR_CONTENT_MAX = 50000
CALENDAR_STOCK_MAX = 64
CALENDAR_REMINDER_TYPES = frozenset({"none", "notification", "email"})
CALENDAR_EVENT_TYPES = frozenset({"single", "multi", "recurring"})


def _parse_iso_date_strict(s):
    if not s or not str(s).strip():
        return None
    t = str(s).strip()[:10]
    try:
        return datetime.strptime(t, "%Y-%m-%d").date()
    except ValueError:
        return None


def _get_calendar_plan_options():
    plan_rows = query_db(
        """SELECT id, title FROM investment_plans
           WHERE status IS NULL OR status != 'cancelled'
           ORDER BY updated_at DESC LIMIT 500"""
    )
    return [{"id": r[0], "title": r[1]} for r in (plan_rows or [])]


def _form_to_staged_event(form, event_id=None):
    """从表单数据构造与 event 模板兼容的字典，用于校验失败后回显。"""
    rp = (form.get("related_plan_id") or "").strip()
    try:
        rtime = int(form.get("reminder_time") or 0)
    except (TypeError, ValueError):
        rtime = 0
    return {
        "id": event_id,
        "title": (form.get("title") or "").strip(),
        "content": form.get("content") or "",
        "start_date": (form.get("start_date") or "").strip(),
        "end_date": (form.get("end_date") or "").strip(),
        "reminder_type": (form.get("reminder_type") or "notification").strip(),
        "reminder_time": rtime,
        "event_type": (form.get("event_type") or "single").strip(),
        "event_category": (form.get("event_category") or "other").strip(),
        "related_stock": (form.get("related_stock") or "").strip(),
        "related_plan_id": int(rp) if rp.isdigit() else None,
        "color": (form.get("color") or "#6366f1").strip(),
    }


def _validate_calendar_payload(data):
    """
    规范化并校验事件数据（来自 JSON 或表单扁平字典）。
    成功: (True, normalized_dict)
    失败: (False, {"error": str, "field": str|None})
    """
    def fail(msg, field=None):
        return False, {"error": msg, "field": field}

    title = (data.get("title") or "").strip()
    if not title:
        return fail("请填写事件标题", "title")
    if len(title) > CALENDAR_TITLE_MAX:
        return fail("标题过长", "title")

    content = data.get("content") or ""
    if len(content) > CALENDAR_CONTENT_MAX:
        return fail("备注内容过长", "content")

    start_d = _parse_iso_date_strict(data.get("start_date"))
    end_s = data.get("end_date")
    end_d = _parse_iso_date_strict(end_s) if end_s else start_d
    if not start_d:
        return fail("开始日期无效", "start_date")
    if not end_d:
        return fail("结束日期无效", "end_date")
    if end_d < start_d:
        return fail("结束日期不能早于开始日期", "end_date")

    rt = (data.get("reminder_type") or "notification").strip()
    if rt not in CALENDAR_REMINDER_TYPES:
        rt = "notification"

    try:
        rtime = int(data.get("reminder_time", 0))
    except (TypeError, ValueError):
        return fail("提醒提前量无效", "reminder_time")
    if rtime < 0 or rtime > 10 * 365 * 24 * 60:
        return fail("提醒提前量超出范围", "reminder_time")

    et = (data.get("event_type") or "single").strip()
    if et not in CALENDAR_EVENT_TYPES:
        et = "single"

    cat = (data.get("event_category") or "other").strip()
    if cat not in CALENDAR_EVENT_CATEGORIES:
        cat = "other"

    stock = (data.get("related_stock") or "").strip()
    if len(stock) > CALENDAR_STOCK_MAX:
        return fail("股票代码过长", "related_stock")
    if stock and not re.match(r"^[\d\w\.\s\-\u4e00-\u9fff]{1,64}$", stock, re.U):
        return fail("股票代码仅支持数字、字母、中文与常见符号", "related_stock")

    rp = data.get("related_plan_id")
    plan_id = None
    if rp not in (None, "", 0, "0"):
        try:
            plan_id = int(rp)
        except (TypeError, ValueError):
            return fail("投资计划选择无效", "related_plan_id")
        found = query_db("SELECT 1 FROM investment_plans WHERE id = ?", (plan_id,), one=True)
        if not found:
            return fail("所选投资计划不存在", "related_plan_id")

    eid = data.get("id")
    if eid not in (None, "", 0, "0"):
        try:
            eid = int(eid)
        except (TypeError, ValueError):
            return fail("事件 ID 无效", "id")
        if eid < 1:
            return fail("事件 ID 无效", "id")
        ex = query_db("SELECT 1 FROM investment_calendar WHERE id = ?", (eid,), one=True)
        if not ex:
            return fail("要更新的事件不存在", "id")
    else:
        eid = None

    color = _category_display_color(cat, data.get("color", "#6366f1"))
    return True, {
        "id": eid,
        "title": title,
        "content": content,
        "start_date": start_d.strftime("%Y-%m-%d"),
        "end_date": end_d.strftime("%Y-%m-%d"),
        "reminder_type": rt,
        "reminder_time": rtime,
        "event_type": et,
        "event_category": cat,
        "related_stock": stock,
        "related_plan_id": plan_id,
        "color": color,
    }


# 日历主页
@app.route('/calendar')
@login_required
def calendar():
    # 获取近期投资计划（有目标日期的）
    today = datetime.now().date()
    next_week = datetime.now().date() + timedelta(days=7)
    
    upcoming_plans = query_db("""SELECT id, title, description, target_date, status, priority, color 
                                 FROM investment_plans 
                                 WHERE target_date IS NOT NULL 
                                 AND target_date != ''
                                 AND target_date >= ? 
                                 AND target_date <= ?
                                 AND status != 'cancelled'
                                 ORDER BY target_date ASC 
                                 LIMIT 10""", 
                             (today.strftime('%Y-%m-%d'), next_week.strftime('%Y-%m-%d')))
    
    plans_list = []
    for plan in upcoming_plans:
        plans_list.append({
            'id': plan[0],
            'title': plan[1],
            'description': plan[2],
            'date': plan[3],
            'status': plan[4],
            'priority': plan[5],
            'color': plan[6] or '#f59e0b',
            'type': 'plan'
        })

    plan_options = _get_calendar_plan_options()
    
    return render_template(
        'calendar.html',
        upcoming_plans=plans_list,
        plan_options=plan_options,
        calendar_categories=CALENDAR_EVENT_CATEGORIES,
    )

# 获取日历事件（JSON API，FullCalendar 友好格式）
@app.route('/api/calendar/events')
@login_required
def get_calendar_events():
    fc_start = request.args.get('start', '')
    fc_end = request.args.get('end', '')

    base_sql = """SELECT c.id, c.title, c.content, c.start_date, c.end_date,
                         c.reminder_type, c.reminder_time, c.reminder_sent, c.color, c.event_type,
                         c.event_category, c.related_stock, c.related_plan_id, p.title AS plan_title
                  FROM investment_calendar c
                  LEFT JOIN investment_plans p ON c.related_plan_id = p.id """

    if fc_start and fc_end:
        try:
            first_day = _cal_date(fc_start)
            excl = _cal_date(fc_end)
            if not excl or len(excl) < 8:
                return jsonify({"success": False, "error": "无效的 end 参数"}), 400
            last_day = (datetime.strptime(excl, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            return jsonify({"success": False, "error": "日期范围参数无效"}), 400
        events = query_db(
            base_sql + """ WHERE c.start_date <= ? 
                           AND coalesce(nullif(trim(c.end_date), ''), c.start_date) >= ?
                           ORDER BY c.start_date ASC""",
            (last_day, first_day),
        )
    else:
        events = query_db(base_sql + " ORDER BY c.start_date ASC")

    events_list = [_event_to_fullcalendar(_calendar_row_to_dict(row)) for row in (events or [])]
    return jsonify(events_list)


@app.route('/api/calendar/events/<int:id>/delete', methods=['POST'])
@login_required
def api_delete_calendar_event(id):
    row = query_db("SELECT 1 FROM investment_calendar WHERE id = ?", (id,), one=True)
    if not row:
        return jsonify({"success": False, "error": "事件不存在或已删除"}), 404
    delete_db("DELETE FROM investment_calendar WHERE id = ?", (id,))
    return jsonify({"success": True})


# 跨页预填（爬虫→日历/计划）的「返回 / 取消 / 保存后」目标，仅白名单防开放重定向
_UI_RETURN_KEYS = frozenset({"calendar", "crawler", "plans", "index"})


def _ui_return_key_from_request(*, is_post=False, default="calendar"):
    if is_post:
        raw = request.form.get("return_to") or request.form.get("back")
    else:
        raw = request.args.get("return_to") or request.args.get("back")
    k = (raw or default).strip().lower()
    return k if k in _UI_RETURN_KEYS else default


def _url_for_ui_return(k):
    key = (k or "calendar").strip().lower()
    if key not in _UI_RETURN_KEYS:
        key = "calendar"
    if key == "crawler":
        return url_for("crawler_dashboard")
    if key == "plans":
        return url_for("plans")
    if key == "index":
        return url_for("index")
    return url_for("calendar")


# 添加/编辑日历事件
@app.route('/calendar/add', methods=['GET', 'POST'])
@app.route('/calendar/edit/<int:id>', methods=['GET', 'POST'])
@login_required
def add_calendar_event(id=None):
    if request.method == 'POST':
        return_key = _ui_return_key_from_request(is_post=True, default="calendar")
        raw = {**request.form}
        if id is not None:
            raw['id'] = id
        ok, vres = _validate_calendar_payload(raw)
        if not ok:
            return render_template(
                'add_calendar_event.html',
                event=_form_to_staged_event(request.form, id),
                form_error=vres['error'],
                plan_options=_get_calendar_plan_options(),
                calendar_categories=CALENDAR_EVENT_CATEGORIES,
                return_key=return_key,
                back_url=_url_for_ui_return(return_key),
            )
        d = vres
        if id is not None:
            update_db(
                """UPDATE investment_calendar 
                SET title=?, content=?, start_date=?, end_date=?, 
                reminder_type=?, reminder_time=?, color=?, event_type=?,
                event_category=?, related_stock=?, related_plan_id=?,
                updated_at=CURRENT_TIMESTAMP 
                WHERE id=?""",
                (
                    d['title'],
                    d['content'],
                    d['start_date'],
                    d['end_date'],
                    d['reminder_type'],
                    d['reminder_time'],
                    d['color'],
                    d['event_type'],
                    d['event_category'],
                    d['related_stock'],
                    d['related_plan_id'],
                    id,
                ),
            )
        else:
            insert_db(
                """INSERT INTO investment_calendar 
                (title, content, start_date, end_date, reminder_type, 
                 reminder_time, color, event_type, event_category, related_stock, related_plan_id) 
                VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    d['title'],
                    d['content'],
                    d['start_date'],
                    d['end_date'],
                    d['reminder_type'],
                    d['reminder_time'],
                    d['color'],
                    d['event_type'],
                    d['event_category'],
                    d['related_stock'],
                    d['related_plan_id'],
                ),
            )
        return redirect(_url_for_ui_return(return_key))
    
    # GET请求
    return_key = _ui_return_key_from_request(is_post=False, default="calendar")
    event = None
    if id:
        row = query_db(
            """SELECT c.id, c.title, c.content, c.start_date, c.end_date,
                      c.reminder_type, c.reminder_time, c.reminder_sent, c.color, c.event_type,
                      c.event_category, c.related_stock, c.related_plan_id, p.title AS plan_title
               FROM investment_calendar c
               LEFT JOIN investment_plans p ON c.related_plan_id = p.id
               WHERE c.id = ?""",
            (id,),
            one=True,
        )
        if row:
            d = _calendar_row_to_dict(row)
            event = {
                'id': d['id'],
                'title': d['title'],
                'content': d['content'],
                'start_date': d['start_date'],
                'end_date': d['end_date'],
                'reminder_type': d['reminder_type'],
                'reminder_time': d['reminder_time'],
                'color': d['color'],
                'event_type': d['event_type'],
                'event_category': d['event_category'],
                'related_stock': d['related_stock'],
                'related_plan_id': d['related_plan_id'],
            }
    elif not id:
        t = (request.args.get('title') or '').strip()
        d_arg = (request.args.get('date') or request.args.get('start_date') or '').strip()
        su = (request.args.get('source_url') or '').strip()
        if t or d_arg or su:
            today = datetime.now().date().isoformat()
            start_d = d_arg[:10] if len(d_arg) >= 10 else today
            content = f"来源链接：{su}\n" if su else ""
            event = {
                'id': None,
                'title': t,
                'content': content,
                'start_date': start_d,
                'end_date': start_d,
                'reminder_type': 'notification',
                'reminder_time': 0,
                'color': '#6366f1',
                'event_type': 'single',
                'event_category': 'other',
                'related_stock': '',
                'related_plan_id': None,
            }
    
    plan_options = _get_calendar_plan_options()
    return render_template(
        'add_calendar_event.html',
        event=event,
        plan_options=plan_options,
        calendar_categories=CALENDAR_EVENT_CATEGORIES,
        return_key=return_key,
        back_url=_url_for_ui_return(return_key),
    )

# 删除日历事件
@app.route('/calendar/delete/<int:id>')
@login_required
def delete_calendar_event(id):
    delete_db("DELETE FROM investment_calendar WHERE id =?", (id,))
    return redirect(url_for('calendar'))

# 保存日历事件（AJAX）
@app.route('/api/calendar/save', methods=['POST'])
@login_required
def save_calendar_event():
    data = request.get_json() or {}
    ok, vres = _validate_calendar_payload(data)
    if not ok:
        return jsonify({"success": False, "error": vres["error"], "field": vres.get("field")}), 400
    d = vres
    try:
        if d["id"]:
            update_db(
                """UPDATE investment_calendar 
                SET title=?, content=?, start_date=?, end_date=?, 
                reminder_type=?, reminder_time=?, color=?, event_type=?,
                event_category=?, related_stock=?, related_plan_id=?,
                updated_at=CURRENT_TIMESTAMP 
                WHERE id=?""",
                (
                    d['title'],
                    d['content'],
                    d['start_date'],
                    d['end_date'],
                    d['reminder_type'],
                    d['reminder_time'],
                    d['color'],
                    d['event_type'],
                    d['event_category'],
                    d['related_stock'],
                    d['related_plan_id'],
                    d['id'],
                ),
            )
            return jsonify({"success": True, "id": d["id"]})
        new_id = insert_db_return_id(
            """INSERT INTO investment_calendar 
            (title, content, start_date, end_date, reminder_type, 
            reminder_time, color, event_type, event_category, related_stock, related_plan_id) 
            VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (
                d['title'],
                d['content'],
                d['start_date'],
                d['end_date'],
                d['reminder_type'],
                d['reminder_time'],
                d['color'],
                d['event_type'],
                d['event_category'],
                d['related_stock'],
                d['related_plan_id'],
            ),
        )
        return jsonify({"success": True, "id": new_id})
    except Exception as e:
        logging.error("保存日历事件失败: %s", e, exc_info=True)
        return jsonify({"success": False, "error": "保存失败，请稍后再试"}), 500

# 农历转换功能
def solar_to_lunar(year, month, day):
    """
    将公历日期转换为农历日期
    优先使用zhdate库（需要安装：pip install zhdate）
    """
    # 农历月份名称
    lunar_month_names = [
        '正', '二', '三', '四', '五', '六', '七', '八', '九', '十', '冬', '腊'
    ]
    
    # 农历日期名称
    lunar_day_names = [
        '初一', '初二', '初三', '初四', '初五', '初六', '初七', '初八', '初九', '初十',
        '十一', '十二', '十三', '十四', '十五', '十六', '十七', '十八', '十九', '二十',
        '廿一', '廿二', '廿三', '廿四', '廿五', '廿六', '廿七', '廿八', '廿九', '三十'
    ]
    
    try:
        # 优先使用zhdate库（如果已安装）
        try:
            from zhdate import ZhDate
            lunar_date = ZhDate.from_datetime(datetime(year, month, day))
            month_name = lunar_month_names[lunar_date.lunar_month - 1]
            day_name = lunar_day_names[lunar_date.lunar_day - 1]
            # 如果有闰月，添加"闰"字
            # zhdate库中闰月属性是 is_leap（带下划线）
            if hasattr(lunar_date, 'is_leap') and lunar_date.is_leap:
                month_name = '闰' + month_name
            return f"{month_name}月{day_name}"
        except ImportError:
            # 如果没有zhdate库，使用基本算法（可能不够准确）
            logging.warning("未安装zhdate库，使用简化农历算法。建议安装：pip install zhdate")
            return _simple_lunar_conversion(year, month, day, lunar_month_names, lunar_day_names)
    except Exception as e:
        logging.error(f"农历转换失败: {e}")
        return ""

def _simple_lunar_conversion(year, month, day, lunar_month_names, lunar_day_names):
    """简化的农历转换（近似计算）"""
    # 注意：这是一个非常简化的实现，可能不够准确
    # 强烈建议安装zhdate库以获得准确的农历转换：pip install zhdate
    
    # 由于完整的农历算法非常复杂，这里返回一个提示信息
    # 实际应用中应该安装zhdate库
    try:
        # 可以尝试使用在线API或其他方式
        # 暂时返回空字符串，让前端显示提示
        return ""
    except Exception as e:
        logging.error(f"农历转换失败: {e}")
        return ""

# 获取农历API
@app.route('/api/calendar/lunar/<date_str>')
@login_required
def get_lunar_date(date_str):
    """获取指定日期的农历"""
    try:
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        lunar = solar_to_lunar(date_obj.year, date_obj.month, date_obj.day)
        return jsonify({'lunar': lunar})
    except Exception as e:
        return jsonify({'lunar': '', 'error': str(e)})

# 检查提醒（可以在后台任务中定期执行）
@app.route('/api/calendar/check_reminders')
@login_required
def check_reminders():
    """检查需要提醒的事件"""
    from datetime import datetime, timedelta
    now = datetime.now()
    today = now.date()
    
    # 查询需要提醒的事件（包括已提醒和未提醒的，因为多日事件需要每天提醒）
    events = query_db("""SELECT id, title, start_date, end_date, reminder_time, reminder_type, event_type
                        FROM investment_calendar 
                        WHERE reminder_type != 'none'""")
    
    reminders = []
    for event in events:
        event_id = event[0]
        title = event[1]
        start_date = datetime.strptime(event[2], '%Y-%m-%d').date()
        end_date = datetime.strptime(event[3] or event[2], '%Y-%m-%d').date() if event[3] else start_date
        reminder_time = event[4] or 0  # 提前多少分钟提醒
        reminder_type = event[5]
        event_type = event[6] or 'single'
        
        # 对于多日事件，检查今天是否在日期范围内
        if event_type in ['multi', 'recurring']:
            # 多日事件：在范围内的每一天都提醒
            if start_date <= today <= end_date:
                # 检查今天是否已经提醒过
                # 使用查询判断：检查数据库中是否有今天的提醒记录
                # 为了简化，我们使用一个方法：查询 reminder_sent，如果为0或今天还没提醒
                
                # 计算今天的提醒时间
                today_datetime = datetime.combine(today, datetime.min.time())
                remind_at = today_datetime - timedelta(minutes=reminder_time)
                
                # 如果提醒时间已到
                if now >= remind_at:
                    reminders.append({
                        'id': event_id,
                        'title': title,
                        'date': today.strftime('%Y-%m-%d'),
                        'type': reminder_type,
                        'is_multi_day': True
                    })
        else:
            # 单日事件：只在开始日期提醒
            event_date = start_date
            reminder_datetime = datetime.combine(event_date, datetime.min.time())
            remind_at = reminder_datetime - timedelta(minutes=reminder_time)
            
            # 如果提醒时间已到，且还没提醒
            if now >= remind_at and event_date == today:
                # 检查是否已经提醒过
                reminder_sent = query_db("SELECT reminder_sent FROM investment_calendar WHERE id = ?", 
                                       (event_id,), one=True)
                if reminder_sent and reminder_sent[0] == 0:
                    reminders.append({
                        'id': event_id,
                        'title': title,
                        'date': event[2],
                        'type': reminder_type,
                        'is_multi_day': False
                    })
                    # 标记为已提醒（单日事件只提醒一次）
                    update_db("UPDATE investment_calendar SET reminder_sent = 1 WHERE id = ?", (event_id,))
    
    return jsonify(reminders)

# ==================== 投资计划功能 ====================
# 投资计划列表页（看板视图）
@app.route('/plans')
@login_required
def plans():
    status_filter = request.args.get('status', '')
    category_filter = request.args.get('category', '')
    
    query = "SELECT * FROM investment_plans WHERE 1=1"
    params = []
    
    if status_filter:
        query += " AND status = ?"
        params.append(status_filter)
    
    if category_filter:
        query += " AND category = ?"
        params.append(category_filter)
    
    query += " ORDER BY created_at DESC"
    
    all_plans = query_db(query, tuple(params))
    
    # 按状态分组
    plans_by_status = {
        'todo': [],
        'in_progress': [],
        'done': [],
        'cancelled': []
    }
    
    for plan in all_plans:
        pd = _plan_tuple_to_dict(plan)
        status = pd["status"]
        plans_by_status[status].append(pd)
    
    # 获取所有分类和标签（用于筛选）
    all_categories = query_db("SELECT DISTINCT category FROM investment_plans WHERE category IS NOT NULL AND category != ''")
    categories = [cat[0] for cat in all_categories]
    
    return render_template('plans.html', 
                         plans_by_status=plans_by_status,
                         status_filter=status_filter,
                         category_filter=category_filter,
                         categories=categories)

# 添加/编辑投资计划
@app.route('/plans/add', methods=['GET', 'POST'])
@app.route('/plans/edit/<int:id>', methods=['GET', 'POST'])
@login_required
def add_plan(id=None):
    if request.method == 'POST':
        return_key = _ui_return_key_from_request(is_post=True, default="plans")
        title = request.form.get('title', '')
        description = request.form.get('description', '')
        status = request.form.get('status', 'todo')
        priority = request.form.get('priority', 'medium')
        category = request.form.get('category', '')
        tags = request.form.get('tags', '')
        target_date = request.form.get('target_date', '')
        progress = int(request.form.get('progress', 0))
        color = request.form.get('color', '#f59e0b')
        is_profitable = request.form.get('is_profitable')
        # 将字符串转换为整数：'1' -> 1, '0' -> 0, '' -> None
        is_profitable = int(is_profitable) if is_profitable in ['0', '1'] else None
        keywords = (request.form.get('keywords') or '').strip()
        instruments = (request.form.get('instruments') or '').strip()
        tracking_items_json = _tracking_items_form_to_json(request.form.get("tracking_items"))
        if not keywords and instruments:
            keywords = instruments
        
        if id:
            # 更新
            update_db("""UPDATE investment_plans 
                        SET title=?, description=?, status=?, priority=?, 
                        category=?, tags=?, target_date=?, progress=?, color=?, is_profitable=?,
                        keywords=?, instruments=?, tracking_items=?, updated_at=CURRENT_TIMESTAMP 
                        WHERE id=?""",
                     (title, description, status, priority, category, tags, 
                      target_date, progress, color, is_profitable, keywords, instruments,
                      tracking_items_json, id))
        else:
            # 新增
            new_pid = insert_db_return_id(
                """INSERT INTO investment_plans 
                (title, description, status, priority, category, tags, 
                 target_date, progress, color, is_profitable, keywords, instruments, tracking_items) 
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (title, description, status, priority, category, tags,
                 target_date, progress, color, is_profitable, keywords, instruments,
                 tracking_items_json),
            )
            cal_row = {
                "title": title,
                "description": description,
                "target_date": target_date,
                "color": color,
                "tracking_items": _tracking_items_json_to_list(tracking_items_json),
            }
            _insert_plan_calendar_events(new_pid, cal_row, True)
        
        return redirect(_url_for_ui_return(return_key))
    
    # GET请求
    return_key = _ui_return_key_from_request(is_post=False, default="plans")
    plan = None
    if id:
        plan_data = query_db("SELECT * FROM investment_plans WHERE id =?", (id,), one=True)
        if plan_data:
            plan = _plan_tuple_to_dict(plan_data)
            tl = _tracking_items_json_to_list(plan.get("tracking_items"))
            plan["tracking_items"] = (
                json.dumps(tl, ensure_ascii=False, indent=2) if tl else ""
            )
    elif (request.args.get('title') or '').strip() or (request.args.get('description') or '').strip():
        t = (request.args.get('title') or '').strip()
        desc = (request.args.get('description') or '').strip()
        # id 显式为 None，避免模板中「if plan」把预填当编辑，去访问不存在的 plan.id
        plan = {
            'id': None,
            'title': t,
            'description': desc,
            'status': 'todo',
            'priority': 'medium',
            'category': '',
            'tags': '',
            'target_date': '',
            'progress': 0,
            'color': '#f59e0b',
            'is_profitable': None,
            'keywords': (request.args.get('keywords') or '').strip(),
            'instruments': (request.args.get('instruments') or '').strip(),
            'tracking_items': '',
        }
    
    return render_template(
        'add_plan.html',
        plan=plan,
        return_key=return_key,
        back_url=_url_for_ui_return(return_key),
    )


@app.route("/api/plans/ai_from_text", methods=["POST"])
@login_required
def api_plans_ai_from_text():
    """
    将自然语言笔记交给 DeepSeek 解析为 investment_plans 字段。
    JSON 体: { "text": "...", "commit": false, "sync_calendar": true }
    commit 为 true 时写入数据库并返回 id。
    sync_calendar 默认 true：在 commit 时，为 target_date 与每条带日期的 tracking_items 写入 investment_calendar（均关联本计划）。
    """
    data = request.get_json() or {}
    text = (data.get("text") or "").strip()
    commit = bool(data.get("commit"))
    sync_calendar = data.get("sync_calendar")
    if sync_calendar is None:
        sync_calendar = True
    else:
        sync_calendar = bool(sync_calendar)
    try:
        row = ai_parse_investment_plan_from_text(text)
    except ValueError as e:
        return jsonify({"success": False, "error": str(e)}), 400
    except Exception as e:
        logging.error("AI 解析投资计划失败: %s", e, exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 502

    if not commit:
        return jsonify({"success": True, "plan": row})

    try:
        ti_store = json.dumps(row.get("tracking_items") or [], ensure_ascii=False)
        new_id = insert_db_return_id(
            """INSERT INTO investment_plans
            (title, description, status, priority, category, tags,
             target_date, progress, color, is_profitable, keywords, instruments, tracking_items)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                row["title"],
                row["description"],
                row["status"],
                row["priority"],
                row["category"],
                row["tags"],
                row["target_date"],
                row["progress"],
                row["color"],
                row["is_profitable"],
                row["keywords"],
                row.get("instruments") or "",
                ti_store,
            ),
        )
    except Exception as e:
        logging.error("AI 投资计划入库失败: %s", e, exc_info=True)
        return jsonify({"success": False, "error": str(e)}), 500

    calendar_event_ids = []
    if sync_calendar:
        try:
            calendar_event_ids = _insert_plan_calendar_events(new_id, row, True)
        except Exception as e:
            logging.warning("AI 计划已保存但同步日历失败: %s", e, exc_info=True)

    return jsonify(
        {
            "success": True,
            "id": new_id,
            "plan": row,
            "calendar_event_ids": calendar_event_ids,
        }
    )


# 删除投资计划
@app.route('/plans/delete/<int:id>')
@login_required
def delete_plan(id):
    delete_db("DELETE FROM investment_plans WHERE id =?", (id,))
    return redirect(url_for('plans'))

# 更新计划状态（AJAX）
@app.route('/api/plans/update_status', methods=['POST'])
@login_required
def update_plan_status():
    try:
        data = request.get_json()
        plan_id = data.get('id')
        status = data.get('status')
        
        update_db("UPDATE investment_plans SET status=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", 
                 (status, plan_id))
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# 更新计划进度（AJAX）
@app.route('/api/plans/update_progress', methods=['POST'])
@login_required
def update_plan_progress():
    try:
        data = request.get_json()
        plan_id = data.get('id')
        progress = int(data.get('progress', 0))
        progress = max(0, min(100, progress))  # 限制在0-100之间
        
        update_db("UPDATE investment_plans SET progress=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", 
                 (progress, plan_id))
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# 更新计划盈利状态（AJAX）
@app.route('/api/plans/update_profitable', methods=['POST'])
@login_required
def update_plan_profitable():
    try:
        data = request.get_json()
        plan_id = data.get('id')
        is_profitable = data.get('is_profitable')
        # 将布尔值或字符串转换为整数
        if is_profitable is None:
            is_profitable_value = None
        elif isinstance(is_profitable, bool):
            is_profitable_value = 1 if is_profitable else 0
        elif isinstance(is_profitable, str):
            is_profitable_value = 1 if is_profitable.lower() in ['true', '1', 'yes'] else 0
        else:
            is_profitable_value = int(is_profitable) if is_profitable else None
        
        update_db("UPDATE investment_plans SET is_profitable=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", 
                 (is_profitable_value, plan_id))
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# ==================== 市场基调功能 ====================
# 设置市场基调（引导式流程）
@app.route('/market_tone/set')
@app.route('/market_tone/set/<date>')
@login_required
def set_market_tone(date=None):
    if not date:
        date = datetime.now().strftime('%Y-%m-%d')
    
    # 检查是否已设置
    existing_tone = query_db("SELECT * FROM market_tone WHERE date = ?", (date,), one=True)
    tone_data = None
    if existing_tone:
        tone_data = {
            'date': existing_tone[1],
            'volume_status': existing_tone[2],
            'emotion_status': existing_tone[3],
            'divergence_status': existing_tone[4],
            'investment_strategies': existing_tone[5] or '',
            'investment_styles': existing_tone[6] or ''
        }
    
    return render_template('set_market_tone.html', date=date, existing_tone=tone_data)

# 保存市场基调
@app.route('/market_tone/save', methods=['POST'])
@login_required
def save_market_tone():
    date = request.form.get('date')
    volume_status = request.form.get('volume_status')
    emotion_status = request.form.get('emotion_status')
    divergence_status = request.form.get('divergence_status')
    investment_strategies = ','.join(request.form.getlist('investment_strategies'))
    investment_styles = ','.join(request.form.getlist('investment_styles'))
    
    # 检查是否已存在
    existing = query_db("SELECT id FROM market_tone WHERE date = ?", (date,), one=True)
    
    if existing:
        # 更新
        update_db("""UPDATE market_tone 
                    SET volume_status=?, emotion_status=?, divergence_status=?,
                    investment_strategies=?, investment_styles=?,
                    updated_at=CURRENT_TIMESTAMP 
                    WHERE date=?""",
                 (volume_status, emotion_status, divergence_status, 
                  investment_strategies, investment_styles, date))
    else:
        # 新增
        insert_db("""INSERT INTO market_tone 
                    (date, volume_status, emotion_status, divergence_status,
                     investment_strategies, investment_styles) 
                    VALUES (?,?,?,?,?,?)""",
                 (date, volume_status, emotion_status, divergence_status,
                  investment_strategies, investment_styles))
    
    return redirect(url_for('index'))

# 获取指定日期的市场基调
@app.route('/api/market_tone/<date>')
@login_required
def get_market_tone(date):
    tone = query_db("SELECT * FROM market_tone WHERE date = ?", (date,), one=True)
    if tone:
        return jsonify({
            'date': tone[1],
            'volume_status': tone[2],
            'emotion_status': tone[3],
            'divergence_status': tone[4],
            'investment_strategies': tone[5],
            'investment_styles': tone[6]
        })
    return jsonify({'error': '未找到'}), 404

# ==================== 指数走势功能 ====================
# 设置指数走势（引导式流程）
@app.route('/index_trend/set')
@app.route('/index_trend/set/<date>')
@login_required
def set_index_trend(date=None):
    if not date:
        date = datetime.now().strftime('%Y-%m-%d')
    
    # 检查是否已设置
    existing_trend = query_db("SELECT * FROM index_trend WHERE date = ?", (date,), one=True)
    trend_data = None
    if existing_trend:
        trend_data = {
            'date': existing_trend[1],
            'shanghai_trend': existing_trend[2],
            'shenzhen_trend': existing_trend[3],
            'chinext_trend': existing_trend[4]
        }
    
    return render_template('set_index_trend.html', date=date, existing_trend=trend_data)

# 保存指数走势
@app.route('/index_trend/save', methods=['POST'])
@login_required
def save_index_trend():
    date = request.form.get('date')
    shanghai_trend = request.form.get('shanghai_trend')
    shenzhen_trend = request.form.get('shenzhen_trend')
    chinext_trend = request.form.get('chinext_trend')
    
    # 检查是否已存在
    existing = query_db("SELECT id FROM index_trend WHERE date = ?", (date,), one=True)
    
    if existing:
        # 更新
        update_db("""UPDATE index_trend 
                    SET shanghai_trend=?, shenzhen_trend=?, chinext_trend=?,
                    updated_at=CURRENT_TIMESTAMP 
                    WHERE date=?""",
                 (shanghai_trend, shenzhen_trend, chinext_trend, date))
    else:
        # 新增
        insert_db("""INSERT INTO index_trend 
                    (date, shanghai_trend, shenzhen_trend, chinext_trend) 
                    VALUES (?,?,?,?)""",
                 (date, shanghai_trend, shenzhen_trend, chinext_trend))
    
    return redirect(url_for('index'))

# 获取指定日期的指数走势
@app.route('/api/index_trend/<date>')
@login_required
def get_index_trend(date):
    trend = query_db("SELECT * FROM index_trend WHERE date = ?", (date,), one=True)
    if trend:
        return jsonify({
            'date': trend[1],
            'shanghai_trend': trend[2],
            'shenzhen_trend': trend[3],
            'chinext_trend': trend[4]
        })
    return jsonify({'error': '未找到'}), 404

# ==================== 深度报告和总结功能 ====================
# 深度报告列表页
@app.route('/reports')
@login_required
def reports():
    category_filter = request.args.get('category', '')
    search_query = request.args.get('search', '')
    
    query = "SELECT * FROM deep_reports WHERE 1=1"
    params = []
    
    if category_filter:
        query += " AND category = ?"
        params.append(category_filter)
    
    if search_query:
        query += " AND (title LIKE ? OR content LIKE ? OR summary LIKE ?)"
        search_pattern = f"%{search_query}%"
        params.extend([search_pattern, search_pattern, search_pattern])
    
    query += " ORDER BY created_at DESC"
    
    all_reports = query_db(query, tuple(params))
    
    reports_list = []
    for report in all_reports:
        reports_list.append({
            'id': report[0],
            'title': report[1],
            'content': report[2] or '',
            'summary': report[3] or '',
            'category': report[4],
            'tags': report[5],
            'related_plan_id': report[6],
            'date': report[7],
            'created_at': report[8],
            'updated_at': report[9]
        })
    
    # 获取所有分类（用于筛选）
    all_categories = query_db("SELECT DISTINCT category FROM deep_reports WHERE category IS NOT NULL AND category != ''")
    categories = [cat[0] for cat in all_categories]
    
    return render_template('reports.html', 
                         reports=reports_list,
                         category_filter=category_filter,
                         search_query=search_query,
                         categories=categories)

# 查看深度报告详情
@app.route('/reports/view/<int:id>')
@login_required
def view_report(id):
    report_data = query_db("SELECT * FROM deep_reports WHERE id = ?", (id,), one=True)
    if not report_data:
        return redirect(url_for('reports'))
    
    report = {
        'id': report_data[0],
        'title': report_data[1],
        'content': report_data[2] or '',
        'summary': report_data[3] or '',
        'category': report_data[4],
        'tags': report_data[5],
        'related_plan_id': report_data[6],
        'date': report_data[7],
        'created_at': report_data[8],
        'updated_at': report_data[9]
    }
    
    # 获取关联的投资计划信息（如果有）
    related_plan = None
    if report['related_plan_id']:
        plan_data = query_db("SELECT id, title FROM investment_plans WHERE id = ?", 
                           (report['related_plan_id'],), one=True)
        if plan_data:
            related_plan = {
                'id': plan_data[0],
                'title': plan_data[1]
            }
    
    return render_template('view_report.html', report=report, related_plan=related_plan)

# 添加/编辑深度报告
@app.route('/reports/add', methods=['GET', 'POST'])
@app.route('/reports/edit/<int:id>', methods=['GET', 'POST'])
@login_required
def add_report(id=None):
    if request.method == 'POST':
        title = request.form.get('title', '')
        content = request.form.get('content', '')
        summary = request.form.get('summary', '')
        category = request.form.get('category', '')
        tags = request.form.get('tags', '')
        related_plan_id = request.form.get('related_plan_id', '')
        date = request.form.get('date', '')
        
        # 处理多维度选择
        sheet_types = ','.join(request.form.getlist('sheet_types'))
        stock_codes = request.form.get('stock_codes', '')
        concept_ids = ','.join(request.form.getlist('concept_ids'))
        industry_names = ','.join(request.form.getlist('industry_names'))
        
        # 处理关联计划ID
        related_plan_id = int(related_plan_id) if related_plan_id else None
        
        if id:
            # 更新
            update_db("""UPDATE deep_reports 
                        SET title=?, content=?, summary=?, category=?, tags=?, 
                        related_plan_id=?, date=?, sheet_types=?, stock_codes=?, 
                        concept_ids=?, industry_names=?, updated_at=CURRENT_TIMESTAMP 
                        WHERE id=?""",
                     (title, content, summary, category, tags, related_plan_id, date, 
                      sheet_types, stock_codes, concept_ids, industry_names, id))
        else:
            # 新增
            insert_db("""INSERT INTO deep_reports 
                        (title, content, summary, category, tags, related_plan_id, date, 
                         sheet_types, stock_codes, concept_ids, industry_names) 
                        VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                     (title, content, summary, category, tags, related_plan_id, date,
                      sheet_types, stock_codes, concept_ids, industry_names))
        
        return redirect(url_for('reports'))
    
    # GET请求
    report = None
    if id:
        report_data = query_db("SELECT * FROM deep_reports WHERE id = ?", (id,), one=True)
        if report_data:
            report = {
                'id': report_data[0],
                'title': report_data[1],
                'content': report_data[2] or '',
                'summary': report_data[3] or '',
                'category': report_data[4],
                'tags': report_data[5],
                'related_plan_id': report_data[6],
                'date': report_data[7],
                'sheet_types': report_data[8] if len(report_data) > 8 else '',
                'stock_codes': report_data[9] if len(report_data) > 9 else '',
                'concept_ids': report_data[10] if len(report_data) > 10 else '',
                'industry_names': report_data[11] if len(report_data) > 11 else ''
            }
    
    # 获取所有投资计划（用于关联）
    all_plans = query_db("SELECT id, title FROM investment_plans ORDER BY created_at DESC")
    plans = [{'id': plan[0], 'title': plan[1]} for plan in all_plans]
    
    # 默认日期为今天
    today = datetime.now().strftime('%Y-%m-%d')
    
    return render_template('add_report.html', report=report, plans=plans, default_date=today)

# 删除深度报告
@app.route('/reports/delete/<int:id>')
@login_required
def delete_report(id):
    delete_db("DELETE FROM deep_reports WHERE id = ?", (id,))
    return redirect(url_for('reports'))

# ==================== 概念股维护功能 ====================
# 概念股列表页
@app.route('/concept_stocks')
@login_required
def concept_stocks():
    all_concepts = query_db("SELECT * FROM concept_stocks ORDER BY created_at DESC")
    concepts_list = []
    for concept in all_concepts:
        # 获取该概念下的股票数量
        stock_count = query_db("SELECT COUNT(*) FROM concept_stock_items WHERE concept_id = ?", (concept[0],), one=True)[0]
        concepts_list.append({
            'id': concept[0],
            'concept_name': concept[1],
            'description': concept[2] or '',
            'stock_count': stock_count,
            'created_at': concept[3],
            'updated_at': concept[4]
        })
    
    return render_template('concept_stocks.html', concepts=concepts_list)

# 添加/编辑概念股
@app.route('/concept_stocks/add', methods=['GET', 'POST'])
@app.route('/concept_stocks/edit/<int:id>', methods=['GET', 'POST'])
@login_required
def add_concept_stock(id=None):
    if request.method == 'POST':
        concept_name = request.form.get('concept_name', '').strip()
        description = request.form.get('description', '').strip()
        stock_codes_str = request.form.get('stock_codes', '').strip()
        
        if not concept_name:
            return jsonify({'success': False, 'error': '概念股名称不能为空'})
        
        if id:
            # 更新概念股信息
            update_db("""UPDATE concept_stocks 
                        SET concept_name=?, description=?, updated_at=CURRENT_TIMESTAMP 
                        WHERE id=?""",
                     (concept_name, description, id))
            
            # 更新股票列表（先删除旧的，再添加新的）
            delete_db("DELETE FROM concept_stock_items WHERE concept_id = ?", (id,))
        else:
            # 检查是否已存在同名概念
            existing = query_db("SELECT id FROM concept_stocks WHERE concept_name = ?", (concept_name,), one=True)
            if existing:
                return jsonify({'success': False, 'error': '该概念股名称已存在'})
            
            # 新增概念股
            insert_db("""INSERT INTO concept_stocks (concept_name, description) 
                        VALUES (?,?)""",
                     (concept_name, description))
            id = query_db("SELECT last_insert_rowid()", one=True)[0]
        
        # 处理股票代码列表
        if stock_codes_str:
            stock_codes = [code.strip() for code in stock_codes_str.split(',') if code.strip()]
            for stock_code in stock_codes:
                # 获取股票名称（如果可能）
                stock_name = None
                try:
                    from data_fetcher import JuyuanDataFetcher
                    fetcher = JuyuanDataFetcher(lazy_init_pool=True)
                    stock_info = fetcher.get_stock_basic_info(stock_code)
                    if stock_info:
                        stock_name = stock_info.get('SecuAbbr', None)
                except:
                    pass
                
                insert_db("""INSERT OR IGNORE INTO concept_stock_items (concept_id, stock_code, stock_name) 
                            VALUES (?,?,?)""",
                         (id, stock_code, stock_name))
        
        return redirect(url_for('concept_stocks'))
    
    # GET请求
    concept = None
    stock_items = []
    if id:
        concept_data = query_db("SELECT * FROM concept_stocks WHERE id = ?", (id,), one=True)
        if concept_data:
            concept = {
                'id': concept_data[0],
                'concept_name': concept_data[1],
                'description': concept_data[2] or ''
            }
            # 获取该概念下的股票列表
            items = query_db("SELECT stock_code, stock_name FROM concept_stock_items WHERE concept_id = ? ORDER BY stock_code", (id,))
            stock_items = [{'code': item[0], 'name': item[1] or ''} for item in items]
    
    return render_template('add_concept_stock.html', concept=concept, stock_items=stock_items)

# 删除概念股
@app.route('/concept_stocks/delete/<int:id>')
@login_required
def delete_concept_stock(id):
    delete_db("DELETE FROM concept_stocks WHERE id = ?", (id,))
    return redirect(url_for('concept_stocks'))


# ==================== 每日市场情绪打分 ====================
@app.route('/market_sentiment')
@login_required
def market_sentiment():
    """每日市场情绪打分页面：多维度风险监控与得分"""
    import json as _json
    initial_result_json = None
    cached_dates_json = '[]'
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute("SELECT result_json FROM market_sentiment_results ORDER BY trading_day DESC LIMIT 1")
        row = c.fetchone()
        if row and row[0]:
            _init_d = _json.loads(row[0])
            # 若主表缺少能量字段，从能量专表补充（单独 try-except 防止表不存在时崩溃）
            if _init_d.get('energy_index') is None:
                try:
                    _td0 = _init_d.get('trading_day')
                    if _td0:
                        c.execute(
                            "SELECT energy_index, energy_reset FROM energy_index_results WHERE trading_day = ?",
                            (_td0,))
                        _ei0 = c.fetchone()
                        if _ei0 and _ei0[0] is not None:
                            _init_d['energy_index'] = float(_ei0[0])
                            _init_d['energy_reset'] = bool(_ei0[1])
                except Exception:
                    pass
            initial_result_json = _json.dumps(_init_d, ensure_ascii=False)
        c.execute("SELECT trading_day FROM market_sentiment_results ORDER BY trading_day")
        cached_dates_json = _json.dumps([r[0] for r in c.fetchall()])
        conn.close()
    except Exception as e:
        logging.error(f'读取市场情绪结果失败: {e}', exc_info=True)
    return render_template('market_sentiment.html',
                           initial_result_json=initial_result_json,
                           cached_dates_json=cached_dates_json)


@app.route('/api/market_sentiment/cached', methods=['GET'])
@login_required
def api_market_sentiment_cached():
    """查询某日缓存结果（有则返回，无则返回 null）"""
    import json as _json
    trading_day = request.args.get('trading_day', '').strip()
    if not trading_day:
        return jsonify({'success': False, 'error': '缺少 trading_day'})
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute("SELECT result_json FROM market_sentiment_results WHERE trading_day = ?", (trading_day,))
        row = c.fetchone()
        if row and row[0]:
            _data = _json.loads(row[0])
            # 若主表 JSON 中缺少能量字段（批量旧结果），从能量专表补充
            if _data.get('energy_index') is None:
                try:
                    c.execute(
                        "SELECT energy_index, energy_reset FROM energy_index_results WHERE trading_day = ?",
                        (trading_day,))
                    ei_row = c.fetchone()
                    if ei_row and ei_row[0] is not None:
                        _data['energy_index'] = float(ei_row[0])
                        _data['energy_reset'] = bool(ei_row[1])
                except Exception:
                    pass
            conn.close()
            return jsonify({'success': True, 'cached': True, 'data': _data})
        conn.close()
        return jsonify({'success': True, 'cached': False})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/market_sentiment/sparkline', methods=['GET'])
@login_required
def api_market_sentiment_sparkline():
    """返回 trading_day 及其之前最多 5 天的关键指标序列，用于迷你趋势图"""
    import json as _json
    trading_day = request.args.get('trading_day', '').strip()
    if not trading_day:
        return jsonify({'success': False, 'error': '缺少 trading_day'})
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute(
            "SELECT trading_day, result_json FROM market_sentiment_results "
            "WHERE trading_day <= ? ORDER BY trading_day DESC LIMIT 5",
            (trading_day,)
        )
        rows = c.fetchall()
        conn.close()
        series = []
        for td, rj in reversed(rows):
            try:
                d = _json.loads(rj)
            except Exception:
                continue
            m1 = d.get('module1') or {}
            m3 = d.get('module3') or {}
            m5 = d.get('module5') or {}
            m7 = d.get('module7') or {}
            series.append({
                'day': td,
                'total': d.get('total_score'),
                'mfr': m1.get('money_flow_ratio'),
                'divergence': m1.get('divergence'),
                'sigma_w': m3.get('sigma_weighted'),
                'spread_size': m5.get('spread_size'),
                'spread_vol': m5.get('spread_vol'),
                'smart_distrib': m7.get('net_distrib_ratio') if m7.get('net_distrib_ratio') is not None else m7.get('smart_distrib_ratio'),
                'turnover_prem': m7.get('top_turnover_avg_return'),
            })
        return jsonify({'success': True, 'series': series})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/market_sentiment/metric_history')
@login_required
def api_market_sentiment_metric_history():
    """返回某个具体指标在指定日期及之前 N 天的历史序列，用于弹窗趋势图。"""
    import json as _json
    metric = request.args.get('metric', '').strip()
    trading_day = request.args.get('trading_day', '').strip()
    days = min(int(request.args.get('days', 90)), 365)

    METRIC_LABELS = {
        'total_score': '截面总分',
        'energy_index': '连续能量指数',
        'mfr': '成交额多空比(MFR)',
        'mv_flow_ratio': '市值多空比',
        'divergence': '多空背离度',
        'limit_up': '涨停家数',
        'limit_down': '跌停家数',
        'one_word_limit_down': '一字跌停家数',
        'turnover_today': '当日成交额(亿)',
        'vol_5': '5日均量(亿)',
        'vol_20': '20日均量(亿)',
        'sigma_weighted': '加权σ(WCSV)',
        'sigma_equal': '等权σ',
        'sigma_percentile': 'WCSV 60日分位(%)',
        'spread_size': 'Size Spread（小盘-大盘）',
        'spread_vol': 'Vol Spread（高波-低波）',
        'hot_count': '昨日涨停家数',
        'hot_avg_return': '昨涨停今均收益',
        'nuke_ratio': '核按钮比例',
        'smart_distrib_ratio': '诱多派发占比',
        'smart_accum_ratio': '诱空吸筹占比',
        'net_distrib_ratio': '净派发能量',
        'unilateral_up_ratio': '单边上涨占比',
        'unilateral_down_ratio': '单边下跌占比',
        'top_turnover_avg_return': '活跃池盈亏',
        'strong_avg_return': '强势股今日均值',
        # 指数（从 M4 提取收盘价）
        'idx_sh':     '上证综指',
        'idx_sz':     '深证成指',
        'idx_cy':     '创业板指',
        'idx_kcb':    '科创50',
        'idx_hs300':  '沪深300',
        'idx_zz500':  '中证500',
        'idx_zz1000': '中证1000',
        'idx_sh50':   '上证50',
    }

    # 指数名称匹配模式（name + abbr 拼接后做 in 判断）
    _IDX_PATTERNS = {
        'idx_sh':     ['上证综合', '上证综指', '上证指数'],
        'idx_sz':     ['深证成指', '深成指', '深证综'],
        'idx_cy':     ['创业板指'],
        'idx_kcb':    ['科创50'],
        'idx_hs300':  ['沪深300'],
        'idx_zz500':  ['中证500'],
        'idx_zz1000': ['中证1000'],
        'idx_sh50':   ['上证50'],
    }

    def _extract(d, key):
        m1 = d.get('module1') or {}
        m2 = d.get('module2') or {}
        m3 = d.get('module3') or {}
        m5 = d.get('module5') or {}
        m6 = d.get('module6') or {}
        m7 = d.get('module7') or {}
        m8 = d.get('module8') or {}
        mapping = {
            'total_score': d.get('total_score'),
            'energy_index': d.get('energy_index'),
            'mfr': m1.get('money_flow_ratio'),
            'mv_flow_ratio': m1.get('mv_flow_ratio'),
            'divergence': m1.get('divergence'),
            'limit_up': m1.get('limit_up'),
            'limit_down': m1.get('limit_down'),
            'one_word_limit_down': m1.get('one_word_limit_down'),
            'turnover_today': m2.get('turnover_today'),
            'vol_5': m2.get('vol_5'),
            'vol_20': m2.get('vol_20'),
            'sigma_weighted': m3.get('sigma_weighted'),
            'sigma_equal': m3.get('sigma_equal'),
            'sigma_percentile': m3.get('percentile'),
            'spread_size': m5.get('spread_size'),
            'spread_vol': m5.get('spread_vol'),
            'hot_count': m6.get('hot_count'),
            'hot_avg_return': m6.get('hot_avg_return'),
            'nuke_ratio': m6.get('nuke_ratio'),
            'smart_distrib_ratio': m7.get('smart_distrib_ratio'),
            'smart_accum_ratio': m7.get('smart_accum_ratio'),
            'net_distrib_ratio': m7.get('net_distrib_ratio'),
            'unilateral_up_ratio': m7.get('unilateral_up_ratio'),
            'unilateral_down_ratio': m7.get('unilateral_down_ratio'),
            'top_turnover_avg_return': m7.get('top_turnover_avg_return'),
            'strong_avg_return': m8.get('strong_avg_return'),
        }
        if key in mapping:
            return mapping[key]
        # 指数：从 M4.indices 中按名称匹配，取收盘价
        if key.startswith('idx_'):
            indices = (d.get('module4') or {}).get('indices') or []
            patterns = _IDX_PATTERNS.get(key, [])
            for item in indices:
                combined = str(item.get('name') or '') + str(item.get('abbr') or '')
                for p in patterns:
                    if p in combined:
                        return item.get('close')
        return None

    if not metric or metric not in METRIC_LABELS:
        return jsonify({'success': False, 'error': f'不支持的指标: {metric}'})
    if not trading_day:
        return jsonify({'success': False, 'error': '缺少 trading_day'})
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute(
            "SELECT trading_day, result_json FROM market_sentiment_results "
            "WHERE trading_day <= ? ORDER BY trading_day DESC LIMIT ?",
            (trading_day, days)
        )
        rows = c.fetchall()
        conn.close()
        series = []
        for td, rj in reversed(rows):
            try:
                d = _json.loads(rj)
                val = _extract(d, metric)
                series.append({'day': td, 'value': val})
            except Exception:
                continue
        return jsonify({
            'success': True,
            'metric': metric,
            'label': METRIC_LABELS.get(metric, metric),
            'series': series,
            'is_index': metric.startswith('idx_'),
        })
    except Exception as e:
        logging.error(f'metric_history 接口出错: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/market_sentiment/report', methods=['GET'])
@login_required
def api_market_sentiment_report():
    """根据缓存结果生成 Markdown 格式的文字诊断报告"""
    import json as _json
    trading_day = request.args.get('trading_day', '').strip()
    if not trading_day:
        return jsonify({'success': False, 'error': '缺少 trading_day'})
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute("SELECT result_json FROM market_sentiment_results WHERE trading_day = ?", (trading_day,))
        row = c.fetchone()
        conn.close()
        if not row or not row[0]:
            return jsonify({'success': False, 'error': f'{trading_day} 尚无缓存结果，请先计算'})
        data = _json.loads(row[0])
        from market_sentiment import generate_sentiment_report
        report_md = generate_sentiment_report(data)
        return jsonify({'success': True, 'trading_day': trading_day, 'report': report_md})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/market_sentiment/ai_report', methods=['POST'])
@login_required
def api_market_sentiment_ai_report():
    """
    调用 DeepSeek 对市场情绪数据进行 AI 解读，支持日报/周报/月报/自定义区间。
    POST JSON: { mode: 'daily'|'weekly'|'monthly'|'range', trading_day: '...', start_date: '...', end_date: '...' }
    """
    import json as _json
    body = request.get_json() or {}
    mode = body.get('mode', 'daily')
    trading_day = (body.get('trading_day') or '').strip()
    start_date = (body.get('start_date') or '').strip()
    end_date = (body.get('end_date') or '').strip()

    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()

        if mode == 'daily':
            if not trading_day:
                return jsonify({'success': False, 'error': '日报模式需提供 trading_day'})
            c.execute("SELECT trading_day, result_json FROM market_sentiment_results WHERE trading_day = ?",
                      (trading_day,))
            rows = c.fetchall()
        elif mode == 'weekly':
            ref = trading_day or end_date
            if not ref:
                return jsonify({'success': False, 'error': '周报模式需提供 trading_day 或 end_date'})
            c.execute(
                "SELECT trading_day, result_json FROM market_sentiment_results "
                "WHERE trading_day <= ? ORDER BY trading_day DESC LIMIT 5", (ref,))
            rows = list(reversed(c.fetchall()))
        elif mode == 'monthly':
            ref = trading_day or end_date
            if not ref:
                return jsonify({'success': False, 'error': '月报模式需提供 trading_day 或 end_date'})
            c.execute(
                "SELECT trading_day, result_json FROM market_sentiment_results "
                "WHERE trading_day <= ? ORDER BY trading_day DESC LIMIT 22", (ref,))
            rows = list(reversed(c.fetchall()))
        elif mode == 'range':
            if not start_date or not end_date:
                return jsonify({'success': False, 'error': '区间模式需提供 start_date 和 end_date'})
            c.execute(
                "SELECT trading_day, result_json FROM market_sentiment_results "
                "WHERE trading_day >= ? AND trading_day <= ? ORDER BY trading_day",
                (start_date, end_date))
            rows = c.fetchall()
        else:
            return jsonify({'success': False, 'error': f'未知 mode: {mode}'})

        conn.close()
        if not rows:
            return jsonify({'success': False, 'error': '该时间段内无已计算的市场情绪数据，请先执行计算'})

        from market_sentiment import generate_sentiment_report
        mode_label = {'daily': '日报', 'weekly': '周报', 'monthly': '月报', 'range': '区间报告'}.get(mode, '报告')
        date_range_str = f"{rows[0][0]} 至 {rows[-1][0]}" if len(rows) > 1 else rows[0][0]

        # 构建结构化数据摘要
        records_summary = []
        for td, rj in rows:
            try:
                d = _json.loads(rj)
                m1 = d.get('module1') or {}
                m2 = d.get('module2') or {}
                m6 = d.get('module6') or {}
                records_summary.append({
                    'date': td,
                    'score': d.get('total_score', 100),
                    'penalty': d.get('total_penalty', 0),
                    'energy': round(d.get('energy_index', 0) or 0, 1),
                    'reset': d.get('energy_reset', False),
                    'mfr': m1.get('money_flow_ratio'),
                    'limit_down': m1.get('limit_down', 0),
                    'turnover': m2.get('turnover_today'),
                    'nuke_ratio': m6.get('nuke_ratio'),
                    'hot_avg': m6.get('hot_avg_return'),
                    'penalties': d.get('penalty_details', []),
                })
            except Exception:
                continue

        if not records_summary:
            return jsonify({'success': False, 'error': '数据解析失败'})

        # 生成完整报告作为上下文
        last_data = _json.loads(rows[-1][1])
        full_report = generate_sentiment_report(last_data)

        # 构造趋势摘要文本
        scores_trend = ' → '.join(str(r['score']) for r in records_summary)
        energy_trend = ' → '.join(str(r['energy']) for r in records_summary)
        reset_days = [r['date'] for r in records_summary if r['reset']]
        low_days = [r['date'] for r in records_summary if r['score'] < 50]

        trend_summary = f"""
数据区间：{date_range_str}（共 {len(records_summary)} 个交易日）

截面评分趋势：{scores_trend}
连续能量指数趋势：{energy_trend}
{"⚡ 冰点重置日：" + "、".join(reset_days) if reset_days else "本期无冰点重置"}
{"⚠ 低分日（<50分）：" + "、".join(low_days) if low_days else "本期无低分日"}

最新日（{rows[-1][0]}）完整诊断报告：
{full_report}
""".strip()

        prompt = f"""你是一位专业的A股市场量化分析师，擅长结合技术面和资金面解读市场情绪。
以下是基于八维度市场情绪评分模型（资金多空比、流动性、截面离散度、宽基趋势、风格因子、赚钱效应、聪明钱、强势股补跌）生成的{mode_label}数据，以及连续能量指数（水库水位模型，记录从绝望点以来积累的市场信心）。

{trend_summary}

请你作为专业分析师，用清晰、专业、有洞察力的语言输出一份{mode_label}分析，要求：

1. **情绪概况**：用1-2句话总结本{mode_label[:-1]}市场情绪的整体状态（热度/冷度/分化程度）
2. **关键信号解读**：逐一分析本期触发扣分的维度，解释其背后的市场含义
3. **能量指数解读**：解读连续能量指数的走势（{"触发冰点重置意味着什么" if reset_days else "当前水位代表什么阶段"}），对中期操作的指引意义
4. **风险与机会**：基于以上数据，点出当前市场的核心风险和潜在机会
5. **操作建议**：给出具体的仓位管理建议和关注方向（大盘风格、板块偏好等）
6. **【DeepSeek拓展】**：联系当前宏观/政策/产业背景，提供3个值得关注的方向或知识点

要求语言简洁专业，每个部分控制在3-5句话，总长度不超过800字。避免复述原始数据，重点提供你的判断和洞见。"""

        analysis = call_deepseek_api(prompt)
        if analysis is None:
            return jsonify({'success': False, 'error': 'DeepSeek API 调用失败，请检查网络或 API Key'})

        return jsonify({
            'success': True,
            'mode': mode,
            'mode_label': mode_label,
            'date_range': date_range_str,
            'days_count': len(records_summary),
            'analysis': analysis,
        })

    except Exception as e:
        logging.error(f'AI市场情绪分析失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})


# ── 情绪数据导出辅助工具 ────────────────────────────────────────────────────────

_SENTIMENT_EXPORT_BASE = r'E:\开发\郑玲\input'


def _trading_day_to_cn_folder(trading_day_str: str) -> str:
    """将 YYYY-MM-DD 转换为 中文日期文件夹名，例如 '2026年3月20日'"""
    import datetime
    dt = datetime.datetime.strptime(trading_day_str, '%Y-%m-%d')
    return f'{dt.year}年{dt.month}月{dt.day}日'


def _export_sentiment_csv_to_folder(trading_day_str: str) -> bool:
    """将 market_sentiment_results 整表导出为 CSV，放到指定中文日期文件夹下。
    返回 True 表示成功，False 表示失败（失败详情见日志）。"""
    import io, csv, os, json as _ej
    try:
        cn_name = _trading_day_to_cn_folder(trading_day_str)
        folder_path = os.path.join(_SENTIMENT_EXPORT_BASE, cn_name)
        os.makedirs(folder_path, exist_ok=True)

        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute("SELECT trading_day, result_json FROM market_sentiment_results ORDER BY trading_day ASC")
        rows = c.fetchall()
        conn.close()

        if not rows:
            logging.warning(f'[情绪导出] market_sentiment_results 为空，跳过导出 {trading_day_str}')
            return False

        csv_path = os.path.join(folder_path, f'market_sentiment_{trading_day_str}.csv')
        with open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['日期', '总分', '总扣分',
                             '模块1扣分', '模块2扣分', '模块3扣分', '模块4扣分',
                             '模块5扣分', '模块6扣分', '模块7扣分', '模块8扣分',
                             '扣分明细'])
            for (td, rj) in rows:
                try:
                    d = _ej.loads(rj)
                except Exception:
                    continue
                writer.writerow([
                    td,
                    d.get('total_score', ''),
                    d.get('total_penalty', ''),
                    d.get('module1', {}).get('penalty', 0),
                    d.get('module2', {}).get('penalty', 0),
                    d.get('module3', {}).get('penalty', 0),
                    d.get('module4', {}).get('penalty', 0),
                    d.get('module5', {}).get('penalty', 0),
                    d.get('module6', {}).get('penalty', 0),
                    d.get('module7', {}).get('penalty', 0),
                    d.get('module8', {}).get('penalty', 0),
                    '; '.join(d.get('penalty_details', [])),
                ])
        logging.info(f'[情绪导出] 成功导出 {trading_day_str} → {csv_path}')
        return True
    except Exception as ex:
        logging.error(f'[情绪导出] 导出 {trading_day_str} 失败: {ex}', exc_info=True)
        return False


def _check_and_fill_historical_exports() -> None:
    """检查最近 10 个交易日的导出文件夹，缺失则补全导出。"""
    import os
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute("SELECT trading_day FROM market_sentiment_results ORDER BY trading_day DESC LIMIT 10")
        recent_days = [row[0] for row in c.fetchall()]
        conn.close()
    except Exception as ex:
        logging.error(f'[情绪导出] 查询历史交易日失败: {ex}', exc_info=True)
        return

    for td in recent_days:
        try:
            cn_name = _trading_day_to_cn_folder(td)
            folder_path = os.path.join(_SENTIMENT_EXPORT_BASE, cn_name)
            if not os.path.isdir(folder_path):
                logging.info(f'[情绪导出] 历史文件夹缺失，补全导出: {td}')
                _export_sentiment_csv_to_folder(td)
        except Exception as ex:
            logging.error(f'[情绪导出] 历史检查处理 {td} 时出错: {ex}', exc_info=True)


# ── 情绪数据导出辅助工具 END ──────────────────────────────────────────────────


@app.route('/api/market_sentiment/score', methods=['POST'])
@login_required
def api_market_sentiment_score():
    """计算指定交易日的市场情绪得分；force=false 时优先返回缓存"""
    import json as _json
    try:
        data = request.get_json() or {}
        trading_day = (data.get('trading_day') or '').strip()
        force = data.get('force', False)
        if not trading_day:
            return jsonify({'success': False, 'error': '请提供 trading_day (YYYY-MM-DD)'})
        if not force:
            try:
                from market_sentiment import SENTIMENT_VERSION as _SV
                conn = sqlite3.connect(DATABASE_FILE)
                c = conn.cursor()
                c.execute("SELECT result_json FROM market_sentiment_results WHERE trading_day = ?", (trading_day,))
                row = c.fetchone()
                conn.close()
                if row and row[0]:
                    _cached = _json.loads(row[0])
                    if _cached.get('version') == _SV:
                        return jsonify({'success': True, 'data': _cached, 'from_cache': True})
            except Exception:
                pass
        from data_fetcher import JuyuanDataFetcher
        from market_sentiment import compute_daily_sentiment, compute_energy_index_series, _extract_energy_inputs, SENTIMENT_VERSION as _SV
        fetcher = JuyuanDataFetcher(lazy_init_pool=True)
        result = compute_daily_sentiment(trading_day, fetcher)

        # 取实际交易日（compute_daily_sentiment 内部会规范化为最近交易日）
        actual_td = result.get('trading_day') or trading_day

        # 计算连续能量指数：基于全量历史 + 当日结果（8 维指标）
        try:
            import pandas as _pd
            import json as _j_energy
            conn_h = sqlite3.connect(DATABASE_FILE)
            ch = conn_h.cursor()
            ch.execute(
                "SELECT trading_day, result_json FROM market_sentiment_results "
                "WHERE trading_day < ? ORDER BY trading_day",
                (actual_td,)
            )
            rows = ch.fetchall()
            conn_h.close()
            records = []
            for td, rj in rows:
                try:
                    records.append(_extract_energy_inputs(_j_energy.loads(rj)))
                except Exception:
                    continue
            records.append(_extract_energy_inputs(result))
            df_hist = _pd.DataFrame.from_records(records)
            if not df_hist.empty:
                df_energy = compute_energy_index_series(df_hist)
                last_row = df_energy.sort_values('trading_day').iloc[-1]
                result['energy_index'] = float(last_row.get('energy_index', 0.0) or 0.0)
                result['energy_reset'] = bool(last_row.get('energy_reset', False))
        except Exception as _e_energy:
            logging.error(f'计算连续能量指数失败: {_e_energy}', exc_info=True)

        _saved_ok = False
        try:
            conn = sqlite3.connect(DATABASE_FILE)
            c = conn.cursor()
            # 使用 result 中的实际交易日作为存储 key，避免非交易日请求覆盖错误记录
            c.execute(
                "INSERT OR REPLACE INTO market_sentiment_results (trading_day, result_json) VALUES (?, ?)",
                (actual_td, _json.dumps(result, ensure_ascii=False))
            )
            conn.commit()
            conn.close()
            _saved_ok = True
        except Exception as e:
            logging.error(f'保存市场情绪结果失败: {e}', exc_info=True)

        if _saved_ok:
            # 导出当日 CSV 到指定文件夹
            _export_sentiment_csv_to_folder(actual_td)
            # 检查并补全最近 10 个交易日的历史导出
            _check_and_fill_historical_exports()

        return jsonify({'success': True, 'data': result, 'from_cache': False})
    except Exception as e:
        logging.error(f'市场情绪计算失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/market_sentiment/range_score', methods=['POST'])
@login_required
def api_market_sentiment_range_score():
    """批量计算（SSE 流式进度推送）"""
    data = request.get_json() or {}
    start_date = (data.get('start_date') or '').strip()
    end_date = (data.get('end_date') or '').strip()
    if not start_date or not end_date:
        return jsonify({'success': False, 'error': '请提供 start_date 和 end_date'})
    # 把参数存到 session，由 SSE GET 端点读取
    session['_range_start'] = start_date
    session['_range_end'] = end_date
    return jsonify({'success': True, 'sse_ready': True})


@app.route('/api/market_sentiment/range_score_stream')
@login_required
def api_market_sentiment_range_score_stream():
    """SSE 端点：流式推送批量计算进度和结果"""
    import time as _time
    import json as _json

    start_date = request.args.get('start_date', '').strip()
    end_date = request.args.get('end_date', '').strip()
    force = request.args.get('force', '0') == '1'
    if not start_date or not end_date:
        def _err():
            yield 'data: ' + _json.dumps({'type': 'error', 'error': '缺少日期参数'}) + '\n\n'
        return app.response_class(_err(), mimetype='text/event-stream')

    def generate():
        t0 = _time.time()
        try:
            from data_fetcher import JuyuanDataFetcher
            from market_sentiment import compute_batch_sentiment_streaming

            yield 'data: ' + _json.dumps({'type': 'stage', 'stage': 'init', 'msg': '正在初始化数据库连接…'}, ensure_ascii=False) + '\n\n'

            fetcher = JuyuanDataFetcher(lazy_init_pool=True)

            for progress in compute_batch_sentiment_streaming(start_date, end_date, fetcher, skip_existing=not force):
                yield 'data: ' + _json.dumps(progress, ensure_ascii=False) + '\n\n'

            elapsed = round(_time.time() - t0, 2)
            yield 'data: ' + _json.dumps({
                'type': 'done',
                'elapsed_seconds': elapsed,
                'msg': f'全部完成，耗时 {elapsed} 秒'
            }, ensure_ascii=False) + '\n\n'
        except Exception as e:
            logging.error(f'SSE 批量计算失败: {e}', exc_info=True)
            yield 'data: ' + _json.dumps({'type': 'error', 'error': str(e)}, ensure_ascii=False) + '\n\n'

    return app.response_class(generate(), mimetype='text/event-stream',
                              headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})


@app.route('/api/market_sentiment/range_export', methods=['GET'])
@login_required
def api_market_sentiment_range_export():
    """导出区间内每日市场情绪得分为CSV"""
    import io, csv
    start_date = request.args.get('start_date', '').strip()
    end_date = request.args.get('end_date', '').strip()
    if not start_date or not end_date:
        return jsonify({'success': False, 'error': '请提供 start_date 和 end_date'})
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute(
            "SELECT trading_day, result_json FROM market_sentiment_results WHERE trading_day >= ? AND trading_day <= ? ORDER BY trading_day",
            (start_date, end_date)
        )
        rows = c.fetchall()
        conn.close()
    except Exception as e:
        logging.error(f'导出市场情绪结果失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})
    if not rows:
        return jsonify({'success': False, 'error': '该区间无已计算的结果，请先执行区间计算'})
    import json as _json
    mem = io.BytesIO()
    writer_wrapper = io.TextIOWrapper(mem, encoding='utf-8-sig', newline='')
    writer = csv.writer(writer_wrapper)
    writer.writerow(['日期', '总分', '总扣分',
                     '模块1扣分', '模块2扣分', '模块3扣分', '模块4扣分',
                     '模块5扣分', '模块6扣分', '模块7扣分', '模块8扣分',
                     '扣分明细'])
    for (td, rj) in rows:
        try:
            d = _json.loads(rj)
        except Exception:
            continue
        writer.writerow([
            td,
            d.get('total_score', ''),
            d.get('total_penalty', ''),
            d.get('module1', {}).get('penalty', 0),
            d.get('module2', {}).get('penalty', 0),
            d.get('module3', {}).get('penalty', 0),
            d.get('module4', {}).get('penalty', 0),
            d.get('module5', {}).get('penalty', 0),
            d.get('module6', {}).get('penalty', 0),
            d.get('module7', {}).get('penalty', 0),
            d.get('module8', {}).get('penalty', 0),
            '; '.join(d.get('penalty_details', [])),
        ])
    writer_wrapper.flush()
    writer_wrapper.detach()
    mem.seek(0)
    filename = f'market_sentiment_{start_date}_{end_date}.csv'
    return send_file(mem, as_attachment=True, download_name=filename, mimetype='text/csv')


# API: 从图片识别股票代码
@app.route('/api/concept_stocks/recognize_stocks', methods=['POST'])
@login_required
def recognize_stocks_from_images():
    """从上传的图片中识别股票代码"""
    try:
        if 'images' not in request.files:
            return jsonify({'success': False, 'error': '未上传图片'})
        
        files = request.files.getlist('images')
        if not files or all(f.filename == '' for f in files):
            return jsonify({'success': False, 'error': '未选择图片文件'})
        
        recognized_stocks = []
        
        for file in files:
            if file.filename == '':
                continue
            
            try:
                # 读取图片
                from PIL import Image
                import io
                
                image_data = file.read()
                image = Image.open(io.BytesIO(image_data))
                
                # 使用OCR识别
                try:
                    import pytesseract
                    # 尝试自动检测Tesseract路径
                    import os
                    possible_paths = [
                        r'C:\Program Files\Tesseract-OCR\tesseract.exe',
                        r'C:\Program Files (x86)\Tesseract-OCR\tesseract.exe',
                        r'C:\Users\{}\AppData\Local\Programs\Tesseract-OCR\tesseract.exe'.format(os.getenv('USERNAME', '')),
                    ]
                    for path in possible_paths:
                        if os.path.exists(path):
                            pytesseract.pytesseract.tesseract_cmd = path
                            break
                    
                    # OCR识别
                    text = pytesseract.image_to_string(image, lang='chi_sim+eng')
                    
                    # 使用正则表达式提取股票代码（6位数字）
                    import re
                    stock_codes = re.findall(r'\b\d{6}\b', text)
                    
                    for code in stock_codes:
                        if code not in [s['code'] for s in recognized_stocks]:
                            recognized_stocks.append({'code': code, 'name': ''})
                    
                except ImportError:
                    return jsonify({'success': False, 'error': 'OCR引擎未安装，请参考OCR_安装说明.md安装Tesseract OCR'})
                except Exception as e:
                    logging.warning(f"OCR识别失败: {e}")
                    continue
                    
            except Exception as e:
                logging.error(f"处理图片失败: {e}")
                continue
        
        if not recognized_stocks:
            return jsonify({'success': False, 'error': '未能从图片中识别出股票代码'})
        
        # 尝试获取股票名称
        try:
            from data_fetcher import JuyuanDataFetcher
            fetcher = JuyuanDataFetcher(lazy_init_pool=True)
            stock_codes = [s['code'] for s in recognized_stocks]
            stock_info_dict = fetcher.batch_get_stock_basic_info(stock_codes)
            
            for stock in recognized_stocks:
                if stock['code'] in stock_info_dict:
                    info = stock_info_dict[stock['code']]
                    stock['name'] = info.get('SecuAbbr', '')
        except Exception as e:
            logging.warning(f"获取股票名称失败: {e}")
        
        return jsonify({
            'success': True,
            'stocks': recognized_stocks
        })
        
    except Exception as e:
        logging.error(f"识别股票代码失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': f'识别失败: {str(e)}'})

# API: 获取所有概念股列表
@app.route('/api/concept_stocks/list', methods=['GET'])
@login_required
def get_concept_stocks_list():
    """获取所有概念股列表（用于报表选择）"""
    try:
        concepts = query_db("SELECT id, concept_name FROM concept_stocks ORDER BY concept_name")
        concepts_list = [{'id': c[0], 'name': c[1]} for c in concepts]
        return jsonify({'success': True, 'concepts': concepts_list})
    except Exception as e:
        logging.error(f"获取概念股列表失败: {e}")
        return jsonify({'success': False, 'error': str(e)})

# API: 获取概念股下的股票列表
@app.route('/api/concept_stocks/<int:concept_id>/stocks', methods=['GET'])
@login_required
def get_concept_stocks(concept_id):
    """获取指定概念股下的股票列表"""
    try:
        stocks = query_db("""SELECT stock_code, stock_name 
                            FROM concept_stock_items 
                            WHERE concept_id = ? 
                            ORDER BY stock_code""", (concept_id,))
        stocks_list = [{'code': s[0], 'name': s[1] or ''} for s in stocks]
        return jsonify({'success': True, 'stocks': stocks_list})
    except Exception as e:
        logging.error(f"获取概念股股票列表失败: {e}")
        return jsonify({'success': False, 'error': str(e)})

# API: 获取行业层级结构
@app.route('/api/industries/hierarchy', methods=['GET'])
@login_required
def get_industries_hierarchy():
    """获取行业层级结构（一级→二级→三级）"""
    try:
        from data_fetcher import JuyuanDataFetcher
        fetcher = JuyuanDataFetcher(lazy_init_pool=True)
        
        # 查询所有行业信息
        sql = """
        SELECT DISTINCT 
            i.FirstIndustryName,
            i.SecondIndustryName,
            i.ThirdIndustryName
        FROM LC_ExgIndustry i
        INNER JOIN SecuMain s ON i.CompanyCode = s.CompanyCode
        WHERE s.SecuCategory = 1
        AND i.Standard = '38'
        AND i.FirstIndustryName IS NOT NULL
        AND i.SecondIndustryName IS NOT NULL
        AND i.ThirdIndustryName IS NOT NULL
        ORDER BY i.FirstIndustryName, i.SecondIndustryName, i.ThirdIndustryName
        """
        
        df = fetcher.query(sql)
        
        # 构建层级结构
        hierarchy = {}
        for _, row in df.iterrows():
            first = str(row['FirstIndustryName']).strip()
            second = str(row['SecondIndustryName']).strip()
            third = str(row['ThirdIndustryName']).strip()
            
            if first not in hierarchy:
                hierarchy[first] = {}
            if second not in hierarchy[first]:
                hierarchy[first][second] = []
            if third not in hierarchy[first][second]:
                hierarchy[first][second].append(third)
        
        return jsonify({'success': True, 'hierarchy': hierarchy})
    except Exception as e:
        logging.error(f"获取行业层级结构失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# API: 根据行业名称获取股票代码列表
@app.route('/api/industries/get_stocks', methods=['POST'])
@login_required
def get_industry_stocks():
    """根据行业名称（三级行业）获取股票代码列表"""
    try:
        data = request.get_json()
        industry_names = data.get('industry_names', [])  # 支持多个行业
        
        if not industry_names or len(industry_names) == 0:
            return jsonify({'success': False, 'error': '请选择至少一个行业'})
        
        from data_fetcher import JuyuanDataFetcher
        fetcher = JuyuanDataFetcher(lazy_init_pool=True)
        
        # 构建IN子句
        industry_names_str = ','.join([f"'{name}'" for name in industry_names])
        
        # 查询指定行业的股票代码
        sql = f"""
        SELECT DISTINCT 
            s.SecuCode,
            s.SecuAbbr
        FROM SecuMain s
        INNER JOIN LC_ExgIndustry i ON s.CompanyCode = i.CompanyCode
        WHERE s.SecuCategory = 1
        AND i.Standard = '38'
        AND i.ThirdIndustryName IN ({industry_names_str})
        ORDER BY s.SecuCode
        """
        
        df = fetcher.query(sql)
        
        if df.empty:
            return jsonify({'success': True, 'stocks': []})
        
        stocks_list = []
        for _, row in df.iterrows():
            stocks_list.append({
                'code': str(row['SecuCode']).zfill(6),
                'name': row['SecuAbbr'] or ''
            })
        
        return jsonify({'success': True, 'stocks': stocks_list})
    except Exception as e:
        logging.error(f"获取行业股票列表失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# ==================== 专题关系图功能 ====================
# 专题列表页
@app.route('/topics')
@login_required
def topics():
    all_topics = query_db("SELECT * FROM topics ORDER BY created_at DESC")
    
    topics_list = []
    for topic in all_topics:
        # 获取每个专题的卡片数量
        card_count = query_db("SELECT COUNT(*) FROM topic_cards WHERE topic_id = ?", 
                            (topic[0],), one=True)
        topics_list.append({
            'id': topic[0],
            'title': topic[1],
            'description': topic[2] or '',
            'card_count': card_count[0] if card_count else 0,
            'created_at': topic[3],
            'updated_at': topic[4]
        })
    
    return render_template('topics.html', topics=topics_list)

# 专题编辑器
@app.route('/topics/edit/<int:id>')
@login_required
def edit_topic(id):
    topic_data = query_db("SELECT * FROM topics WHERE id = ?", (id,), one=True)
    if not topic_data:
        return redirect(url_for('topics'))
    
    topic = {
        'id': topic_data[0],
        'title': topic_data[1],
        'description': topic_data[2] or ''
    }
    
    # 获取所有卡片 - 检查表结构以确定字段位置
    # 新结构: id, topic_id, title, content, card_type, x, y, width, height, color, created_at, updated_at
    # 旧结构: id, topic_id, title, content, x, y, width, height, color, created_at, updated_at
    cards_data = query_db("SELECT * FROM topic_cards WHERE topic_id = ? ORDER BY created_at", (id,))
    cards = []
    for card in cards_data:
        # 检查card_type字段是否存在（在第5个位置，索引4）
        # 如果第5个字段是字符串且是'company'或'industry'，说明是card_type字段
        has_card_type = len(card) >= 5 and isinstance(card[4], str) and card[4] in ['company', 'industry']
        
        if has_card_type:
            cards.append({
                'id': card[0],
                'topic_id': card[1],
                'title': card[2],
                'content': card[3] or '',
                'card_type': card[4],
                'x': card[5] if len(card) > 5 else 100,
                'y': card[6] if len(card) > 6 else 100,
                'width': card[7] if len(card) > 7 else 220,
                'height': card[8] if len(card) > 8 else 160,
                'color': card[9] if len(card) > 9 and isinstance(card[9], str) else '#6366f1'
            })
        else:
            # 旧格式，没有card_type字段
            cards.append({
                'id': card[0],
                'topic_id': card[1],
                'title': card[2],
                'content': card[3] or '',
                'card_type': 'company',  # 默认为公司
                'x': card[4] if len(card) > 4 else 100,
                'y': card[5] if len(card) > 5 else 100,
                'width': card[6] if len(card) > 6 else 220,
                'height': card[7] if len(card) > 7 else 160,
                'color': card[8] if len(card) > 8 and isinstance(card[8], str) else '#6366f1'
            })
    
    # 获取所有连线
    connections_data = query_db("SELECT * FROM card_connections WHERE topic_id = ?", (id,))
    connections = []
    for conn in connections_data:
        connections.append({
            'id': conn[0],
            'from_card_id': conn[2],
            'to_card_id': conn[3],
            'relation_type': conn[4] if len(conn) > 4 and conn[4] else 'downstream',
            'connection_type': conn[5] if len(conn) > 5 and conn[5] else 'bezier',
            'color': conn[6] if len(conn) > 6 and conn[6] else '#999999'
        })
    
    return render_template('edit_topic.html', topic=topic, cards=cards, connections=connections)

# 创建专题
@app.route('/topics/add', methods=['GET', 'POST'])
@login_required
def add_topic():
    if request.method == 'POST':
        title = request.form.get('title', '')
        description = request.form.get('description', '')
        
        insert_db("INSERT INTO topics (title, description) VALUES (?, ?)", 
                 (title, description))
        
        # 获取刚创建的专题ID
        new_topic = query_db("SELECT id FROM topics WHERE title = ? ORDER BY created_at DESC LIMIT 1", 
                            (title,), one=True)
        if new_topic:
            return redirect(url_for('edit_topic', id=new_topic[0]))
        
        return redirect(url_for('topics'))
    
    return render_template('add_topic.html')

# 删除专题
@app.route('/topics/delete/<int:id>')
@login_required
def delete_topic(id):
    delete_db("DELETE FROM topics WHERE id = ?", (id,))
    return redirect(url_for('topics'))

# 保存专题信息（AJAX）
@app.route('/api/topics/save', methods=['POST'])
@login_required
def save_topic_info():
    try:
        data = request.get_json()
        topic_id = data.get('id')
        title = data.get('title', '')
        description = data.get('description', '')
        
        update_db("UPDATE topics SET title=?, description=?, updated_at=CURRENT_TIMESTAMP WHERE id=?", 
                 (title, description, topic_id))
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# 保存卡片（AJAX）
@app.route('/api/topics/save_card', methods=['POST'])
@login_required
def save_card():
    try:
        data = request.get_json()
        card_id = data.get('id')
        topic_id = data.get('topic_id')
        title = data.get('title', '')
        content = data.get('content', '')
        card_type = data.get('card_type', 'company')
        x = int(data.get('x', 100))
        y = int(data.get('y', 100))
        width = int(data.get('width', 220))
        height = int(data.get('height', 160))
        color = data.get('color', '#6366f1')
        
        if card_id:
            # 更新
            update_db("""UPDATE topic_cards 
                        SET title=?, content=?, card_type=?, x=?, y=?, width=?, height=?, color=?, 
                        updated_at=CURRENT_TIMESTAMP 
                        WHERE id=?""",
                     (title, content, card_type, x, y, width, height, color, card_id))
            return jsonify({'success': True, 'id': card_id})
        else:
            # 新增
            insert_db("""INSERT INTO topic_cards 
                        (topic_id, title, content, card_type, x, y, width, height, color) 
                        VALUES (?,?,?,?,?,?,?,?,?)""",
                     (topic_id, title, content, card_type, x, y, width, height, color))
            new_card = query_db("SELECT id FROM topic_cards WHERE topic_id = ? ORDER BY created_at DESC LIMIT 1", 
                               (topic_id,), one=True)
            return jsonify({'success': True, 'id': new_card[0] if new_card else None})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# 删除卡片（AJAX）
@app.route('/api/topics/delete_card/<int:card_id>', methods=['DELETE'])
@login_required
def delete_card(card_id):
    try:
        delete_db("DELETE FROM topic_cards WHERE id = ?", (card_id,))
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# 保存连线（AJAX）
@app.route('/api/topics/save_connection', methods=['POST'])
@login_required
def save_connection():
    try:
        data = request.get_json()
        connection_id = data.get('id')
        topic_id = data.get('topic_id')
        from_card_id = int(data.get('from_card_id'))
        to_card_id = int(data.get('to_card_id'))
        relation_type = data.get('relation_type', 'downstream')
        connection_type = data.get('connection_type', 'bezier')
        color = data.get('color', '#999999')
        
        # 根据关系类型设置默认颜色
        if not color or color == '#999999':
            if relation_type == 'upstream':
                color = '#3b82f6'  # 蓝色 - 上游
            elif relation_type == 'downstream':
                color = '#10b981'  # 绿色 - 下游
            elif relation_type == 'belongs_to':
                color = '#f59e0b'  # 橙色 - 属于
        
        # 检查是否已存在相同的连线
        existing = query_db("""SELECT id FROM card_connections 
                              WHERE topic_id = ? AND from_card_id = ? AND to_card_id = ?""",
                           (topic_id, from_card_id, to_card_id), one=True)
        
        if connection_id or existing:
            conn_id = connection_id or existing[0]
            update_db("""UPDATE card_connections 
                        SET relation_type=?, connection_type=?, color=? 
                        WHERE id=?""",
                     (relation_type, connection_type, color, conn_id))
            return jsonify({'success': True, 'id': conn_id})
        else:
            # 新增
            insert_db("""INSERT INTO card_connections 
                        (topic_id, from_card_id, to_card_id, relation_type, connection_type, color) 
                        VALUES (?,?,?,?,?,?)""",
                     (topic_id, from_card_id, to_card_id, relation_type, connection_type, color))
            new_conn = query_db("""SELECT id FROM card_connections 
                                  WHERE topic_id = ? AND from_card_id = ? AND to_card_id = ? 
                                  ORDER BY created_at DESC LIMIT 1""",
                               (topic_id, from_card_id, to_card_id), one=True)
            return jsonify({'success': True, 'id': new_conn[0] if new_conn else None})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# 删除连线（AJAX）
@app.route('/api/topics/delete_connection/<int:connection_id>', methods=['DELETE'])
@login_required
def delete_connection(connection_id):
    try:
        delete_db("DELETE FROM card_connections WHERE id = ?", (connection_id,))
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})

# ==================== 行业库存分析功能 ====================
# 行业库存分析页面
@app.route('/industry_inventory')
@login_required
def industry_inventory():
    return render_template('industry_inventory.html')

# 搜索股票API
@app.route('/api/industry_inventory/search_stock', methods=['POST'])
@login_required
def search_stock():
    try:
        data = request.get_json()
        query = data.get('query', '').strip()
        
        if not query:
            return jsonify({'success': False, 'error': '请输入搜索关键词'})
        
        # 导入Web模块
        try:
            from industry_inventory_module import get_module
            module = get_module()
            stocks = module.get_stock_list_by_code_or_name(query)
            
            return jsonify({
                'success': True,
                'stocks': stocks
            })
        except ImportError as e:
            return jsonify({'success': False, 'error': f'模块导入失败: {str(e)}'})
        except Exception as e:
            logging.error(f"搜索股票失败: {e}", exc_info=True)
            return jsonify({'success': False, 'error': f'搜索失败: {str(e)}'})
    except Exception as e:
        logging.error(f"搜索股票API错误: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# 获取行业列表API
@app.route('/api/industry_inventory/industries')
@login_required
def get_industries():
    try:
        from industry_inventory_module import get_module
        module = get_module()
        industries = module.get_industry_list()
        
        return jsonify({
            'success': True,
            'industries': industries
        })
    except Exception as e:
        logging.error(f"获取行业列表失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# 分析股票API
@app.route('/api/industry_inventory/analyze_stock', methods=['POST'])
@login_required
def analyze_stock():
    try:
        data = request.get_json()
        stock_code = data.get('stock_code', '').strip()
        years = int(data.get('years', 10))
        
        if not stock_code:
            return jsonify({'success': False, 'error': '请输入股票代码'})
        
        from industry_inventory_module import get_module
        module = get_module()
        result = module.analyze_by_stock_code(stock_code, years=years)
        
        return jsonify(result)
    except Exception as e:
        logging.error(f"分析股票失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# 分析行业API
@app.route('/api/industry_inventory/analyze_industry', methods=['POST'])
@login_required
def analyze_industry():
    try:
        data = request.get_json()
        industry_name = data.get('industry_name', '').strip()
        years = int(data.get('years', 10))
        
        if not industry_name:
            return jsonify({'success': False, 'error': '请选择行业'})
        
        from industry_inventory_module import get_module
        module = get_module()
        result = module.analyze_by_industry(industry_name, years=years)
        
        return jsonify(result)
    except Exception as e:
        logging.error(f"分析行业失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# ==================== 数据查询构建器功能 ====================
# 数据表配置页面
@app.route('/data_query_config')
@login_required
def data_query_config():
    return render_template('data_query_config.html')

# 数据查询页面
@app.route('/data_query_builder')
@login_required
def data_query_builder():
    return render_template('data_query.html')

# 获取所有数据表配置
@app.route('/api/data_query/tables')
@login_required
def get_table_configs():
    try:
        # 使用明确的列名查询，避免字段顺序问题
        tables = query_db("""
            SELECT id, table_name, table_display_name, description, primary_key_field, 
                   date_field, code_field, name_field, join_type, join_field, created_at, updated_at
            FROM data_table_configs 
            ORDER BY created_at DESC
        """)
        result = []
        for table in tables:
            # 使用明确的索引，确保字段对应正确
            # 表结构：id(0), table_name(1), table_display_name(2), description(3), 
            #         primary_key_field(4), date_field(5), code_field(6), name_field(7), 
            #         join_type(8), join_field(9), created_at(10), updated_at(11)
            join_type = table[8] if len(table) > 8 and table[8] else 'CompanyCode'
            join_field = table[9] if len(table) > 9 and table[9] else ''
            
            # 确保join_type和join_field是字符串
            if join_type:
                join_type = str(join_type).strip()
            else:
                join_type = 'CompanyCode'
            
            if join_field:
                join_field = str(join_field).strip()
            else:
                join_field = ''
            
            result.append({
                'id': table[0],
                'table_name': table[1],
                'table_display_name': table[2],
                'description': table[3] if len(table) > 3 else '',
                'primary_key_field': table[4] if len(table) > 4 else '',
                'date_field': table[5] if len(table) > 5 else '',
                'code_field': table[6] if len(table) > 6 else '',
                'name_field': table[7] if len(table) > 7 else '',
                'join_type': join_type,
                'join_field': join_field
            })
        return jsonify({'success': True, 'tables': result})
    except Exception as e:
        logging.error(f"获取数据表配置失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# 保存数据表配置
@app.route('/api/data_query/tables', methods=['POST'])
@login_required
def save_table_config():
    try:
        data = request.get_json()
        table_id = data.get('id')
        table_name = data.get('table_name', '').strip()
        table_display_name = data.get('table_display_name', '').strip()
        description = data.get('description', '')
        primary_key_field = data.get('primary_key_field', '')
        date_field = data.get('date_field', '')
        code_field = data.get('code_field', '')
        name_field = data.get('name_field', '')
        
        if not table_name or not table_display_name:
            return jsonify({'success': False, 'error': '表名和显示名称不能为空'})
        
        join_type = data.get('join_type', 'CompanyCode')  # InnerCode 或 CompanyCode
        join_field = data.get('join_field', '').strip()  # 关联字段名
        
        # 验证关联字段配置
        if not join_field:
            return jsonify({'success': False, 'error': '关联字段名不能为空，这是与SecuMain关联的关键字段'})
        
        # 验证字段名格式
        import re
        if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', join_field):
            return jsonify({'success': False, 'error': f'关联字段名格式无效: "{join_field}"。字段名必须是有效的SQL标识符（字母、数字、下划线，且以字母或下划线开头）'})
        
        # 验证join_type和join_field的匹配性（给出提示但不强制）
        if join_type == 'InnerCode' and 'company' in join_field.lower() and 'inner' not in join_field.lower():
            logging.warning(f"表 {table_name} 的关联方式设置为InnerCode，但关联字段名 '{join_field}' 看起来像是CompanyCode字段，请确认配置是否正确")
        elif join_type == 'CompanyCode' and 'inner' in join_field.lower() and 'company' not in join_field.lower():
            logging.warning(f"表 {table_name} 的关联方式设置为CompanyCode，但关联字段名 '{join_field}' 看起来像是InnerCode字段，请确认配置是否正确")
        
        if table_id:
            # 更新
            update_db("""UPDATE data_table_configs SET 
                        table_name = ?, table_display_name = ?, description = ?,
                        primary_key_field = ?, date_field = ?, code_field = ?, name_field = ?,
                        join_type = ?, join_field = ?, updated_at = CURRENT_TIMESTAMP WHERE id = ?""",
                     (table_name, table_display_name, description, primary_key_field,
                      date_field, code_field, name_field, join_type, join_field, table_id))
            return jsonify({'success': True, 'id': table_id})
        else:
            # 新增
            insert_db("""INSERT INTO data_table_configs 
                        (table_name, table_display_name, description, primary_key_field,
                         date_field, code_field, name_field, join_type, join_field) 
                        VALUES (?,?,?,?,?,?,?,?,?)""",
                     (table_name, table_display_name, description, primary_key_field,
                      date_field, code_field, name_field, join_type, join_field))
            new_table = query_db("SELECT id FROM data_table_configs WHERE table_name = ? ORDER BY created_at DESC LIMIT 1",
                                (table_name,), one=True)
            return jsonify({'success': True, 'id': new_table[0] if new_table else None})
    except Exception as e:
        logging.error(f"保存数据表配置失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# 删除数据表配置
@app.route('/api/data_query/tables/<int:table_id>', methods=['DELETE'])
@login_required
def delete_table_config(table_id):
    try:
        delete_db("DELETE FROM data_table_configs WHERE id = ?", (table_id,))
        return jsonify({'success': True})
    except Exception as e:
        logging.error(f"删除数据表配置失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# 获取数据表的字段配置
@app.route('/api/data_query/tables/<int:table_id>/fields')
@login_required
def get_table_fields(table_id):
    try:
        fields = query_db("""SELECT * FROM data_field_configs 
                            WHERE table_config_id = ? 
                            ORDER BY order_index, id""", (table_id,))
        result = []
        for field in fields:
            result.append({
                'id': field[0],
                'table_config_id': field[1],
                'field_name': field[2],
                'field_display_name': field[3],
                'field_type': field[4],
                'description': field[5],
                'is_sortable': bool(field[6]),
                'is_filterable': bool(field[7]),
                'order_index': field[8]
            })
        return jsonify({'success': True, 'fields': result})
    except Exception as e:
        logging.error(f"获取字段配置失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# 保存字段配置
@app.route('/api/data_query/fields', methods=['POST'])
@login_required
def save_field_config():
    try:
        data = request.get_json()
        field_id = data.get('id')
        table_config_id = data.get('table_config_id')
        field_name = data.get('field_name', '').strip()
        field_display_name = data.get('field_display_name', '').strip()
        field_type = data.get('field_type', 'TEXT')
        description = data.get('description', '')
        is_sortable = 1 if data.get('is_sortable', True) else 0
        is_filterable = 1 if data.get('is_filterable', True) else 0
        order_index = data.get('order_index', 0)
        
        if not table_config_id or not field_name or not field_display_name:
            return jsonify({'success': False, 'error': '表配置ID、字段名和显示名称不能为空'})
        
        if field_id:
            # 更新
            update_db("""UPDATE data_field_configs SET 
                        field_name = ?, field_display_name = ?, field_type = ?, description = ?,
                        is_sortable = ?, is_filterable = ?, order_index = ? WHERE id = ?""",
                     (field_name, field_display_name, field_type, description,
                      is_sortable, is_filterable, order_index, field_id))
            return jsonify({'success': True, 'id': field_id})
        else:
            # 新增
            insert_db("""INSERT INTO data_field_configs 
                        (table_config_id, field_name, field_display_name, field_type, description,
                         is_sortable, is_filterable, order_index) 
                        VALUES (?,?,?,?,?,?,?,?)""",
                     (table_config_id, field_name, field_display_name, field_type, description,
                      is_sortable, is_filterable, order_index))
            new_field = query_db("""SELECT id FROM data_field_configs 
                                   WHERE table_config_id = ? AND field_name = ? 
                                   ORDER BY created_at DESC LIMIT 1""",
                                (table_config_id, field_name), one=True)
            return jsonify({'success': True, 'id': new_field[0] if new_field else None})
    except Exception as e:
        logging.error(f"保存字段配置失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# 验证字段是否存在
@app.route('/api/data_query/validate_fields', methods=['POST'])
@login_required
def validate_fields():
    """验证字段是否在数据库表中存在"""
    try:
        data = request.get_json()
        table_name = data.get('table_name', '').strip()
        field_names = data.get('field_names', [])  # 字段名列表
        
        if not table_name:
            return jsonify({'success': False, 'error': '表名不能为空'})
        
        if not field_names:
            return jsonify({'success': True, 'valid_fields': [], 'invalid_fields': []})
        
        try:
            from data_fetcher import JuyuanDataFetcher
            fetcher = JuyuanDataFetcher(lazy_init_pool=True)
            
            # 查询表的结构（列名）
            # SQL Server 查询列名的方法
            sql = f"""
            SELECT COLUMN_NAME 
            FROM INFORMATION_SCHEMA.COLUMNS 
            WHERE TABLE_NAME = '{table_name}'
            """
            
            try:
                df = fetcher.query(sql)
                if df.empty:
                    return jsonify({
                        'success': False,
                        'error': f'表 "{table_name}" 不存在或无法访问'
                    })
                
                # 获取所有有效的列名
                valid_columns = set(df['COLUMN_NAME'].str.strip().str.upper().tolist())
                
                # 验证每个字段名
                valid_fields = []
                invalid_fields = []
                
                for field_name in field_names:
                    field_upper = field_name.strip().upper()
                    if field_upper in valid_columns:
                        valid_fields.append(field_name)
                    else:
                        invalid_fields.append(field_name)
                
                return jsonify({
                    'success': True,
                    'valid_fields': valid_fields,
                    'invalid_fields': invalid_fields,
                    'all_columns': list(valid_columns)  # 返回所有有效列名，供参考
                })
            except Exception as e:
                logging.error(f"查询表结构失败: {e}", exc_info=True)
                # 如果查询失败，尝试另一种方法：SELECT TOP 1 来验证表是否存在
                try:
                    test_sql = f"SELECT TOP 1 * FROM {table_name}"
                    fetcher.query(test_sql)
                    # 如果执行成功但无法获取列信息，返回部分成功
                    return jsonify({
                        'success': True,
                        'valid_fields': [],
                        'invalid_fields': field_names,
                        'warning': '无法获取表结构信息，请手动验证字段名',
                        'all_columns': []
                    })
                except Exception as query_err:
                    return jsonify({
                        'success': False,
                        'error': f'表 "{table_name}" 不存在或无法访问: {str(query_err)}'
                    })
        except ImportError:
            return jsonify({
                'success': False,
                'error': '数据库连接模块未找到'
            })
            
    except Exception as e:
        logging.error(f"验证字段失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# 删除字段配置
@app.route('/api/data_query/fields/<int:field_id>', methods=['DELETE'])
@login_required
def delete_field_config(field_id):
    try:
        delete_db("DELETE FROM data_field_configs WHERE id = ?", (field_id,))
        return jsonify({'success': True})
    except Exception as e:
        logging.error(f"删除字段配置失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# 获取所有股票列表（用于缓存，每天更新一次）
@app.route('/api/data_query/all_stocks', methods=['GET'])
@login_required
def get_all_stocks():
    """获取所有符合条件的股票列表（用于客户端缓存）"""
    try:
        from industry_inventory_module import get_module
        from juyuan_config import STOCK_FILTER
        from data_fetcher import JuyuanDataFetcher
        
        fetcher = JuyuanDataFetcher(lazy_init_pool=True)
        
        # 获取所有符合条件的股票（应用与筛选任务相同的筛选条件）
        sql = f"""
        SELECT DISTINCT 
            s.SecuCode,
            s.SecuAbbr,
            s.CompanyCode,
            i.ThirdIndustryName,
            i.SecondIndustryName,
            i.FirstIndustryName
        FROM SecuMain s
        LEFT JOIN LC_ExgIndustry i ON s.CompanyCode = i.CompanyCode
            AND i.Standard = '38'
            AND i.IfPerformed = 1
        WHERE s.SecuCategory = 1
        AND ({STOCK_FILTER})
        AND s.SecuCode NOT LIKE 'X%'
        ORDER BY s.SecuCode
        """
        
        df = fetcher.query(sql)
        
        stocks_list = []
        for _, row in df.iterrows():
            stocks_list.append({
                'code': str(row.get('SecuCode', '')).zfill(6),
                'name': row.get('SecuAbbr', ''),
                'company_code': int(row.get('CompanyCode', 0)),
                'third_industry': row.get('ThirdIndustryName', ''),
                'second_industry': row.get('SecondIndustryName', ''),
                'first_industry': row.get('FirstIndustryName', '')
            })
        
        logging.info(f"返回所有股票列表，共 {len(stocks_list)} 只")
        
        return jsonify({
            'success': True,
            'stocks': stocks_list,
            'count': len(stocks_list)
        })
    except Exception as e:
        logging.error(f"获取所有股票列表失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# 搜索股票（用于查询构建器）
@app.route('/api/data_query/search_stock', methods=['POST'])
@login_required
def search_stock_for_query():
    try:
        data = request.get_json()
        query = data.get('query', '').strip()
        
        if not query:
            return jsonify({'success': False, 'error': '请输入搜索关键词'})
        
        from industry_inventory_module import get_module
        module = get_module()
        stocks = module.get_stock_list_by_code_or_name(query)
        
        return jsonify({
            'success': True,
            'stocks': stocks
        })
    except Exception as e:
        logging.error(f"搜索股票失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# ==================== SQL模板管理功能 ====================
# 获取所有SQL模板
@app.route('/api/data_query/templates')
@login_required
def get_sql_templates():
    try:
        templates = query_db("""
            SELECT id, template_name, sql_template, description, created_at, updated_at
            FROM sql_query_templates 
            ORDER BY created_at DESC
        """)
        result = []
        for template in templates:
            result.append({
                'id': template[0],
                'template_name': template[1],
                'sql_template': template[2],
                'description': template[3] if len(template) > 3 else '',
                'created_at': template[4] if len(template) > 4 else '',
                'updated_at': template[5] if len(template) > 5 else ''
            })
        return jsonify({'success': True, 'templates': result})
    except Exception as e:
        logging.error(f"获取SQL模板失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# 保存SQL模板
@app.route('/api/data_query/templates', methods=['POST'])
@login_required
def save_sql_template():
    try:
        data = request.get_json()
        template_id = data.get('id')
        template_name = data.get('template_name', '').strip()
        sql_template = data.get('sql_template', '').strip()
        description = data.get('description', '').strip()
        
        if not template_name:
            return jsonify({'success': False, 'error': '模板名称不能为空'})
        
        if not sql_template:
            return jsonify({'success': False, 'error': 'SQL模板内容不能为空'})
        
        if template_id:
            # 更新
            update_db("""UPDATE sql_query_templates SET 
                        template_name = ?, sql_template = ?, description = ?,
                        updated_at = CURRENT_TIMESTAMP WHERE id = ?""",
                     (template_name, sql_template, description, template_id))
            return jsonify({'success': True, 'id': template_id})
        else:
            # 新增
            insert_db("""INSERT INTO sql_query_templates 
                        (template_name, sql_template, description) 
                        VALUES (?,?,?)""",
                     (template_name, sql_template, description))
            new_template = query_db("SELECT id FROM sql_query_templates WHERE template_name = ? ORDER BY created_at DESC LIMIT 1",
                                (template_name,), one=True)
            return jsonify({'success': True, 'id': new_template[0] if new_template else None})
    except Exception as e:
        logging.error(f"保存SQL模板失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# 删除SQL模板
@app.route('/api/data_query/templates/<int:template_id>', methods=['DELETE'])
@login_required
def delete_sql_template(template_id):
    try:
        delete_db("DELETE FROM sql_query_templates WHERE id = ?", (template_id,))
        return jsonify({'success': True})
    except Exception as e:
        logging.error(f"删除SQL模板失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# 执行SQL模板查询
@app.route('/api/data_query/execute_template', methods=['POST'])
@login_required
def execute_template_query():
    try:
        data = request.get_json()
        template_id = data.get('template_id')
        stock_codes = data.get('stock_codes', [])  # 股票代码列表（可选）
        date_range = data.get('date_range', {})  # 日期范围（可选）
        
        if not template_id:
            return jsonify({'success': False, 'error': '请选择SQL模板'})
        
        # 获取模板
        template = query_db("""
            SELECT id, template_name, sql_template, description
            FROM sql_query_templates WHERE id = ?
        """, (template_id,), one=True)
        
        if not template:
            return jsonify({'success': False, 'error': 'SQL模板不存在'})
        
        sql_template = template[2]  # sql_template
        template_name = template[1]  # template_name
        
        # 如果有股票代码，需要在SQL中添加过滤条件
        # 注意：这里假设SQL模板可能包含占位符，或者用户自己处理
        # 为了简单起见，我们直接执行SQL模板，如果模板中需要过滤股票代码，用户需要自己写
        # 或者可以支持简单的占位符替换，比如 {STOCK_CODES} 或 {DATE_RANGE}
        
        final_sql = sql_template
        
        # 执行查询
        from data_fetcher import JuyuanDataFetcher
        fetcher = JuyuanDataFetcher(lazy_init_pool=True)
        
        try:
            df = fetcher.query(final_sql)
            
            if df is None or df.empty:
                return jsonify({
                    'success': True,
                    'count': 0,
                    'columns': [],
                    'data': [],
                    'sql': final_sql,
                    'template_name': template_name
                })
            
            # 转换为列表格式
            columns = df.columns.tolist()
            data = df.values.tolist()
            
            return jsonify({
                'success': True,
                'count': len(data),
                'columns': columns,
                'data': data,
                'sql': final_sql,
                'template_name': template_name
            })
        except Exception as query_error:
            logging.error(f"执行SQL模板查询失败: {query_error}", exc_info=True)
            return jsonify({
                'success': False,
                'error': f'SQL执行失败: {str(query_error)}',
                'sql': final_sql
            })
            
    except Exception as e:
        logging.error(f"执行SQL模板查询失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# ==================== 执行查询功能 ====================
# 执行多表数据查询
@app.route('/api/data_query/execute_multi', methods=['POST'])
@login_required
def execute_multi_table_query():
    try:
        data = request.get_json()
        table_configs = data.get('table_configs', [])  # [{'table_id': 1, 'field_ids': [1,2,3]}, ...]
        stock_codes = data.get('stock_codes', [])
        date_range = data.get('date_range', {})
        
        if not table_configs or len(table_configs) == 0:
            return jsonify({'success': False, 'error': '请至少配置一个数据表'})
        
        from data_fetcher import JuyuanDataFetcher
        fetcher = JuyuanDataFetcher(lazy_init_pool=True)
        
        all_results = []
        all_columns = []
        table_names_map = {}  # 表ID到表名的映射
        
        try:
            # 第一步：收集所有表的配置信息
            table_info_list = []
            for table_config in table_configs:
                table_id = table_config.get('table_id')
                field_ids = table_config.get('field_ids', [])
                
                if not table_id or not field_ids:
                    continue
                
                # 获取表配置
                table_config_row = query_db("""
                    SELECT id, table_name, table_display_name, description, primary_key_field, 
                           date_field, code_field, name_field, join_type, join_field, created_at, updated_at
                    FROM data_table_configs WHERE id = ?
                """, (table_id,), one=True)
                
                if not table_config_row:
                    continue
                
                table_name = table_config_row[1]
                table_display_name = table_config_row[2]
                code_field = table_config_row[6]
                date_field = table_config_row[5]
                join_type_raw = table_config_row[8] if len(table_config_row) > 8 else None
                join_type = (join_type_raw.strip() if join_type_raw and isinstance(join_type_raw, str) else 'CompanyCode') or 'CompanyCode'
                join_field = (table_config_row[9] or '').strip() if len(table_config_row) > 9 and table_config_row[9] else ''
                
                # 验证关联字段配置（这是关键字段，必须存在）
                if not join_field:
                    logging.error(f"表 {table_name} (ID: {table_id}) 未配置关联字段名(join_field)，无法与SecuMain关联。请检查表配置。")
                    return jsonify({
                        'success': False, 
                        'error': f'表 "{table_display_name}" 未配置关联字段名，这是与SecuMain关联的关键字段。请在表配置页面设置"关联字段名"。'
                    })
                
                # 验证字段名格式
                import re
                if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', join_field):
                    logging.error(f"表 {table_name} (ID: {table_id}) 的关联字段名格式无效: {join_field}")
                    return jsonify({
                        'success': False,
                        'error': f'表 "{table_display_name}" 的关联字段名格式无效: "{join_field}"。字段名必须是有效的SQL标识符。'
                    })
                
                # 获取字段配置
                field_configs = query_db("""SELECT field_name, field_display_name FROM data_field_configs 
                                           WHERE id IN ({}) 
                                           ORDER BY order_index""".format(','.join(['?'] * len(field_ids))),
                                        tuple(field_ids))
                
                if not field_configs:
                    continue
                
                field_names = [f[0] for f in field_configs]
                field_display_names = {f[0]: f[1] for f in field_configs}
                
                # 使用join_field作为关联字段（不再回退到code_field，因为join_field是必需的）
                actual_join_field = join_field
                
                table_info_list.append({
                    'table_id': table_id,
                    'table_name': table_name,
                    'table_display_name': table_display_name,
                    'table_alias': f't{len(table_info_list) + 1}',
                    'date_field': date_field,
                    'join_type': join_type.strip().lower() if join_type else 'companycode',
                    'join_field': actual_join_field,
                    'field_names': field_names,
                    'field_display_names': field_display_names
                })
            
            if not table_info_list:
                return jsonify({'success': False, 'error': '没有有效的表配置'})
            
            # 第二步：如果有股票代码，先获取SecuCode到CompanyCode/InnerCode的映射
            # 重要：应用与筛选任务相同的筛选逻辑（STOCK_FILTER），确保数据口径一致
            secu_mapping = {}
            if stock_codes:
                from juyuan_config import STOCK_FILTER
                secu_query = f"""
                    SELECT DISTINCT SecuCode, CompanyCode, InnerCode 
                    FROM SecuMain 
                    WHERE SecuCode IN ({','.join(['?'] * len(stock_codes))})
                    AND ({STOCK_FILTER})
                    AND SecuCode NOT LIKE 'X%'
                """
                secu_data = fetcher.query(secu_query, tuple(stock_codes))
                if not secu_data.empty:
                    for _, row in secu_data.iterrows():
                        secu_mapping[row['SecuCode']] = {
                            'CompanyCode': row.get('CompanyCode'),
                            'InnerCode': row.get('InnerCode')
                        }
                # 记录过滤信息（仅在有股票被过滤时记录）
                if len(stock_codes) > len(secu_mapping):
                    filtered_count = len(stock_codes) - len(secu_mapping)
                    logging.debug(f"数据查询构建器：从{len(stock_codes)}只股票中过滤了{filtered_count}只不符合筛选条件的股票")
            
            # 第三步：构建多表关联SQL
            # 使用SecuMain作为中心表，通过SecuCode关联所有表
            select_fields_list = []
            from_clause = ""
            join_clauses = []
            where_conditions = []
            where_params = []
            
            # 添加SecuCode和SecuAbbr到SELECT（只添加一次）
            if stock_codes:
                select_fields_list.append("s.SecuCode as StockCode")
                select_fields_list.append("s.SecuAbbr as StockName")
                all_columns.append({'field': 'StockCode', 'display': '股票代码'})
                all_columns.append({'field': 'StockName', 'display': '股票名称'})
            
            # 为每个表构建JOIN和SELECT
            for idx, table_info in enumerate(table_info_list):
                table_alias = table_info['table_alias']
                table_name = table_info['table_name']
                table_display_name = table_info['table_display_name']
                join_type = table_info['join_type']
                join_field = table_info['join_field']
                date_field = table_info['date_field']
                field_names = table_info['field_names']
                field_display_names = table_info['field_display_names']
                
                # 添加字段到SELECT（带表名前缀）
                for field_name in field_names:
                    select_fields_list.append(f"{table_alias}.{field_name} as [{table_display_name}_{field_name}]")
                    all_columns.append({
                        'field': f"{table_display_name}_{field_name}",
                        'display': f"{table_display_name}.{field_display_names[field_name]}"
                    })
                
                # 构建FROM子句（第一个表）
                if idx == 0:
                    from_clause = f"FROM {table_name} {table_alias}"
                    
                    # 第一个表直接JOIN SecuMain
                    # 使用用户配置的join_type和join_field（这是关键！）
                    if stock_codes:
                        join_field_name = 'InnerCode' if join_type == 'innercode' else 'CompanyCode'
                        join_clauses.append(f"INNER JOIN SecuMain s ON {table_alias}.{join_field} = s.{join_field_name}")
                        
                        # 添加WHERE条件限制SecuCode（确保唯一性）
                        secu_codes_placeholders = ','.join(['?'] * len(stock_codes))
                        where_conditions.append(f"s.SecuCode IN ({secu_codes_placeholders})")
                        where_params.extend(stock_codes)
                        
                        # 正常关联不记录日志，只在错误时记录
                    # logging.debug(f"表 {table_display_name}: 通过 {join_field} = SecuMain.{join_field_name} 关联")
                else:
                    # 后续表通过SecuMain关联（使用相同的SecuMain别名s）
                    # 使用用户配置的join_type和join_field（这是关键！）
                    join_field_name = 'InnerCode' if join_type == 'innercode' else 'CompanyCode'
                    # 使用LEFT JOIN处理日期粒度不一致问题（季度表 vs 日度表）
                    # 这样即使某个日期在第二张表中没有数据，也能显示第一张表的数据
                    join_clauses.append(f"LEFT JOIN {table_name} {table_alias} ON {table_alias}.{join_field} = s.{join_field_name}")
                    
                    # 正常关联不记录日志，只在错误时记录
                    # logging.debug(f"表 {table_display_name}: 通过 {join_field} = SecuMain.{join_field_name} 关联到中心表")
                
                # 添加日期范围条件（每个表独立处理）
                if date_range and date_field:
                    date_start = date_range.get('start')
                    date_end = date_range.get('end')
                    
                    if date_start:
                        where_conditions.append(f"{table_alias}.{date_field} >= ?")
                        where_params.append(date_start)
                    if date_end:
                        where_conditions.append(f"{table_alias}.{date_field} <= ?")
                        where_params.append(date_end)
                
                # 重要：处理不同表的日期粒度不一致问题
                # 如果第一张表是季度/月度数据，第二张表是日度数据
                # 使用LEFT JOIN确保即使某个日期没有数据也能关联上
                # 这已经在SQL的JOIN类型中处理（INNER JOIN改为LEFT JOIN会更好，但需要权衡）
            
            # 构建完整SQL
            select_clause = ", ".join(select_fields_list)
            join_clause = "\n".join(join_clauses)
            where_clause = " AND ".join(where_conditions) if where_conditions else "1=1"
            
            sql = f"""
                SELECT TOP 10000 {select_clause}
                {from_clause}
                {join_clause}
                WHERE {where_clause}
            """
            
            # 只在调试时记录完整SQL（生产环境不记录，避免日志过大）
            # logging.debug(f"多表关联SQL: {sql[:200]}...")  # 只记录前200字符
            
            # 执行查询
            df = fetcher.query(sql, tuple(where_params) if where_params else None)
            
            if df.empty:
                # 格式化SQL以便显示（替换参数占位符为实际值）
                formatted_sql = sql
                if where_params:
                    # 按顺序替换所有?占位符
                    import re
                    param_index = 0
                    def replace_param(match):
                        nonlocal param_index
                        if param_index < len(where_params):
                            param = where_params[param_index]
                            param_index += 1
                            if isinstance(param, str):
                                # 转义单引号
                                escaped_param = param.replace("'", "''")
                                return f"'{escaped_param}'"
                            else:
                                return str(param)
                        return match.group(0)
                    formatted_sql = re.sub(r'\?', replace_param, formatted_sql)
                
                return jsonify({
                    'success': True,
                    'count': 0,
                    'table_count': len(table_info_list),
                    'data': [],
                    'columns': all_columns,
                    'sql': formatted_sql,  # 返回格式化的SQL
                    'warning': '查询结果为空，请检查筛选条件'
                })
            
            # 转换为字典列表
            for _, row in df.iterrows():
                row_dict = {}
                for col in df.columns:
                    value = row[col]
                    # 处理NaN值
                    import pandas as pd
                    if pd.isna(value):
                        value = None
                    elif isinstance(value, pd.Timestamp):
                        value = value.strftime('%Y-%m-%d')
                    row_dict[col] = value
                all_results.append(row_dict)
            
            # 过滤空行
            import pandas as pd
            filtered_results = []
            for row in all_results:
                # 检查是否有非空值（排除StockCode和StockName）
                has_data = any(
                    k not in ['StockCode', 'StockName'] and 
                    v is not None and 
                    (not isinstance(v, float) or not pd.isna(v)) 
                    for k, v in row.items()
                )
                if has_data:
                    filtered_results.append(row)
            
            # 格式化SQL以便显示（替换参数占位符为实际值）
            formatted_sql = sql
            if where_params:
                # 按顺序替换所有?占位符
                import re
                param_index = 0
                def replace_param(match):
                    nonlocal param_index
                    if param_index < len(where_params):
                        param = where_params[param_index]
                        param_index += 1
                        if isinstance(param, str):
                            # 转义单引号
                            escaped_param = param.replace("'", "''")
                            return f"'{escaped_param}'"
                        else:
                            return str(param)
                    return match.group(0)
                formatted_sql = re.sub(r'\?', replace_param, formatted_sql)
            
            return jsonify({
                'success': True,
                'count': len(filtered_results),
                'table_count': len(table_info_list),
                'data': filtered_results,
                'columns': all_columns,
                'sql': formatted_sql  # 返回格式化的SQL
            })
            
        finally:
            # 不立即清理连接池，由空闲超时机制管理（20分钟无活动后自动关闭）
            # fetcher.cleanup()  # 注释掉，让空闲超时机制管理
            pass
            
    except Exception as e:
        logging.error(f"执行多表查询失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# 执行数据查询
@app.route('/api/data_query/execute', methods=['POST'])
@login_required
def execute_data_query():
    try:
        data = request.get_json()
        table_id = data.get('table_id')
        stock_codes = data.get('stock_codes', [])  # 股票代码列表
        field_ids = data.get('field_ids', [])  # 字段ID列表
        date_range = data.get('date_range')  # {'start': '2024-01-01', 'end': '2024-12-31'}
        
        if not table_id:
            return jsonify({'success': False, 'error': '请选择数据表'})
        
        # 获取表配置
        # 使用明确的列名查询，避免索引错误
        # 表结构：id(0), table_name(1), table_display_name(2), description(3), 
        #         primary_key_field(4), date_field(5), code_field(6), name_field(7), 
        #         join_type(8), join_field(9), created_at(10), updated_at(11)
        table_config = query_db("""
            SELECT id, table_name, table_display_name, description, primary_key_field, 
                   date_field, code_field, name_field, join_type, join_field, created_at, updated_at
            FROM data_table_configs WHERE id = ?
        """, (table_id,), one=True)
        if not table_config:
            return jsonify({'success': False, 'error': '数据表配置不存在'})
        
        table_name = table_config[1]  # table_name
        code_field = table_config[6]  # code_field
        date_field = table_config[5]  # date_field
        # 确保join_type正确读取，如果为空、None或未定义则使用默认值
        join_type_raw = table_config[8] if len(table_config) > 8 else None
        join_type = (join_type_raw.strip() if join_type_raw and isinstance(join_type_raw, str) else 'CompanyCode') or 'CompanyCode'
        join_field = (table_config[9] or '').strip() if len(table_config) > 9 and table_config[9] else ''
        
        # 只在配置错误时记录详细信息
        # logging.debug(f"[数据表配置] id={table_id}, table_name={table_name}, join_type={join_type}, join_field={join_field}")
        
        # 提前验证字段名格式，避免后续SQL错误
        import re
        if date_field and isinstance(date_field, str):
            date_field = date_field.strip()
            if re.match(r'^\d{4}-\d{2}-\d{2}', date_field):
                return jsonify({'success': False, 'error': f'日期字段配置错误："{date_field}" 看起来像日期值而不是字段名。请检查数据表配置中的"日期字段"设置，应该填写字段名（如"TradingDay"）而不是日期值。'})
        if code_field and isinstance(code_field, str):
            code_field = code_field.strip()
            if re.match(r'^\d{4}-\d{2}-\d{2}', code_field):
                return jsonify({'success': False, 'error': f'代码字段配置错误："{code_field}" 看起来像日期值而不是字段名。请检查数据表配置中的"代码字段"设置。'})
        if join_field and isinstance(join_field, str):
            join_field = join_field.strip()
            if re.match(r'^\d{4}-\d{2}-\d{2}', join_field):
                return jsonify({'success': False, 'error': f'关联字段配置错误："{join_field}" 看起来像日期值而不是字段名。请检查数据表配置中的"关联字段名"设置，应该填写字段名（如"InnerCode"或"CompanyCode"）而不是日期值。'})
        
        # 获取字段配置
        if not field_ids:
            return jsonify({'success': False, 'error': '请至少选择一个字段'})
        
        field_configs = query_db("""SELECT field_name, field_display_name FROM data_field_configs 
                                   WHERE id IN ({}) 
                                   ORDER BY order_index""".format(','.join(['?'] * len(field_ids))),
                                tuple(field_ids))
        
        if not field_configs:
            return jsonify({'success': False, 'error': '字段配置不存在'})
        
        # 构建SQL查询
        field_names = [f[0] for f in field_configs]
        field_display_names = {f[0]: f[1] for f in field_configs}
        
        # 构建SELECT子句和FROM子句
        base_table_alias = 't'
        select_fields_list = [f"{base_table_alias}.{field}" for field in field_names]
        
        # 构建FROM和JOIN子句
        from_clause = f"FROM {table_name} {base_table_alias}"
        join_clause = ""
        
        # 构建WHERE子句
        where_conditions = []
        
        # 如果有股票代码，根据join_type决定关联方式
        # 重要：避免笛卡尔积的关键逻辑：
        # 1. 先查询SecuMain获取对应的CompanyCode/InnerCode，并同时获取SecuCode
        # 2. JOIN时使用严格的关联条件，并在WHERE中限制SecuCode，确保一对一关联
        # 3. 使用INNER JOIN确保只返回能匹配的数据，避免NULL值导致的笛卡尔积
        if stock_codes and (join_field or code_field):
            from data_fetcher import JuyuanDataFetcher
            fetcher = JuyuanDataFetcher(lazy_init_pool=True)
            
            # 获取关联字段名（优先使用join_field，否则使用code_field）
            actual_join_field = join_field if join_field else code_field
            
            # 验证关联字段名是否有效（必须是有效的SQL标识符，不能是日期字符串等）
            if not actual_join_field or not isinstance(actual_join_field, str):
                return jsonify({'success': False, 'error': f'关联字段名无效: join_field={join_field}, code_field={code_field}，请检查数据表配置'})
            
            actual_join_field = actual_join_field.strip()
            
            # 检查字段名格式：必须是有效的SQL标识符（字母、数字、下划线开头，不能包含日期格式）
            import re
            # 验证是否是有效的SQL标识符
            if not re.match(r'^[a-zA-Z_][a-zA-Z0-9_]*$', actual_join_field):
                return jsonify({
                    'success': False, 
                    'error': f'关联字段名格式无效: "{actual_join_field}"。字段名必须是有效的SQL标识符（字母、数字、下划线，且以字母或下划线开头），不能包含日期、空格或特殊字符。请检查数据表配置中的"关联字段名"或"代码字段"设置。'
                })
            
            # 检查是否意外使用了日期字段的值（如果字段名看起来像日期格式，报错）
            date_pattern = r'^\d{4}-\d{2}-\d{2}'
            if re.match(date_pattern, actual_join_field):
                return jsonify({
                    'success': False, 
                    'error': f'关联字段名看起来像日期值而不是字段名: "{actual_join_field}"。请检查数据表配置：可能错误地将日期字段的值设置为关联字段名，应该使用字段名（如"InnerCode"、"CompanyCode"）而不是字段的值。'
                })
            
            # 只在配置错误时记录
            # logging.debug(f"[关联字段验证] actual_join_field={actual_join_field}, join_field={join_field}, code_field={code_field}")
            
            # 转义股票代码中的单引号，防止SQL注入
            escaped_stock_codes = [code.replace("'", "''") for code in stock_codes]
            codes_str = "','".join(escaped_stock_codes)
            
            # 确保join_type是字符串并去除空格，转换为大写进行比较
            join_type_clean = join_type.strip().upper() if isinstance(join_type, str) else str(join_type).upper()
            logging.info(f"[JOIN类型检查] 原始join_type={join_type}, 清理后={join_type_clean}")
            
            if join_type_clean == 'COMPANYCODE':
                # 通过CompanyCode关联
                # 查询SecuMain获取CompanyCode和SecuCode的对应关系
                # 关键：同时获取SecuCode，用于在WHERE中精确限制，避免一对多导致的笛卡尔积
                # 重要：应用与筛选任务相同的筛选逻辑（STOCK_FILTER），确保数据口径一致
                from juyuan_config import STOCK_FILTER
                sql_get_secu_mapping = f"""
                SELECT DISTINCT CompanyCode, SecuCode 
                FROM SecuMain 
                WHERE SecuCode IN ('{codes_str}') AND SecuCategory = 1
                AND ({STOCK_FILTER})
                AND SecuCode NOT LIKE 'X%'
                """
                df_mapping = fetcher.query(sql_get_secu_mapping)
                if not df_mapping.empty and 'CompanyCode' in df_mapping.columns:
                    company_codes = df_mapping['CompanyCode'].unique().tolist()
                    secu_codes = df_mapping['SecuCode'].unique().tolist()
                    
                    if company_codes:
                        # 使用INNER JOIN而不是LEFT JOIN，确保只返回能匹配的数据，避免NULL导致的笛卡尔积
                        join_clause = f"""
                        INNER JOIN SecuMain s ON {base_table_alias}.{actual_join_field} = s.CompanyCode AND s.SecuCategory = 1
                        """
                        # 添加WHERE条件：同时限制CompanyCode和SecuCode
                        # 1. 限制CompanyCode确保只查询对应的公司
                        company_codes_str = ','.join(map(str, company_codes))
                        where_conditions.append(f"{base_table_alias}.{actual_join_field} IN ({company_codes_str})")
                        
                        # 2. 限制SecuCode确保只返回用户选择的股票代码（防止一个CompanyCode对应多个SecuCode导致的笛卡尔积）
                        secu_codes_str = "','".join([code.replace("'", "''") for code in secu_codes])
                        where_conditions.append(f"s.SecuCode IN ('{secu_codes_str}')")
                        
                        # 只在调试时记录
                        # logging.debug(f"CompanyCode关联: 找到 {len(company_codes)} 个CompanyCode, {len(secu_codes)} 个SecuCode")
                else:
                    return jsonify({'success': False, 'error': '未找到对应的CompanyCode，请检查股票代码是否正确'})
            elif join_type_clean == 'INNERCODE':
                # 通过InnerCode关联
                # 查询SecuMain获取InnerCode和SecuCode的对应关系
                # 重要：应用与筛选任务相同的筛选逻辑（STOCK_FILTER），确保数据口径一致
                from juyuan_config import STOCK_FILTER
                sql_get_secu_mapping = f"""
                SELECT DISTINCT InnerCode, SecuCode 
                FROM SecuMain 
                WHERE SecuCode IN ('{codes_str}') AND SecuCategory = 1
                AND ({STOCK_FILTER})
                AND SecuCode NOT LIKE 'X%'
                """
                df_mapping = fetcher.query(sql_get_secu_mapping)
                if not df_mapping.empty and 'InnerCode' in df_mapping.columns:
                    inner_codes = df_mapping['InnerCode'].unique().tolist()
                    secu_codes = df_mapping['SecuCode'].unique().tolist()
                    
                    if inner_codes:
                        # 使用INNER JOIN而不是LEFT JOIN，确保只返回能匹配的数据，避免NULL导致的笛卡尔积
                        join_clause = f"""
                        INNER JOIN SecuMain s ON {base_table_alias}.{actual_join_field} = s.InnerCode AND s.SecuCategory = 1
                        """
                        # 添加WHERE条件：同时限制InnerCode和SecuCode
                        # 1. 限制InnerCode确保只查询对应的股票
                        inner_codes_str = ','.join(map(str, inner_codes))
                        where_conditions.append(f"{base_table_alias}.{actual_join_field} IN ({inner_codes_str})")
                        
                        # 2. 限制SecuCode确保只返回用户选择的股票代码（防止一个InnerCode对应多个SecuCode导致的笛卡尔积）
                        # 注意：InnerCode通常是唯一的，但为了安全性仍然添加此条件
                        secu_codes_str = "','".join([code.replace("'", "''") for code in secu_codes])
                        where_conditions.append(f"s.SecuCode IN ('{secu_codes_str}')")
                        
                        # 只在调试时记录
                        # logging.debug(f"InnerCode关联: 找到 {len(inner_codes)} 个InnerCode, {len(secu_codes)} 个SecuCode")
                else:
                    return jsonify({'success': False, 'error': '未找到对应的InnerCode，请检查股票代码是否正确'})
        
        # 如果成功构建了JOIN子句，则在SELECT中添加股票代码和名称字段
        # 注意：必须在这里添加，确保JOIN成功后才使用s.*
        if join_clause and stock_codes:
            select_fields_list.insert(0, f"s.SecuCode AS StockCode")
            select_fields_list.insert(1, f"s.SecuAbbr AS StockName")
        
        select_fields = ', '.join(select_fields_list)
        
        # 如果有日期范围，添加日期条件
        if date_range and date_field:
            start_date = date_range.get('start', '')
            end_date = date_range.get('end', '')
            if start_date:
                where_conditions.append(f"{date_field} >= '{start_date}'")
            if end_date:
                where_conditions.append(f"{date_field} <= '{end_date}'")
        
        # 构建完整SQL
        sql = f"SELECT TOP 10000 {select_fields} {from_clause}"
        if join_clause:
            sql += join_clause
        if where_conditions:
            sql += " WHERE " + " AND ".join(where_conditions)
        
        # 只在错误时记录SQL（避免日志过大）
        # logging.debug(f"生成的SQL查询: {sql[:200]}...")  # 只记录前200字符
        
        # 执行查询
        # 如果之前没有创建fetcher（没有股票代码筛选），现在创建
        if fetcher is None:
            from data_fetcher import JuyuanDataFetcher
            fetcher = JuyuanDataFetcher(lazy_init_pool=True)
        
        try:
            df = fetcher.query(sql)
            
            # 只在错误或空结果时记录
            if df.empty:
                logging.warning(f"查询结果为空: table={table_name}, stock_codes={len(stock_codes) if stock_codes else 0}只")
            # 正常查询不记录详细信息，避免日志过大
            
            # 转换为JSON格式
            result_data = []
            
            # 如果有关联SecuMain，需要添加StockCode和StockName到列信息
            result_columns = []
            if join_clause and stock_codes:
                result_columns.append({'field': 'StockCode', 'display': '股票代码'})
                result_columns.append({'field': 'StockName', 'display': '股票名称'})
            
            # 检查DataFrame是否为空
            if df.empty:
                logging.warning(f"查询结果为空！SQL: {sql}")
                return jsonify({
                    'success': True,
                    'data': [],
                    'columns': result_columns + [{'field': name, 'display': field_display_names.get(name, name)} 
                                                 for name in field_names],
                    'count': 0,
                    'sql': sql,
                    'warning': '查询结果为空，请检查筛选条件是否正确'
                })
            
            # 处理每一行数据
            actual_row_count = 0
            empty_row_count = 0
            for idx, row in df.iterrows():
                record = {}
                
                # 如果有关联SecuMain，添加股票代码和名称
                if join_clause and stock_codes:
                    record['StockCode'] = row.get('StockCode')
                    record['StockName'] = row.get('StockName')
                
                # 添加用户选择的字段
                has_non_null_data = False
                for field_name in field_names:
                    value = row.get(field_name)
                    # 处理NaN值
                    if pd.isna(value):
                        value = None
                    elif isinstance(value, pd.Timestamp):
                        value = value.strftime('%Y-%m-%d')
                    record[field_name] = value
                    # 检查是否有非空数据
                    if value is not None:
                        has_non_null_data = True
                
                # 检查是否有实际数据（至少有一个非空字段）
                # 如果有关联SecuMain，检查StockCode或StockName
                if join_clause and stock_codes:
                    if record.get('StockCode') or record.get('StockName'):
                        has_non_null_data = True
                
                # 只有当记录中有实际数据时才添加
                if has_non_null_data:
                    result_data.append(record)
                    actual_row_count += 1
                else:
                    empty_row_count += 1
            
            # 添加字段列信息
            result_columns.extend([{'field': name, 'display': field_display_names.get(name, name)} 
                                 for name in field_names])
            
            # 只在结果异常时记录（正常查询不记录）
            if actual_row_count == 0 and len(df) > 0:
                logging.warning(f"查询返回 {len(df)} 行但有效记录为0，可能存在数据质量问题")
            
            # 不立即清理连接池，由空闲超时机制管理（20分钟无活动后自动关闭）
            # 这样可以避免频繁创建和关闭连接，提高性能
            # fetcher.cleanup()  # 注释掉，让空闲超时机制管理
            
            return jsonify({
                'success': True,
                'data': result_data,
                'columns': result_columns,
                'count': actual_row_count,
                'sql': sql
            })
        except Exception as e:
            logging.error(f"执行查询失败: {e}", exc_info=True)
            # 不立即清理连接，由空闲超时机制管理
            # try:
            #     if 'fetcher' in locals():
            #         fetcher.cleanup()
            # except:
            #     pass
            return jsonify({'success': False, 'error': f'查询失败: {str(e)}'})
        
    except Exception as e:
        logging.error(f"执行数据查询失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# 从文字内容识别表结构（无需OCR）
@app.route('/api/data_query/recognize_table_from_text', methods=['POST'])
@login_required
def recognize_table_from_text():
    """从文字内容识别表结构"""
    try:
        data = request.get_json()
        text = data.get('text', '').strip()
        
        if not text:
            return jsonify({'success': False, 'error': '请输入表结构文字内容'})
        
        # 构建提示词
        prompt = f"""请分析以下表结构文字内容，识别出所有字段信息，并返回JSON格式的数据。

文字内容：
{text}

请识别并返回字段信息，格式如下：
{{
    "fields": [
        {{
            "field_name": "字段名（数据库字段名）",
            "field_display_name": "显示名称",
            "field_type": "字段类型（TEXT/INTEGER/REAL/DECIMAL/DATE/DATETIME/TIMESTAMP）",
            "description": "字段描述",
            "is_sortable": true,
            "is_filterable": true,
            "order_index": 0
        }}
    ]
}}

注意事项：
- 如果文字中有多个表，请全部识别
- 字段类型请根据常见数据库类型推断（VARCHAR/TEXT -> TEXT, INT/BIGINT -> INTEGER, DECIMAL/FLOAT -> REAL, DATE -> DATE, DATETIME/TIMESTAMP -> DATETIME）
- 如果没有明确的显示名称，使用字段名
- order_index按照文字中的顺序从0开始递增
- 尽量识别所有可见的字段信息

请只返回JSON格式的数据，不要添加其他说明文字。"""
        
        # 使用纯文本格式调用DeepSeek API
        messages = [
            {
                "role": "user",
                "content": prompt
            }
        ]
        
        # 调用DeepSeek API
        headers = {
            "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": "deepseek-chat",
            "messages": messages,
            "temperature": 0.3,
            "max_tokens": 4000
        }
        
        try:
            # 增加超时时间到120秒，因为文字内容可能较长
            response = requests.post(DEEPSEEK_API_URL, headers=headers, json=payload, timeout=120)
            
            if response.status_code == 200:
                result = response.json()
                content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
                
                if not content:
                    return jsonify({'success': False, 'error': 'API返回空内容'})
                
                # 尝试从返回内容中提取JSON
                import json as json_module
                import re
                json_match = re.search(r'\{[\s\S]*\}', content)
                if json_match:
                    json_str = json_match.group(0)
                    try:
                        field_data = json_module.loads(json_str)
                        fields = field_data.get('fields', [])
                        
                        if fields:
                            return jsonify({
                                'success': True,
                                'fields': fields
                            })
                        else:
                            return jsonify({'success': False, 'error': '未识别到字段信息'})
                    except json_module.JSONDecodeError as e:
                        logging.error(f"JSON解析失败: {e}, 内容: {json_str[:200]}")
                        return jsonify({'success': False, 'error': f'JSON解析失败: {str(e)}'})
                else:
                    return jsonify({'success': False, 'error': 'API返回内容中未找到JSON格式数据'})
            else:
                error_msg = f"API调用失败: {response.status_code}"
                try:
                    error_data = response.json()
                    error_msg += f" - {error_data}"
                except:
                    error_msg += f" - {response.text[:200]}"
                logging.error(error_msg)
                return jsonify({'success': False, 'error': error_msg})
        except requests.exceptions.RequestException as e:
            logging.error(f"API请求异常: {e}")
            return jsonify({'success': False, 'error': f'API请求异常: {str(e)}'})
            
    except Exception as e:
        logging.error(f"识别失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': f'识别失败: {str(e)}'})

# 识别图片中的表结构（DeepSeek Vision API，需要OCR）
@app.route('/api/data_query/recognize_table', methods=['POST'])
@login_required
def recognize_table_from_image():
    try:
        if 'images' not in request.files:
            return jsonify({'success': False, 'error': '请上传图片'})
        
        files = request.files.getlist('images')
        if not files or len(files) == 0:
            return jsonify({'success': False, 'error': '请至少上传一张图片'})
        
        import base64
        import io
        
        # 读取所有图片并转换为base64
        image_data_list = []
        for file in files:
            if file.filename == '':
                continue
            
            # 读取图片数据
            image_bytes = file.read()
            
            # 转换为base64
            image_base64 = base64.b64encode(image_bytes).decode('utf-8')
            
            # 根据文件扩展名判断MIME类型
            filename = file.filename.lower()
            if filename.endswith('.png'):
                mime_type = 'image/png'
            elif filename.endswith('.jpg') or filename.endswith('.jpeg'):
                mime_type = 'image/jpeg'
            elif filename.endswith('.gif'):
                mime_type = 'image/gif'
            elif filename.endswith('.webp'):
                mime_type = 'image/webp'
            else:
                mime_type = 'image/jpeg'  # 默认
            
            image_data_list.append({
                "type": "image_url",
                "image_url": {
                    "url": f"data:{mime_type};base64,{image_base64}"
                }
            })
        
        if not image_data_list:
            return jsonify({'success': False, 'error': '没有有效的图片文件'})
        
        # 构建提示词
        prompt = """请仔细分析这些图片中的数据库表结构。

请识别图片中显示的数据表字段信息，包括：
1. 字段名（Field Name / Column Name）
2. 字段类型（Type / Data Type）
3. 字段描述或说明（Description / Comment）
4. 其他相关信息

请以JSON格式返回识别结果，格式如下：
{
    "fields": [
        {
            "field_name": "字段名",
            "field_display_name": "字段显示名称（如果没有则使用字段名）",
            "field_type": "字段类型（TEXT/INTEGER/REAL/DATE/DATETIME）",
            "description": "字段描述",
            "is_sortable": true,
            "is_filterable": true,
            "order_index": 0
        }
    ]
}

注意事项：
- 如果图片中有多个表，请全部识别
- 字段类型请根据常见数据库类型推断（VARCHAR/TEXT -> TEXT, INT/BIGINT -> INTEGER, DECIMAL/FLOAT -> REAL, DATE -> DATE, DATETIME/TIMESTAMP -> DATETIME）
- 如果没有明确的显示名称，使用字段名
- order_index按照图片中的顺序从0开始递增
- 尽量识别所有可见的字段信息

请只返回JSON格式的数据，不要添加其他说明文字。"""
        
        # DeepSeek API不支持image_url格式，只支持text
        # 我们需要使用OCR提取图片中的文字，然后发送给AI
        # 如果没有OCR库，则提示用户手动输入或使用其他方式
        
        try:
            # 尝试使用PIL和pytesseract进行OCR
            from PIL import Image
            try:
                import pytesseract
                
                # 尝试自动检测 Tesseract 路径（Windows 常见安装位置）
                if os.name == 'nt':  # Windows系统
                    possible_paths = [
                        r'C:\Program Files\Tesseract-OCR\tesseract.exe',
                        r'C:\Program Files (x86)\Tesseract-OCR\tesseract.exe',
                        r'C:\Users\{}\AppData\Local\Programs\Tesseract-OCR\tesseract.exe'.format(os.getenv('USERNAME', '')),
                    ]
                    # 如果环境变量中没有 tesseract，尝试设置路径
                    try:
                        pytesseract.get_tesseract_version()
                    except Exception:
                        # 遍历可能的路径
                        for path in possible_paths:
                            if os.path.exists(path):
                                pytesseract.pytesseract.tesseract_cmd = path
                                break
                        else:
                            # 如果都找不到，给用户提示
                            logging.warning("未找到 Tesseract OCR，请安装或配置路径。参考 OCR_安装说明.md")
            except ImportError:
                # 如果没有安装pytesseract，提示用户
                return jsonify({
                    'success': False,
                    'error': 'DeepSeek API不支持图片输入。请安装OCR库以提取图片文字：\n\n1. 安装 Tesseract OCR：\n   - Windows: https://github.com/UB-Mannheim/tesseract/wiki\n   - 下载中文语言包: chi_sim.traineddata\n\n2. 安装 Python 库：\n   pip install pytesseract pillow\n\n详细安装说明请参考项目根目录下的 OCR_安装说明.md 文件\n\n或者，您可以先使用"输入文字"功能，直接粘贴表结构文字。'
                })
            
            # 提取所有图片中的文字
            extracted_texts = []
            success_count = 0
            fail_count = 0
            
            # 只在开始和结束时记录汇总信息
            logging.info(f"OCR识别开始: {len(image_data_list)} 张图片")
            
            # 验证Tesseract是否可用
            try:
                tesseract_version = pytesseract.get_tesseract_version()
                # OCR版本信息只在调试时记录
                # logging.debug(f"Tesseract OCR 版本: {tesseract_version}")
            except Exception as e:
                logging.error(f"Tesseract OCR 验证失败: {e}")
                return jsonify({
                    'success': False,
                    'error': f'Tesseract OCR 未正确配置或未找到。\n\n请确保：\n1. 已安装 Tesseract OCR 引擎\n2. 已配置环境变量或路径\n3. 已下载中文语言包 (chi_sim.traineddata)\n\n错误详情: {str(e)}\n\n或者，您可以先使用"输入文字"功能，直接粘贴表结构文字。'
                })
            
            for i, img_data in enumerate(image_data_list):
                try:
                    # 从base64解码图片
                    base64_data = img_data["image_url"]["url"].split(',')[1] if ',' in img_data["image_url"]["url"] else img_data["image_url"]["url"]
                    if base64_data:
                        image_bytes = base64.b64decode(base64_data)
                        image = Image.open(io.BytesIO(image_bytes))
                        
                        # 不记录每张图片的识别过程，避免日志过多
                        # logging.debug(f"正在对图片{i+1}进行OCR识别...")
                        # 使用OCR提取文字（支持中英文）
                        text = pytesseract.image_to_string(image, lang='chi_sim+eng')
                        
                        if text.strip():
                            success_count += 1
                            # 限制提取的文字长度，避免API请求过大（最多8000字符）
                            max_text_length = 8000
                            if len(text) > max_text_length:
                                text = text[:max_text_length] + f"\n\n[文字过长，已截断，原长度: {len(text)} 字符]"
                                # 只在汇总时记录
                                # logging.debug(f"图片{i+1} OCR提取的文字过长，已截断")
                            
                            extracted_texts.append(f"\n\n=== 图片{i+1}中提取的文字 ===\n{text}")
                        else:
                            fail_count += 1
                            extracted_texts.append(f"\n\n=== 图片{i+1}未能提取到文字，请确保图片清晰 ===")
                except Exception as e:
                    fail_count += 1
                    # 只记录错误，不记录详细信息
                    logging.error(f"OCR提取图片{i+1}失败: {e}")
                    extracted_texts.append(f"\n\n=== 图片{i+1}OCR提取失败: {str(e)} ===")
            
            # 记录OCR汇总信息
            if success_count > 0 or fail_count > 0:
                logging.info(f"OCR识别完成: 成功 {success_count} 张，失败 {fail_count} 张，共 {len(image_data_list)} 张")
            
            # 构建增强的提示词
            enhanced_prompt = prompt
            if extracted_texts:
                enhanced_prompt += "\n\n以下是从图片中提取的文字信息，请根据这些信息识别表结构："
                enhanced_prompt += "".join(extracted_texts)
            else:
                return jsonify({
                    'success': False, 
                    'error': '未能从图片中提取文字，请确保图片清晰且包含表结构信息，或手动输入字段配置'
                })
            
        except ImportError as e:
            # 如果没有OCR库，返回错误提示
            return jsonify({
                'success': False,
                'error': f'DeepSeek API不支持图片输入。请安装OCR库以提取图片文字：\n\npip install pytesseract pillow\n\n错误详情: {str(e)}'
            })
        except Exception as e:
            error_msg = str(e)
            # 检查是否是 Tesseract 未找到的错误
            if 'tesseract' in error_msg.lower() or 'not found' in error_msg.lower():
                return jsonify({
                    'success': False,
                    'error': f'Tesseract OCR 未找到或未正确配置。\n\n请确保：\n1. 已安装 Tesseract OCR 引擎\n2. 已配置环境变量或路径\n3. 已下载中文语言包 (chi_sim.traineddata)\n\n详细安装说明请参考项目根目录下的 OCR_安装说明.md 文件\n\n错误详情: {error_msg}\n\n或者，您可以先使用"输入文字"功能，直接粘贴表结构文字。'
                })
            logging.error(f"OCR处理失败: {e}", exc_info=True)
            return jsonify({
                'success': False,
                'error': f'图片处理失败: {error_msg}。\n\n请检查：\n1. 图片是否清晰\n2. OCR 库是否正确安装\n3. 中文语言包是否已下载\n\n详细安装说明请参考项目根目录下的 OCR_安装说明.md 文件\n\n或者，您可以先使用"输入文字"功能，直接粘贴表结构文字。'
            })
        
        # 使用纯文本格式（DeepSeek API标准格式）
        messages = [
            {
                "role": "user",
                "content": enhanced_prompt
            }
        ]
        
        # 调用DeepSeek API
        headers = {
            "Authorization": f"Bearer {DEEPSEEK_API_KEY}",
            "Content-Type": "application/json"
        }
        
        payload = {
            "model": "deepseek-chat",
            "messages": messages,
            "temperature": 0.3,
            "max_tokens": 4000
        }
        
        try:
            # 增加超时时间到180秒（3分钟），因为OCR提取的文字可能很长，需要更长的处理时间
            # 只在开始和结束时记录
            logging.info(f"DeepSeek API识别开始: 提示词长度 {len(enhanced_prompt)} 字符")
            response = requests.post(DEEPSEEK_API_URL, headers=headers, json=payload, timeout=180)
            
            if response.status_code == 200:
                result = response.json()
                content = result.get("choices", [{}])[0].get("message", {}).get("content", "")
                
                if not content:
                    return jsonify({'success': False, 'error': 'API返回空内容'})
                
                # 尝试从响应中提取JSON
                import json as json_module
                import re
                
                # 尝试直接解析JSON
                try:
                    # 查找JSON部分（可能在代码块中）
                    json_match = re.search(r'\{[\s\S]*\}', content)
                    if json_match:
                        json_str = json_match.group(0)
                        parsed_result = json_module.loads(json_str)
                    else:
                        parsed_result = json_module.loads(content)
                    
                    if 'fields' in parsed_result and isinstance(parsed_result['fields'], list):
                        logging.info(f"DeepSeek API识别成功: 识别到 {len(parsed_result['fields'])} 个字段")
                        return jsonify({
                            'success': True,
                            'fields': parsed_result['fields']
                        })
                    else:
                        return jsonify({'success': False, 'error': 'API返回格式不正确，未找到fields字段'})
                except json_module.JSONDecodeError as e:
                    logging.error(f"解析JSON失败: {e}, 原始内容: {content[:500]}")
                    return jsonify({
                        'success': False, 
                        'error': f'解析识别结果失败，请检查图片质量或重试。原始响应: {content[:200]}'
                    })
            else:
                logging.error(f"DeepSeek API错误: 状态码 {response.status_code}, 响应: {response.text[:200]}...")
                return jsonify({
                    'success': False, 
                    'error': f'API调用失败: {response.status_code} - {response.text[:200]}'
                })
        except requests.exceptions.Timeout as e:
            error_msg = str(e)
            logging.error(f"DeepSeek API请求超时: {error_msg}")
            return jsonify({
                'success': False, 
                'error': f'API请求超时（已等待3分钟）。\n\n可能的原因：\n1. OCR提取的文字过长，导致API处理时间过长\n2. 网络连接不稳定\n3. API服务器响应慢\n\n建议：\n1. 尝试减少上传的图片数量\n2. 使用"输入文字"功能，手动输入表结构\n3. 检查网络连接后重试\n\n错误详情: {error_msg}'
            })
        except requests.exceptions.ConnectionError as e:
            error_msg = str(e)
            logging.error(f"DeepSeek API连接错误: {error_msg}")
            return jsonify({
                'success': False,
                'error': f'无法连接到DeepSeek API服务器。\n\n请检查：\n1. 网络连接是否正常\n2. 是否能访问 api.deepseek.com\n3. 防火墙或代理设置是否正确\n\n错误详情: {error_msg}'
            })
        except Exception as e:
            error_msg = str(e)
            logging.error(f"调用DeepSeek API失败: {error_msg}", exc_info=True)
            # 检查是否是超时相关的错误
            if 'timeout' in error_msg.lower() or 'timed out' in error_msg.lower():
                return jsonify({
                    'success': False,
                    'error': f'API请求超时。\n\n可能的原因：\n1. OCR提取的文字过长\n2. 网络连接不稳定\n3. API服务器响应慢\n\n建议：\n1. 尝试减少上传的图片数量\n2. 使用"输入文字"功能\n3. 检查网络连接后重试\n\n错误详情: {error_msg}'
                })
            return jsonify({'success': False, 'error': f'API调用失败: {error_msg}'})
            
    except Exception as e:
        logging.error(f"识别表结构失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# ==================== 筛选任务管理功能 ====================
@app.route('/screening_tasks')
@login_required
def screening_tasks():
    """筛选任务管理页面"""
    return render_template('screening_tasks.html')

# 获取任务类型列表
@app.route('/api/screening_tasks/types')
@login_required
def get_task_types():
    """获取所有任务类型"""
    try:
        from screening_tasks import TASK_TYPES
        task_types = []
        for task_id, task_info in TASK_TYPES.items():
            task_types.append({
                'id': task_id,
                'name': task_info['name']
            })
        return jsonify({'success': True, 'task_types': task_types})
    except Exception as e:
        logging.error(f"获取任务类型失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# 执行任务
@app.route('/api/screening_tasks/execute', methods=['POST'])
@login_required
def execute_screening_task():
    """执行筛选任务（异步）"""
    try:
        data = request.get_json()
        task_type = data.get('task_type', '').strip()
        execution_params = data.get('params', {})
        
        if not task_type:
            return jsonify({'success': False, 'error': '请选择任务类型'})
        
        # 获取当前用户
        username = session.get('username', 'system')
        
        # 导入任务执行模块
        from screening_tasks import execute_task
        
        # 异步执行任务（使用线程）
        import threading
        def run_task():
            try:
                execute_task(
                    task_type=task_type,
                    execution_params=execution_params,
                    created_by=username
                )
            except Exception as e:
                logging.error(f"后台任务执行失败: {e}", exc_info=True)
        
        thread = threading.Thread(target=run_task)
        thread.daemon = True
        thread.start()
        
        return jsonify({
            'success': True,
            'message': '任务已提交，正在后台执行...'
        })
        
    except Exception as e:
        logging.error(f"提交任务失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

def _ensure_screening_task_table(conn):
    """确保 screening_task_executions 表存在（若用 run.py 等启动未走 init_db 时）"""
    conn.execute('''CREATE TABLE IF NOT EXISTS screening_task_executions
                    (id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_type TEXT NOT NULL,
                    task_name TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    output_file_path TEXT,
                    execution_params TEXT,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    duration_seconds INTEGER,
                    error_message TEXT,
                    result_summary TEXT,
                    created_by TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)''')
    conn.commit()


# 获取执行历史
@app.route('/api/screening_tasks/history')
@login_required
def screening_task_history():
    """获取执行历史记录"""
    import json as _json
    conn = None
    try:
        task_type = request.args.get('task_type') or None
        try:
            limit = int(request.args.get('limit', 50))
            limit = min(max(1, limit), 500)
        except (TypeError, ValueError):
            limit = 50

        conn = sqlite3.connect(DATABASE_FILE)
        conn.row_factory = sqlite3.Row
        _ensure_screening_task_table(conn)
        c = conn.cursor()

        if task_type:
            c.execute('''SELECT * FROM screening_task_executions
                         WHERE task_type = ? ORDER BY created_at DESC LIMIT ?''', (task_type, limit))
        else:
            c.execute('''SELECT * FROM screening_task_executions
                         ORDER BY created_at DESC LIMIT ?''', (limit,))
        rows = c.fetchall()

        history = []
        for row in rows:
            try:
                params = _json.loads(row['execution_params']) if row['execution_params'] else None
            except (_json.JSONDecodeError, TypeError):
                params = None
            history.append({
                'id': row['id'],
                'task_type': row['task_type'],
                'task_name': row['task_name'],
                'status': row['status'],
                'output_file_path': row['output_file_path'],
                'execution_params': params,
                'start_time': row['start_time'],
                'end_time': row['end_time'],
                'duration_seconds': row['duration_seconds'],
                'error_message': row['error_message'],
                'result_summary': row['result_summary'],
                'created_by': row['created_by'],
                'created_at': row['created_at'],
            })

        return jsonify({'success': True, 'history': history})
    except Exception as e:
        logging.error(f"获取执行历史失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})
    finally:
        if conn:
            conn.close()

# 获取任务结果详情
@app.route('/api/screening_tasks/result/<int:execution_id>')
@login_required
def get_task_result(execution_id):
    """获取任务执行结果详情"""
    import sqlite3
    import json

    conn = None
    try:
        # 与 init_db、query_db 一致，使用 database.db
        conn = sqlite3.connect(DATABASE_FILE)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute('''SELECT * FROM screening_task_executions WHERE id = ?''', (execution_id,))
        row = c.fetchone()
        if not row:
            return jsonify({'success': False, 'error': '执行记录不存在'})

        try:
            params = json.loads(row['execution_params']) if row['execution_params'] else None
        except (json.JSONDecodeError, TypeError):
            params = None

        result = {
            'id': row['id'],
            'task_type': row['task_type'],
            'task_name': row['task_name'],
            'status': row['status'],
            'output_file_path': row['output_file_path'],
            'execution_params': params,
            'start_time': row['start_time'],
            'end_time': row['end_time'],
            'duration_seconds': row['duration_seconds'],
            'error_message': row['error_message'],
            'result_summary': row['result_summary'],
            'created_by': row['created_by'],
            'created_at': row['created_at']
        }

        # 如果任务成功且有输出文件，尝试读取结果数据
        if row['status'] == 'success' and row['output_file_path']:
            try:
                import pandas as pd
                file_path = row['output_file_path']
                
                # 检查文件是否存在
                if not os.path.exists(file_path):
                    logging.warning(f"结果文件不存在: {file_path}")
                    result['error'] = f'结果文件不存在: {os.path.basename(file_path)}'
                else:
                    # 获取文件修改时间，确保读取最新数据
                    file_mtime = os.path.getmtime(file_path)
                    file_mtime_str = datetime.fromtimestamp(file_mtime).strftime('%Y-%m-%d %H:%M:%S')
                    
                    # 读取完整Excel数据（移除5000条限制，支持完整数据分页）
                    xls = pd.ExcelFile(file_path)
                    result_data = {}
                    total_rows = 0
                    
                    for sheet_name in xls.sheet_names:
                        try:
                            df = pd.read_excel(file_path, sheet_name=sheet_name)
                            # 读取完整数据，不限制条数（前端分页处理）
                            result_data[sheet_name] = df.to_dict('records')
                            total_rows += len(df)
                            logging.debug(f"读取Sheet '{sheet_name}': {len(df)} 条数据")
                        except Exception as sheet_error:
                            logging.error(f"读取Sheet '{sheet_name}' 失败: {sheet_error}", exc_info=True)
                            result_data[sheet_name] = []
                    
                    result['result_data'] = result_data
                    result['file_mtime'] = file_mtime_str
                    result['total_rows'] = total_rows
                    logging.info(f"成功读取结果文件: {file_path}, 共 {len(result_data)} 个Sheet, 总计 {total_rows} 条数据, 文件修改时间: {file_mtime_str}")
                    
            except Exception as e:
                logging.error(f"读取结果数据失败: {e}", exc_info=True)
                result['error'] = f'读取结果数据失败: {str(e)}'

        return jsonify({'success': True, 'result': result})

    except Exception as e:
        logging.error(f"获取任务结果失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})
    finally:
        if conn:
            conn.close()

# 下载任务结果文件
@app.route('/api/screening_tasks/download/<int:execution_id>')
@login_required
def download_task_result(execution_id):
    """下载任务执行结果文件"""
    import sqlite3

    conn = None
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute('''SELECT output_file_path FROM screening_task_executions WHERE id = ?''', (execution_id,))
        row = c.fetchone()
        if not row or not row[0]:
            return jsonify({'success': False, 'error': '结果文件不存在'})

        file_path = row[0]
        if not os.path.exists(file_path):
            return jsonify({'success': False, 'error': '文件不存在'})

        return send_file(file_path, as_attachment=True, download_name=os.path.basename(file_path))

    except Exception as e:
        logging.error(f"下载任务结果失败: {e}", exc_info=True)
        return jsonify({'success': False, 'error': str(e)})
    finally:
        if conn:
            conn.close()


# ==================== 动量评分功能 ====================

@app.route('/momentum_scores', endpoint='momentum_scores_page')
@login_required
def momentum_scores_page():
    """动量评分展示页面"""
    username = session.get('username')
    log_access(username, '/momentum_scores', 'GET')
    return render_template('momentum_scores.html')


@app.route('/api/momentum_scores/run', methods=['POST'])
@login_required
def api_run_momentum_scores():
    """
    手动触发动量流水线：
    - 默认跑当天数据；可传 JSON {"date":"YYYY-MM-DD"} 指定目标日
    - 结果写入 momentum_scores 表
    - 每日产结果时：需保证数据区间覆盖 [目标日-500 天, 目标日]，此处按目标日动态配置
    - 返回 task_id，前端可通过轮询 /api/momentum_scores/logs 获取执行日志
    """
    try:
        from momentum_pipeline import DailyMomentumPipeline, DataLoader, DataLoaderConfig
        
        # 生成任务ID
        task_id = f"momentum_{int(time.time() * 1000)}"
        
        # 设置日志收集器
        log_handler = MomentumLogHandler(task_id)
        log_handler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(message)s')
        log_handler.setFormatter(formatter)
        
        # 获取 momentum_pipeline 的 logger 并添加 handler
        # momentum_pipeline 模块使用 logger = logging.getLogger(__name__)，所以名称是 'momentum_pipeline'
        momentum_logger = logging.getLogger('momentum_pipeline')
        momentum_logger.addHandler(log_handler)
        momentum_logger.setLevel(logging.INFO)
        
        # 捕获 high_performance_threading 的日志
        threading_logger = logging.getLogger('high_performance_threading')
        threading_logger.addHandler(log_handler)
        threading_logger.setLevel(logging.INFO)
        
        # 保存引用以便后续移除
        threading_logger_ref = threading_logger
        
        # 捕获根 logger 的所有 INFO 级别日志（但避免重复）
        root_logger = logging.getLogger()
        # 只添加一次，避免重复日志
        if log_handler not in root_logger.handlers:
            root_logger.addHandler(log_handler)
        
        # 添加初始日志
        with momentum_logs_lock:
            if task_id not in momentum_logs:
                momentum_logs[task_id] = deque(maxlen=MOMENTUM_LOG_MAX_SIZE)
            momentum_logs[task_id].append(f"[{datetime.now().strftime('%H:%M:%S')}] 开始运行动量评分流水线...")

        data = request.get_json(silent=True) or {}
        # 目标日：优先使用聚源数据库最新交易日（数据源的最新日期）
        date_str = data.get('date')
        if not date_str:
            # 如果没有指定日期，使用聚源数据库的最新交易日
            try:
                from data_fetcher import JuyuanDataFetcher
                fetcher = JuyuanDataFetcher(lazy_init_pool=True)
                latest_trading_date = fetcher.get_latest_trading_date()
                date_str = latest_trading_date.strftime('%Y-%m-%d') if hasattr(latest_trading_date, 'strftime') else str(latest_trading_date)
                with momentum_logs_lock:
                    momentum_logs[task_id].append(f"[{datetime.now().strftime('%H:%M:%S')}] 未指定日期，使用聚源数据库最新交易日: {date_str}")
            except Exception as e:
                # 如果获取聚源数据库最新交易日失败，使用今天
                date_str = datetime.now().strftime('%Y-%m-%d')
                with momentum_logs_lock:
                    momentum_logs[task_id].append(f"[{datetime.now().strftime('%H:%M:%S')}] 获取聚源数据库最新交易日失败，使用今天: {date_str}")
        else:
            # 如果指定了日期，检查是否比聚源数据库最新交易日新
            try:
                from data_fetcher import JuyuanDataFetcher
                fetcher = JuyuanDataFetcher(lazy_init_pool=True)
                latest_trading_date = fetcher.get_latest_trading_date()
                latest_trading_date_str = latest_trading_date.strftime('%Y-%m-%d') if hasattr(latest_trading_date, 'strftime') else str(latest_trading_date)
                if date_str > latest_trading_date_str:
                    # 如果指定的日期比聚源数据库最新交易日新，使用聚源数据库最新交易日
                    with momentum_logs_lock:
                        momentum_logs[task_id].append(f"[{datetime.now().strftime('%H:%M:%S')}] 指定日期 {date_str} 比聚源数据库最新交易日 {latest_trading_date_str} 新，使用聚源数据库最新交易日")
                    date_str = latest_trading_date_str
            except Exception as e:
                # 如果获取失败，继续使用用户指定的日期
                pass
        # 可配置参数（与界面「配置」一致，不传则用默认）
        lookback_days = int(data.get('lookback_days', 600))      # 取数区间：目标日前多少天
        calc_window_days = int(data.get('calc_window_days', 500)) # 因子计算窗口（天）
        min_trading_days = int(data.get('min_trading_days', 300)) # 单只股票最少交易日
        winsorize_alpha = float(data.get('winsorize_alpha', 0.025))
        if calc_window_days > lookback_days:
            calc_window_days = lookback_days

        try:
            target = datetime.strptime(date_str, '%Y-%m-%d')
            start_str = (target - timedelta(days=lookback_days)).strftime('%Y-%m-%d')
            
            with momentum_logs_lock:
                momentum_logs[task_id].append(f"[{datetime.now().strftime('%H:%M:%S')}] 配置参数: 目标日={date_str}, 取数区间={lookback_days}天, 计算窗口={calc_window_days}天")
            
            config = DataLoaderConfig(start_date=start_str, end_date=date_str, calc_window_days=calc_window_days)
            dl = DataLoader(config=config)
            
            # 处理状态分析配置
            status_config = None
            status_config_data = data.get('status_config')
            if status_config_data:
                from momentum_pipeline import StatusAnalyzerConfig
                status_config = StatusAnalyzerConfig(**status_config_data)
                with momentum_logs_lock:
                    momentum_logs[task_id].append(f"[{datetime.now().strftime('%H:%M:%S')}] 状态分析配置: MASS短期={status_config.mass_short_period}, MASS长期={status_config.mass_long_period}, 震荡回看={status_config.oscillation_lookback_days}天")
            
            pipeline = DailyMomentumPipeline(data_loader=dl, status_config=status_config)
            run_params = {'min_trading_days': min_trading_days, 'winsorize_alpha': winsorize_alpha}
            
            with momentum_logs_lock:
                momentum_logs[task_id].append(f"[{datetime.now().strftime('%H:%M:%S')}] 开始执行流水线...")
                momentum_logs[task_id].append(f"[{datetime.now().strftime('%H:%M:%S')}] 正在加载数据...")
            
            df = pipeline.run_daily_pipeline(date_str, params=run_params)
            
            if df is None or df.empty:
                with momentum_logs_lock:
                    momentum_logs[task_id].append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ 错误: {date_str} 未生成动量结果")
                # 延迟移除 handler
                def delayed_remove():
                    time.sleep(2)
                    momentum_logger.removeHandler(log_handler)
                    root_logger.removeHandler(log_handler)
                    if 'threading_logger_ref' in locals():
                        threading_logger_ref.removeHandler(log_handler)
                import threading
                threading.Thread(target=delayed_remove, daemon=True).start()
                return jsonify({
                    'success': False, 
                    'error': f'{date_str} 未生成动量结果，请检查数据源或 DataLoader 实现',
                    'task_id': task_id
                })
            
            with momentum_logs_lock:
                momentum_logs[task_id].append(f"[{datetime.now().strftime('%H:%M:%S')}] 流水线执行完成，共 {len(df)} 只股票")
                momentum_logs[task_id].append(f"[{datetime.now().strftime('%H:%M:%S')}] 开始保存到数据库，保存日期: {date_str}...")
            
            # 确保保存时使用正确的日期（date_str已经在前面处理过，确保使用数据库最新日期）
            count = save_momentum_scores_to_db(df, date_str)
            
            with momentum_logs_lock:
                momentum_logs[task_id].append(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ 成功保存 {count} 条记录到数据库")
                momentum_logs[task_id].append(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ 任务完成！")
            
            # 不要立即移除 handler，让前端有时间获取最后的日志
            # 延迟移除，给前端时间获取日志
            def delayed_remove():
                time.sleep(5)  # 等待5秒
                momentum_logger.removeHandler(log_handler)
                root_logger.removeHandler(log_handler)
                if 'threading_logger_ref' in locals():
                    threading_logger_ref.removeHandler(log_handler)
            
            import threading
            threading.Thread(target=delayed_remove, daemon=True).start()
            
            return jsonify({
                'success': True,
                'date': date_str,
                'count': count,
                'task_id': task_id
            })
        except Exception as e:
            with momentum_logs_lock:
                momentum_logs[task_id].append(f"[{datetime.now().strftime('%H:%M:%S')}] ❌ 执行失败: {str(e)}")
            # 延迟移除 handler
            def delayed_remove():
                time.sleep(2)
                momentum_logger.removeHandler(log_handler)
                root_logger.removeHandler(log_handler)
                if threading_logger:
                    threading_logger.removeHandler(log_handler)
            import threading
            threading.Thread(target=delayed_remove, daemon=True).start()
            logging.error(f'运行动量流水线失败: {e}', exc_info=True)
            return jsonify({
                'success': False, 
                'error': str(e),
                'task_id': task_id
            })
    except Exception as e:
        logging.error(f'运行动量流水线失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/momentum_scores/logs', methods=['GET'])
@login_required
def api_get_momentum_logs():
    """
    获取动量评分任务的执行日志
    - 参数：task_id（必需）、last_index（可选，上次获取的日志索引）
    """
    try:
        task_id = request.args.get('task_id')
        if not task_id:
            return jsonify({'success': False, 'error': '缺少 task_id 参数'})
        
        last_index = int(request.args.get('last_index', 0))
        logs, total_count = get_momentum_logs(task_id, last_index)
        
        return jsonify({
            'success': True,
            'logs': logs,
            'total_count': total_count,
            'last_index': total_count
        })
    except Exception as e:
        logging.error(f'获取动量评分日志失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/momentum_scores', methods=['GET'])
@login_required
def api_get_momentum_scores():
    """
    查询某日动量分列表：
    - 参数：date（可选，不填则取最新有数据的日期）、limit、offset
    """
    try:
        date_str = request.args.get('date')
        if not date_str:
            # 如果没有指定日期，使用数据库中最新的日期
            date_str = get_latest_momentum_date()
            if not date_str:
                return jsonify({'success': False, 'error': '暂无动量评分数据'})
        else:
            # 检测数据库格式并标准化日期
            db_format = get_db_date_format()
            if db_format == 'compact':
                date_str_query = normalize_date(date_str, target_format='compact')
            else:
                date_str_query = normalize_date(date_str, target_format='standard')
            
            # 如果指定了日期，验证该日期是否存在数据
            # 如果不存在，尝试使用最新日期
            conn = sqlite3.connect(DATABASE_FILE)
            c = conn.cursor()
            c.execute("SELECT COUNT(*) FROM momentum_scores WHERE date = ?", (date_str_query,))
            count = c.fetchone()[0] or 0
            conn.close()
            
            if count == 0:
                # 该日期没有数据，使用最新日期
                latest_date = get_latest_momentum_date()
                if latest_date:
                    date_str = latest_date
                else:
                    return jsonify({'success': False, 'error': f'日期 {date_str} 无数据，且数据库为空'})
            else:
                # 使用查询到的日期格式
                date_str = date_str_query
        
        try:
            limit = int(request.args.get('limit', 200))
            offset = int(request.args.get('offset', 0))
        except (TypeError, ValueError):
            limit, offset = 200, 0
        
        try:
            total, items = get_momentum_scores(date_str, limit=limit, offset=offset)
        except Exception as e:
            logging.error(f'[api_get_momentum_scores] 调用 get_momentum_scores 失败: {e}', exc_info=True)
            return jsonify({'success': False, 'error': f'查询失败: {str(e)}'})
        
        if items and len(items) > 0:
            
            # 使用json.dumps时，确保None值也被包含，并且处理NaN
            # 自定义default函数来处理NaN和Inf
            def json_default(obj):
                import math
                if isinstance(obj, float):
                    if math.isnan(obj):
                        return None
                    if math.isinf(obj):
                        return None
                return str(obj)
            
            # 检查status字段是否存在
            if 'status' not in items[0]:
                logging.warning(f'[api_get_momentum_scores] ⚠️ 第一条数据缺少 status 字段！')
                logging.warning(f'[api_get_momentum_scores] ⚠️ 第一条数据实际字段: {list(items[0].keys())}')
            else:
                logging.info(f'[api_get_momentum_scores] 第一条数据 status={items[0].get("status")}, status_start_date={items[0].get("status_start_date")}')
        else:
            logging.warning(f'[api_get_momentum_scores] 查询日期: {date_str}, 但返回0条记录, 总计: {total}')
        
        # 确保返回的items包含所有字段（即使为None）
        # Flask的jsonify会自动处理None值，但需要确保NaN被转换为None
        # 在返回前再次清理所有items中的NaN值
        import math
        def clean_item_for_json(item):
            cleaned = {}
            for key, val in item.items():
                if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
                    cleaned[key] = None
                else:
                    cleaned[key] = val
            return cleaned
        
        cleaned_items = [clean_item_for_json(item) for item in items]
        
        return jsonify({
            'success': True,
            'date': date_str,  # 返回实际使用的日期（可能是调整后的）
            'total': total,
            'items': cleaned_items  # Flask的jsonify会自动处理None值，不会过滤
        })
    except Exception as e:
        logging.error(f'查询动量评分失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/momentum_scores/config', methods=['GET'])
@login_required
def api_get_momentum_config():
    """返回动量评分参数的默认值，供前端「配置」弹窗使用。"""
    return jsonify({
        'success': True,
        'defaults': {
            'lookback_days': 600,
            'calc_window_days': 500,
            'min_trading_days': 300,
            'winsorize_alpha': 0.025,
        },
    })


@app.route('/api/momentum_scores/latest_trading_date', methods=['GET'])
@login_required
def api_get_latest_trading_date():
    """
    查询聚源数据库最新交易日，供前端「刷新交易日」按钮和运行动量打分使用。
    注意：这里查询的是聚源数据库（数据源）的最新交易日，而不是 SQLite 中已计算过的日期。
    """
    try:
        from data_fetcher import JuyuanDataFetcher
        fetcher = JuyuanDataFetcher(lazy_init_pool=True)
        latest_date = fetcher.get_latest_trading_date()
        latest_date_str = latest_date.strftime('%Y-%m-%d') if hasattr(latest_date, 'strftime') else str(latest_date)
        logging.info(f'聚源数据库最新交易日: {latest_date_str}')
        return jsonify({
            'success': True,
            'latest_trading_date': latest_date_str
        })
    except Exception as e:
        logging.error(f'获取最新交易日失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/momentum_scores/dates', methods=['GET'])
@login_required
def api_get_momentum_dates():
    """返回已有动量评分的日期列表（按时间倒序）"""
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute("SELECT DISTINCT date FROM momentum_scores ORDER BY date DESC")
        rows = c.fetchall()
        conn.close()
        
        dates = [r[0] for r in rows]
        return jsonify({'success': True, 'dates': dates})
    except Exception as e:
        logging.error(f'查询动量评分日期失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})


# ==================== 模拟仓分析功能 ====================
# 模拟仓分析页面
@app.route('/portfolio_analysis')
@login_required
def portfolio_analysis():
    return render_template('portfolio_analysis.html')

# API: 解析Excel表头
@app.route('/api/portfolio/parse_headers', methods=['POST'])
@login_required
def parse_excel_headers():
    """解析上传的Excel文件，返回表头信息"""
    try:
        if 'file' not in request.files:
            return jsonify({'success': False, 'error': '未上传文件'})
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'success': False, 'error': '文件名为空'})
        
        if not file.filename.endswith(('.xlsx', '.xls')):
            return jsonify({'success': False, 'error': '只支持Excel文件(.xlsx, .xls)'})
        
        import pandas as pd
        import io
        
        # 读取Excel文件
        file_content = file.read()
        df = pd.read_excel(io.BytesIO(file_content), nrows=0)  # 只读取表头
        
        headers = list(df.columns)
        
        return jsonify({
            'success': True,
            'headers': headers
        })
    except Exception as e:
        logging.error(f'解析Excel表头失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': f'解析失败: {str(e)}'})

# API: 保存字段映射配置
@app.route('/api/portfolio/save_mapping', methods=['POST'])
@login_required
def save_field_mapping():
    """保存字段映射配置"""
    try:
        data = request.get_json()
        mapping_name = data.get('mapping_name', 'default')
        is_default = data.get('is_default', False)
        mapping_config = data.get('mapping_config', {})
        
        if not mapping_config:
            return jsonify({'success': False, 'error': '映射配置不能为空'})
        
        # 验证必需字段
        required_fields = ['portfolio_name', 'date', 'stock_code', 'stock_name', 'stock_market', 
                          'position_quantity', 'position_value', 'buy_quantity', 'sell_quantity', 'avg_price']
        for field in required_fields:
            if field not in mapping_config or not mapping_config[field]:
                return jsonify({'success': False, 'error': f'缺少必需字段: {field}'})
        
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        
        # 如果设为默认，先取消其他默认配置
        if is_default:
            c.execute("UPDATE portfolio_field_mappings SET is_default = 0")
        
        # 检查是否已存在同名映射
        c.execute("SELECT id FROM portfolio_field_mappings WHERE mapping_name = ?", (mapping_name,))
        existing = c.fetchone()
        
        if existing:
            # 更新现有映射
            c.execute("""
                UPDATE portfolio_field_mappings 
                SET is_default = ?, mapping_config = ?, updated_at = CURRENT_TIMESTAMP
                WHERE mapping_name = ?
            """, (1 if is_default else 0, json.dumps(mapping_config), mapping_name))
        else:
            # 插入新映射
            c.execute("""
                INSERT INTO portfolio_field_mappings (mapping_name, is_default, mapping_config)
                VALUES (?, ?, ?)
            """, (mapping_name, 1 if is_default else 0, json.dumps(mapping_config)))
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': '映射配置保存成功'})
    except Exception as e:
        logging.error(f'保存字段映射失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': f'保存失败: {str(e)}'})

# API: 获取字段映射配置
@app.route('/api/portfolio/get_mappings', methods=['GET'])
@login_required
def get_field_mappings():
    """获取所有字段映射配置"""
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute("SELECT id, mapping_name, is_default, mapping_config FROM portfolio_field_mappings ORDER BY is_default DESC, created_at DESC")
        rows = c.fetchall()
        conn.close()
        
        mappings = []
        for row in rows:
            mappings.append({
                'id': row[0],
                'mapping_name': row[1],
                'is_default': bool(row[2]),
                'mapping_config': json.loads(row[3])
            })
        
        return jsonify({'success': True, 'mappings': mappings})
    except Exception as e:
        logging.error(f'获取字段映射失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# API: 导入Excel数据
@app.route('/api/portfolio/import_data', methods=['POST'])
@login_required
def import_portfolio_data():
    """导入Excel数据并保存到数据库"""
    try:
        if 'file' not in request.files:
            return jsonify({'success': False, 'error': '未上传文件'})
        
        file = request.files['file']
        mapping_id = request.form.get('mapping_id')
        
        if not mapping_id:
            return jsonify({'success': False, 'error': '请选择字段映射配置'})
        
        # 获取映射配置
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute("SELECT mapping_config FROM portfolio_field_mappings WHERE id = ?", (mapping_id,))
        row = c.fetchone()
        conn.close()
        
        if not row:
            return jsonify({'success': False, 'error': '映射配置不存在'})
        
        mapping_config = json.loads(row[0])
        
        # 读取Excel文件
        import pandas as pd
        import io
        
        file_content = file.read()
        df = pd.read_excel(io.BytesIO(file_content))
        
        # 根据映射配置提取数据
        required_fields = {
            'portfolio_name': mapping_config.get('portfolio_name'),
            'date': mapping_config.get('date'),
            'stock_code': mapping_config.get('stock_code'),
            'stock_name': mapping_config.get('stock_name'),
            'stock_market': mapping_config.get('stock_market'),
            'position_quantity': mapping_config.get('position_quantity'),
            'position_value': mapping_config.get('position_value'),
            'buy_quantity': mapping_config.get('buy_quantity'),
            'sell_quantity': mapping_config.get('sell_quantity'),
            'avg_price': mapping_config.get('avg_price')
        }
        
        # 验证所有必需字段都存在
        for field, excel_col in required_fields.items():
            if not excel_col or excel_col not in df.columns:
                return jsonify({'success': False, 'error': f'Excel中缺少字段: {excel_col} (映射到 {field})'})
        
        # 提取数据并转换
        extracted_data = []
        for _, row in df.iterrows():
            try:
                # 处理日期字段
                date_value = row[required_fields['date']]
                if pd.isna(date_value):
                    continue
                if isinstance(date_value, pd.Timestamp):
                    date_str = date_value.strftime('%Y-%m-%d')
                else:
                    date_str = str(date_value).strip()
                
                # 处理股票代码（确保6位）
                stock_code = str(row[required_fields['stock_code']]).strip()
                # 如果是浮点数格式（如603352.0），去掉小数点
                if '.' in stock_code:
                    stock_code = stock_code.split('.')[0]
                stock_code = stock_code.zfill(6)
                
                data_item = {
                    'portfolio_name': str(row[required_fields['portfolio_name']]).strip(),
                    'date': date_str,
                    'stock_code': stock_code,
                    'stock_name': str(row[required_fields['stock_name']]).strip() if pd.notna(row[required_fields['stock_name']]) else '',
                    'stock_market': str(row[required_fields['stock_market']]).strip() if pd.notna(row[required_fields['stock_market']]) else '',
                    'position_quantity': float(row[required_fields['position_quantity']]) if pd.notna(row[required_fields['position_quantity']]) else 0,
                    'position_value': float(row[required_fields['position_value']]) if pd.notna(row[required_fields['position_value']]) else 0,
                    'buy_quantity': float(row[required_fields['buy_quantity']]) if pd.notna(row[required_fields['buy_quantity']]) else 0,
                    'sell_quantity': float(row[required_fields['sell_quantity']]) if pd.notna(row[required_fields['sell_quantity']]) else 0,
                    'avg_price': float(row[required_fields['avg_price']]) if pd.notna(row[required_fields['avg_price']]) else 0
                }
                extracted_data.append(data_item)
            except Exception as e:
                logging.warning(f'处理数据行失败: {e}')
                continue
        
        if not extracted_data:
            return jsonify({'success': False, 'error': '未能提取到有效数据'})
        
        # 保存到数据库
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        
        inserted_count = 0
        updated_count = 0
        
        # 统计新增和更新数量
        inserted_count = 0
        updated_count = 0
        
        for item in extracted_data:
            try:
                # 先检查是否已存在（用于统计）
                c.execute("""
                    SELECT id FROM portfolio_data 
                    WHERE portfolio_name = ? AND date = ? AND stock_code = ?
                """, (item['portfolio_name'], item['date'], item['stock_code']))
                exists = c.fetchone()
                
                # 使用 INSERT OR REPLACE 实现高效的重复导入机制
                # 由于表有 UNIQUE(portfolio_name, date, stock_code) 约束，重复导入会自动更新
                c.execute("""
                    INSERT OR REPLACE INTO portfolio_data 
                    (portfolio_name, date, stock_code, stock_name, stock_market, 
                     position_quantity, position_value, buy_quantity, sell_quantity, avg_price)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    item['portfolio_name'], item['date'], item['stock_code'], item['stock_name'],
                    item['stock_market'], item['position_quantity'], item['position_value'],
                    item['buy_quantity'], item['sell_quantity'], item['avg_price']
                ))
                
                # 统计
                if exists:
                    updated_count += 1
                else:
                    inserted_count += 1
                    
            except Exception as e:
                logging.warning(f'插入/更新数据失败: {item.get("portfolio_name")}, {item.get("date")}, {item.get("stock_code")}: {e}')
                continue
        
        conn.commit()
        conn.close()
        
        return jsonify({
            'success': True,
            'message': f'导入成功: 新增 {inserted_count} 条，更新 {updated_count} 条',
            'count': inserted_count + updated_count,
            'inserted': inserted_count,
            'updated': updated_count
        })
    except Exception as e:
        logging.error(f'导入数据失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': f'导入失败: {str(e)}'})

# API: 预处理数据（获取行业信息）
@app.route('/api/portfolio/preprocess', methods=['POST'])
@login_required
def preprocess_portfolio_data():
    """预处理数据，为所有股票添加行业信息"""
    try:
        data = request.get_json()
        portfolio_names = data.get('portfolio_names', [])
        date_range = data.get('date_range', {})
        
        # 构建查询条件
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        
        query = "SELECT DISTINCT stock_code FROM portfolio_data WHERE 1=1"
        params = []
        
        if portfolio_names:
            placeholders = ','.join(['?'] * len(portfolio_names))
            query += f" AND portfolio_name IN ({placeholders})"
            params.extend(portfolio_names)
        
        if date_range.get('start_date'):
            query += " AND date >= ?"
            params.append(date_range['start_date'])
        
        if date_range.get('end_date'):
            query += " AND date <= ?"
            params.append(date_range['end_date'])
        
        c.execute(query, params)
        rows = c.fetchall()
        # 处理股票代码格式：转换为字符串并格式化为6位数字
        stock_codes = []
        for row in rows:
            code = str(row[0]).strip()
            # 如果是浮点数格式（如603352.0），去掉小数点
            if '.' in code:
                code = code.split('.')[0]
            # 确保是6位数字
            code = code.zfill(6)
            if code not in stock_codes:
                stock_codes.append(code)
        conn.close()
        
        if not stock_codes:
            return jsonify({'success': True, 'message': '没有需要处理的股票', 'processed': 0})
        
        logging.info(f'预处理: 找到 {len(stock_codes)} 只股票需要处理行业信息')
        logging.info(f'预处理: 股票代码示例: {stock_codes[:5]}')
        
        # 批量获取股票行业信息（从聚源数据库）
        from data_fetcher import JuyuanDataFetcher
        fetcher = JuyuanDataFetcher(lazy_init_pool=True)
        stock_info_dict = fetcher.batch_get_stock_basic_info(stock_codes)
        
        logging.info(f'预处理: 成功从聚源数据库获取 {len(stock_info_dict)} 只股票的行业信息')
        
        # 检查哪些股票没有获取到行业信息
        missing_codes = [code for code in stock_codes if code not in stock_info_dict]
        if missing_codes:
            logging.warning(f'预处理: {len(missing_codes)} 只股票未从聚源数据库获取到行业信息: {missing_codes[:10]}')
        
        # 更新数据库中的行业信息
        # 先构建基础WHERE条件（用于查找需要更新的记录）
        base_where_conditions = []
        base_where_params = []
        
        if portfolio_names:
            placeholders = ','.join(['?'] * len(portfolio_names))
            base_where_conditions.append(f"portfolio_name IN ({placeholders})")
            base_where_params.extend(portfolio_names)
        
        if date_range.get('start_date'):
            base_where_conditions.append("date >= ?")
            base_where_params.append(date_range['start_date'])
        
        if date_range.get('end_date'):
            base_where_conditions.append("date <= ?")
            base_where_params.append(date_range['end_date'])
        
        base_where_clause = " AND ".join(base_where_conditions) if base_where_conditions else "1=1"
        
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        
        updated_count = 0
        failed_count = 0
        not_found_stocks = []
        
        for stock_code in stock_codes:
            # 确保股票代码格式正确（6位字符串）
            normalized_code = str(stock_code).strip()
            if '.' in normalized_code:
                normalized_code = normalized_code.split('.')[0]
            normalized_code = normalized_code.zfill(6)
            
            # 尝试多种格式匹配
            info = stock_info_dict.get(normalized_code)
            if not info:
                # 尝试原始格式
                info = stock_info_dict.get(str(stock_code).strip())
            if not info:
                # 尝试去掉前导零
                try:
                    code_int = int(normalized_code)
                    info = stock_info_dict.get(str(code_int).zfill(6))
                except:
                    pass
            if info:
                # 获取行业名称，如果为空则设为NULL而不是空字符串
                first_industry = info.get('FirstIndustryName')
                second_industry = info.get('SecondIndustryName')
                third_industry = info.get('ThirdIndustryName')
                
                # 更新该股票代码的所有记录（在指定条件下）
                # 注意：需要同时匹配原始格式和标准化格式的股票代码
                # 使用CAST确保类型匹配，处理浮点数格式的股票代码
                try:
                    code_int = int(float(str(stock_code).replace('.0', '')))
                    where_clause = f"(stock_code = ? OR stock_code = ? OR CAST(stock_code AS TEXT) = ? OR CAST(CAST(stock_code AS REAL) AS INTEGER) = ?) AND {base_where_clause}"
                    where_params = [stock_code, normalized_code, str(stock_code).strip(), code_int] + base_where_params
                except:
                    where_clause = f"(stock_code = ? OR stock_code = ? OR CAST(stock_code AS TEXT) = ?) AND {base_where_clause}"
                    where_params = [stock_code, normalized_code, str(stock_code).strip()] + base_where_params
                
                try:
                    c.execute(f"""
                        UPDATE portfolio_data 
                        SET first_industry_name = ?, 
                            second_industry_name = ?, 
                            third_industry_name = ?
                        WHERE {where_clause}
                    """, (
                        first_industry if first_industry and first_industry.strip() else None,
                        second_industry if second_industry and second_industry.strip() else None,
                        third_industry if third_industry and third_industry.strip() else None
                    ) + tuple(where_params))
                    updated_count += c.rowcount
                except Exception as e:
                    logging.warning(f'更新股票 {stock_code} (标准化: {normalized_code}) 行业信息失败: {e}')
                    failed_count += 1
            else:
                failed_count += 1
                not_found_stocks.append(normalized_code)
                # 检查是否在stock_info_dict中但行业信息为空
                if normalized_code in stock_info_dict:
                    logging.warning(f'股票 {stock_code} (标准化: {normalized_code}) 在聚源数据库中但未匹配到')
                else:
                    logging.warning(f'股票 {stock_code} (标准化: {normalized_code}) 未在聚源数据库中找到')
        
        conn.commit()
        
        # 验证更新结果：检查是否有记录被更新（在关闭连接前检查）
        if updated_count == 0:
            # 检查是否有匹配的记录但没有被更新
            check_query = f"SELECT COUNT(*) FROM portfolio_data WHERE {base_where_clause}"
            c.execute(check_query, base_where_params)
            matching_records = c.fetchone()[0]
            conn.close()
            
            if matching_records > 0:
                return jsonify({
                    'success': False,
                    'error': f'找到 {matching_records} 条匹配的记录，但未能更新行业信息。可能是股票代码格式不匹配或数据库中没有对应的股票信息。未找到行业信息的股票代码: {", ".join(not_found_stocks[:10])}'
                })
            else:
                return jsonify({
                    'success': False,
                    'error': '没有找到匹配的记录。请检查选择的组合名称和日期范围是否正确。'
                })
        
        conn.close()
        
        message = f'预处理完成: 处理了 {len(stock_codes)} 只股票，成功更新 {updated_count} 条记录的行业信息'
        if failed_count > 0:
            message += f'，{failed_count} 只股票未找到行业信息'
        
        return jsonify({
            'success': True,
            'message': message,
            'processed': updated_count,
            'total_stocks': len(stock_codes),
            'failed_stocks': failed_count
        })
    except Exception as e:
        logging.error(f'预处理数据失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': f'预处理失败: {str(e)}'})

# API: 获取分析数据 - 持仓市值变化
@app.route('/api/portfolio/analysis/market_value', methods=['POST'])
@login_required
def get_market_value_analysis():
    """获取持仓市值变化数据"""
    try:
        data = request.get_json()
        portfolio_names = data.get('portfolio_names', [])
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        
        query = """
            SELECT portfolio_name, date, SUM(position_value) as total_value
            FROM portfolio_data
            WHERE 1=1
        """
        params = []
        
        if portfolio_names:
            placeholders = ','.join(['?'] * len(portfolio_names))
            query += f" AND portfolio_name IN ({placeholders})"
            params.extend(portfolio_names)
        
        if start_date:
            query += " AND date >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND date <= ?"
            params.append(end_date)
        
        query += " GROUP BY portfolio_name, date ORDER BY portfolio_name, date"
        
        c.execute(query, params)
        rows = c.fetchall()
        conn.close()
        
        # 组织数据
        result = {}
        for row in rows:
            portfolio_name = row[0]
            date = row[1]
            total_value = row[2] or 0
            
            if portfolio_name not in result:
                result[portfolio_name] = {'dates': [], 'values': []}
            
            result[portfolio_name]['dates'].append(date)
            result[portfolio_name]['values'].append(total_value)
        
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        logging.error(f'获取持仓市值分析失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# API: 获取分析数据 - 持仓股票变化
@app.route('/api/portfolio/analysis/stock_changes', methods=['POST'])
@login_required
def get_stock_changes_analysis():
    """获取持仓股票变化数据"""
    try:
        data = request.get_json()
        portfolio_names = data.get('portfolio_names', [])
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        
        query = """
            SELECT portfolio_name, date, COUNT(DISTINCT stock_code) as stock_count
            FROM portfolio_data
            WHERE position_quantity > 0
        """
        params = []
        
        if portfolio_names:
            placeholders = ','.join(['?'] * len(portfolio_names))
            query += f" AND portfolio_name IN ({placeholders})"
            params.extend(portfolio_names)
        
        if start_date:
            query += " AND date >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND date <= ?"
            params.append(end_date)
        
        query += " GROUP BY portfolio_name, date ORDER BY portfolio_name, date"
        
        c.execute(query, params)
        rows = c.fetchall()
        conn.close()
        
        result = {}
        for row in rows:
            portfolio_name = row[0]
            date = row[1]
            stock_count = row[2] or 0
            
            if portfolio_name not in result:
                result[portfolio_name] = {'dates': [], 'counts': []}
            
            result[portfolio_name]['dates'].append(date)
            result[portfolio_name]['counts'].append(stock_count)
        
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        logging.error(f'获取持仓股票变化分析失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# API: 获取分析数据 - 持仓行业变化
@app.route('/api/portfolio/analysis/industry_changes', methods=['POST'])
@login_required
def get_industry_changes_analysis():
    """获取持仓行业变化数据"""
    try:
        data = request.get_json()
        portfolio_names = data.get('portfolio_names', [])
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        industry_level = data.get('industry_level', 'third')  # first, second, third
        
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        
        industry_field = {
            'first': 'first_industry_name',
            'second': 'second_industry_name',
            'third': 'third_industry_name'
        }.get(industry_level, 'third_industry_name')
        
        query = f"""
            SELECT portfolio_name, date, {industry_field} as industry_name, 
                   SUM(position_value) as industry_value
            FROM portfolio_data
            WHERE position_quantity > 0 
            AND {industry_field} IS NOT NULL 
            AND {industry_field} != ''
            AND TRIM({industry_field}) != ''
        """
        params = []
        
        if portfolio_names:
            placeholders = ','.join(['?'] * len(portfolio_names))
            query += f" AND portfolio_name IN ({placeholders})"
            params.extend(portfolio_names)
        
        if start_date:
            query += " AND date >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND date <= ?"
            params.append(end_date)
        
        query += f" GROUP BY portfolio_name, date, {industry_field} ORDER BY portfolio_name, date, industry_value DESC"
        
        c.execute(query, params)
        rows = c.fetchall()
        conn.close()
        
        if not rows:
            # 检查是否有数据但没有行业信息
            check_query = """
                SELECT COUNT(*) FROM portfolio_data
                WHERE position_quantity > 0
            """
            check_params = []
            if portfolio_names:
                placeholders = ','.join(['?'] * len(portfolio_names))
                check_query += f" AND portfolio_name IN ({placeholders})"
                check_params.extend(portfolio_names)
            if start_date:
                check_query += " AND date >= ?"
                check_params.append(start_date)
            if end_date:
                check_query += " AND date <= ?"
                check_params.append(end_date)
            
            # 检查没有行业信息的记录数
            no_industry_query = check_query + f" AND ({industry_field} IS NULL OR {industry_field} = '' OR TRIM({industry_field}) = '')"
            
            conn = sqlite3.connect(DATABASE_FILE)
            c = conn.cursor()
            c.execute(check_query, check_params)
            total_count = c.fetchone()[0]
            
            c.execute(no_industry_query, check_params)
            no_industry_count = c.fetchone()[0]
            conn.close()
            
            if total_count > 0:
                if no_industry_count > 0:
                    return jsonify({
                        'success': False, 
                        'error': f'找到 {total_count} 条持仓记录，但其中 {no_industry_count} 条没有行业信息。请先点击"预处理数据（获取行业信息）"按钮，并确保选择了正确的组合和日期范围。'
                    })
                else:
                    return jsonify({
                        'success': False,
                        'error': f'找到 {total_count} 条持仓记录，但没有符合查询条件的行业数据。请检查筛选条件。'
                    })
            else:
                return jsonify({
                    'success': False,
                    'error': '没有找到符合条件的持仓数据'
                })
        
        # 组织数据：按日期和组合分组
        result = {}
        for row in rows:
            portfolio_name = row[0]
            date = row[1]
            industry_name = row[2] or '未知行业'
            industry_value = row[3] or 0
            
            if portfolio_name not in result:
                result[portfolio_name] = {}
            
            if date not in result[portfolio_name]:
                result[portfolio_name][date] = {}
            
            result[portfolio_name][date][industry_name] = industry_value
        
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        logging.error(f'获取持仓行业变化分析失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# API: 按行业获取最新持仓股票明细（用于饼图悬停展示）
@app.route('/api/portfolio/analysis/industry_stocks', methods=['POST'])
@login_required
def get_industry_stock_details():
    """
    获取某一行业级别下、最新日期的持仓股票明细。
    用于前端在饼图悬停时展示该行业对应的股票列表。
    """
    try:
        data = request.get_json()
        portfolio_names = data.get('portfolio_names', [])
        date = data.get('date')  # 前端传入的最新日期
        industry_level = data.get('industry_level', 'third')  # first, second, third

        if not date:
            return jsonify({'success': False, 'error': '缺少日期参数'})

        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()

        industry_field = {
            'first': 'first_industry_name',
            'second': 'second_industry_name',
            'third': 'third_industry_name'
        }.get(industry_level, 'third_industry_name')

        query = f"""
            SELECT 
                {industry_field} AS industry_name,
                stock_code,
                stock_name,
                SUM(position_value) AS total_value
            FROM portfolio_data
            WHERE position_quantity > 0
              AND date = ?
              AND {industry_field} IS NOT NULL
              AND {industry_field} != ''
              AND TRIM({industry_field}) != ''
        """
        params = [date]

        if portfolio_names:
            placeholders = ','.join(['?'] * len(portfolio_names))
            query += f" AND portfolio_name IN ({placeholders})"
            params.extend(portfolio_names)

        query += f"""
            GROUP BY {industry_field}, stock_code, stock_name
            ORDER BY {industry_field}, total_value DESC
        """

        c.execute(query, params)
        rows = c.fetchall()
        conn.close()

        if not rows:
            return jsonify({'success': True, 'data': {}})

        # 组织数据：按行业分组
        result = {}
        for row in rows:
            industry_name = row[0] or '未知行业'
            stock_code = row[1] or ''
            stock_name = row[2] or ''
            total_value = row[3] or 0

            if industry_name not in result:
                result[industry_name] = []

            result[industry_name].append({
                'stock_code': stock_code,
                'stock_name': stock_name,
                'position_value': total_value
            })

        return jsonify({'success': True, 'data': result})
    except Exception as e:
        logging.error(f'获取行业股票明细失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# API: 获取分析数据 - 买入股票表现
@app.route('/api/portfolio/analysis/buy_performance', methods=['POST'])
@login_required
def get_buy_performance_analysis():
    """获取买入股票表现数据"""
    try:
        data = request.get_json()
        portfolio_names = data.get('portfolio_names', [])
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        
        query = """
            SELECT portfolio_name, stock_code, stock_name, date, 
                   buy_quantity, avg_price, position_value
            FROM portfolio_data
            WHERE buy_quantity > 0
        """
        params = []
        
        if portfolio_names:
            placeholders = ','.join(['?'] * len(portfolio_names))
            query += f" AND portfolio_name IN ({placeholders})"
            params.extend(portfolio_names)
        
        if start_date:
            query += " AND date >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND date <= ?"
            params.append(end_date)
        
        query += " ORDER BY portfolio_name, date, stock_code"
        
        c.execute(query, params)
        rows = c.fetchall()
        conn.close()
        
        # 组织数据：按组合和股票分组
        result = {}
        for row in rows:
            portfolio_name = row[0]
            stock_code = row[1]
            stock_name = row[2] or ''
            date = row[3]
            buy_quantity = row[4] or 0
            avg_price = row[5] or 0
            position_value = row[6] or 0
            
            if portfolio_name not in result:
                result[portfolio_name] = {}
            
            stock_key = f"{stock_code}_{stock_name}"
            if stock_key not in result[portfolio_name]:
                result[portfolio_name][stock_key] = {
                    'stock_code': stock_code,
                    'stock_name': stock_name,
                    'buy_records': []
                }
            
            result[portfolio_name][stock_key]['buy_records'].append({
                'date': date,
                'buy_quantity': buy_quantity,
                'avg_price': avg_price,
                'position_value': position_value
            })
        
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        logging.error(f'获取买入股票表现分析失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# API: 获取可用的组合名称列表
@app.route('/api/portfolio/portfolios', methods=['GET'])
@login_required
def get_portfolio_list():
    """获取所有模拟组合名称列表"""
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute("SELECT DISTINCT portfolio_name FROM portfolio_data ORDER BY portfolio_name")
        rows = c.fetchall()
        conn.close()
        
        portfolios = [row[0] for row in rows]
        return jsonify({'success': True, 'portfolios': portfolios})
    except Exception as e:
        logging.error(f'获取组合列表失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# API: 获取日期范围
@app.route('/api/portfolio/date_range', methods=['GET'])
@login_required
def get_portfolio_date_range():
    """获取数据的日期范围"""
    try:
        portfolio_name = request.args.get('portfolio_name')
        
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        
        if portfolio_name:
            c.execute("""
                SELECT MIN(date) as min_date, MAX(date) as max_date 
                FROM portfolio_data 
                WHERE portfolio_name = ?
            """, (portfolio_name,))
        else:
            c.execute("SELECT MIN(date) as min_date, MAX(date) as max_date FROM portfolio_data")
        
        row = c.fetchone()
        conn.close()
        
        if row and row[0]:
            return jsonify({
                'success': True,
                'min_date': row[0],
                'max_date': row[1]
            })
        else:
            return jsonify({'success': True, 'min_date': None, 'max_date': None})
    except Exception as e:
        logging.error(f'获取日期范围失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# API: 获取交易汇总数据（买入量、卖出量、净买量）
@app.route('/api/portfolio/analysis/trading_summary', methods=['POST'])
@login_required
def get_trading_summary():
    """获取交易汇总数据：按日期和组合汇总买入量、卖出量、净买量"""
    try:
        data = request.get_json()
        portfolio_names = data.get('portfolio_names', [])
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        
        query = """
            SELECT 
                portfolio_name,
                date,
                SUM(COALESCE(buy_quantity, 0)) as total_buy,
                SUM(COALESCE(sell_quantity, 0)) as total_sell,
                SUM(COALESCE(buy_quantity, 0) - COALESCE(sell_quantity, 0)) as net_buy
            FROM portfolio_data
            WHERE 1=1
        """
        params = []
        
        if portfolio_names:
            placeholders = ','.join(['?'] * len(portfolio_names))
            query += f" AND portfolio_name IN ({placeholders})"
            params.extend(portfolio_names)
        
        if start_date:
            query += " AND date >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND date <= ?"
            params.append(end_date)
        
        query += " GROUP BY portfolio_name, date ORDER BY portfolio_name, date"
        
        c.execute(query, params)
        rows = c.fetchall()
        conn.close()
        
        # 组织数据：按组合和日期分组
        result = {}
        for row in rows:
            portfolio_name = row[0]
            date = row[1]
            total_buy = row[2] or 0
            total_sell = row[3] or 0
            net_buy = row[4] or 0
            
            if portfolio_name not in result:
                result[portfolio_name] = []
            
            result[portfolio_name].append({
                'date': date,
                'buy_quantity': total_buy,
                'sell_quantity': total_sell,
                'net_buy': net_buy
            })
        
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        logging.error(f'获取交易汇总数据失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})

# API: 获取持仓股票特征指标（波动率等）
@app.route('/api/portfolio/analysis/stock_metrics', methods=['POST'])
@login_required
def get_stock_metrics():
    """获取持仓股票的特征指标：波动率、收益率等（高性能多线程版本）"""
    try:
        import pandas as pd
        import numpy as np
        from datetime import datetime, timedelta
        import concurrent.futures
        from high_performance_threading import HighPerformanceThreadPool
        from config import MAX_WORKERS
        
        data = request.get_json()
        portfolio_names = data.get('portfolio_names', [])
        start_date = data.get('start_date')
        end_date = data.get('end_date')
        
        # 获取持仓股票列表
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        
        query = """
            SELECT DISTINCT stock_code, stock_name
            FROM portfolio_data
            WHERE 1=1
        """
        params = []
        
        if portfolio_names:
            placeholders = ','.join(['?'] * len(portfolio_names))
            query += f" AND portfolio_name IN ({placeholders})"
            params.extend(portfolio_names)
        
        if start_date:
            query += " AND date >= ?"
            params.append(start_date)
        
        if end_date:
            query += " AND date <= ?"
            params.append(end_date)
        
        c.execute(query, params)
        stock_rows = c.fetchall()
        conn.close()
        
        if not stock_rows:
            return jsonify({'success': True, 'data': {}})
        
        # 从聚源数据库获取股票日线数据并计算指标
        from data_fetcher import JuyuanDataFetcher
        from juyuan_config import TABLE_CONFIG
        
        fetcher = JuyuanDataFetcher(lazy_init_pool=True)
        
        # 计算需要的历史数据范围（至少60天用于计算波动率）
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d') if end_date else datetime.now()
        start_date_obj = end_date_obj - timedelta(days=120)  # 获取120天数据用于计算
        data_start_date = start_date_obj.strftime('%Y-%m-%d')
        data_end_date = end_date if end_date else datetime.now().strftime('%Y-%m-%d')
        
        # 标准化股票代码并构建股票信息字典
        stock_info_dict = {}
        stock_codes_list = []
        for stock_code, stock_name in stock_rows:
            code = str(stock_code).strip()
            if '.' in code:
                code = code.split('.')[0]
            code = code.zfill(6)
            stock_info_dict[code] = stock_name
            stock_codes_list.append(code)
        
        # 高性能：批量获取所有股票数据（使用多线程）
        logging.info(f'开始批量获取 {len(stock_codes_list)} 只股票的日线数据...')
        all_stock_data = fetcher.batch_get_stock_data(stock_codes_list, days=120)
        logging.info(f'成功获取 {len(all_stock_data)} 只股票的数据')
        
        # 定义计算单只股票指标的函数
        def calculate_stock_metrics(code):
            """计算单只股票的指标"""
            try:
                stock_name = stock_info_dict.get(code, '')
                
                # 从批量获取的数据中获取该股票的数据
                stock_data = all_stock_data.get(code)
                if stock_data is None or stock_data.empty:
                    logging.warning(f'股票 {code} ({stock_name}) 未获取到日线数据')
                    return None
                
                # get_stock_data 返回的DataFrame中Date是索引，需要重置为列
                if stock_data.index.name == 'Date' or (hasattr(stock_data.index, 'names') and 'Date' in stock_data.index.names):
                    stock_data = stock_data.reset_index()
                
                # 确保Date列存在且是datetime类型
                if 'Date' not in stock_data.columns:
                    # 如果Date是索引，重置索引
                    if stock_data.index.name == 'Date' or (hasattr(stock_data.index, 'names') and 'Date' in stock_data.index.names):
                        stock_data = stock_data.reset_index()
                    else:
                        logging.warning(f'股票 {code} ({stock_name}) 数据中缺少Date列，可用列: {list(stock_data.columns)}')
                        return None
                
                # 确保日期在范围内
                stock_data['Date'] = pd.to_datetime(stock_data['Date'], errors='coerce')
                # 过滤掉无效日期
                stock_data = stock_data[stock_data['Date'].notna()]
                
                if len(stock_data) == 0:
                    logging.warning(f'股票 {code} ({stock_name}) 日期转换后无有效数据')
                    return None
                
                stock_data = stock_data[
                    (stock_data['Date'] >= data_start_date) & 
                    (stock_data['Date'] <= data_end_date)
                ].sort_values('Date')
                
                if len(stock_data) < 20:  # 至少需要20天数据
                    logging.warning(f'股票 {code} ({stock_name}) 数据不足20天，当前: {len(stock_data)} 天')
                    return None
                
                # 确保必要的列存在
                required_columns = ['Close', 'High', 'Low']
                missing_columns = [col for col in required_columns if col not in stock_data.columns]
                if missing_columns:
                    logging.warning(f'股票 {code} ({stock_name}) 数据中缺少列: {missing_columns}，可用列: {list(stock_data.columns)}')
                    return None
                
                # 计算收益率
                stock_data['return'] = stock_data['Close'].pct_change()
                
                # 计算指标
                returns = stock_data['return'].dropna()
                
                # 1. 波动率（20日）
                if len(returns) >= 20:
                    daily_volatility = returns.tail(20).std()
                    annualized_volatility = daily_volatility * np.sqrt(252)
                else:
                    daily_volatility = returns.std() if len(returns) > 1 else 0
                    annualized_volatility = daily_volatility * np.sqrt(252) if daily_volatility > 0 else 0
                
                # 2. 多周期收益率
                close_prices = stock_data['Close'].values
                returns_dict = {}
                for period in [5, 10, 20, 60]:
                    if len(close_prices) >= period + 1:
                        period_return = (close_prices[-1] / close_prices[-period-1]) - 1
                        returns_dict[f'return_{period}d'] = float(period_return) if not np.isnan(period_return) else None
                    else:
                        returns_dict[f'return_{period}d'] = None
                
                # 3. 振幅（20日、60日）
                high_prices = stock_data['High'].values
                low_prices = stock_data['Low'].values
                amplitude_dict = {}
                for period in [20, 60]:
                    if len(high_prices) >= period:
                        period_high = high_prices[-period:].max()
                        period_low = low_prices[-period:].min()
                        if period_low > 0:
                            amplitude = (period_high - period_low) / period_low
                            amplitude_dict[f'amplitude_{period}d'] = float(amplitude) if not np.isnan(amplitude) else None
                        else:
                            amplitude_dict[f'amplitude_{period}d'] = None
                    else:
                        amplitude_dict[f'amplitude_{period}d'] = None
                
                # 4. 最大单日涨幅（20日）
                if len(returns) >= 20:
                    max_gain_20d = float(returns.tail(20).max()) if len(returns.tail(20)) > 0 else None
                else:
                    max_gain_20d = float(returns.max()) if len(returns) > 0 else None
                
                # 5. 最大回撤（20日、60日）
                max_drawdown_dict = {}
                for period in [20, 60]:
                    if len(close_prices) >= period:
                        period_prices = close_prices[-period:]
                        # 计算累计收益率
                        period_returns = returns.tail(period) if len(returns) >= period else returns
                        if len(period_returns) > 0:
                            cumulative = (1 + period_returns).cumprod()
                            running_max = cumulative.expanding().max()
                            drawdown = (cumulative - running_max) / running_max
                            max_drawdown = float(drawdown.min()) if len(drawdown) > 0 and not np.isnan(drawdown.min()) else None
                            max_drawdown_dict[f'max_drawdown_{period}d'] = max_drawdown
                        else:
                            max_drawdown_dict[f'max_drawdown_{period}d'] = None
                    else:
                        max_drawdown_dict[f'max_drawdown_{period}d'] = None
                
                return {
                    'stock_code': code,
                    'stock_name': stock_name or '',
                    'daily_volatility': float(daily_volatility) if not np.isnan(daily_volatility) else None,
                    'annualized_volatility': float(annualized_volatility) if not np.isnan(annualized_volatility) else None,
                    **returns_dict,
                    **amplitude_dict,
                    'max_gain_20d': max_gain_20d,
                    **max_drawdown_dict
                }
                
            except Exception as e:
                import traceback
                logging.warning(f'计算股票 {code} 指标失败: {e}')
                logging.debug(f'详细错误信息: {traceback.format_exc()}')
                return None
        
        # 使用高性能线程池并行计算所有股票的指标
        logging.info(f'开始并行计算 {len(stock_codes_list)} 只股票的指标...')
        thread_pool = HighPerformanceThreadPool(
            progress_desc="计算股票指标",
            enable_progress_bar=True,
            max_workers=MAX_WORKERS
        )
        
        # 并行执行计算任务
        results_list = thread_pool.execute_batch(
            tasks=stock_codes_list,
            task_function=calculate_stock_metrics
        )
        
        # 将结果转换为字典格式
        result = {}
        for res in results_list:
            if res is not None:
                code = res.get('stock_code')
                if code:
                    result[code] = res
        
        logging.info(f'成功计算 {len(result)} 只股票的指标')
        
        return jsonify({'success': True, 'data': result})
    except Exception as e:
        logging.error(f'获取股票特征指标失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})


# ==================== RBAC 管理后台 ====================

@app.route('/admin/rbac')
@login_required
def admin_rbac_page():
    # 给模板生成与当前应用一致的 API 根路径，避免写死 /api/rbac 在反代/子路径下 404（前端会显示 NOT FOUND）
    rbac_api_base = url_for('api_rbac_me', _external=False).rsplit('/', 1)[0]
    return render_template('admin_rbac.html', rbac_api_base=rbac_api_base)


@app.route('/api/rbac/me', methods=['GET'])
@login_required
def api_rbac_me():
    uname = session.get('username')
    codes = sorted(_rbac_get_user_permission_codes(uname))
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute(
            '''SELECT r.name, r.slug FROM roles r
               INNER JOIN user_roles ur ON ur.role_id = r.id
               INNER JOIN users u ON COALESCE(u.id, u.rowid) = ur.user_id
               WHERE lower(trim(u.username)) = lower(trim(?))''',
            ((uname or '').strip(),),
        )
        roles = [{'name': row[0], 'slug': row[1]} for row in c.fetchall()]
        conn.close()
    except Exception:
        roles = []
    return jsonify({'success': True, 'username': uname, 'permissions': codes, 'roles': roles})


@app.route('/api/rbac/permissions', methods=['GET'])
@login_required
def api_rbac_permissions_list():
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute('SELECT id, code, name, module, description FROM permissions ORDER BY module, code')
        rows = c.fetchall()
        conn.close()
        items = [
            {'id': r[0], 'code': r[1], 'name': r[2], 'module': r[3] or '', 'description': r[4] or ''}
            for r in rows
        ]
        return jsonify({'success': True, 'permissions': items})
    except Exception as e:
        logging.error(f'rbac permissions list: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/rbac/roles', methods=['GET'])
@login_required
def api_rbac_roles_list():
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute(
            'SELECT COALESCE(id, rowid) AS rid, name, slug, description, is_system FROM roles ORDER BY rid'
        )
        roles = []
        for r in c.fetchall():
            rid = int(r[0]) if r[0] is not None else None
            if rid is None:
                continue
            c.execute(
                '''SELECT p.code FROM permissions p
                   INNER JOIN role_permissions rp ON rp.permission_id = p.id
                   WHERE rp.role_id = ? ORDER BY p.code''',
                (rid,),
            )
            codes = [x[0] for x in c.fetchall()]
            roles.append({
                'id': rid, 'name': r[1], 'slug': r[2], 'description': r[3] or '',
                'is_system': int(r[4] or 0), 'permissions': codes,
            })
        conn.close()
        return jsonify({'success': True, 'roles': roles})
    except Exception as e:
        logging.error(f'rbac roles list: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/rbac/roles', methods=['POST'])
@login_required
def api_rbac_roles_create():
    body = request.get_json(silent=True) or {}
    name = (body.get('name') or '').strip()
    slug = (body.get('slug') or '').strip().lower().replace(' ', '_')
    desc = (body.get('description') or '').strip()
    if not name or not slug:
        return jsonify({'success': False, 'error': '名称与 slug 必填'}), 400
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute(
            'INSERT INTO roles (name, slug, description, is_system) VALUES (?,?,?,0)',
            (name, slug, desc),
        )
        conn.commit()
        rid = c.lastrowid
        conn.close()
        return jsonify({'success': True, 'id': rid, 'slug': slug})
    except sqlite3.IntegrityError:
        return jsonify({'success': False, 'error': 'slug 已存在'}), 400
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/rbac/roles/<int:role_id>', methods=['DELETE'])
@login_required
def api_rbac_roles_delete(role_id):
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute('SELECT is_system, slug FROM roles WHERE id=?', (role_id,))
        row = c.fetchone()
        if not row:
            conn.close()
            return jsonify({'success': False, 'error': '角色不存在'}), 404
        if int(row[0] or 0) == 1:
            conn.close()
            return jsonify({'success': False, 'error': '系统内置角色不可删除'}), 400
        c.execute('DELETE FROM role_permissions WHERE role_id=?', (role_id,))
        c.execute('DELETE FROM user_roles WHERE role_id=?', (role_id,))
        c.execute('DELETE FROM roles WHERE id=?', (role_id,))
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/rbac/roles/<int:role_id>/permissions', methods=['PUT'])
@login_required
def api_rbac_role_permissions_update(role_id):
    body = request.get_json(silent=True) or {}
    codes = body.get('codes')
    if not isinstance(codes, list):
        return jsonify({'success': False, 'error': 'codes 须为权限 code 数组'}), 400
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute(
            'SELECT slug, COALESCE(is_system, 0) FROM roles WHERE id=?',
            (role_id,),
        )
        rrow = c.fetchone()
        if not rrow:
            conn.close()
            return jsonify({'success': False, 'error': '角色不存在'}), 404
        is_system = int(rrow[1] or 0)
        if is_system == 1:
            conn.close()
            return jsonify(
                {'success': False, 'error': '系统内置角色的权限不允许在此处修改'},
            ), 400
        c.execute('DELETE FROM role_permissions WHERE role_id=?', (role_id,))
        for code in codes:
            if not isinstance(code, str):
                continue
            code = code.strip()
            if not code:
                continue
            c.execute('SELECT id FROM permissions WHERE code=?', (code,))
            pr = c.fetchone()
            if pr:
                c.execute(
                    'INSERT OR IGNORE INTO role_permissions (role_id, permission_id) VALUES (?,?)',
                    (role_id, pr[0]),
                )
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


def _rbac_users_with_admin_count(conn=None):
    """统计仍拥有 rbac:admin 权限的活跃用户；可传入同一 conn 以在事务内读取未提交变更。"""
    own_conn = conn is None
    if own_conn:
        conn = sqlite3.connect(DATABASE_FILE)
    c = conn.cursor()
    c.execute('PRAGMA table_info(users)')
    has_active = 'is_active' in {row[1] for row in c.fetchall()}
    if has_active:
        c.execute(
            '''SELECT COUNT(DISTINCT COALESCE(u.id, u.rowid)) FROM users u
               INNER JOIN user_roles ur ON ur.user_id = COALESCE(u.id, u.rowid)
               INNER JOIN role_permissions rp ON rp.role_id = ur.role_id
               INNER JOIN permissions p ON p.id = rp.permission_id
               WHERE p.code = ? AND COALESCE(u.is_active,1)=1''',
            ('rbac:admin',),
        )
    else:
        c.execute(
            '''SELECT COUNT(DISTINCT COALESCE(u.id, u.rowid)) FROM users u
               INNER JOIN user_roles ur ON ur.user_id = COALESCE(u.id, u.rowid)
               INNER JOIN role_permissions rp ON rp.role_id = ur.role_id
               INNER JOIN permissions p ON p.id = rp.permission_id
               WHERE p.code = ?''',
            ('rbac:admin',),
        )
    n = int((c.fetchone() or [0])[0] or 0)
    if own_conn:
        conn.close()
    return n


@app.route('/api/rbac/users', methods=['GET'])
@login_required
def api_rbac_users_list():
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute('PRAGMA table_info(users)')
        has_active = 'is_active' in {row[1] for row in c.fetchall()}
        if has_active:
            c.execute(
                'SELECT COALESCE(id, rowid) AS uid, username, COALESCE(is_active,1) FROM users ORDER BY uid'
            )
        else:
            c.execute('SELECT COALESCE(id, rowid) AS uid, username, 1 AS is_active FROM users ORDER BY uid')
        users = []
        for row in c.fetchall():
            uid = int(row[0]) if row[0] is not None else None
            if uid is None:
                continue
            c.execute(
                '''SELECT r.slug, r.name FROM roles r
                   INNER JOIN user_roles ur ON ur.role_id = r.id WHERE ur.user_id=?''',
                (uid,),
            )
            rrows = c.fetchall()
            users.append({
                'id': uid,
                'username': row[1],
                'is_active': int(row[2]),
                'roles': [{'slug': a[0], 'name': a[1]} for a in rrows],
            })
        conn.close()
        return jsonify({'success': True, 'users': users})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/rbac/users', methods=['POST'])
@login_required
def api_rbac_users_create():
    body = request.get_json(silent=True) or {}
    username = (body.get('username') or '').strip()
    password = body.get('password') or ''
    role_slugs = body.get('role_slugs') or ['member']
    if not username or len(password) < 8:
        return jsonify({'success': False, 'error': '用户名必填，密码至少 8 位'}), 400
    if query_db(
        'SELECT 1 FROM users WHERE lower(trim(username)) = lower(trim(?)) LIMIT 1',
        (username,),
        one=True,
    ):
        return jsonify({'success': False, 'error': '用户名已存在'}), 400
    ph = hash_password(password)
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute('INSERT INTO users (username, password) VALUES (?,?)', (username, ph))
        uid = c.lastrowid
        try:
            c.execute('UPDATE users SET is_active=1 WHERE id=?', (uid,))
        except sqlite3.OperationalError:
            pass
        _rbac_assign_role_slugs(uid, conn, c, role_slugs)
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'id': uid})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/rbac/users/<int:user_id>', methods=['PUT'])
@login_required
def api_rbac_users_update(user_id):
    body = request.get_json(silent=True) or {}
    self_id = _rbac_user_id_by_username(session.get('username'))
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute(
            'SELECT id, rowid, username FROM users WHERE COALESCE(id, rowid) = ?',
            (int(user_id),),
        )
        row = c.fetchone()
        if not row:
            conn.close()
            return jsonify({'success': False, 'error': '用户不存在'}), 404
        user_pk = int(user_id)
        admin_before = _rbac_users_with_admin_count(conn)
        if 'is_active' in body:
            val = 1 if body.get('is_active') in (True, 1, '1', 'true') else 0
            if val == 0 and user_pk == self_id:
                conn.close()
                return jsonify({'success': False, 'error': '不能禁用当前登录用户'}), 400
            c.execute('UPDATE users SET is_active=? WHERE COALESCE(id, rowid)=?', (val, user_pk))
        if body.get('password'):
            pwd = body.get('password')
            if len(str(pwd)) < 8:
                conn.rollback()
                conn.close()
                return jsonify({'success': False, 'error': '新密码至少 8 位'}), 400
            c.execute(
                'UPDATE users SET password=? WHERE COALESCE(id, rowid)=?',
                (hash_password(str(pwd)), user_pk),
            )
        if isinstance(body.get('role_slugs'), list):
            _rbac_assign_role_slugs(user_pk, conn, c, body.get('role_slugs') or [])
        admin_after = _rbac_users_with_admin_count(conn)
        if admin_before >= 1 and admin_after == 0:
            conn.rollback()
            conn.close()
            return jsonify(
                {
                    'success': False,
                    'error': '不能禁用或移除最后一位拥有 rbac:admin 权限的活跃用户',
                },
            ), 400
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/rbac/users/<int:user_id>', methods=['DELETE'])
@login_required
def api_rbac_users_delete(user_id):
    self_id = _rbac_user_id_by_username(session.get('username'))
    if user_id == self_id:
        return jsonify({'success': False, 'error': '不能删除当前登录用户'}), 400
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute('SELECT 1 FROM users WHERE COALESCE(id, rowid)=?', (int(user_id),))
        if not c.fetchone():
            conn.close()
            return jsonify({'success': False, 'error': '用户不存在'}), 404
        admin_before = _rbac_users_with_admin_count(conn)
        upk = int(user_id)
        c.execute('DELETE FROM user_roles WHERE user_id=?', (upk,))
        c.execute('DELETE FROM users WHERE COALESCE(id, rowid)=?', (upk,))
        admin_after = _rbac_users_with_admin_count(conn)
        if admin_before >= 1 and admin_after == 0:
            conn.rollback()
            conn.close()
            return jsonify(
                {
                    'success': False,
                    'error': '不能删除最后一位拥有 rbac:admin 权限的活跃用户',
                },
            ), 400
        conn.commit()
        conn.close()
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)})


# ==================== 爬虫看板 ====================
CRAWLER_DB_CONFIG = {
    'host': os.getenv('DB_HOST', '120.53.250.208'),
    'user': os.getenv('DB_USER', 'root'),
    'password': os.getenv('DB_PASSWORD', 'Qwer!1234567'),
    'database': os.getenv('DB_NAME', 'mydb'),
    'charset': os.getenv('DB_CHARSET', 'utf8mb4'),
}


def get_crawler_mysql_connection(dict_cursor=False):
    import pymysql
    cursor_class = pymysql.cursors.DictCursor if dict_cursor else pymysql.cursors.Cursor
    return pymysql.connect(
        host=CRAWLER_DB_CONFIG['host'],
        user=CRAWLER_DB_CONFIG['user'],
        password=CRAWLER_DB_CONFIG['password'],
        database=CRAWLER_DB_CONFIG['database'],
        charset=CRAWLER_DB_CONFIG['charset'],
        cursorclass=cursor_class,
        autocommit=True,
    )


def _crawler_news_effective_time_sql():
    """与入库侧「无发布时间」渠道一致：用抓取时间参与排序与区间筛选。

    爬虫看板 `/api/crawler_dashboard/latest` 使用 COUNT(*) OVER() 合并计数与分页，需 MySQL 8+。
    """
    return 'COALESCE(publish_time, crawl_time)'


@app.route('/crawler_dashboard')
@login_required
def crawler_dashboard():
    # 与 admin_rbac 一致：用 url_for 生成前缀，避免反代 SCRIPT_NAME 下写死 /api/... 导致 404
    crawler_api_base = url_for('api_crawler_dashboard_latest', _external=False).rsplit('/', 1)[0]
    return render_template('crawler_dashboard.html', crawler_api_base=crawler_api_base)


@app.route('/crawler_dashboard/wordcloud')
@login_required
def crawler_dashboard_wordcloud():
    return render_template('crawler_dashboard_wordcloud.html')


@app.route('/timeline')
@login_required
def timeline_page():
    """独立历史复盘时间轴页面。"""
    return render_template('timeline.html')


@app.route('/api/crawler_dashboard/overview')
@login_required
def api_crawler_dashboard_overview():
    try:
        conn = get_crawler_mysql_connection()
        c = conn.cursor()

        c.execute("SELECT COUNT(*) FROM news_items WHERE DATE(crawl_time)=CURDATE()")
        today_total = c.fetchone()[0] or 0

        c.execute("SELECT COUNT(*) FROM news_items WHERE LOWER(source) LIKE '%ai%'")
        ai_processed = c.fetchone()[0] or 0

        c.execute("SELECT MIN(crawl_time) FROM news_items")
        min_time_row = c.fetchone()
        min_time_val = min_time_row[0] if min_time_row else None

        db_runtime = '00:00:00'
        if min_time_val:
            try:
                start_dt = min_time_val if isinstance(min_time_val, datetime) else datetime.fromisoformat(str(min_time_val).replace(' ', 'T'))
                delta = datetime.now() - start_dt
                total_seconds = max(0, int(delta.total_seconds()))
                h = total_seconds // 3600
                m = (total_seconds % 3600) // 60
                s = total_seconds % 60
                db_runtime = f"{h:02d}:{m:02d}:{s:02d}"
            except Exception:
                pass

        c.close()
        conn.close()
        return jsonify({
            'success': True,
            'today_total': today_total,
            'ai_processed': ai_processed,
            'db_runtime': db_runtime
        })
    except Exception as e:
        logging.error(f'爬虫看板概览查询失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/crawler_dashboard/channel_summary')
@login_required
def api_crawler_dashboard_channel_summary():
    try:
        conn = get_crawler_mysql_connection(dict_cursor=True)
        c = conn.cursor()

        eff = _crawler_news_effective_time_sql()
        c.execute(f'''
            SELECT
              source,
              COUNT(*) AS message_count,
              MIN({eff}) AS min_publish_time,
              MAX({eff}) AS max_publish_time
            FROM news_items
            GROUP BY source
            ORDER BY message_count DESC, source ASC
        ''')
        rows = c.fetchall()

        c.execute(f'''
            SELECT
              COUNT(*) AS total_count,
              MIN({eff}) AS total_min_publish_time,
              MAX({eff}) AS total_max_publish_time
            FROM news_items
        ''')
        total_row = c.fetchone() or {}

        c.close()
        conn.close()

        channels = []
        for r in rows:
            channels.append({
                'source': r.get('source') or '-',
                'message_count': int(r.get('message_count') or 0),
                'min_publish_time': str(r.get('min_publish_time') or ''),
                'max_publish_time': str(r.get('max_publish_time') or '')
            })

        summary = {
            'total_count': int(total_row.get('total_count') or 0),
            'total_min_publish_time': str(total_row.get('total_min_publish_time') or ''),
            'total_max_publish_time': str(total_row.get('total_max_publish_time') or '')
        }

        return jsonify({'success': True, 'channels': channels, 'summary': summary})
    except Exception as e:
        logging.error(f'爬虫渠道汇总查询失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})


def _crawler_feed_state_revision(total, max_eff):
    """与列表排序一致的「条数 + 最新有效时间」指纹；不依赖自增 id（部分库无 id 列）。"""
    try:
        n = int(total or 0)
    except (TypeError, ValueError):
        n = 0
    if max_eff is None:
        s = ''
    else:
        s = str(max_eff).strip()
    return '%s|%s' % (n, s)


def _build_crawler_where(source, keyword, start_time, end_time, has_link):
    clauses = []
    params = []

    if source:
        clauses.append("source = %s")
        params.append(source)

    if keyword:
        clauses.append("(title LIKE %s OR url LIKE %s)")
        kw = f"%{keyword}%"
        params.extend([kw, kw])

    eff = _crawler_news_effective_time_sql()
    if start_time:
        clauses.append(f"{eff} >= %s")
        params.append(start_time)

    if end_time:
        clauses.append(f"{eff} <= %s")
        params.append(end_time)

    if has_link == 'yes':
        clauses.append("url IS NOT NULL AND url <> ''")
    elif has_link == 'no':
        clauses.append("(url IS NULL OR url = '')")

    where_sql = " WHERE " + " AND ".join(clauses) if clauses else ""
    return where_sql, params


def _crawler_search_where_clauses(keyword, start_time, end_time, has_link=""):
    """时间窗 + 可选关键词；关键词支持英文逗号分隔，多词 OR 匹配标题/链接。"""
    where_sql, params = _build_crawler_where("", "", start_time, end_time, has_link)
    raw = (keyword or "").strip()
    if not raw:
        return where_sql, params
    parts = [p.strip() for p in raw.split(",") if p.strip()][:8]
    if not parts:
        return where_sql, params
    ors, extra = [], []
    for p in parts:
        ors.append("(title LIKE %s OR url LIKE %s)")
        k = f"%{p}%"
        extra.extend([k, k])
    kw_sql = "(" + " OR ".join(ors) + ")"
    if where_sql:
        return f"{where_sql} AND {kw_sql}", list(params) + extra
    return f" WHERE {kw_sql}", extra


# ---------------------------------------------------------------------------
# 爬虫标题词云 · 筛选管道（面向 A 股：宏观—微观、基本面—技术面都可能在标题里出现）
#
# L0 标题归一化：去电头、去「(记者…)」、去尾部来源，削弱「通稿形态」对分词的支配。
# L1 分词：优先 jieba.posseg；无 jieba 时用汉字块正则（噪声更大，依赖 L2–L5 兜底）。
# L2 词性与长度：有 jieba 时只保留名词族 n* + eng/j；限制 token 最大长度，避免整句被粘成一个「伪名词」。
# L2b 用户黑名单：登录用户在词云页点击词后加入 SQLite，此后分词结果再排除该词（仅本人数据）。
# L3 高频虚词与连接词：_CRAWLER_WORDCLOUD_STOP（稿件衔接、指代、泛化副词等）。
# L4 来源与工具：_CRAWLER_WORDCLOUD_MEDIA（通讯社、快讯源、企查查类；不把交易所/监管全称放这里）。
# L5 财经噪声分层：
#    · PHRASE_COMMON：数量单位、同比环比口径、财报套话、时间口径——entities / balanced 都过滤。
#    · PHRASE_ENTITIES_EXTRA：盘面与 K 线用语（涨停、连板、震荡、反弹等）——仅 entities 过滤；balanced 保留，兼顾技术面。
# L6 形态规则：纯数字、中文数字串、序数「第…」、金额/量级后缀、长词内含财报套话子串（预编译正则）。
#
# API 参数 focus：entities（默认，偏公司/政策/标的） | balanced（额外保留盘面词，适合宏观+技术一起看标题）。
# ---------------------------------------------------------------------------

_CRAWLER_WORDCLOUD_STOP = frozenset(
    '的 了 和 与 及 或 于 对 为 以 将 则 从 来 把 被 所 之 等 也 都 要 在 是 有 个 一 不 人 这 中 大 上 其 各 可 并 该 已 向 由 至 关于 通过 通知 公示 部门 国务院 会议 政策 意见 办法 规定 标准 有关 需要 根据 按照 相关 以及 进行 开展 工作 管理 服务 信息 社会 国家 地区 年月日 发布 表示 显示 据悉 消息 报道 记者 网讯 快讯 讯 公告 披露 数据 来源 编辑 作者 '
    '其中 此外 此前 目前 另外 同时 然而 因此 所以 但是 不过 由于 鉴于 据此 如若 与其 基于 对于 有关 综合 来看 而言 整体 方面 值得一提 整体来看 整体而言 '
    '据 称 指 系 拟 若 即 又 仍 尚 乃 既 亦 倘 纵使 纵然 即便 哪怕 无论 不管 尽管'.split()
)
# 通讯社/快讯源、数据工具（弱化「谁发的」；交易所/监管主体不放入此表，以免误伤政策类主题）
_CRAWLER_WORDCLOUD_MEDIA = frozenset('''
财联社 新华社 新华网 人民网 中新网 央视网 央视新闻 央视 澎湃新闻 界面新闻 经济观察报 新京报 南方日报 光明日报
证券时报 上海证券报 中国证券报 证券日报 第一财经 每经网 每日经济新闻 华尔街见闻 云财经 金十数据 快兰闪讯 中证网
企查查 天眼查 启信宝
'''.split())
# 数量、口径、涨跌套话、财报块、时间口径——两种 focus 都生效（不含「涨停」等盘面词，见下表）
_CRAWLER_WORDCLOUD_PHRASE_COMMON = frozenset('''
亿元 万元 千元 百万 千万 十万 百万美元 千万美元 亿美元 万欧元 日元 韩元 港元 港币 卢比 澳元 加元 瑞士法郎 美元 欧元 英镑
万股 亿股 亿份 万份 千股 手 万手 万张 万吨 万升 万桶 万千瓦 万千瓦时 兆瓦 吉瓦
万人次 亿人次 万户 万家 万个 余项 余个 个百分点 百分点 个基点 基差
同比增长 同比下降 同比上升 环比下降 环比上升 同比 环比 增速 增幅 降幅 涨幅 跌幅 涨超 跌超 涨逾 跌逾
收涨 收跌 走高 走低
扭亏为盈 亏损 盈利 预增 预减 略超 不及 符合 预计 有望 或将 约 逾 近 超 突破 刷新 创 新高 新低
一季度 二季度 三季度 四季度 上半年 下半年 第一 第二 第三 第四 季度 财年 报告期内 报告期
近日 日前 当日 昨日 今日 明日 本周 上周 下周 本月 上月 下月 今年 去年 明年 年内 年底 年初
截至 截止目前 截至目前 当地时间 北京时间
净利润 营业收入 每股收益 年净利润 年营业收入 含税 不含税 毛利 净利 营收 归母 扣非
归属于上市公司 上市公司股东 归属于上市公司股东 股东净利润 归属母公司 母公司净利润
派发现金红利 现金红利 现金分红 利润分配 权益分派 送转 每股派息
董事会 监事会 股东大会 临时股东大会 董事会会议 审议通过 审议并通过 会议决议 会议公告
第一季度 第二季度 第三季度 第四季度 一季度 二季度 三季度 四季度 半年度 年度报告 半年度报告 季报 中报 年报
消息面上 据报道 业内人士 有分析人士 消息人士 日电
与其同时 与此同时 值得一提 整体来看 整体而言
'''.split())
# 仅 focus=entities 时追加过滤（A 股标题里常见但偏「盘面语言」，balanced 下保留供技术面扫一眼）
_CRAWLER_WORDCLOUD_PHRASE_ENTITIES_EXTRA = frozenset(
    '涨停 跌停 连板 封板 开板 震荡 盘整 上行 下行 拉升 下挫 回调 反弹'.split()
)
_CRAWLER_WORDCLOUD_PHRASE_STOP = _CRAWLER_WORDCLOUD_PHRASE_COMMON | _CRAWLER_WORDCLOUD_PHRASE_ENTITIES_EXTRA

_CRAWLER_WORDCLOUD_MEASURE_SUFFIX = (
    '万元', '亿元', '万亿美元', '亿美元', '万美元', '亿欧元', '亿港元', '万英镑',
    '亿日元', '万日元', '万港元', '万欧元', '万澳元', '万加元', '万瑞郎', '万韩元', '万卢比',
    '万股', '亿股', '亿份', '万份', '万张', '万手', '万笔',
    '万吨', '万升', '万桶', '万千瓦', '万千瓦时', '亿千瓦时',
    '万人次', '亿人次', '个百分点', '百分点', '个基点',
    '元/吨', '元/股', '元/份', '美元/桶', '元/千瓦时',
)

_CRAWLER_WORDCLOUD_NUMERICISH = re.compile(
    r'^[\d０-９１２３４５６７８９,\.％%\-\+\~～]+(万|亿|千|百|多|余|约|逾)?[元吨股张份桶辆]?$'
)
_CRAWLER_WORDCLOUD_CN_NUMERIC_ONLY = re.compile(
    r'^[零一二三四五六七八九十百千万亿两〇○]+$'
)
_CRAWLER_WORDCLOUD_ORDINAL = re.compile(r'^第[零一二三四五六七八九十百千万]{1,5}$')

_CRAWLER_WORDCLOUD_INNER_FORBID = (
    '同比增长', '同比下降', '营业收入', '归属于上市公司', '上市公司股东', '派发现金红利',
    '派发现金', '现金红利', '现金分红', '每股收益', '年净利润', '年营业收入', '一季度净利润',
    '净利润同比', '营收同比', '董事会审议', '审议通过', '消息面上', '据报道',
    '半年度报告', '年度报告', '董事会会议', '上市公司', '归属于上市公司股东',
    '公司董事会', '公司监事会', '利润分配方案', '权益分派实施',
)
_CRAWLER_WORDCLOUD_INNER_RE = re.compile(
    '|'.join(re.escape(s) for s in _CRAWLER_WORDCLOUD_INNER_FORBID)
)


def _crawler_wordcloud_phrase_lookup(focus):
    """entities：通用财报噪声 + 盘面词；balanced：仅通用财报噪声，保留涨停/连板等。"""
    if (focus or 'entities') == 'balanced':
        return _CRAWLER_WORDCLOUD_PHRASE_COMMON
    return _CRAWLER_WORDCLOUD_PHRASE_STOP


def _crawler_wordcloud_preprocess_title(title):
    """去掉电头、记者署名等，减轻通讯社与稿件套话对词云的支配。"""
    if not title:
        return ''
    t = title.strip()
    t = re.sub(r'^【[^\]]{0,48}】\s*', '', t)
    t = re.sub(r'\d{1,2}\s*月\s*\d{1,2}\s*日\s*电', ' ', t)
    t = re.sub(r'\d{1,2}\s*月\s*\d{1,2}\s*日\s*讯', ' ', t)
    t = re.sub(r'\d{4}\s*年\s*\d{1,2}\s*月\s*\d{1,2}\s*日\s*电', ' ', t)
    t = re.sub(r'\([^)]{0,120}记者[^)]{0,120}\)', ' ', t)
    t = re.sub(r'（[^）]{0,120}记者[^）]{0,120}）', ' ', t)
    t = re.sub(r'\s*[\|｜]\s*财联社\s*$', '', t, flags=re.I)
    t = re.sub(r'\s+', ' ', t)
    return t.strip()


def _crawler_wordcloud_inner_boilerplate(w):
    """长名词串里夹带财报套话子串（预编译 alternation，避免逐条 in）。"""
    if len(w) < 5:
        return False
    return _CRAWLER_WORDCLOUD_INNER_RE.search(w) is not None


def _crawler_wordcloud_should_drop_token(w, phrase_lookup, user_blacklist=None):
    """按 L2b→L3→L6 过滤；user_blacklist 为 frozenset（当前用户点击加入的黑名单）。"""
    if not w or len(w) < 2:
        return True
    if user_blacklist and w in user_blacklist:
        return True
    if w in _CRAWLER_WORDCLOUD_STOP or w in phrase_lookup:
        return True
    if w in _CRAWLER_WORDCLOUD_MEDIA:
        return True
    if w.isdigit():
        return True
    if _CRAWLER_WORDCLOUD_CN_NUMERIC_ONLY.match(w):
        return True
    if _CRAWLER_WORDCLOUD_ORDINAL.match(w):
        return True
    if _CRAWLER_WORDCLOUD_NUMERICISH.match(w):
        return True
    for suf in _CRAWLER_WORDCLOUD_MEASURE_SUFFIX:
        if w.endswith(suf):
            return True
    if _crawler_wordcloud_inner_boilerplate(w):
        return True
    # 纯数字 + 少量汉字单位
    digit_cnt = sum(1 for ch in w if ch.isdigit() or ch in '０１２３４５６７８９')
    if digit_cnt >= max(1, len(w) - 2) and len(w) <= 14:
        return True
    return False


def _crawler_wordcloud_pos_keep(flag):
    """仅保留名词性成分：机构、地名、专名、普通名物等（过滤 m/q/d/v/a 等）。"""
    if not flag:
        return False
    if flag.startswith('n'):
        return True
    return flag in ('eng', 'j')


def _parse_crawler_dashboard_datetime(val, end_of_day=False):
    """解析看板查询时间字符串。"""
    if val is None:
        return None
    s = str(val).strip().replace('T', ' ')
    if not s:
        return None
    try:
        if len(s) <= 10:
            dt = datetime.strptime(s[:10], '%Y-%m-%d')
            if end_of_day:
                return dt.replace(hour=23, minute=59, second=59)
            return dt.replace(hour=0, minute=0, second=0)
        if len(s) >= 19:
            return datetime.strptime(s[:19], '%Y-%m-%d %H:%M:%S')
        return datetime.strptime(s[:16], '%Y-%m-%d %H:%M')
    except Exception:
        return None


def _crawler_get_jieba():
    if not hasattr(_crawler_get_jieba, '_mod'):
        try:
            import jieba
            jieba.setLogLevel(40)
            _crawler_get_jieba._mod = jieba
        except Exception:
            _crawler_get_jieba._mod = None
    return _crawler_get_jieba._mod


def _crawler_wordcloud_blacklist_frozenset(username):
    """当前用户词云黑名单（SQLite database.db）。"""
    if not username:
        return frozenset()
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        _ensure_wordcloud_blacklist_table(c)
        c.execute(
            'SELECT word FROM crawler_wordcloud_blacklist WHERE username = ?',
            (username,),
        )
        rows = c.fetchall() or []
        conn.close()
        return frozenset(r[0] for r in rows if r and r[0])
    except Exception:
        return frozenset()


def _crawler_wordcloud_blacklist_word_normalize(word):
    w = (word or '').strip()
    if not w or len(w) < 2 or len(w) > 64:
        return None
    return w


def _ensure_wordcloud_blacklist_table(cursor):
    """与 init_db 中 DDL 一致；热更新代码后未重启时也能用黑名单接口。"""
    cursor.execute('''CREATE TABLE IF NOT EXISTS crawler_wordcloud_blacklist (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     username TEXT NOT NULL,
                     word TEXT NOT NULL,
                     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                     UNIQUE(username, word))''')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_cwb_username ON crawler_wordcloud_blacklist(username)')


def _crawler_title_tokenize(text, focus='entities', user_blacklist=None):
    """L0→L1→L2→L2b→L3… 见模块顶部说明。focus：entities | balanced"""
    text = _crawler_wordcloud_preprocess_title(text)
    if not text:
        return []
    focus = (focus or 'entities').strip().lower()
    if focus not in ('entities', 'balanced'):
        focus = 'entities'
    phrase_lookup = _crawler_wordcloud_phrase_lookup(focus)
    bl = user_blacklist if user_blacklist is not None else frozenset()
    _max_tok = 18
    j = _crawler_get_jieba()
    if j is not None:
        import jieba.posseg as pseg
        words = []
        for pair in pseg.cut(text):
            w = (pair.word or '').strip()
            if len(w) < 2 or len(w) > _max_tok:
                continue
            if not _crawler_wordcloud_pos_keep(pair.flag):
                continue
            if _crawler_wordcloud_should_drop_token(w, phrase_lookup, bl):
                continue
            words.append(w)
    else:
        raw = re.findall(r'[\u4e00-\u9fff]{2,8}', text)
        words = []
        for w in raw:
            if len(w) > _max_tok:
                continue
            if _crawler_wordcloud_should_drop_token(w, phrase_lookup, bl):
                continue
            words.append(w)
    return words


def _counter_to_wordcloud_series(cnt, max_words=120):
    top = cnt.most_common(max_words)
    return [{'name': k, 'value': int(v)} for k, v in top if v > 0]


def _crawler_eff_to_datetime(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    try:
        return datetime.fromisoformat(str(v).replace(' ', 'T')[:19])
    except Exception:
        return None


@app.route('/api/crawler_dashboard/wordcloud')
@login_required
def api_crawler_dashboard_wordcloud():
    """标题词云：按区间拉取标题分词。聚合 group=whole|day|interval；侧重 focus=entities|balanced（见模块顶 L0–L6 说明）。"""
    try:
        source = (request.args.get('source') or '').strip()
        has_link = (request.args.get('has_link') or '').strip()
        start_raw = (request.args.get('start_time') or '').strip()
        end_raw = (request.args.get('end_time') or '').strip()
        group = (request.args.get('group') or 'whole').strip().lower()
        if group not in ('whole', 'day', 'interval'):
            group = 'whole'
        try:
            interval_hours = int(request.args.get('interval_hours', '24'))
        except ValueError:
            interval_hours = 24
        interval_hours = max(1, min(interval_hours, 168))

        focus = (request.args.get('focus') or 'entities').strip().lower()
        if focus not in ('entities', 'balanced'):
            focus = 'entities'

        username = (session.get('username') or '').strip()
        user_bl = _crawler_wordcloud_blacklist_frozenset(username)

        now = datetime.now()
        end_dt = _parse_crawler_dashboard_datetime(end_raw, end_of_day=True) or now
        start_dt = _parse_crawler_dashboard_datetime(start_raw, end_of_day=False) or (end_dt - timedelta(days=7))
        if start_dt > end_dt:
            start_dt, end_dt = end_dt - timedelta(days=7), end_dt

        start_time = start_dt.strftime('%Y-%m-%d %H:%M:%S')
        end_time = end_dt.strftime('%Y-%m-%d %H:%M:%S')

        where_sql, params = _build_crawler_where(source, '', start_time, end_time, has_link)
        if where_sql:
            where_sql += " AND title IS NOT NULL AND TRIM(title) <> ''"
        else:
            where_sql = " WHERE title IS NOT NULL AND TRIM(title) <> ''"

        eff = _crawler_news_effective_time_sql()
        max_rows = 20000
        conn = get_crawler_mysql_connection(dict_cursor=True)
        c = conn.cursor()
        c.execute(
            f'SELECT title, {eff} AS eff_time FROM news_items{where_sql} ORDER BY {eff} ASC LIMIT %s',
            (*params, max_rows),
        )
        rows = c.fetchall() or []
        c.close()
        conn.close()

        if group == 'whole':
            buckets_map = {'__all__': []}
            for r in rows:
                buckets_map['__all__'].append(r.get('title') or '')
        elif group == 'day':
            buckets_map = defaultdict(list)
            for r in rows:
                eff_d = _crawler_eff_to_datetime(r.get('eff_time'))
                if eff_d is None:
                    continue
                key = eff_d.strftime('%Y-%m-%d')
                buckets_map[key].append(r.get('title') or '')
        else:
            buckets_map = defaultdict(list)
            step = max(3600, interval_hours * 3600)
            for r in rows:
                eff_d = _crawler_eff_to_datetime(r.get('eff_time'))
                if eff_d is None:
                    continue
                ts = int(eff_d.timestamp())
                slot = ts - (ts % step)
                key = str(slot)
                buckets_map[key].append(r.get('title') or '')

        bucket_list = []
        for key, titles in buckets_map.items():
            cnt = Counter()
            for t in titles:
                for w in _crawler_title_tokenize(t, focus=focus, user_blacklist=user_bl):
                    cnt[w] += 1
            if group == 'interval' and key.isdigit():
                slot_start = datetime.fromtimestamp(int(key))
                slot_end = slot_start + timedelta(seconds=step)
                label = f"{slot_start.strftime('%m-%d %H:%M')} ~ {slot_end.strftime('%m-%d %H:%M')}"
            elif group == 'day':
                label = key
            else:
                label = '整段区间合并'
            bucket_list.append({
                'key': key,
                'label': label,
                'article_count': len(titles),
                'words': _counter_to_wordcloud_series(cnt),
            })

        if group == 'interval':
            bucket_list.sort(key=lambda x: int(x['key']) if x['key'].isdigit() else 0)
        elif group == 'day':
            bucket_list.sort(key=lambda x: x['key'])
        else:
            bucket_list.sort(key=lambda x: x['key'])

        return jsonify({
            'success': True,
            'group': group,
            'focus': focus,
            'interval_hours': interval_hours if group == 'interval' else None,
            'start_time': start_time,
            'end_time': end_time,
            'source': source or None,
            'total_rows_scanned': len(rows),
            'buckets': bucket_list,
            'blacklist_count': len(user_bl),
        })
    except Exception as e:
        logging.error(f'爬虫词云统计失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/crawler_dashboard/wordcloud/blacklist', methods=['GET'])
@login_required
def api_crawler_wordcloud_blacklist_get():
    """列出当前用户词云黑名单；cloud 字段供词云图展示。"""
    username = (session.get('username') or '').strip()
    if not username:
        return jsonify({'success': False, 'error': '未登录'}), 401
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        _ensure_wordcloud_blacklist_table(c)
        c.execute(
            '''SELECT word, created_at FROM crawler_wordcloud_blacklist
               WHERE username = ? ORDER BY id DESC''',
            (username,),
        )
        rows = c.fetchall() or []
        conn.close()
        items = [{'word': r[0], 'created_at': str(r[1] or '')} for r in rows]
        n = len(items)
        cloud = [{'name': it['word'], 'value': max(8, (n - i) * 3)} for i, it in enumerate(items)]
        return jsonify({'success': True, 'items': items, 'cloud': cloud})
    except Exception as e:
        logging.error(f'读取词云黑名单失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/crawler_dashboard/wordcloud/blacklist', methods=['POST'])
@login_required
def api_crawler_wordcloud_blacklist_post():
    """加入黑名单（主词云点击后调用）。"""
    username = (session.get('username') or '').strip()
    if not username:
        return jsonify({'success': False, 'error': '未登录'}), 401
    body = request.get_json(silent=True) or {}
    word = body.get('word') if isinstance(body.get('word'), str) else None
    if word is None:
        word = (request.form.get('word') or '').strip()
    word = _crawler_wordcloud_blacklist_word_normalize(word)
    if not word:
        return jsonify({'success': False, 'error': '词条需 2～64 个字符'}), 400
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        _ensure_wordcloud_blacklist_table(c)
        c.execute(
            'INSERT OR IGNORE INTO crawler_wordcloud_blacklist (username, word) VALUES (?, ?)',
            (username, word),
        )
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'word': word})
    except Exception as e:
        logging.error(f'写入词云黑名单失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/crawler_dashboard/wordcloud/blacklist', methods=['DELETE'])
@login_required
def api_crawler_wordcloud_blacklist_delete():
    """从黑名单移除；query: word=词条"""
    username = (session.get('username') or '').strip()
    if not username:
        return jsonify({'success': False, 'error': '未登录'}), 401
    word = _crawler_wordcloud_blacklist_word_normalize(request.args.get('word'))
    if not word:
        return jsonify({'success': False, 'error': '缺少或无效参数 word'}), 400
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        _ensure_wordcloud_blacklist_table(c)
        c.execute(
            'DELETE FROM crawler_wordcloud_blacklist WHERE username = ? AND word = ?',
            (username, word),
        )
        conn.commit()
        deleted = c.rowcount
        conn.close()
        return jsonify({'success': True, 'deleted': int(deleted), 'word': word})
    except Exception as e:
        logging.error(f'删除词云黑名单失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/crawler_dashboard/latest')
@login_required
def api_crawler_dashboard_latest():
    try:
        limit = int(request.args.get('limit', 10))
        offset = int(request.args.get('offset', 0))
        source = (request.args.get('source') or '').strip()
        keyword = (request.args.get('q') or '').strip()
        start_time = (request.args.get('start_time') or '').strip()
        end_time = (request.args.get('end_time') or '').strip()
        has_link = (request.args.get('has_link') or '').strip()
        limit = max(1, min(limit, 50))
        offset = max(0, offset)

        where_sql, params = _build_crawler_where(source, keyword, start_time, end_time, has_link)

        conn = get_crawler_mysql_connection(dict_cursor=True)
        c = conn.cursor()

        eff = _crawler_news_effective_time_sql()
        # MySQL 8+：COUNT(*) OVER() 与分页合并为一次往返
        c.execute(f'''
            SELECT source, title, url, publish_time, crawl_time, _row_total
            FROM (
                SELECT source, title, url, publish_time, crawl_time,
                       COUNT(*) OVER() AS _row_total
                FROM news_items
                {where_sql}
            ) t
            ORDER BY {eff} DESC, crawl_time DESC
            LIMIT %s OFFSET %s
        ''', (*params, limit, offset))
        rows = c.fetchall()
        if rows:
            total = int(rows[0].get('_row_total') or 0)
        else:
            c.execute(f"SELECT COUNT(*) AS total FROM news_items{where_sql}", params)
            total = int((c.fetchone() or {}).get('total') or 0)
        c.close()
        conn.close()

        items = []
        for r in rows:
            title = (r.get('title') or '').strip().replace('\n', ' ')
            if len(title) > 120:
                title = title[:120] + '...'
            pub = r.get('publish_time')
            cr = r.get('crawl_time')
            pub_str = str(pub or '').strip()
            cr_str = str(cr or '').strip()
            items.append({
                'source': r.get('source') or '-',
                'title': title or '(空标题)',
                'url': r.get('url') or '',
                'publish_time': str(pub or ''),
                'crawl_time': str(cr or ''),
                # 无发布时间时与其它渠道一致：展示层用抓取时间作为「发布时间」
                'publish_time_display': pub_str if pub_str else cr_str,
            })

        has_more = (offset + limit) < total
        return jsonify({
            'success': True,
            'items': items,
            'has_more': has_more,
            'total': total,
            'source': source,
            'q': keyword,
            'start_time': start_time,
            'end_time': end_time,
            'has_link': has_link
        })
    except Exception as e:
        logging.error(f'爬虫最新数据查询失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/crawler_dashboard/feed_state', methods=['GET'])
@login_required
def api_crawler_dashboard_feed_state():
    """
    轻量指纹：与 latest 同筛选、同有效时间口径（COALESCE(publish_time, crawl_time)）。
    返回 filter_revision / global_revision（条数|max_eff），不依赖 news_items.id（部分库无该列）。
    """
    try:
        source = (request.args.get('source') or '').strip()
        keyword = (request.args.get('q') or '').strip()
        start_time = (request.args.get('start_time') or '').strip()
        end_time = (request.args.get('end_time') or '').strip()
        has_link = (request.args.get('has_link') or '').strip()
        where_sql, params = _build_crawler_where(source, keyword, start_time, end_time, has_link)
        eff = _crawler_news_effective_time_sql()
        conn = get_crawler_mysql_connection(dict_cursor=True)
        c = conn.cursor()
        c.execute(
            (
                "SELECT COUNT(*) AS total, COALESCE(MAX({eff}), '1970-01-01 00:00:00') AS max_eff "
                'FROM news_items{where}'
            ).format(eff=eff, where=where_sql),
            params,
        )
        fr = c.fetchone() or {}
        c.execute(
            (
                "SELECT COUNT(*) AS total, COALESCE(MAX({eff}), '1970-01-01 00:00:00') AS max_eff "
                'FROM news_items'
            ).format(eff=eff),
        )
        gr = c.fetchone() or {}
        c.close()
        conn.close()
        filter_revision = _crawler_feed_state_revision(fr.get('total'), fr.get('max_eff'))
        global_revision = _crawler_feed_state_revision(gr.get('total'), gr.get('max_eff'))
        return jsonify(
            {
                'success': True,
                'filter_revision': filter_revision,
                'global_revision': global_revision,
                'filter_total': int(fr.get('total') or 0),
                'global_total': int(gr.get('total') or 0),
            }
        )
    except Exception as e:
        logging.error('feed_state: %s', e, exc_info=True)
        return jsonify({'success': False, 'error': str(e)})


def _timeline_normalize_hhmm(s):
    s = (s or '').strip()
    if not s:
        return ''
    m = re.match(r'^(\d{1,2})[:：](\d{1,2})', s)
    if not m:
        return s
    try:
        h = max(0, min(23, int(m.group(1))))
        mm = max(0, min(59, int(m.group(2))))
        return f'{h:02d}:{mm:02d}'
    except (TypeError, ValueError):
        return s


@app.route('/api/timeline/add', methods=['POST'])
@login_required
def api_timeline_add():
    try:
        payload = request.get_json(silent=True) or {}
        date_str = (payload.get('date') or '').strip()
        if not date_str:
            date_str = datetime.now().strftime('%Y-%m-%d')
        time_str = (payload.get('time') or '').strip()
        if not time_str:
            time_str = datetime.now().strftime('%H:%M')
        else:
            time_str = _timeline_normalize_hhmm(time_str)
        title = (payload.get('title') or '').strip()
        if not title:
            return jsonify({'success': False, 'error': '标题不能为空'}), 400
        url = (payload.get('url') or '').strip() or None
        remark = (payload.get('remark') or '').strip() or None

        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute(
            '''INSERT INTO market_timeline (date, time, title, url, remark)
               VALUES (?, ?, ?, ?, ?)''',
            (date_str, time_str, title, url, remark),
        )
        new_id = c.lastrowid
        conn.commit()
        conn.close()
        return jsonify({'success': True, 'id': new_id, 'date': date_str})
    except Exception as e:
        logging.error('timeline add: %s', e, exc_info=True)
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/timeline/list', methods=['GET'])
@login_required
def api_timeline_list():
    try:
        date_raw = (request.args.get('date') or '').strip()
        if not date_raw:
            date_raw = datetime.now().strftime('%Y-%m-%d')
        conn = sqlite3.connect(DATABASE_FILE)
        conn.row_factory = sqlite3.Row
        c = conn.cursor()
        c.execute(
            '''SELECT id, date, time, title, url, remark, created_at
               FROM market_timeline WHERE date = ?
               ORDER BY time ASC, id ASC''',
            (date_raw,),
        )
        rows = c.fetchall()
        conn.close()
        items = [dict(r) for r in rows]
        return jsonify({'success': True, 'date': date_raw, 'items': items})
    except Exception as e:
        logging.error('timeline list: %s', e, exc_info=True)
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/timeline/delete/<int:entry_id>', methods=['DELETE'])
@login_required
def api_timeline_delete(entry_id):
    try:
        conn = sqlite3.connect(DATABASE_FILE)
        c = conn.cursor()
        c.execute('DELETE FROM market_timeline WHERE id = ?', (entry_id,))
        deleted = c.rowcount
        conn.commit()
        conn.close()
        if not deleted:
            return jsonify({'success': False, 'error': '记录不存在'}), 404
        return jsonify({'success': True})
    except Exception as e:
        logging.error('timeline delete: %s', e, exc_info=True)
        return jsonify({'success': False, 'error': str(e)})


@app.route('/api/crawler/search', methods=['GET'])
@login_required
def api_crawler_search():
    """日历/计划侧栏与「计划关键词」用：轻量资讯搜索（MySQL news_items）。"""
    try:
        keyword = (request.args.get('keyword') or request.args.get('q') or '').strip()
        limit = int(request.args.get('limit', 10))
        limit = max(1, min(limit, 30))
        days = int(request.args.get('days', 30))
        days = max(1, min(days, 365))
        has_link = (request.args.get('has_link') or '').strip()

        end_dt = datetime.now()
        start_dt = end_dt - timedelta(days=days)
        start_time = start_dt.strftime('%Y-%m-%d %H:%M:%S')
        end_time = end_dt.strftime('%Y-%m-%d %H:%M:%S')
        where_sql, params = _crawler_search_where_clauses(
            keyword, start_time, end_time, has_link
        )
        conn = get_crawler_mysql_connection(dict_cursor=True)
        c = conn.cursor()
        eff = _crawler_news_effective_time_sql()
        c.execute(
            f"""
            SELECT source, title, url, publish_time, crawl_time
            FROM news_items
            {where_sql}
            ORDER BY {eff} DESC, crawl_time DESC
            LIMIT %s
            """,
            (*params, limit),
        )
        rows = c.fetchall()
        c.close()
        conn.close()
        items = []
        for r in rows:
            title = (r.get('title') or '').strip().replace('\n', ' ')
            if len(title) > 120:
                title = title[:120] + '...'
            pub = r.get('publish_time')
            cr = r.get('crawl_time')
            pub_str = str(pub or '').strip()
            cr_str = str(cr or '').strip()
            items.append(
                {
                    'source': r.get('source') or '-',
                    'title': title or '(空标题)',
                    'url': r.get('url') or '',
                    'publish_time': str(pub or ''),
                    'crawl_time': str(cr or ''),
                    'publish_time_display': pub_str if pub_str else cr_str,
                }
            )
        return jsonify(
            {
                'success': True,
                'items': items,
                'keyword': keyword,
                'limit': limit,
            }
        )
    except Exception as e:
        logging.error('爬虫 /api/crawler/search 失败: %s', e, exc_info=True)
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/api/crawler_dashboard/export')
@login_required
def api_crawler_dashboard_export():
    try:
        from io import BytesIO
        import pandas as pd

        source = (request.args.get('source') or '').strip()
        keyword = (request.args.get('q') or '').strip()
        start_time = (request.args.get('start_time') or '').strip()
        end_time = (request.args.get('end_time') or '').strip()
        has_link = (request.args.get('has_link') or '').strip()
        export_name = (request.args.get('export_name') or '').strip()

        where_sql, params = _build_crawler_where(source, keyword, start_time, end_time, has_link)

        conn = get_crawler_mysql_connection(dict_cursor=True)
        c = conn.cursor()
        eff = _crawler_news_effective_time_sql()
        c.execute(f'''
            SELECT source, title, url, publish_time, crawl_time
            FROM news_items
            {where_sql}
            ORDER BY {eff} DESC, crawl_time DESC
        ''', params)
        rows = c.fetchall()
        c.close()
        conn.close()

        export_rows = []
        for r in rows or []:
            pub = r.get('publish_time')
            cr = r.get('crawl_time')
            export_rows.append({
                **r,
                'publish_time_display': pub if pub is not None and str(pub).strip() != '' else cr,
            })

        df = pd.DataFrame(export_rows)
        buffer = BytesIO()
        df.to_excel(buffer, index=False, engine='openpyxl')
        buffer.seek(0)

        safe_name = ''.join(ch if ch.isalnum() or ch in ('-', '_') else '_' for ch in export_name).strip('_')
        filename = f"{safe_name}.xlsx" if safe_name else f"crawler_news_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        return send_file(
            buffer,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        logging.error(f'爬虫数据导出失败: {e}', exc_info=True)
        return jsonify({'success': False, 'error': str(e)})


if __name__ == '__main__':
    init_db()
    # Windows 上 reloader 在文件变更重载时会关闭旧进程 socket，工作线程 select() 触发 OSError [WinError 10038]，故在 Windows 禁用
    use_reloader = (os.name != 'nt')
    app.run(host='0.0.0.0', port=5888, debug=True, use_reloader=use_reloader)