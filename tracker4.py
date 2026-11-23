#!/usr/bin/env python3
"""Hotspot Time Tracker: يحسب بدقة مدة اتصال كل جهاز بالهوتسبوت."""
import os, re, sys, html, time, sqlite3, socket, logging, threading, datetime
import subprocess
import concurrent.futures as cf

# ===================== الإعدادات =====================
BOT_TOKEN = os.environ.get("BOT_TOKEN", "PASTE_BOT_TOKEN_HERE")      # <-- التوكن هون
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID", "PASTE_CHAT_ID_HERE")  # <-- رقم الشات هون
DB_PATH = os.path.expanduser("~/hotspot.db")
SCAN_EVERY = 20          # فحص الأجهزة المسجلة كل كم ثانية
FULL_EVERY = 300         # فحص كامل للشبكة (لاكتشاف الغرباء)
MISS_LIMIT = 3           # عدد الفحوصات الفاشلة قبل اعتبار الجهاز فاصل
PING_TIMEOUT = 2
WORKERS = 8              # لا تزيده: أندرويد بيقتل Termux لو كثرت العمليات
SEC_PER_SHEKEL = 4 * 3600  # ٤ ساعات = شيكل
AUDIT_DAYS = 3
MIN_RESTART_GAP = 15     # أي إعادة تشغيل أطول من هيك بتتسجل كفجوة
# ======================================================

log = logging.getLogger("hotspot")


class Store:
    """طبقة SQLite. كل التعديلات بترجع بالـ commit، فالقتل المفاجئ ما بتلف البيانات."""

    def __init__(self, path):
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.lock = threading.RLock()
        with self.lock:
            self.db.executescript("""
            PRAGMA journal_mode=WAL;
            PRAGMA synchronous=NORMAL;
            CREATE TABLE IF NOT EXISTS devices(
                host INTEGER PRIMARY KEY, name TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS sessions(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                host INTEGER NOT NULL, start REAL NOT NULL,
                last_seen REAL NOT NULL, closed INTEGER NOT NULL DEFAULT 0);
            CREATE INDEX IF NOT EXISTS ix_sess ON sessions(last_seen);
            CREATE TABLE IF NOT EXISTS gaps(
                id INTEGER PRIMARY KEY AUTOINCREMENT, start REAL, end REAL);
            CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT);
            CREATE TABLE IF NOT EXISTS audit(ts REAL, host INTEGER, alive INTEGER);
            CREATE TABLE IF NOT EXISTS outbox(
                id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL,
                text TEXT, reg_host INTEGER);
            """)
            self.db.commit()
            try:
                self.db.execute("ALTER TABLE outbox ADD COLUMN "
                                "silent INTEGER NOT NULL DEFAULT 0")
                self.db.commit()
            except sqlite3.OperationalError:
                pass  # العمود موجود

    def q(self, sql, args=(), commit=True):
        with self.lock:
            cur = self.db.execute(sql, args)
            if commit:
                self.db.commit()
            return cur.lastrowid

    def many(self, sql, rows, commit=True):
        with self.lock:
            self.db.executemany(sql, rows)
            if commit:
                self.db.commit()

    def commit(self):
        with self.lock:
            self.db.commit()

    def all(self, sql, args=()):
        with self.lock:
            return self.db.execute(sql, args).fetchall()

    def get_meta(self, k, default=None):
        r = self.all("SELECT v FROM meta WHERE k=?", (k,))
        return r[0][0] if r else default

    def set_meta(self, k, v, commit=True):
        self.q("INSERT INTO meta(k,v) VALUES(?,?) "
               "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (k, str(v)), commit)

    def devices(self):
        return {h: n for h, n in self.all("SELECT host, name FROM devices")}

    def prune(self, now):
        self.q("DELETE FROM audit WHERE ts < ?", (now - AUDIT_DAYS * 86400,))
        self.q("DELETE FROM outbox WHERE ts < ?", (now - 2 * 86400,))

    def enqueue(self, ts, text, reg_host=None, silent=0):
        self.q("INSERT INTO outbox(ts,text,reg_host,silent) VALUES(?,?,?,?)",
               (ts, text, reg_host, silent))

    def pending(self, limit=20):
        return self.all("SELECT id, ts, text, reg_host, silent FROM outbox "
                        "ORDER BY id LIMIT ?", (limit,))

    def done(self, oid):
        self.q("DELETE FROM outbox WHERE id=?", (oid,))


class Tracker:
    """منطق الحساب فقط (بدون شبكة ولا تلجرام) عشان نقدر نختبره."""

    def __init__(self, store, miss_limit=MISS_LIMIT):
        self.s = store
        self.miss_limit = miss_limit
        self.open = {}  # host -> {"id": session_id, "miss": n}
        self.pending_gap = None
        if self.s.get_meta("reset_at") is None:
            self.s.set_meta("reset_at", time.time())

    def recover(self, now):
        """عند التشغيل: سكّر أي جلسة معلّقة، وسجّل فجوة لو المراقب كان متوقف."""
        hb = self.s.get_meta("heartbeat")
        self.s.q("UPDATE sessions SET closed=1 WHERE closed=0")
        self.open.clear()
        self.pending_gap = None
        if hb is not None and now - float(hb) > MIN_RESTART_GAP:
            self.pending_gap = self.s.q(
                "INSERT INTO gaps(start,end) VALUES(?,?)", (float(hb), now))
            return float(hb)
        return None

    def close_gap(self, now):
        """بنمدّ الفجوة لحد أول فحص ناجح بعد التشغيل (مش لحد لحظة التشغيل)."""
        if self.pending_gap is not None:
            self.s.q("UPDATE gaps SET end=? WHERE id=?", (now, self.pending_gap))
            self.pending_gap = None

    def _close(self, h):
        st = self.open.pop(h, None)
        if not st:
            return None
        row = self.s.all("SELECT start, last_seen FROM sessions WHERE id=?", (st["id"],))
        self.s.q("UPDATE sessions SET closed=1 WHERE id=?", (st["id"],), commit=False)
        return (row[0][1] - row[0][0]) if row else 0

    def close_all(self):
        ev = []
        for h in list(self.open):
            ev.append(("disconnected", h, self._close(h)))
        self.s.commit()
        return ev

    def process(self, now, results, known):
        """results: {host: (alive, timestamp)}. يرجع قائمة أحداث (نوع, host, معلومة)."""
        ev, audit = [], []
        for h, (alive, ts) in results.items():
            st = self.open.get(h)
            if alive:
                if st:
                    st["miss"] = 0
                    self.s.q("UPDATE sessions SET last_seen=? WHERE id=?",
                             (ts, st["id"]), commit=False)
                else:
                    sid = self.s.q(
                        "INSERT INTO sessions(host,start,last_seen) VALUES(?,?,?)",
                        (h, ts, ts), commit=False)
                    self.open[h] = {"id": sid, "miss": 0}
                    ev.append(("connected" if h in known else "stranger", h, None))
            elif st:
                st["miss"] += 1
                if st["miss"] >= self.miss_limit:
                    ev.append(("disconnected", h, self._close(h)))
            if alive or h in known or st:
                audit.append((ts, h, 1 if alive else 0))
        if audit:
            self.s.many("INSERT INTO audit(ts,host,alive) VALUES(?,?,?)",
                        audit, commit=False)
        self.s.set_meta("heartbeat", now, commit=False)
        self.s.commit()
        return ev

    def seconds(self, since, now, host=None):
        """مجموع ثواني كل جهاز منذ since (مقصوص بحدود الفترة)."""
        tot = {}
        sql = "SELECT host, start, last_seen FROM sessions WHERE last_seen > ?"
        args = (since,)
        if host is not None:
            sql += " AND host = ?"
            args = (since, host)
        for h, a, b in self.s.all(sql, args):
            lo, hi = max(a, since), min(b, now)
            if hi > lo:
                tot[h] = tot.get(h, 0) + hi - lo
        return tot

    def gap_seconds(self, since, now):
        t = 0
        for a, b in self.s.all("SELECT start, end FROM gaps WHERE end > ?", (since,)):
            lo, hi = max(a, since), min(b, now)
            if hi > lo:
                t += hi - lo
        return t

    def reset(self, now):
        self.s.set_meta("reset_at", now)

    def reset_user(self, host, now):
        self.s.set_meta(f"reset_{host}", now)

    def open_starts(self):
        """{host: وقت بداية الجلسة المفتوحة}."""
        ids = {st["id"]: h for h, st in list(self.open.items())}
        if not ids:
            return {}
        q = ",".join("?" * len(ids))
        rows = self.s.all(f"SELECT id, start FROM sessions WHERE id IN ({q})",
                          tuple(ids))
        return {ids[i]: a for i, a in rows}


# ===================== التقارير =====================
def fmt(sec):
    m = int(sec // 60)
    if m < 1:
        return "أقل من دقيقة" if sec > 0 else "0 دقيقة"
    h, m = divmod(m, 60)
    if h and m:
        return f"{h} ساعة و {m} دقيقة"
    return f"{h} ساعة" if h else f"{m} دقيقة"


def fmt_short(sec):
    m = int(sec // 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}س {m}د"
    return f"{m}د" if m else "أقل من دقيقة"


def since_for(tr, h):
    """بداية حساب المستخدم: تصفيره الخاص أو التصفير العام."""
    g = float(tr.s.get_meta("reset_at"))
    v = tr.s.get_meta(f"reset_{h}")
    return max(float(v), g) if v is not None else g


def user_seconds(tr, h, now):
    return tr.seconds(since_for(tr, h), now, host=h).get(h, 0)


def user_cost(tr, h, now):
    return user_seconds(tr, h, now) / SEC_PER_SHEKEL


LINE = "━━━━━━━━━━━━"


def build_user_report(tr, now, h):
    name = tr.s.devices().get(h, f"غير مسجل .{h}")
    sec = user_seconds(tr, h, now)
    msg = (f"📊 <b>{html.escape(name)}</b>\n{LINE}\n"
           f"⏱ المدة: <b>{fmt(sec)}</b>\n"
           f"💰 المبلغ: <b>{sec / SEC_PER_SHEKEL:.2f} شيكل</b>")
    gap = tr.gap_seconds(since_for(tr, h), now)
    if gap > 60:
        msg += f"\n\n⚠️ المراقب كان متوقفاً {fmt(gap)} (هاد الوقت غير محسوب)."
    return msg


def build_all_report(tr, now):
    known = tr.s.devices()
    if not known:
        return "📋 <b>تقرير الجميع</b>\nما في أجهزة مسجلة بعد."
    msg, total = f"📋 <b>تقرير الجميع</b>\n{LINE}\n", 0.0
    for h, name in sorted(known.items(), key=lambda x: x[1]):
        sec = user_seconds(tr, h, now)
        total += sec / SEC_PER_SHEKEL
        msg += (f"👤 <b>{html.escape(name)}</b>\n"
                f"   ⏱ {fmt(sec)}  ·  💰 {sec / SEC_PER_SHEKEL:.2f} شيكل\n")
    msg += f"{LINE}\n💰 الإجمالي: <b>{total:.2f} شيكل</b>"
    gap = tr.gap_seconds(min(since_for(tr, h) for h in known), now)
    if gap > 60:
        msg += f"\n\n⚠️ المراقب كان متوقفاً {fmt(gap)} (هاد الوقت غير محسوب)."
    return msg


def build_online(tr, now):
    known = tr.s.devices()
    starts = tr.open_starts()
    stamp = datetime.datetime.fromtimestamp(now).strftime("%I:%M %p")
    msg = f"👥 <b>المتصلين الآن</b> · {stamp}\n{LINE}\n"
    if not starts:
        return msg + "لا يوجد أحد متصل."
    for h, st in sorted(starts.items()):
        name = known.get(h)
        label = (f"<b>{html.escape(name)}</b>" if name
                 else f"⚠️ غير مسجل .{h}")
        msg += f"🟢 {label} — منذ {fmt_short(now - st)}\n"
    return msg.rstrip()


def alert_policy(mode, stranger):
    """None = لا ترسل، 0 = مع صوت، 1 = صامت. الغريب دايماً مع صوت."""
    if stranger:
        return 0
    if mode == "off":
        return None
    return 1 if mode == "silent" else 0


def tfmt(ts, now):
    d = datetime.datetime.fromtimestamp(ts)
    same = d.date() == datetime.datetime.fromtimestamp(now).date()
    return d.strftime("%I:%M %p" if same else "%d/%m %I:%M %p")


def build_log(tr, now, limit=15):
    """سجل الجلسات والفجوات (الأحدث أولاً) منذ آخر تصفير."""
    since = float(tr.s.get_meta("reset_at"))
    known = tr.s.devices()
    items = []
    for h, a, b, closed in tr.s.all(
            "SELECT host, start, last_seen, closed FROM sessions "
            "WHERE last_seen > ?", (since,)):
        a = max(a, since)
        end = "الآن" if not closed else tfmt(b, now)
        items.append((a, f"{'🟢' if not closed else '⚪'} "
                         f"{known.get(h, 'غير مسجل .' + str(h))}: "
                         f"{tfmt(a, now)} ← {end}  ({fmt(b - a)})"))
    for a, b in tr.s.all("SELECT start, end FROM gaps WHERE end > ?", (since,)):
        a = max(a, since)
        items.append((a, f"⚠️ المراقب كان متوقفاً: {tfmt(a, now)} ← "
                         f"{tfmt(b, now)}  ({fmt(b - a)})"))
    items.sort(key=lambda x: x[0], reverse=True)
    head = (f"🕘 سجل الاتصال (منذ {tfmt(since, now)})\n"
            "─────────────\n")
    if not items:
        return head + "لا يوجد شي لعرضه."
    return head + "\n".join(t for _, t in items[:limit])


def flush_outbox(store, deliver, now, late_after=60):
    """بيرسل التنبيهات المؤجلة بالترتيب، وبيوقف عند أول فشل (النت مقطوع)."""
    sent = 0
    for oid, ts, text, reg, silent in store.pending():
        if now - ts > late_after:
            text += "\n🕒 " + tfmt(ts, now)
        if not deliver(text, reg, silent):
            break
        store.done(oid)
        sent += 1
    return sent


# ===================== الشبكة =====================
def detect_hotspot():
    """يرجع (BASE مثل '10.250.227.', آخر رقم من عنوان الجوال) أو (None, None)."""
    try:
        out = subprocess.run(["ifconfig"], capture_output=True, text=True,
                             timeout=5).stdout
    except Exception:
        return None, None
    for iface in ("ap0", "swlan0", "wlan1"):
        m = re.search(r"^" + iface + r":[^\n]*\n\s+inet (\d+\.\d+\.\d+)\.(\d+)",
                      out, re.M)
        if m:
            return m.group(1) + ".", int(m.group(2))
    return None, None


def is_alive(ip, me_ip, deep):
    try:
        r = subprocess.run(["ping", "-c", "1", "-W", str(PING_TIMEOUT), ip],
                           capture_output=True, timeout=PING_TIMEOUT + 2)
        if r.returncode == 0:
            return True
    except Exception:
        pass
    if not deep:
        return False
    s = socket.socket()
    s.settimeout(0.5)
    try:
        s.bind((me_ip, 0))
        s.connect((ip, 80))
        return True
    except ConnectionRefusedError:
        return True
    except Exception:
        return False
    finally:
        s.close()


def describe(ev, known, base):
    kind, h, extra = ev
    name = known.get(h, f".{h}")
    if kind == "connected":
        return f"🟢 {name}", None
    if kind == "stranger":
        return f"⚠️ جهاز غير مسجل اتصل: {base}{h}", h
    return f"⚪ {name} · {fmt_short(extra or 0)}", None


def monitor_loop(tr, send):
    ex = cf.ThreadPoolExecutor(WORKERS)
    gap_start = tr.recover(time.time())
    # أول دورة: الأجهزة المسجلة بس (سريعة)، والفحص الكامل بعد ~40 ثانية
    last_full, last_prune = time.time() - FULL_EVERY + 40, 0.0
    while True:
        cycle_start = time.time()
        try:
            base, me = detect_hotspot()
            known = tr.s.devices()
            if not base:
                for e in tr.close_all():
                    send(*describe(e, known, "?"))
                tr.s.set_meta("heartbeat", time.time())
                if gap_start is not None:
                    t = time.time(); tr.close_gap(t)
                    send(f"ℹ️ المراقب رجع بعد توقف {fmt(t - gap_start)} "
                         f"(هاد الوقت غير محسوب).", None)
                    gap_start = None
                time.sleep(SCAN_EVERY)
                continue
            old = tr.s.get_meta("base")
            if old != base:
                for e in tr.close_all():
                    send(*describe(e, known, old or "?"))
                tr.s.set_meta("base", base)
                last_full = 0.0
                if old:
                    send(f"ℹ️ شبكة الهوتسبوت تغيّرت من {old}x إلى {base}x. "
                         f"راجع الـ IP الثابت للأجهزة.", None)
            me_ip = base + str(me)
            if cycle_start - last_full >= FULL_EVERY:
                hosts = [h for h in range(1, 255) if h != me]
                last_full = cycle_start
            else:
                hosts = sorted((set(known) | set(tr.open)) - {me})

            def probe(h):
                ok = is_alive(base + str(h), me_ip, h in known or h in tr.open)
                return h, (ok, time.time())

            results = dict(ex.map(probe, hosts))
            events = tr.process(time.time(), results, known)
            if gap_start is not None:
                t = time.time(); tr.close_gap(t)
                send(f"ℹ️ المراقب رجع بعد توقف {fmt(t - gap_start)} "
                     f"(هاد الوقت غير محسوب).", None)
                gap_start = None
            for e in events:
                send(*describe(e, known, base))
            if cycle_start - last_prune > 3600:
                tr.s.prune(cycle_start)
                last_prune = cycle_start
        except Exception:
            log.exception("monitor loop error")
        time.sleep(max(1, SCAN_EVERY - (time.time() - cycle_start)))


# ===================== بوت تلجرام =====================
def run_bot():
    import telebot
    from telebot import types

    if "PASTE_" in BOT_TOKEN or "PASTE_" in ADMIN_CHAT_ID:
        sys.exit("حط التوكن ورقم الشات بأعلى الملف (أو كمتغيرات بيئة).")
    admin = int(ADMIN_CHAT_ID)
    bot = telebot.TeleBot(BOT_TOKEN)
    store = Store(DB_PATH)
    tr = Tracker(store)
    MODES = {"on": "🔔 التنبيهات: كاملة", "silent": "🔕 التنبيهات: صامتة",
             "off": "⛔ التنبيهات: متوقفة"}
    NEXT = {"on": "silent", "silent": "off", "off": "on"}

    # ---------- إرسال التنبيهات (عبر طابور) ----------
    def send(text, reg_host=None):
        silent = alert_policy(store.get_meta("alert_mode", "on"),
                              reg_host is not None)
        if silent is not None:
            store.enqueue(time.time(), text, reg_host, silent)

    def deliver(text, reg_host, silent):
        try:
            kb = None
            if reg_host is not None:
                kb = types.InlineKeyboardMarkup()
                kb.add(types.InlineKeyboardButton(
                    "➕ تسجيله", callback_data=f"reg_{reg_host}"))
            bot.send_message(admin, text, reply_markup=kb,
                             disable_notification=bool(silent))
            return True
        except Exception:
            return False

    def flusher():
        while True:
            try:
                flush_outbox(store, deliver, time.time())
            except Exception:
                log.exception("outbox error")
            time.sleep(5)

    # ---------- الواجهة ----------
    def btn(text, data):
        return types.InlineKeyboardButton(text, callback_data=data)

    def menu_text():
        known = store.devices()
        n = sum(1 for h in list(tr.open) if h in known)
        return f"💚 <b>لوحة التحكم</b>\n{LINE}\n🟢 المتصلين الآن: <b>{n}</b>"

    def menu_kb():
        mode = store.get_meta("alert_mode", "on")
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.add(btn("📊 التقرير", "rep"), btn("👥 المتصلين", "onl"))
        kb.add(btn("🕘 السجل", "log"), btn(MODES.get(mode, MODES["on"]), "alert"))
        return kb

    def picker():
        known = store.devices()
        if not known:
            return ("📊 <b>التقرير</b>\nما في أجهزة مسجلة بعد.",
                    types.InlineKeyboardMarkup().add(btn("🔙 القائمة", "home")))
        now = time.time()
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.add(*[btn(f"👤 {n} · {user_cost(tr, h, now):.2f}₪", f"u_{h}")
                 for h, n in sorted(known.items(), key=lambda x: x[1])])
        kb.add(btn("📋 الكل", "all"), btn("🔙 القائمة", "home"))
        return f"📊 <b>التقرير</b>\n{LINE}\nاختر المستخدم:", kb

    def nav(view, back):
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.add(btn("🔄 تحديث", view), btn("🔙 رجوع", back))
        return kb

    def user_kb(h):
        kb = types.InlineKeyboardMarkup(row_width=2)
        kb.add(btn("🔄 تحديث", f"u_{h}"), btn("🧹 تصفير", f"rst_{h}"))
        kb.add(btn("🔙 المستخدمين", "rep"))
        return kb

    def is_admin(m):
        return m.chat.id == admin

    def parse_host(x):
        try:
            h = int(x.strip().split(".")[-1])
            return h if 1 <= h <= 254 else None
        except ValueError:
            return None

    def save_device(h, name):
        store.q("INSERT INTO devices(host,name) VALUES(?,?) "
                "ON CONFLICT(host) DO UPDATE SET name=excluded.name", (h, name))

    # ---------- الأوامر ----------
    @bot.message_handler(commands=["start", "menu"], func=is_admin)
    def _start(m):
        bot.send_message(m.chat.id, menu_text(), reply_markup=menu_kb(),
                         parse_mode="HTML")

    @bot.message_handler(commands=["report"], func=is_admin)
    def _report(m):
        text, kb = picker()
        bot.send_message(m.chat.id, text, reply_markup=kb, parse_mode="HTML")

    @bot.message_handler(commands=["online"], func=is_admin)
    def _online(m):
        bot.send_message(m.chat.id, build_online(tr, time.time()),
                         parse_mode="HTML")

    @bot.message_handler(commands=["log"], func=is_admin)
    def _log(m):
        bot.send_message(m.chat.id, build_log(tr, time.time()))

    @bot.message_handler(commands=["add", "rename"], func=is_admin)
    def _add(m):
        p = m.text.split(maxsplit=2)
        h = parse_host(p[1]) if len(p) > 1 else None
        if len(p) < 3 or h is None:
            return bot.reply_to(m, "اكتب: /add 205 جوال_أمي")
        save_device(h, p[2].strip())
        bot.reply_to(m, f"✅ تم: .{h} = {p[2].strip()}")

    @bot.message_handler(commands=["del"], func=is_admin)
    def _del(m):
        p = m.text.split()
        h = parse_host(p[1]) if len(p) > 1 else None
        if h is None:
            return bot.reply_to(m, "اكتب: /del 205")
        store.q("DELETE FROM devices WHERE host=?", (h,))
        bot.reply_to(m, "✅ تم الحذف (السجل القديم بيضل محفوظ)")

    def do_reg(m, h):
        if not m.text or m.text.startswith("/"):
            return bot.reply_to(m, "انلغى التسجيل. اضغط الزر مرة ثانية.")
        save_device(h, m.text.strip())
        bot.reply_to(m, f"✅ تم تسجيل .{h} باسم {m.text.strip()}")

    # ---------- الأزرار ----------
    @bot.callback_query_handler(func=lambda c: c.message.chat.id == admin)
    def _cb(c):
        d, cid, mid = c.data, c.message.chat.id, c.message.message_id
        now = time.time()

        def show(text, kb):
            try:
                bot.edit_message_text(text, cid, mid, reply_markup=kb,
                                      parse_mode="HTML")
            except Exception as e:
                if "not modified" not in str(e):
                    raise

        toast = None
        try:
            if d.startswith("reg_"):
                h = int(d[4:])
                bot.answer_callback_query(c.id)
                msg = bot.send_message(cid, f"اكتب اسم الجهاز (.{h}):")
                bot.register_next_step_handler(msg, lambda m: do_reg(m, h))
                return
            if d == "home":
                show(menu_text(), menu_kb())
            elif d == "rep":
                text, kb = picker()
                show(text, kb)
            elif d == "all":
                show(build_all_report(tr, now), nav("all", "rep"))
            elif d.startswith("u_"):
                h = int(d[2:])
                show(build_user_report(tr, now, h), user_kb(h))
            elif d.startswith("rsty_"):
                h = int(d[5:])
                tr.reset_user(h, now)
                toast = "✅ تم التصفير"
                show(build_user_report(tr, now, h), user_kb(h))
            elif d.startswith("rst_"):
                h = int(d[4:])
                name = html.escape(store.devices().get(h, f".{h}"))
                kb = types.InlineKeyboardMarkup(row_width=2)
                kb.add(btn("✅ نعم، صفّر", f"rsty_{h}"), btn("إلغاء", f"u_{h}"))
                show(f"🧹 تصفير عدّاد <b>{name}</b>؟\n"
                     f"بيبدأ حسابه من الصفر، وباقي المستخدمين ما بيتأثروا.", kb)
            elif d == "onl":
                show(build_online(tr, now), nav("onl", "home"))
            elif d == "log":
                show(html.escape(build_log(tr, now)), nav("log", "home"))
            elif d == "alert":
                cur = store.get_meta("alert_mode", "on")
                nxt = NEXT.get(cur, "on")
                store.set_meta("alert_mode", nxt)
                toast = MODES[nxt]
                show(menu_text(), menu_kb())
        except Exception as e:
            log.warning("callback: %s", e)
        try:
            bot.answer_callback_query(c.id, toast)
        except Exception:
            pass

    threading.Thread(target=flusher, daemon=True).start()
    threading.Thread(target=monitor_loop, args=(tr, send), daemon=True).start()
    log.info("started")
    bot.infinity_polling(skip_pending=True)


# ===================== اختبار ذاتي =====================
def selftest():
    import tempfile
    ok = 0

    def check(cond, msg):
        nonlocal ok
        assert cond, "FAIL: " + msg
        ok += 1
        print("✔", msg)

    def fresh():
        s = Store(":memory:")
        t = Tracker(s)
        s.set_meta("reset_at", 0)
        return s, t

    def sim(tr, start, end, host, known, step=20):
        t, ev = start, []
        while t <= end:
            ev += tr.process(t, {host: (True, t)}, known)
            t += step
        return ev

    # 1) ساعة متواصلة بالضبط
    s, tr = fresh(); known = {205: "x"}
    ev = sim(tr, 1000, 4600, 205, known)
    for i in range(1, 4):
        ev += tr.process(4600 + 20 * i, {205: (False, 4600 + 20 * i)}, known)
    d = [e for e in ev if e[0] == "disconnected"]
    check(len(d) == 1 and abs(d[0][2] - 3600) < 1e-6, "جلسة ساعة = 3600 ثانية بالضبط")
    check(abs(tr.seconds(0, 10**6)[205] - 3600) < 1e-6, "التقرير بيطابق الجلسة")
    check(not tr.open, "الجلسة انسكرت")

    # 2) فحصين فاشلين ما بيقطعوا الجلسة (جوال نايم)
    s, tr = fresh()
    tr.process(0, {1: (True, 0)}, {1: "a"})
    tr.process(20, {1: (False, 20)}, {1: "a"})
    tr.process(40, {1: (False, 40)}, {1: "a"})
    ev = tr.process(60, {1: (True, 60)}, {1: "a"})
    n = s.all("SELECT COUNT(*) FROM sessions")[0][0]
    check(n == 1 and not ev, "فصل متقطع قصير = جلسة وحدة بدون تنبيه")
    check(abs(tr.seconds(0, 99)[1] - 60) < 1e-6, "وقت الفجوة القصيرة محسوب")

    # 3) الفصل الحقيقي بيتسكّر عند آخر ظهور مش عند الاكتشاف
    s, tr = fresh()
    tr.process(0, {1: (True, 0)}, {1: "a"})
    tr.process(100, {1: (True, 100)}, {1: "a"})
    for t in (120, 140, 160):
        ev = tr.process(t, {1: (False, t)}, {1: "a"})
    check(ev and ev[0][0] == "disconnected" and ev[0][2] == 100,
          "الفصل بينحسب لآخر ظهور (100 مش 160)")

    # 4) غريب
    s, tr = fresh()
    ev = tr.process(0, {77: (True, 0)}, {1: "a"})
    ev2 = tr.process(20, {77: (True, 20)}, {1: "a"})
    check(ev == [("stranger", 77, None)] and ev2 == [], "تنبيه الغريب مرة وحدة")
    r = build_online(tr, 100)
    check("غير مسجل .77" in r, "الغريب بيظهر بقائمة المتصلين")

    # 5) التصفير بيقص الجلسة
    s, tr = fresh()
    sim(tr, 0, 7200, 1, {1: "a"})
    tr.reset(3600)
    check(abs(tr.seconds(3600, 7200)[1] - 3600) < 1e-6, "بعد التصفير بينحسب الباقي بس")

    # 6) توقف المراقب + استرجاع
    s, tr = fresh()
    sim(tr, 1000, 2000, 1, {1: "a"})
    s.q("INSERT INTO devices(host,name) VALUES(1,'a')")
    tr2 = Tracker(s)
    gs = tr2.recover(2500)
    tr2.close_gap(2600)
    check(gs == 2000 and tr2.gap_seconds(0, 3000) == 600,
          "الفجوة بتمتد لأول فحص ناجح (600ث)")
    check(s.all("SELECT COUNT(*) FROM sessions WHERE closed=0")[0][0] == 0,
          "الجلسات المعلّقة انسكرت")
    check("متوقفاً" in build_all_report(tr2, 3000), "التقرير بيحذّر من الفجوة")
    check("متوقفاً" in build_user_report(tr2, 3000, 1), "تقرير المستخدم بيحذّر من الفجوة")

    # 6b) إعادة تشغيل سريعة (30ث) لازم تنسجّل كمان، وأقل من 15ث لا
    s, tr = fresh()
    sim(tr, 1000, 2000, 1, {1: "a"})
    tr2 = Tracker(s)
    check(tr2.recover(2030) == 2000, "إعادة تشغيل 30ث بتنسجّل كفجوة")
    s, tr = fresh()
    sim(tr, 1000, 2000, 1, {1: "a"})
    check(Tracker(s).recover(2010) is None, "إعادة تشغيل 10ث ما بتنسجّل")

    # 7) تغيّر الشبكة
    s, tr = fresh()
    tr.process(0, {1: (True, 0)}, {1: "a"})
    ev = tr.close_all()
    check(ev and ev[0][0] == "disconnected" and not tr.open, "close_all بيسكّر كل شي")

    # 8) النجاة من القتل المفاجئ (ملف حقيقي)
    path = os.path.join(tempfile.mkdtemp(), "t.db")
    s = Store(path); tr = Tracker(s)
    sim(tr, 0, 600, 5, {5: "a"})
    del tr, s  # "قتل" بدون إغلاق نظيف
    s2 = Store(path); tr2 = Tracker(s2)
    tr2.recover(10000)
    check(abs(tr2.seconds(-1, 20000).get(5, 0) - 600) < 1e-6, "البيانات سليمة بعد القتل")

    # 9) ضغط: 250 جهاز × 1000 دورة
    s, tr = fresh()
    known = {i: str(i) for i in range(1, 4)}
    t0 = time.time()
    for c in range(1000):
        now = c * 20.0
        tr.process(now, {h: (True, now) for h in range(1, 251)}, known)
    el = time.time() - t0
    secs = tr.seconds(0, 10**7)
    check(all(abs(v - 19980) < 1e-6 for v in secs.values()) and len(secs) == 250,
          "ضغط: 250 جهاز × 1000 دورة، كل الأرقام صحيحة")
    check(el < 60, f"ضغط: السرعة مقبولة ({el:.1f}ث)")
    s.prune(10**7)
    check(s.all("SELECT COUNT(*) FROM audit")[0][0] == 0, "تنظيف سجل التدقيق")

    # 9b) السجل
    s, tr = fresh()
    sim(tr, 1000, 2000, 5, {5: "Mona"})
    for i in range(1, 4):
        tr.process(2000 + 20 * i, {5: (False, 2000 + 20 * i)}, {5: "Mona"})
    s.q("INSERT INTO devices(host,name) VALUES(5,'Mona')")
    tr.process(5000, {5: (True, 5000)}, {5: "Mona"})
    Tracker(s)  # لا تأثير
    lg = build_log(tr, 5100)
    check(lg.count("Mona") == 2 and "الآن" in lg, "السجل بيعرض الجلسة المسكّرة والمفتوحة")
    tr3 = Tracker(s); tr3.recover(6000)
    check("متوقفاً" in build_log(tr3, 6100), "السجل بيعرض فجوات التوقف")

    # 9c) طابور التنبيهات (النت مقطوع ثم يرجع)
    s, tr = fresh()
    s.enqueue(100, "فصل: Mona", None)
    s.enqueue(110, "اتصل: Mona", 5)
    got = []
    check(flush_outbox(s, lambda t, r, sl: False, 120) == 0 and len(s.pending()) == 2,
          "النت مقطوع: التنبيهات بتنحفظ ومش بتضيع")
    n = flush_outbox(s, lambda t, r, sl: got.append((t, r)) or True, 500)
    check(n == 2 and not s.pending() and got[0][0].startswith("فصل")
          and "🕒" in got[0][0] and got[1][1] == 5,
          "النت رجع: بتنبعت بالترتيب مع وقت الحدث")
    k = [0]
    def flaky(t, r, sl):
        k[0] += 1
        return k[0] != 2
    s.enqueue(1, "a"); s.enqueue(2, "b"); s.enqueue(3, "c")
    flush_outbox(s, flaky, 4)
    check([x[2] for x in s.pending()] == ["b", "c"], "الفشل بيوقف الإرسال والباقي بينتظر")

    # 9d) تصفير خاص بكل مستخدم
    s, tr = fresh()
    s.q("INSERT INTO devices VALUES(1,'Mona')")
    s.q("INSERT INTO devices VALUES(2,'May')")
    kn = {1: "Mona", 2: "May"}
    t = 0
    while t <= 7200:
        tr.process(t, {1: (True, t), 2: (True, t)}, kn)
        t += 20
    check(abs(user_seconds(tr, 1, 7200) - 7200) < 1e-6
          and abs(user_seconds(tr, 2, 7200) - 7200) < 1e-6, "ساعتين لكل مستخدم")
    tr.reset_user(1, 3600)
    check(abs(user_seconds(tr, 1, 7200) - 3600) < 1e-6
          and abs(user_seconds(tr, 2, 7200) - 7200) < 1e-6,
          "تصفير Mona ما بيمس May")
    rp = build_user_report(tr, 7200, 1)
    check("المبلغ" in rp and "0.25" in rp and "May" not in rp,
          "تقرير المستخدم: المدة والمبلغ بس")
    ra = build_all_report(tr, 7200)
    check("Mona" in ra and "May" in ra and "0.75" in ra, "تقرير الجميع والإجمالي (0.25 + 0.50)")
    check(abs(user_cost(tr, 2, 7200) - 0.5) < 1e-9, "تكلفة 2 ساعة = 0.50 شيكل")
    ol = build_online(tr, 7200)
    check("Mona" in ol and "May" in ol, "قائمة المتصلين بتعرض المدة")

    # 9e) سياسة التنبيهات
    check(alert_policy("on", False) == 0 and alert_policy("silent", False) == 1
          and alert_policy("off", False) is None
          and alert_policy("off", True) == 0 and alert_policy("silent", True) == 0,
          "وضع التنبيهات: كاملة/صامتة/متوقفة (الغريب دايماً بصوت)")
    s.enqueue(1, "x", None, 1)
    check(s.pending()[0][4] == 1, "علامة الصامت بتنحفظ بالطابور")
    check(fmt_short(29 * 60) == "29د" and fmt_short(6 * 3600 + 27 * 60) == "6س 27د",
          "التنسيق المختصر")

    # 10) تنسيق
    check(fmt(5400) == "1 ساعة و 30 دقيقة" and fmt(30) == "أقل من دقيقة", "تنسيق المدة")
    print(f"\nكل الاختبارات نجحت ({ok})")
    return 0


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")
    if "--selftest" in sys.argv:
        sys.exit(selftest())
    run_bot()
