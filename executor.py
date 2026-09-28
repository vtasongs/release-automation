import io
import os
import sys
import time
from datetime import datetime, timezone

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import psycopg2
import MetaTrader5 as mt5

DB_URL = os.environ["DB_URL"]
MT5_PW = os.environ["MT5_PW"]
MT5_SRV = "Exness-MT5Trial9"
OUR_LOGINS = {"436060779", "477243064"}
MAGIC = 20260401
ALIASES = {
    "BTCUSD": ["BTCUSDm", "BTCUSD", "BTCUSDT", "BTCUSD."],
    "XAUUSD": ["XAUUSDm", "XAUUSD", "GOLD", "XAUUSD."],
    "EURUSD": ["EURUSDm", "EURUSD", "EURUSD."],
    "GBPUSD": ["GBPUSDm", "GBPUSD"],
    "USDJPY": ["USDJPYm", "USDJPY"],
    "NAS100": ["NAS100", "NAS100m", "US100"],
    "US100": ["US100", "NAS100", "NAS100m"],
}


def connect(retries=10):
    last = None
    for i in range(retries):
        try:
            return psycopg2.connect(DB_URL, connect_timeout=60, options="-c statement_timeout=30000")
        except Exception as e:
            last = e
            print("db retry", i, e, flush=True)
            time.sleep(4)
    raise last


def resolve_symbol(requested):
    req = (requested or "").strip().upper()
    cands = ALIASES.get(req, [req])
    for c in cands:
        si = mt5.symbol_info(c)
        if si:
            if not si.visible:
                mt5.symbol_select(c, True)
            return c
    for c in cands:
        if mt5.symbol_select(c, True):
            si = mt5.symbol_info(c)
            if si:
                return c
    return None


def ensure_login(login_str):
    try:
        login_int = int(str(login_str).strip())
    except Exception:
        return False
    info = mt5.account_info()
    if info and info.login == login_int:
        return True
    return mt5.login(login_int, password=MT5_PW, server=MT5_SRV)


def lot_for(conn, license_id, symbol_name):
    cur = conn.cursor()
    cur.execute(
        "SELECT lot_size FROM client_symbol_settings WHERE license_id=%s AND symbol_name=%s",
        (license_id, symbol_name),
    )
    row = cur.fetchone()
    if row and row[0] is not None:
        try:
            lot = float(row[0])
            if lot > 0:
                return lot
        except Exception:
            pass
    return 0.01


def place_open(login_str, symbol_name, action, lot, sl=None, tp=None, comment="robot"):
    if not ensure_login(login_str):
        return None, "login_failed"
    sym = resolve_symbol(symbol_name)
    if not sym:
        return None, f"symbol_not_found_{symbol_name}"
    tick = mt5.symbol_info_tick(sym)
    if not tick:
        return None, f"no_market_data_{sym}"
    si = mt5.symbol_info(sym)
    lot = max(float(lot), float(si.volume_min or 0.01))
    lot = min(lot, float(si.volume_max or lot))
    act = (action or "buy").lower()
    order_type = mt5.ORDER_TYPE_BUY if act in ("buy", "b") else mt5.ORDER_TYPE_SELL
    price = tick.ask if act in ("buy", "b") else tick.bid
    req = {
        "action": mt5.TRADE_ACTION_DEAL,
        "symbol": sym,
        "volume": float(lot),
        "type": order_type,
        "price": float(price),
        "deviation": 20,
        "magic": MAGIC,
        "comment": comment[:31],
        "type_time": mt5.ORDER_TIME_GTC,
        "type_filling": mt5.ORDER_FILLING_IOC,
    }
    try:
        if sl:
            req["sl"] = float(sl)
        if tp:
            req["tp"] = float(tp)
    except Exception:
        pass
    res = mt5.order_send(req)
    if res is None:
        return None, f"order_send_none_{mt5.last_error()}"
    if res.retcode != mt5.TRADE_RETCODE_DONE:
        return None, f"retcode_{res.retcode}_{res.comment}"
    return int(res.order), "ok"


def close_tickets(login_str, tickets):
    if not ensure_login(login_str):
        return False, "login_failed"
    errs = []
    ok = 0
    for t in tickets:
        try:
            tid = int(t)
        except Exception:
            continue
        pos = mt5.positions_get(ticket=tid)
        if not pos:
            errs.append(f"not_found_{tid}")
            continue
        p = pos[0]
        tick = mt5.symbol_info_tick(p.symbol)
        if not tick:
            errs.append(f"no_tick_{p.symbol}")
            continue
        if p.type == mt5.POSITION_TYPE_BUY:
            order_type = mt5.ORDER_TYPE_SELL
            price = tick.bid
        else:
            order_type = mt5.ORDER_TYPE_BUY
            price = tick.ask
        req = {
            "action": mt5.TRADE_ACTION_DEAL,
            "symbol": p.symbol,
            "volume": float(p.volume),
            "type": order_type,
            "position": tid,
            "price": float(price),
            "deviation": 20,
            "magic": MAGIC,
            "comment": "robot",
            "type_time": mt5.ORDER_TIME_GTC,
            "type_filling": mt5.ORDER_FILLING_IOC,
        }
        res = mt5.order_send(req)
        if res and res.retcode == mt5.TRADE_RETCODE_DONE:
            ok += 1
        else:
            rc = res.retcode if res else None
            cm = res.comment if res else mt5.last_error()
            errs.append(f"close_{tid}_{rc}_{cm}")
    if ok and not errs:
        return True, "ok"
    if ok:
        return True, ";".join(errs)
    return False, ";".join(errs) if errs else "no_tickets"


def fail_exec(conn, teid, err, bump_retry):
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(retry_count,0) FROM trade_executions WHERE id=%s", (teid,))
    rc = cur.fetchone()
    rc = int(rc[0]) if rc else 0
    if bump_retry and rc + 1 < 3:
        cur.execute(
            """UPDATE trade_executions
               SET status='retry', error_message=%s, retry_count=%s, updated_at=now()
               WHERE id=%s""",
            (err, rc + 1, teid),
        )
    else:
        cur.execute(
            "UPDATE trade_executions SET status='failed', error_message=%s, updated_at=now() WHERE id=%s",
            (err, teid),
        )
    conn.commit()


def process_copier(conn):
    cur = conn.cursor()
    cur.execute(
        """UPDATE trade_executions SET status='pending', updated_at=now()
           WHERE status='processing' AND updated_at < now() - interval '2 minutes'"""
    )
    conn.commit()
    cur.execute(
        """SELECT te.id, te.license_id, te.ea_id, te.master_ticket, te.client_ticket,
                  te.symbol, te.action, te.lot_size, te.sl, te.tp, te.price, te.event_type
           FROM trade_executions te
           JOIN client_mt5_accounts a ON a.license_id = te.license_id
           WHERE te.status IN ('pending','retry')
             AND (te.retry_count IS NULL OR te.retry_count < 3)
             AND a.is_active AND a.is_verified
             AND a.login = ANY(%s)
           ORDER BY te.id ASC LIMIT 40""",
        (list(OUR_LOGINS),),
    )
    rows = cur.fetchall()
    print("pending", len(rows), flush=True)
    for (teid, lic, ea, mticket, cticket, symbol, action, lot, sl, tp, price, etype) in rows:
        cur.execute("UPDATE trade_executions SET status='processing', updated_at=now() WHERE id=%s", (teid,))
        conn.commit()
        cur.execute(
            "SELECT login FROM client_mt5_accounts WHERE license_id=%s AND is_active AND is_verified LIMIT 1",
            (lic,),
        )
        acct = cur.fetchone()
        if not acct or str(acct[0]) not in OUR_LOGINS:
            fail_exec(conn, teid, "no_verified_mt5", 0)
            continue
        mt5_login = acct[0]
        et = (etype or "open").lower()
        if et == "close":
            tickets = [t for t in str(cticket or "").split(",") if t.strip()]
            if not tickets:
                cur.execute(
                    """SELECT client_ticket FROM ticket_maps
                       WHERE license_id=%s AND master_ticket=%s AND is_closed=false
                       AND client_ticket IS NOT NULL""",
                    (lic, str(mticket)),
                )
                tickets = [r[0] for r in cur.fetchall() if r[0]]
            if not tickets:
                fail_exec(conn, teid, "no_open_tickets", 0)
                continue
            ok, err = close_tickets(mt5_login, tickets)
            if ok:
                cur.execute(
                    "UPDATE trade_executions SET status='executed', updated_at=now() WHERE id=%s", (teid,)
                )
                cur.execute(
                    """UPDATE ticket_maps SET is_closed=true, closed_at=now()
                       WHERE license_id=%s AND master_ticket=%s AND is_closed=false""",
                    (lic, str(mticket)),
                )
                conn.commit()
                print("te", teid, "closed", tickets, flush=True)
            else:
                fail_exec(conn, teid, err, 0)
                print("te", teid, "close fail", err, flush=True)
            continue

        use_lot = lot if lot else lot_for(conn, lic, symbol)
        ticket, err = place_open(mt5_login, symbol, action, use_lot, sl=sl, tp=tp)
        if ticket:
            cur.execute(
                "UPDATE trade_executions SET status='executed', client_ticket=%s, updated_at=now() WHERE id=%s",
                (str(ticket), teid),
            )
            cur.execute(
                """INSERT INTO ticket_maps
                   (license_id, execution_id, master_ticket, client_ticket, symbol, is_closed, created_at)
                   VALUES (%s,%s,%s,%s,%s,false,now())""",
                (lic, teid, str(mticket), str(ticket), str(symbol).upper()),
            )
            conn.commit()
            print("te", teid, "OPEN", ticket, symbol, use_lot, flush=True)
        else:
            fail_exec(conn, teid, err, 1)
            print("te", teid, "FAIL", err, flush=True)


def find_mt5():
    candidates = [
        r"C:\Program Files\MetaTrader 5\terminal64.exe",
        r"C:\Program Files (x86)\MetaTrader 5\terminal64.exe",
        r"C:\Program Files\Exness MT5\terminal64.exe",
        os.path.join(os.environ.get("ProgramFiles", r"C:\Program Files"), "MetaTrader 5", "terminal64.exe"),
    ]
    for p in candidates:
        if os.path.isfile(p):
            return p
    for root in (os.environ.get("ProgramFiles", r"C:\Program Files"), os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")):
        if not root or not os.path.isdir(root):
            continue
        for name in os.listdir(root):
            cand = os.path.join(root, name, "terminal64.exe")
            if os.path.isfile(cand):
                return cand
    return None


def enable_autotrading(path):
    cfg_dir = os.path.join(os.path.dirname(path), "config")
    try:
        os.makedirs(cfg_dir, exist_ok=True)
    except Exception:
        pass
    # common.ini
    common = os.path.join(cfg_dir, "common.ini")
    try:
        with open(common, "w", encoding="utf-8") as f:
            f.write("[Common]\nAllowLiveTrading=1\nAllowDllImport=1\n")
    except Exception as e:
        print("common.ini", e, flush=True)
    # terminal.ini
    terminal = os.path.join(cfg_dir, "terminal.ini")
    try:
        with open(terminal, "w", encoding="utf-8") as f:
            f.write("[Experts]\nAllowLiveTrading=1\nAllowExport=1\n")
    except Exception as e:
        print("terminal.ini", e, flush=True)
    print("autotrading config written", cfg_dir, flush=True)


def kill_terminals():
    import subprocess
    try:
        subprocess.run(["taskkill", "/F", "/IM", "terminal64.exe"], capture_output=True, timeout=10)
    except Exception as e:
        print("kill err", e, flush=True)


def launch_terminal(path):
    import subprocess
    kill_terminals()
    enable_autotrading(path)
    time.sleep(2)
    # Start with autotrading on flag if supported
    try:
        subprocess.Popen([path, "/portable", "/autotrading:on"], close_fds=True)
        print("launched with /autotrading:on", path, flush=True)
    except Exception:
        try:
            subprocess.Popen([path, "/portable"], close_fds=True)
            print("launched /portable", path, flush=True)
        except Exception as e:
            print("launch err", e, flush=True)
    time.sleep(10)


def main():
    minutes = float(os.environ.get("RUN_MINUTES", "10"))
    path = find_mt5()
    print("mt5 path", path, flush=True)
    if not path:
        print("mt5 not found", flush=True)
        sys.exit(1)
    launch_terminal(path)
    first_login = sorted(OUR_LOGINS)[0]
    ok = False
    for attempt in range(5):
        if mt5.initialize(
            path=path,
            timeout=120000,
            login=int(first_login),
            password=MT5_PW,
            server=MT5_SRV,
            portable=True,
        ):
            ok = True
            break
        print("mt5 init attempt", attempt, mt5.last_error(), flush=True)
        time.sleep(10)
        launch_terminal(path)
    if not ok:
        if mt5.initialize(timeout=120000):
            ok = True
        else:
            print("mt5 init failed", mt5.last_error(), flush=True)
            sys.exit(1)
    print("mt5 init ok", flush=True)
    conn = connect()
    deadline = time.time() + minutes * 60
    cycle = 0
    while time.time() < deadline:
        cycle += 1
        try:
            process_copier(conn)
        except Exception as e:
            print("cycle error", e, flush=True)
            try:
                conn.rollback()
            except Exception:
                pass
            try:
                conn.close()
            except Exception:
                pass
            time.sleep(3)
            try:
                conn = connect()
            except Exception as e2:
                print("reconnect fail", e2, flush=True)
                time.sleep(5)
        time.sleep(2)
    print("done cycles", cycle, flush=True)
    try:
        conn.close()
    except Exception:
        pass
    mt5.shutdown()


if __name__ == "__main__":
    main()
