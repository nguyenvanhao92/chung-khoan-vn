#!/usr/bin/env python3
"""VN Stock Analyzer - may chu cuc bo lay gia that. Chi dung thu vien chuan cua Python 3.8+.
Chay:  python server.py   roi mo  http://localhost:8000
"""
import re, xml.etree.ElementTree as ET, email.utils, json, os, sys, time, threading, webbrowser, urllib.request, urllib.parse, base64, hmac
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone, timedelta

PORT = int(os.environ.get("PORT", "8000"))
HOST = os.environ.get("HOST", "127.0.0.1")          # dua len may chu: HOST=0.0.0.0
APP_PASSWORD = os.environ.get("APP_PASSWORD", "")    # dat mat khau khi dua len mang
HERE = os.path.dirname(os.path.abspath(__file__))
VN = timezone(timedelta(hours=7))
UA = {"User-Agent": "Mozilla/5.0 (VN-Stock-Analyzer)", "Accept": "application/json"}
ENTRADE = "https://services.entrade.com.vn/chart-api/v2/ohlcs/stock"
ENTRADE_INDEX = "https://services.entrade.com.vn/chart-api/v2/ohlcs/index"   # da kiem tra: VNINDEX
VND_RATIOS = "https://api-finfo.vndirect.com.vn/v4/ratios/latest"           # da kiem tra: P/E, P/B, ROE, ROA, EPS
VNDIRECT = "https://finfo-api.vndirect.com.vn/v4/stock_prices"
BOARD = "https://banggia.cafef.vn/stockhandler.ashx?center=%d"  # bang gia CafeF: HOSE=1, HNX=2 (da kiem tra cau truc)
FIREANT = "https://restapi.fireant.vn/symbols/%s/historical-quotes"
_cache, _lock = {}, threading.Lock()


def fireant_token():
    t = os.environ.get("FIREANT_TOKEN", "").strip()
    f = os.path.join(HERE, "fireant_token.txt")
    if not t and os.path.exists(f):
        t = open(f, encoding="utf-8").read().strip()
    return t.replace("Bearer ", "")


def fireant_rows(sym, days):
    tok = fireant_token()
    if not tok:
        raise RuntimeError("chua co FIREANT_TOKEN")
    end = datetime.now(VN).date()
    q = urllib.parse.urlencode({"startDate": (end - timedelta(days=days)).isoformat(), "endDate": end.isoformat(), "offset": 0, "limit": 400})
    req = urllib.request.Request(FIREANT % sym + "?" + q, headers=dict(UA, Authorization="Bearer " + tok))
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read().decode("utf-8"))


def parse_fireant(rows):
    k = scale([r.get("priceClose") for r in rows])
    out = []
    for r in reversed(rows):  # moi nhat truoc
        try:
            d = datetime.fromisoformat(r["date"][:10]).replace(hour=9, tzinfo=VN)
            out.append({"t": int(d.timestamp()), "o": r["priceOpen"] / k, "h": r["priceHigh"] / k, "l": r["priceLow"] / k,
                        "c": r["priceClose"] / k, "v": r.get("totalVolume") or 0})
        except Exception:
            continue
    return out


_UP = threading.BoundedSemaphore(int(os.environ.get("UP_CONC", "8")))   # gioi han so ket noi toi nguon du lieu cung luc (tranh bi nguon chan/loi 500)


def fetch_json(url, timeout=10, tries=2):
    req = urllib.request.Request(url, headers=UA)
    last = None
    for k in range(tries):
        try:
            with _UP:
                with urllib.request.urlopen(req, timeout=timeout) as r:
                    return json.loads(r.read().decode("utf-8"))
        except urllib.error.HTTPError as e:
            last = e
            if e.code < 500 and e.code != 429:
                raise
        except Exception as e:
            last = e
        time.sleep(0.8 * (k + 1))
    raise last


def cached(key, ttl, fn):
    now = time.time()
    with _lock:
        hit = _cache.get(key)
        if hit and now - hit[0] < ttl:
            return hit[1]
    val = fn()
    with _lock:
        _cache[key] = (now, val)
    return val


def board():
    """Bang gia CafeF thoi gian thuc. Truong: a=ma, b=tham chieu, l=gia khop gan nhat, k=thay doi (l-b),
    v=cao nhat, w=thap nhat, n=tong khoi luong, Time=gio cap nhat; so lenh 3 buoc: mua (e,f)(g,h)(i,j), ban (s,t)(q,r)(o,p), m=KL khop gan nhat. Gia tinh bang nghin dong."""
    def go():
        out = {}
        for center in (1, 2, 9):  # 1=HOSE, 2=HNX, 9=UPCoM (UPCoM chua kiem tra; loi thi bo qua)
            try:
                for e in fetch_json(BOARD % center, 10):
                    sym, b = e.get("a"), e.get("b") or 0
                    if not sym or not b:
                        continue
                    last = e.get("l") or 0
                    if not last:
                        last = b + (e.get("k") or 0) if e.get("k") else b
                    out[sym] = {"ex": {1: "HOSE", 2: "HNX", 9: "UPCoM"}[center], "ref": float(b), "p": float(last), "h": float(e.get("v") or last), "lo": float(e.get("w") or last),
                                "vol": int(e.get("totalvolume") or e.get("n") or 0), "time": e.get("Time", ""),
                                "bid": [[float(e.get(a) or 0), int(e.get(b2) or 0)] for a, b2 in (("e", "f"), ("g", "h"), ("i", "j"))],
                                "ask": [[float(e.get(a) or 0), int(e.get(b2) or 0)] for a, b2 in (("s", "t"), ("q", "r"), ("o", "p"))],
                                "lv": int(e.get("m") or 0)}
            except Exception as ex:
                print("cafef bang gia center=%d loi: %s" % (center, ex), file=sys.stderr)
        if not out:
            raise RuntimeError("khong lay duoc bang gia CafeF")
        return out
    return cached("board", 3, go)


FINFO_EVENTS = "https://api-finfo.vndirect.com.vn/v4/events"


def dividends():
    """Lich co tuc / co phieu thuong that tu VNDIRECT finfo (effectiveDate = ngay GDKHQ, expiredDate = ngay thanh toan)."""
    def go():
        today = time.strftime("%Y-%m-%d", time.gmtime(time.time() + 7 * 3600 - 86400))
        out = []
        for typ in ("DIVIDEND", "STOCKDIV", "KINDDIV"):
            try:
                q = "type:%s~locale:VN~effectiveDate:gte:%s" % (typ, today)
                j = fetch_json(FINFO_EVENTS + "?" + urllib.parse.urlencode({"q": q, "size": 80, "sort": "effectiveDate:asc"}, safe=":~"), 12)
                for e in j.get("data", []):
                    if not e.get("code") or not e.get("effectiveDate"):
                        continue
                    out.append({"s": e["code"], "t": typ, "desc": e.get("typeDesc", ""), "note": e.get("note", ""), "ex": e["effectiveDate"],
                                "pay": e.get("expiredDate") or e.get("actualDate") or "", "div": e.get("dividend"), "ratio": e.get("ratio")})
            except Exception as ex:
                print("co tuc %s loi: %s" % (typ, ex), file=sys.stderr)
        if not out:
            raise RuntimeError("khong lay duoc lich co tuc")
        out.sort(key=lambda x: x["ex"])
        return out
    return cached("divs", 3 * 3600, go)


_pref = {"busy": False, "fail": {}}


def prefetch_history(syms):
    """Nap lich su 25 ngay cho cac ma (chay nen, cham) de tinh dong tien nganh tu du lieu that."""
    if _pref["busy"]:
        return
    def run():
        _pref["busy"] = True
        try:
            for s in syms:
                hit = _cache.get("q" + s)
                if hit and time.time() - hit[0] < 900:
                    continue
                if time.time() - _pref["fail"].get(s, 0) < 600:
                    continue
                try:
                    v = history(s, 25)
                    with _lock:
                        _cache["q" + s] = (time.time(), v)
                except Exception:
                    _pref["fail"][s] = time.time()
                time.sleep(0.3)
        finally:
            _pref["busy"] = False
    threading.Thread(target=run, daemon=True).start()


def scale(vals):
    """Gia tren cac API thuong tinh bang dong hoac nghin dong -> chuan hoa ve nghin dong."""
    nums = sorted(v for v in vals if v)
    if nums and nums[len(nums) // 2] > 1000:
        return 1000.0
    return 1.0


def parse_entrade(j):
    t, o, h, l, c, v = (j.get(k) or [] for k in ("t", "o", "h", "l", "c", "v"))
    o, h, l = (x or c for x in (o, h, l))
    v = v or [0] * len(c)
    n = min(len(t), len(o), len(h), len(l), len(c), len(v))
    k = scale(c[:n])
    return [{"t": int(t[i]), "o": o[i] / k, "h": h[i] / k, "l": l[i] / k, "c": c[i] / k, "v": v[i]} for i in range(n)]


def parse_vndirect(j):
    rows = j.get("data") or []
    k = scale([r.get("close") for r in rows])
    out = []
    for r in reversed(rows):  # api tra moi nhat truoc
        try:
            d = datetime.strptime(r["date"], "%Y-%m-%d").replace(hour=7, tzinfo=VN)
            out.append({"t": int(d.timestamp()), "o": r["open"] / k, "h": r["high"] / k, "l": r["low"] / k,
                        "c": r["close"] / k, "v": (r.get("nmVolume") or r.get("nmVolumeMatched") or r.get("totalVolume") or 0) + (r.get("ptVolume") or 0)})
        except Exception:
            continue
    return out


_LASTGOOD = {}


def history(sym, days=420, slow_fallback=True):
    if fireant_token():
        try:
            c = parse_fireant(fireant_rows(sym, days))
            if len(c) >= 30 or (days < 30 and len(c) >= 2):
                return c, "FireAnt"
        except Exception as e:
            print("fireant history loi:", e, file=sys.stderr)
    now = int(time.time())
    q = urllib.parse.urlencode({"from": now - days * 86400, "to": now + 86400, "symbol": sym, "resolution": "1D"})
    try:
        c = parse_entrade(fetch_json(ENTRADE + "?" + q))
        if len(c) >= 30:
            _LASTGOOD[sym] = (c, "DNSE/Entrade")
            return c, "DNSE/Entrade"
        print("entrade history ngan:", sym, days, "ngay ->", len(c), "nen", file=sys.stderr)
    except Exception as e:
        print("entrade history loi:", e, file=sys.stderr)
    if not slow_fallback:
        raise RuntimeError("entrade khong tra du nen")
    try:
        q = urllib.parse.urlencode({"sort": "date", "size": min(1000, int(days * 0.75) + 10), "q": "code:" + sym})
        c = parse_vndirect(fetch_json(VNDIRECT + "?" + q, timeout=6, tries=1))
        if len(c) >= 30:
            _LASTGOOD[sym] = (c, "VNDIRECT")
            return c, "VNDIRECT"
    except Exception as e:
        print("vndirect history loi:", e, file=sys.stderr)
    if sym in _LASTGOOD:                      # nguon loi tam thoi: dung lai du lieu gan nhat da lay duoc
        c, src = _LASTGOOD[sym]
        return c, src + " (dữ liệu lưu gần nhất)"
    raise RuntimeError("khong lay duoc lich su gia")


def intraday(sym):
    """Uu tien: nen 1 phut (DNSE) -> bang gia CafeF -> FireAnt."""
    try:
        b = intraday_entrade(sym)
        if b:
            return b, "DNSE/Entrade"
    except Exception as e:
        print("entrade intraday loi:", e, file=sys.stderr)
    try:
        q = board().get(sym)
        if q:
            day9 = datetime.now(VN).replace(hour=9, minute=0, second=0, microsecond=0)
            return [{"t": int(day9.timestamp()), "o": None, "h": q["h"], "l": q["lo"], "c": q["p"], "v": q["vol"]}], "CafeF"
    except Exception as e:
        print("cafef intraday loi:", e, file=sys.stderr)
    if fireant_token():
        c = parse_fireant(fireant_rows(sym, 7))
        if c:
            return c[-1:], "FireAnt"
    raise RuntimeError("khong co du lieu trong phien")


def intraday_entrade(sym):
    now = datetime.now(VN)
    start = now.replace(hour=0, minute=0, second=0, microsecond=0)
    q = urllib.parse.urlencode({"from": int(start.timestamp()), "to": int(time.time()) + 600, "symbol": sym, "resolution": "1"})
    bars = parse_entrade(fetch_json(ENTRADE + "?" + q))
    return bars


def bars_multi(sym, days):
    """Nen phut gop (1 ngay -> 5 phut, 3 ngay -> 15 phut) tu nen 1 phut DNSE/Entrade (da kiem tra do phan giai '1')."""
    size = 5 if days <= 1 else 15
    now = int(time.time())
    q = urllib.parse.urlencode({"from": now - 6 * 86400, "to": now + 600, "symbol": sym, "resolution": "1"})
    raw = parse_entrade(fetch_json(ENTRADE + "?" + q))
    if not raw:
        raise RuntimeError("khong co nen phut")
    def day(t):
        return datetime.fromtimestamp(t, VN).date()
    keep = sorted({day(b["t"]) for b in raw})[-days:]
    out = {}
    for b in raw:
        if day(b["t"]) not in keep:
            continue
        k = b["t"] - b["t"] % (size * 60)
        a = out.get(k)
        if not a:
            out[k] = {"t": k, "o": b["o"], "h": b["h"], "l": b["l"], "c": b["c"], "v": b["v"]}
        else:
            a["h"] = max(a["h"], b["h"]); a["l"] = min(a["l"], b["l"]); a["c"] = b["c"]; a["v"] += b["v"]
    return [out[k] for k in sorted(out)], "DNSE/Entrade %d phut" % size


def ratios(sym):
    q = urllib.parse.urlencode({"filter": "ratioCode:PRICE_TO_EARNINGS,PRICE_TO_BOOK,ROAE_TR_AVG5Q,ROAA_TR_AVG5Q,EPS_TR",
                                "where": "code:" + sym, "order": "reportDate", "fields": "ratioCode,value,reportDate"}, safe=":,")
    out = {}
    for e in fetch_json(VND_RATIOS + "?" + q).get("data", []):
        out[e.get("ratioCode")] = e.get("value")
    return {"pe": out.get("PRICE_TO_EARNINGS"), "pb": out.get("PRICE_TO_BOOK"), "roe": out.get("ROAE_TR_AVG5Q"),
            "roa": out.get("ROAA_TR_AVG5Q"), "eps": out.get("EPS_TR")}


_scan = {"data": {}, "busy": False, "index": None, "fail": {}}


_scan_q = set()


def scan_start(syms):
    """Nap nen lich su gia + chi so tai chinh cho ca danh sach ma (de xep hang ngan/trung/dai han)."""
    if _scan["busy"]:
        _scan_q.update(x for x in syms if x not in _scan["data"])   # dang quet: xep hang cac ma moi de quet tiep sau
        return
    def run():
        _scan["busy"] = True
        try:
            work(syms)
            while _scan_q:
                more = list(_scan_q)
                _scan_q.clear()
                work(more)
            tail()
        finally:
            _scan["busy"] = False
    def work(lst):
        if True:
            for s in lst:
                hit = _scan["data"].get(s)
                if hit and time.time() - hit["ts"] < 900:
                    continue
                if time.time() - _scan["fail"].get(s, 0) < 600:
                    continue
                try:
                    try:
                        c, src = history(s, 3650, slow_fallback=False)
                    except Exception:
                        c, src = history(s, 420)
                    try:
                        r = ratios(s)
                    except Exception:
                        r = (hit or {}).get("r")
                    top = max(c, key=lambda x: x["h"]) if c else None
                    _scan["data"][s] = {"ts": time.time(), "c": c, "src": src, "r": r,
                                        "ath": top["h"] if top else None, "athT": top["t"] if top else None, "n": len(c)}
                except Exception as e:
                    _scan["fail"][s] = time.time()
                    print("scan loi", s, e, file=sys.stderr)
                time.sleep(0.25)
    def tail():
        if True:
            idx = _scan["index"]
            if not idx or time.time() - idx[0] > 600:
                try:
                    now = int(time.time())
                    q = urllib.parse.urlencode({"from": now - 420 * 86400, "to": now + 86400, "symbol": "VNINDEX", "resolution": "1D"})
                    _scan["index"] = (time.time(), parse_entrade(fetch_json(ENTRADE_INDEX + "?" + q)))
                except Exception as e:
                    print("scan chi so VNINDEX loi", e, file=sys.stderr)
    threading.Thread(target=run, daemon=True).start()


def scan_payload(syms):
    try:
        bd = board()
    except Exception:
        bd = {}
    data = {}
    for s in syms:
        h = _scan["data"].get(s)
        if not h or not h.get("c"):
            continue
        c = [dict(x) for x in h["c"][-300:]]
        q = bd.get(s)
        if q and c:
            def day(t):
                return datetime.fromtimestamp(t, VN).date()
            tday = datetime.now(VN).date()
            last = c[-1]
            if day(last["t"]) == tday:
                last["c"], last["h"], last["l"] = q["p"], max(last["h"], q["h"]), min(last["l"], q["lo"])
                last["v"] = max(last["v"], q["vol"])
            elif day(last["t"]) < tday and q["vol"] > 0:
                c.append({"t": int(datetime.now(VN).replace(hour=9, minute=0, second=0, microsecond=0).timestamp()),
                          "o": q["ref"], "h": q["h"], "l": q["lo"], "c": q["p"], "v": q["vol"]})
        data[s] = {"o": [round(x["o"], 3) for x in c], "h": [round(x["h"], 3) for x in c], "l": [round(x["l"], 3) for x in c],
                   "c": [round(x["c"], 3) for x in c], "v": [int(x["v"]) for x in c], "t": [x["t"] for x in c], "r": h.get("r"), "src": h.get("src"), "ath": h.get("ath"), "athT": h.get("athT"), "nAll": h.get("n")}
    idx = _scan["index"][1][-300:] if _scan["index"] else []
    return {"ok": True, "ready": len(data), "total": len(syms), "busy": _scan["busy"], "data": data,
            "index": [round(x["c"], 2) for x in idx], "indexT": [x["t"] for x in idx]}


FINFO_NEWS = "https://api-finfo.vndirect.com.vn/v4/news"


def stock_news():
    """Tin doanh nghiep (VNDIRECT finfo company_news, 32 ngay gan nhat); moi tin co danh sach ma (tagCodes)."""
    def go():
        since = time.strftime("%Y-%m-%d", time.gmtime(time.time() + 7 * 3600 - 32 * 86400))
        out, page = [], 1
        while page <= 14:
            q = "newsType:company_news~locale:VN~newsDate:gte:%s" % since
            j = fetch_json(FINFO_NEWS + "?" + urllib.parse.urlencode({"q": q, "size": 100, "page": page, "sort": "newsDate:desc"}, safe=":~"), 20)
            for e in j.get("data", []):
                title = (e.get("newsTitle") or "").strip()
                tags = [x.strip().upper() for x in (e.get("tagCodes") or "").split(",") if x.strip()]
                if not title or not tags:
                    continue
                ab = (e.get("newsAbstract") or "").strip()
                out.append({"title": title, "abs": ab[:220], "d": e.get("newsDate"), "src": e.get("newsSource") or "VNDIRECT",
                            "link": e.get("newsUrl") or e.get("dstockUrl") or "", "syms": tags[:6], "sent": news_sentiment(title + " " + ab),
                            "hot": e.get("hotNews") == "Y"})
            if page >= int(j.get("totalPages") or 1):
                break
            page += 1
        if not out:
            raise RuntimeError("khong lay duoc tin doanh nghiep")
        return out
    return cached("stocknews", 15 * 60, go)


def stock_events(sym):
    """Su kien quyen / co tuc / DHCD / giao dich bo sung cua 1 ma trong 45 ngay gan nhat va sap toi (VNDIRECT finfo)."""
    def go():
        since = time.strftime("%Y-%m-%d", time.gmtime(time.time() + 7 * 3600 - 45 * 86400))
        q = "code:%s~locale:VN~effectiveDate:gte:%s" % (sym, since)
        j = fetch_json(FINFO_EVENTS + "?" + urllib.parse.urlencode({"q": q, "size": 12, "sort": "effectiveDate:desc"}, safe=":~"), 12)
        return [{"s": sym, "t": e.get("type"), "desc": e.get("typeDesc", ""), "note": e.get("note", ""), "ex": e.get("effectiveDate"),
                 "pay": e.get("expiredDate") or e.get("actualDate") or "", "div": e.get("dividend"), "ratio": e.get("ratio")}
                for e in j.get("data", []) if e.get("effectiveDate")]
    return cached("ev" + sym, 3 * 3600, go)


def google_news(sym):
    """Tin theo tung ma trong 30 ngay tu Google News RSS (tong hop CafeF, VnEconomy, Vietstock, bao chi...). Loc trung tieu de."""
    def go():
        q = urllib.parse.quote('"%s" cổ phiếu when:30d' % sym)
        root = ET.fromstring(fetch_text("https://news.google.com/rss/search?q=%s&hl=vi&gl=VN&ceid=VN:vi" % q, 12))
        lim = time.time() - 32 * 86400
        out = []
        for rank, it in enumerate(root.iter("item")):
            title = (it.findtext("title") or "").strip()
            src = (it.findtext("source") or "").strip()
            if src and title.endswith(" - " + src):
                title = title[: -len(src) - 3].strip()
            try:
                ts = email.utils.parsedate_to_datetime(it.findtext("pubDate")).timestamp()
            except Exception:
                continue
            # chi giu tin co ma trong tieu de, hoac nam trong 3 ket qua lien quan nhat (tieu de thuong ghi ten cong ty)
            if not title or ts < lim or not (re.search(r"\b%s\b" % re.escape(sym), title.upper()) or rank < 3):
                continue
            out.append({"title": title, "abs": "", "d": time.strftime("%Y-%m-%d", time.gmtime(ts + 7 * 3600)), "src": src or "Google News",
                        "link": (it.findtext("link") or "").strip(), "syms": [sym], "sent": news_sentiment(title), "hot": False, "_t": ts})
        out.sort(key=lambda x: -x["_t"])
        for x in out:
            x.pop("_t", None)
        return out[:10]
    return cached("gn" + sym, 30 * 60, go)


SECTOR_RULES = [
    ("Ngân hàng", [r"ngân hàng"], True),
    ("Chứng khoán", [r"chứng khoán"], True),
    ("Bảo hiểm", [r"bảo hiểm"], True),
    ("Bất động sản", [r"bất động sản", r"địa ốc", r"khu đô thị", r"khu công nghiệp", r"nhà ở"], False),
    ("Thép", [r"thép", r"tôn mạ", r"tôn "], False),
    ("Dầu khí", [r"dầu khí", r"xăng dầu", r"khí hóa lỏng", r"lọc dầu", r"khí đốt"], False),
    ("Công nghệ", [r"phần mềm", r"công nghệ thông tin", r"viễn thông", r"tin học", r"chuyển đổi số"], False),
    ("Bán lẻ", [r"bán lẻ", r"siêu thị", r"cửa hàng"], False),
    ("Điện & Tiện ích", [r"thủy điện", r"nhiệt điện", r"điện gió", r"điện mặt trời", r"cấp nước", r"nước sạch", r"sản xuất và kinh doanh điện", r"truyền tải điện", r"phân phối điện"], False),
    ("Xây dựng & ĐTC", [r"xây dựng", r"xây lắp", r"hạ tầng", r"cầu đường", r"giao thông", r"công viên cây xanh", r"thi công"], False),
    ("Hàng tiêu dùng", [r"thủy sản", r"thực phẩm", r"\bsữa\b", r"đồ uống", r"\bbia\b", r"nước giải khát", r"dệt may", r"may mặc", r"nông sản", r"\bđường\b", r"tiêu dùng", r"gạo", r"chăn nuôi", r"bánh kẹo"], False),
    ("Dược phẩm", [r"dược", r"y tế", r"bệnh viện"], False),
    ("Hóa chất", [r"hóa chất", r"phân bón", r"nhựa", r"cao su"], False),
    ("Khai khoáng", [r"khai thác", r"khoáng sản", r"than"], False),
    ("Hàng không", [r"hàng không", r"sân bay", r"mặt đất"], False),
    ("Vận tải & Cảng", [r"vận tải", r"cảng", r"logistics", r"kho bãi", r"vận chuyển"], False),
    ("Du lịch & Khách sạn", [r"du lịch", r"khách sạn", r"lữ hành"], False),
]


def classify_sector(name, summary):
    """Phan loai nganh theo ten cong ty va mo ta hoat dong (VNDIRECT company_profiles); chi la uoc luong theo tu khoa."""
    nm = (name or "").lower()
    sm = re.sub(r"bưu điện", "", (summary or "").lower()[:500])
    for label, pats, name_only in SECTOR_RULES:
        if any(re.search(p, nm) for p in pats):
            return label
    for label, pats, name_only in SECTOR_RULES:
        if not name_only and any(re.search(p, sm) for p in pats):
            return label
    return ""


def sectors(syms):
    out, need = {}, []
    for x in syms:
        v = _cache.get("sec" + x)
        if v and time.time() - v[0] < 86400:
            out[x] = v[1]
        else:
            need.append(x)
    for i in range(0, len(need), 25):
        chunk = need[i:i + 25]
        try:
            q = "code:" + ",".join(chunk)
            j = fetch_json("https://api-finfo.vndirect.com.vn/v4/company_profiles?" + urllib.parse.urlencode({"q": q, "size": 50}, safe=":,"), 15)
            got = {e.get("code"): classify_sector(e.get("vnName"), e.get("vnSummary")) for e in j.get("data", [])}
        except Exception as e:
            print("nganh loi:", e, file=sys.stderr)
            got = {}
        for x in chunk:
            if x in got:
                with _lock:
                    _cache["sec" + x] = (time.time(), got[x])
                out[x] = got[x]
    return out


def stock_info(syms):
    news = []
    try:
        sset = set(syms)
        news = [n for n in stock_news() if sset & set(n["syms"])]
    except Exception as e:
        print("tin doanh nghiep loi:", e, file=sys.stderr)
    try:
        from concurrent.futures import ThreadPoolExecutor
        def one(x):
            try:
                return google_news(x)
            except Exception as e:
                print("google news %s loi: %s" % (x, e), file=sys.stderr)
                return []
        seen = {n["title"].lower() for n in news}
        with ThreadPoolExecutor(max_workers=6) as ex:
            for lst in ex.map(one, syms[:30]):
                for n in lst:
                    if n["title"].lower() not in seen:
                        seen.add(n["title"].lower())
                        news.append(n)
    except Exception as e:
        print("google news loi:", e, file=sys.stderr)
    events, errs = [], 0
    for s in syms[:40]:
        try:
            events += stock_events(s)
        except Exception as e:
            errs += 1
            print("su kien %s loi: %s" % (s, e), file=sys.stderr)
    return {"ok": True, "news": news[:600], "events": events, "evErr": errs, "time": time.strftime("%H:%M %d/%m", time.gmtime(time.time() + 7 * 3600))}


NEWS_FEEDS = [("VnEconomy", "https://vneconomy.vn/chung-khoan.rss"), ("VnEconomy", "https://vneconomy.vn/nhip-cau-doanh-nghiep.rss"),
              ("VnEconomy", "https://vneconomy.vn/tai-chinh.rss"),
              ("Vietstock", "https://vietstock.vn/830/chung-khoan/co-phieu.rss"),
              ("Vietstock PTKT", "https://vietstock.vn/585/chung-khoan/phan-tich-ky-thuat.rss"),
              ("CafeF", "https://cafef.vn/thi-truong-chung-khoan.rss")]
# Them nguon RSS khac: dat bien moi truong TIN_RSS, vi du:  TIN_RSS="TNCK|https://.../chung-khoan.rss,CafeF|https://.../x.rss"
for _x in (os.environ.get("TIN_RSS") or "").split(","):
    if "|" in _x:
        _n, _u = _x.split("|", 1)
        if _u.strip().startswith("https://"):
            NEWS_FEEDS.append((_n.strip() or "RSS", _u.strip()))
POS = [r"tăng trần", r"tăng mạnh", r"bứt phá", r"khởi sắc", r"lãi kỷ lục", r"lợi nhuận tăng", r"lãi tăng", r"tăng trưởng", r"mua ròng",
       r"vượt kế hoạch", r"hoàn thành (?:vượt )?kế hoạch", r"(?:chia|trả) cổ tức", r"hồi phục", r"nâng (?:dự báo|khuyến nghị|hạng|mục tiêu)",
       r"trúng thầu", r"ký (?:kết )?hợp đồng", r"lãi ròng", r"doanh thu tăng", r"lãi (?!suất)\d", r"tăng\s+\d", r"kỷ lục", r"tích cực"]
NEG = [r"giảm sâu", r"giảm mạnh", r"giảm sàn", r"bán tháo", r"bán ròng", r"báo lỗ", r"lỗ ròng", r"thua lỗ", r"lỗ quý", r"lỗ lũy kế", r"sụt giảm",
       r"vi phạm", r"bị phạt", r"xử phạt", r"đình chỉ", r"cảnh báo", r"hủy niêm yết", r"hạn chế giao dịch", r"diện (?:kiểm soát|cảnh báo)",
       r"hạ (?:dự báo|khuyến nghị|xếp hạng|triển vọng|mục tiêu)", r"nợ xấu", r"thanh tra", r"khởi tố", r"bắt tạm giam", r"lợi nhuận giảm",
       r"lãi giảm", r"doanh thu giảm", r"giảm lợi nhuận", r"chậm công bố", r"pha loãng", r"giải chấp", r"tiêu cực", r"giảm\s+\d"]


def news_sentiment(text):
    t = text.lower()
    sc = sum(1 for w in POS if re.search(w, t)) - sum(1 for w in NEG if re.search(w, t))
    return 1 if sc > 0 else -1 if sc < 0 else 0


def fetch_text(url, timeout=10):
    req = urllib.request.Request(url, headers=UA)
    with _UP:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8", "replace")


def load_news():
    items, seen = [], set()
    for src, url in NEWS_FEEDS:
        try:
            root = ET.fromstring(fetch_text(url))
        except Exception as e:
            print("tin tuc loi", url, e, file=sys.stderr)
            continue
        for it in root.iter("item"):
            title = (it.findtext("title") or "").strip()
            link = (it.findtext("link") or "").strip()
            if not title or link in seen:
                continue
            seen.add(link)
            desc = re.sub(r"<[^>]+>", " ", it.findtext("description") or "")
            try:
                ts = int(email.utils.parsedate_to_datetime(it.findtext("pubDate")).timestamp())
            except Exception:
                ts = 0
            items.append({"title": title, "link": link, "t": ts, "src": src, "desc": desc.strip()[:240]})
    items.sort(key=lambda x: -x["t"])
    return items[:150]


def news_payload(syms):
    ss = set(syms)
    out = []
    for it in cached("news", 300, load_news):
        txt = it["title"] + " " + it["desc"]
        tags = sorted({m for m in re.findall(r"\b[A-Z][A-Z0-9]{2}\b", txt) if m in ss})
        out.append({"title": it["title"], "link": it["link"], "t": it["t"], "src": it["src"], "syms": tags,
                    "sent": news_sentiment(txt)})
    srcs = {}
    for o in out:
        srcs[o["src"]] = srcs.get(o["src"], 0) + 1
    return {"ok": True, "items": out[:120], "srcs": srcs}


def send(h, code, obj):
    body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
    h.send_response(code)
    h.send_header("Content-Type", "application/json; charset=utf-8")
    h.send_header("Access-Control-Allow-Origin", "*")
    h.send_header("Cache-Control", "no-store")
    h.send_header("Content-Length", str(len(body)))
    h.end_headers()
    h.wfile.write(body)


class H(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "*")
        self.end_headers()

    def authorized(self):
        if not APP_PASSWORD:
            return True
        h = self.headers.get("Authorization", "")
        if h.startswith("Basic "):
            try:
                pw = base64.b64decode(h[6:]).decode("utf-8").split(":", 1)[1]
                if hmac.compare_digest(pw, APP_PASSWORD):
                    return True
            except Exception:
                pass
        self.send_response(401)
        self.send_header("WWW-Authenticate", 'Basic realm="VN Stock Analyzer", charset="UTF-8"')
        self.send_header("Content-Length", "0")
        self.end_headers()
        return False

    def do_GET(self):
        if not self.authorized():
            return
        u = urllib.parse.urlparse(self.path)
        qs = urllib.parse.parse_qs(u.query)
        sym = (qs.get("symbol", [""])[0] or "").upper().strip()
        try:
            if u.path == "/api/ping":
                return send(self, 200, {"ok": True, "server": "vn-stock-analyzer"})
            if u.path == "/api/status":
                return send(self, 200, {"ok": True, "fireant_token": bool(fireant_token()), "tests": self_test()})
            if u.path == "/api/stocknews":
                syms = [x for x in (qs.get("symbols", [""])[0] or "").upper().split(",") if x.isalnum() and len(x) <= 5][:40]
                return send(self, 200, stock_info(syms))
            if u.path == "/api/dividends":
                d = dividends()
                return send(self, 200, {"ok": True, "n": len(d), "rows": d, "source": "VNDIRECT finfo"})
            if u.path == "/api/symbols":
                bd = board()
                rows = [[k, v.get("ex", ""), v["p"], v["ref"], v["vol"], v["h"], v["lo"]] for k, v in bd.items() if k.isalnum() and len(k) <= 5]
                return send(self, 200, {"ok": True, "n": len(rows), "time": max([v.get("time", "") for v in bd.values()] or [""]), "rows": rows})
            if u.path == "/api/scan":
                syms = [x for x in (qs.get("symbols", [""])[0] or "").upper().split(",") if x.isalnum() and len(x) <= 5][:150]
                scan_start(syms)
                return send(self, 200, scan_payload(syms))
            if u.path == "/api/news":
                syms = [x for x in (qs.get("symbols", [""])[0] or "").upper().split(",") if x.isalnum() and len(x) <= 5][:120]
                return send(self, 200, news_payload(syms))
            if u.path == "/api/sectors":
                syms = [x for x in (qs.get("symbols", [""])[0] or "").upper().split(",") if x.isalnum() and len(x) <= 5][:60]
                return send(self, 200, {"ok": True, "sectors": sectors(syms)})
            if u.path == "/api/ratios":
                if not sym.isalnum() or len(sym) > 5:
                    return send(self, 400, {"ok": False, "error": "ma khong hop le"})
                return send(self, 200, {"ok": True, "symbol": sym, "ratios": cached("r" + sym, 6 * 3600, lambda: ratios(sym))})
            if u.path == "/api/history":
                if not sym.isalnum() or len(sym) > 5:
                    return send(self, 400, {"ok": False, "error": "ma khong hop le"})
                c, src = cached("h" + sym, 60, lambda: history(sym, 420))
                return send(self, 200, {"ok": True, "source": src, "symbol": sym, "candles": c})
            if u.path == "/api/bars":
                if not sym.isalnum() or len(sym) > 5:
                    return send(self, 400, {"ok": False, "error": "ma khong hop le"})
                nd = 1 if (qs.get("days", ["1"])[0] == "1") else 3
                b, src = cached("b%s%d" % (sym, nd), 20, lambda: bars_multi(sym, nd))
                return send(self, 200, {"ok": True, "source": src, "symbol": sym, "days": nd, "bars": b})
            if u.path == "/api/intraday":
                if not sym.isalnum() or len(sym) > 5:
                    return send(self, 400, {"ok": False, "error": "ma khong hop le"})
                b, src = cached("i" + sym, 3, lambda: intraday(sym))
                return send(self, 200, {"ok": True, "source": src, "symbol": sym, "bars": b, "now": int(time.time())})
            if u.path == "/api/quotes":
                syms = [s for s in (qs.get("symbols", [""])[0] or "").upper().split(",") if s.isalnum() and len(s) <= 5][:120]
                out = {}
                try:
                    bd = board()
                    for s in syms:
                        if s in bd:
                            q = bd[s]
                            out[s] = {"src": "CafeF", "p": q["p"], "ref": q["ref"], "h": q["h"], "lo": q["lo"], "vol": q["vol"], "time": q["time"], "bid": q.get("bid"), "ask": q.get("ask"), "lv": q.get("lv", 0)}
                    if out:
                        prefetch_history(syms)
                        for s in syms:  # kem lich su 10 phien (neu co nguon) de tinh dong tien nganh
                            try:
                                hc = _cache.get("q" + s) or _cache.get("h" + s)
                                if hc:
                                    out[s]["hist"] = [{"o": x["o"], "c": x["c"], "v": x["v"]} for x in hc[1][0][-10:]]
                            except Exception:
                                pass
                        return send(self, 200, {"ok": True, "quotes": out})
                except Exception as ex:
                    print("quotes: dung bang gia CafeF that bai, chuyen sang lich su:", ex, file=sys.stderr)
                for s in syms:
                    try:
                        c, qsrc = cached("q" + s, 15, lambda s=s: history(s, 25))
                        if len(c) >= 2:
                            out[s] = {"src": qsrc, "p": c[-1]["c"], "ref": c[-2]["c"],
                                      "hist": [{"o": x["o"], "c": x["c"], "v": x["v"]} for x in c[-10:]]}
                    except Exception:
                        pass
                return send(self, 200, {"ok": bool(out), "quotes": out})
        except Exception as e:
            return send(self, 502, {"ok": False, "error": str(e)})
        # static
        name = "VN-Stock-Analyzer.html" if u.path in ("/", "/index.html") else None
        p = os.path.join(HERE, name) if name else None
        if p and os.path.exists(p):
            data = open(p, "rb").read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        else:
            self.send_response(404)
            self.end_headers()


def self_test():
    """Kiem tra nhanh tung nguon khi khoi dong va in ket qua de de chan doan."""
    print("Kiem tra cac nguon du lieu (cho vai giay)...")
    res = {}
    tests = [("CafeF (bang gia)", lambda: [board()["FPT"] and {"c": board()["FPT"]["p"]}]),
             ("FireAnt", lambda: parse_fireant(fireant_rows("FPT", 20)) if fireant_token() else (_ for _ in ()).throw(RuntimeError("chua co fireant_token.txt"))),
             ("DNSE/Entrade", lambda: parse_entrade(fetch_json(ENTRADE + "?" + urllib.parse.urlencode({"from": int(time.time()) - 20 * 86400, "to": int(time.time()) + 86400, "symbol": "FPT", "resolution": "1D"})))),
             ("VNDIRECT (P/E, ROE)", lambda: [{"c": ratios("FPT")["pe"] or 0}]),
             ("VNDIRECT", lambda: parse_vndirect(fetch_json(VNDIRECT + "?" + urllib.parse.urlencode({"sort": "date", "size": 10, "q": "code:FPT"}))))]
    for name, fn in tests:
        try:
            c = fn()
            res[name] = "OK" if c else "LOI: khong co du lieu"
            if c:
                res[name] += " (FPT gan nhat: %.2f nghin dong)" % c[-1]["c"]
        except Exception as e:
            res[name] = "LOI: %s" % e
        print("  - %-13s %s" % (name, res[name]))
    ok = [k for k, v in res.items() if v.startswith("OK")]
    print("=> Dang dung:", ok[0] if ok else "KHONG co nguon nao - trang se chay che do mo phong")
    if "FireAnt" in res and not res["FireAnt"].startswith("OK") and fireant_token():
        print("   Goi y: token FireAnt co the sai/het han - lay lai token moi roi dan vao fireant_token.txt")
    return res


if __name__ == "__main__":
    srv = ThreadingHTTPServer((HOST, PORT), H)
    url = "http://localhost:%d" % PORT
    if HOST != "127.0.0.1" and not APP_PASSWORD:
        print("CANH BAO: dang mo ra ngoai ma chua dat APP_PASSWORD - ai co dia chi cung xem duoc.")
    print("VN Stock Analyzer dang chay tai", url, "(Ctrl+C de dung)")
    threading.Thread(target=self_test, daemon=True).start()
    if os.environ.get("NO_BROWSER") != "1":
        threading.Timer(1, lambda: webbrowser.open(url)).start()
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
