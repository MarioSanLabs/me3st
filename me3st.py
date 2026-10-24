#!/usr/bin/env python3
"""
Ranking de listas Xtream IPTV por fecha de caducidad (solo las que funcionan).

Uso:
    python ranking_iptv.py                 -> pegas las listas y escribes FIN
    python ranking_iptv.py mis_listas.txt  -> lee las listas desde un archivo

Novedades:
    - Barra de progreso con porcentaje mientras se comprueban las listas.
    - El ranking se guarda en tu carpeta de Descargas (y se verifica que existe).
    - Al terminar: 1 = ver detalle de una lista, 2 = empezar de nuevo.

Formatos de entrada admitidos (uno por línea):
    http://host:puerto/get.php?username=USER&password=PASS&type=m3u_plus
    http://host:puerto/player_api.php?username=USER&password=PASS
    http://host:puerto/live/USER/PASS/12345.ts
    http://host:puerto|USER|PASS   (también separado por espacios, comas o ;)

No necesita instalar nada (solo librería estándar de Python 3.7+).
"""

import csv
import io
import json
import os
import random
import re
import shutil
import subprocess
import sys
import threading
import time
import unicodedata
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from urllib.parse import parse_qs, quote, urljoin, urlparse
from urllib.request import Request, urlopen

TIMEOUT = 8               # segundos de espera por servidor
WORKERS = 100             # listas comprobadas a la vez (sube o baja según tu conexión)
IGNORAR_ILIMITADAS = True # True = las listas sin fecha de caducidad no entran en el ranking
USER_AGENT = "Mozilla/5.0"
STREAM_UA = "VLC/3.0.18 LibVLC/3.0.18"   # muchos servidores bloquean otros agentes en los canales
OUTPUT_NAME = "ranking_iptv.txt"
STREAM_TEST_SECONDS = 8   # duración de la prueba de estabilidad por canal
HOST_LIMIT = 4            # peticiones simultáneas máximas por servidor (evita saturarlo)
DEAD_AFTER = 4            # fallos seguidos sin ningún éxito = servidor caído (se salta el resto)
CHANNEL_DELAY = 0.6       # pausa entre canales para no saturar el servidor
RETRIES_429 = 3           # reintentos cuando el servidor dice "demasiadas peticiones"
STALL_GAP = 2.0           # segundos sin datos = un "corte"
BAR_WIDTH = 30


# ---------------------------------------------------------------- lectura ---
def read_input(use_file=True):
    if use_file and len(sys.argv) > 1:
        with open(sys.argv[1], encoding="utf-8", errors="ignore") as f:
            return f.read().splitlines()

    print("Pega tus listas (una por línea). Cuando termines escribe FIN y pulsa Enter")
    print("(o usa Ctrl+D en Linux/Mac, Ctrl+Z + Enter en Windows):\n")
    lines = []
    try:
        while True:
            line = input()
            if line.strip().upper() == "FIN":
                break
            lines.append(line)
    except EOFError:
        pass
    return lines


def parse_line(line):
    """Devuelve dict con base, user, pw, raw o None si no se reconoce."""
    line = line.strip()
    if not line:
        return None

    m = re.search(r"https?://\S+", line)
    if m:
        url = m.group(0)
        p = urlparse(url)
        base = f"{p.scheme}://{p.netloc}"
        q = parse_qs(p.query)

        if "username" in q and "password" in q:
            return {"base": base, "user": q["username"][0], "pw": q["password"][0], "raw": line}

        parts = [x for x in p.path.split("/") if x]
        if parts and parts[0] in ("live", "movie", "series"):
            parts = parts[1:]
        if len(parts) >= 2:
            return {"base": base, "user": parts[0], "pw": parts[1], "raw": line}

        rest = re.split(r"[|\s;,]+", line)
        if len(rest) >= 3 and rest[0].startswith("http"):
            return {"base": rest[0].rstrip("/"), "user": rest[1], "pw": rest[2], "raw": line}
        return None

    rest = re.split(r"[|\s;,]+", line)
    if len(rest) >= 3:
        return {"base": "http://" + rest[0].rstrip("/"), "user": rest[1], "pw": rest[2], "raw": line}
    return None


# ------------------------------------------------------------ comprobación ---
_host_lock = threading.Lock()
_hosts = {}


def host_state(base):
    with _host_lock:
        if base not in _hosts:
            _hosts[base] = {"sem": threading.Semaphore(HOST_LIMIT), "ok": 0, "fail": 0}
        return _hosts[base]


def api(entry, action=None, timeout=TIMEOUT, extra="", retries=0):
    """Petición a player_api.php con límite por servidor y reintentos."""
    url = (
        f"{entry['base']}/player_api.php"
        f"?username={quote(entry['user'])}&password={quote(entry['pw'])}"
    )
    if action:
        url += f"&action={action}"
    url += extra
    st = host_state(entry["base"])
    last = None
    for attempt in range(retries + 1):
        if st["ok"] == 0 and st["fail"] >= DEAD_AFTER:
            raise ConnectionError("servidor caído")
        try:
            with st["sem"]:
                req = Request(url, headers={"User-Agent": USER_AGENT})
                with urlopen(req, timeout=timeout) as r:
                    data = json.loads(r.read().decode("utf-8", "ignore"))
            st["ok"] += 1
            return data
        except Exception as e:
            last = e
            if attempt < retries:
                time.sleep(min(1.5 * (2 ** attempt), 6))
    st["fail"] += 1
    raise last


def get_categories(entry, action):
    """Lista de nombres de categorías, o None si el servidor no respondió."""
    try:
        data = api(entry, action, timeout=15, retries=3)
    except Exception:
        return None
    if not isinstance(data, list):
        return []
    return [str(c.get("category_name", "?")) for c in data if isinstance(c, dict)]


def check(entry):
    result = {**entry, "ok": False, "reason": "", "exp": None, "created": None,
              "trial": False, "cons": "?", "max_cons": "?", "formats": [],
              "live": None, "vod": None, "series": None}
    try:
        data = api(entry, retries=1)
    except Exception:
        result["reason"] = "Sin conexión / servidor caído"
        return result

    ui = data.get("user_info") if isinstance(data, dict) else None
    if not ui or str(ui.get("auth")) != "1":
        result["reason"] = "Credenciales inválidas"
        return result

    status = str(ui.get("status", ""))
    exp = ui.get("exp_date")
    result["trial"] = str(ui.get("is_trial")) == "1"
    result["cons"] = ui.get("active_cons", "?")
    result["max_cons"] = ui.get("max_connections", "?")
    fm = ui.get("allowed_output_formats")
    result["formats"] = [str(x).lower() for x in fm] if isinstance(fm, list) else []
    try:
        result["created"] = float(ui.get("created_at"))
    except (TypeError, ValueError):
        result["created"] = None

    if exp in (None, "", "null"):
        if IGNORAR_ILIMITADAS:
            result["ignored"] = True
            return result
        result["exp"] = float("inf")
    else:
        try:
            result["exp"] = float(exp)
        except ValueError:
            result["reason"] = "Fecha de caducidad ilegible"
            return result

    if status != "Active":
        result["reason"] = f"Estado: {status}"
        return result
    if result["exp"] < time.time():
        result["reason"] = "Caducada"
        return result

    result["ok"] = True
    # TV, VOD y Series se piden a la vez (en vez de una detrás de otra)
    with ThreadPoolExecutor(max_workers=3) as ex:
        f_live = ex.submit(get_categories, entry, "get_live_categories")
        f_vod = ex.submit(get_categories, entry, "get_vod_categories")
        f_series = ex.submit(get_categories, entry, "get_series_categories")
        result["live"] = f_live.result()
        result["vod"] = f_vod.result()
        result["series"] = f_series.result()
    return result


def progress(done, total):
    pct = done * 100 // total
    filled = pct * BAR_WIDTH // 100
    bar = "█" * filled + "░" * (BAR_WIDTH - filled)
    print(f"\r[{bar}] {pct:3d}%  ({done}/{total})", end="", flush=True)


def interleave(entries):
    """Reparte las listas por servidor para que ninguno acapare todos los hilos."""
    groups = {}
    for e in entries:
        groups.setdefault(e["base"], []).append(e)
    out, cols = [], list(groups.values())
    while cols:
        for g in cols:
            out.append(g.pop(0))
        cols = [g for g in cols if g]
    return out


def second_pass(results):
    """Reintenta las que fallaron por saturación y completa categorías que quedaron en '?'."""
    redo = [i for i, r in enumerate(results)
            if not r["ok"] and str(r.get("reason", "")).startswith("Sin conexión")
            and host_state(r["base"])["ok"] > 0]
    if redo:
        print(f"Reintentando {len(redo)} listas que fallaron por saturación...")
        progress(0, len(redo))
        with ThreadPoolExecutor(max_workers=10) as ex:
            futs = {ex.submit(check, {k: results[i][k] for k in ("base", "user", "pw", "raw")}): i
                    for i in redo}
            for done, f in enumerate(as_completed(futs), 1):
                results[futs[f]] = f.result()
                progress(done, len(redo))
        print("\n")

    miss = [r for r in results if r["ok"] and None in (r["live"], r["vod"], r["series"])]
    if miss:
        print(f"Completando categorías de {len(miss)} listas...")
        progress(0, len(miss))

        def fill(r):
            for key, act in (("live", "get_live_categories"), ("vod", "get_vod_categories"),
                             ("series", "get_series_categories")):
                if r[key] is None:
                    r[key] = get_categories(r, act)

        with ThreadPoolExecutor(max_workers=12) as ex:
            for done, f in enumerate(as_completed([ex.submit(fill, r) for r in miss]), 1):
                f.result()
                progress(done, len(miss))
        print("\n")
    return results


def check_all(entries):
    results = []
    total = len(entries)
    progress(0, total)
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = [ex.submit(check, e) for e in interleave(entries)]
        for done, fut in enumerate(as_completed(futures), 1):
            results.append(fut.result())
            progress(done, total)
    print("\n")
    return results


# ------------------------------------------------------------------ salida ---
def fmt_date(ts):
    if ts is None:
        return "Desconocida"
    return "Ilimitada" if ts == float("inf") else datetime.fromtimestamp(ts).strftime("%d/%m/%Y")


def fmt_days(ts):
    return "∞" if ts == float("inf") else str(int((ts - time.time()) // 86400))


def fmt_tv(cats):
    return "?" if cats is None else f"{len(cats)} categorías"


def fmt_extra(cats):
    if cats is None:
        return "?"
    return "No" if len(cats) == 0 else f"Sí ({len(cats)} cat.)"


def full_link(r):
    m = re.search(r"https?://\S+", r["raw"])
    if m:
        return m.group(0)
    return (f"{r['base']}/get.php?username={r['user']}&password={r['pw']}"
            f"&type=m3u_plus&output=ts")


def describe(i, r):
    cons = f"{r['cons']}/{r['max_cons']}"
    trial = "Sí" if r["trial"] else "No"
    return (
        f"#{i}  Caduca: {fmt_date(r['exp'])} ({fmt_days(r['exp'])} días)"
        f"  |  Conexiones: {cons}  |  Trial: {trial}\n"
        f"    TV: {fmt_tv(r['live'])}  |  VOD: {fmt_extra(r['vod'])}  |  Series: {fmt_extra(r['series'])}\n"
        f"    {full_link(r)}\n"
    )


def _show_detail_base(i, r):
    print("\n" + "=" * 60)
    print(f"Lista nº {i}")
    print("=" * 60)
    print(f"Creada:   {fmt_date(r['created'])}")
    print(f"Caduca:   {fmt_date(r['exp'])} ({fmt_days(r['exp'])} días restantes)")
    print(f"Enlace:   {full_link(r)}\n")

    sections = (
        ("TV en directo", r["live"], True),
        ("VOD (películas)", r["vod"], False),
        ("Series", r["series"], False),
    )
    for title, cats, always in sections:
        if cats is None:
            if always:
                print(f"{title}: no se pudo consultar\n")
            continue
        if not cats:
            if always:
                print(f"{title}: sin categorías\n")
            continue  # VOD / Series: solo se muestran si hay
        print(f"{title} - {len(cats)} categorías:")
        for c in cats:
            print(f"  • {c}")
        print()


# ------------------------------------------- calidad y estabilidad (opción 1) ---
QUALITY_ORDER = ["4K", "FHD", "HD", "SD"]
_B = r"(?<![A-Za-z0-9])(%s)(?![A-Za-z0-9])"
QUALITY_RE = [
    ("4K", re.compile(_B % "4K|UHD|2160P?", re.I)),
    ("FHD", re.compile(_B % "FHD|1080[PI]?", re.I)),
    ("HD", re.compile(_B % "HD|720P?", re.I)),
    ("SD", re.compile(_B % "SD|480P?", re.I)),
]


def detect_quality(text):
    for q, rx in QUALITY_RE:
        if rx.search(text or ""):
            return q
    return None


def get_live_streams(entry):
    try:
        data = api(entry, "get_live_streams", timeout=40, retries=2)
    except Exception:
        return None
    return data if isinstance(data, list) else []


def probe_resolution(url):
    """Resolución real con ffprobe, solo si está instalado."""
    if not shutil.which("ffprobe"):
        return None
    try:
        out = subprocess.run(
            ["ffprobe", "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=width,height", "-of", "csv=p=0", url],
            capture_output=True, text=True, timeout=20).stdout.strip()
        w, h = out.split(",")[:2]
        return f"{w}x{h}"
    except Exception:
        return None


def stream_urls(r, sid):
    """URLs a probar (ts y/o m3u8) según los formatos que permita el servidor."""
    base = f"{r['base']}/live/{quote(r['user'])}/{quote(r['pw'])}/{sid}"
    fm = r.get("formats") or []
    out = []
    if not fm or "ts" in fm:
        out.append(("ts", base + ".ts"))
    if not fm or "m3u8" in fm:
        out.append(("m3u8", base + ".m3u8"))
    return out or [("ts", base + ".ts"), ("m3u8", base + ".m3u8")]


def err_name(e):
    code = getattr(e, "code", None)
    return f"HTTP {code}" if code else type(e).__name__


def open_req(url, timeout=6):
    return urlopen(Request(url, headers={"User-Agent": STREAM_UA}), timeout=timeout)


def iter_chunks(url, fmt, deadline):
    """Devuelve trozos de vídeo de un canal en directo hasta 'deadline' (ts o HLS)."""
    if fmt == "ts":
        with open_req(url) as resp:
            read = getattr(resp, "read1", resp.read)
            while time.time() < deadline:
                chunk = read(16384)
                if not chunk:
                    return
                yield chunk
        return

    seen, pl_url = set(), url
    while time.time() < deadline:
        with open_req(pl_url) as resp:
            final = resp.geturl()
            text = resp.read(300000).decode("utf-8", "ignore")
        if not text.lstrip().startswith("#EXTM3U"):
            raise ValueError("respuesta no válida")
        lines = [l.strip() for l in text.splitlines() if l.strip() and not l.startswith("#")]
        if not lines:
            raise ValueError("lista vacía")
        if any(".m3u8" in l.split("?")[0] for l in lines):   # lista maestra
            pl_url = urljoin(final, lines[0])
            continue
        new = [l for l in lines if l not in seen]
        seen.update(lines)
        if not new:
            time.sleep(1)
            continue
        for l in new[-2:]:
            with open_req(urljoin(final, l)) as seg:
                while time.time() < deadline:
                    chunk = seg.read(16384)
                    if not chunk:
                        break
                    yield chunk


def _test_one(url, fmt, seconds):
    res = {"bytes": 0, "stalls": 0, "secs": 0.0, "error": None, "aborted": False, "fmt": fmt}
    start = last = time.time()
    try:
        deadline = start + seconds
        started = False
        for chunk in iter_chunks(url, fmt, deadline):
            now = time.time()
            if not started:
                start = last = now
                deadline = start + seconds
                started = True
            elif now - last > STALL_GAP:
                res["stalls"] += 1
            res["bytes"] += len(chunk)
            last = now
        if started and time.time() < deadline - 0.5 and fmt == "ts":
            res["aborted"] = True        # el servidor cortó la conexión
    except Exception as e:
        res["error"] = err_name(e)
        res["aborted"] = True
    res["secs"] = max(time.time() - start, 0.001)
    res["mbps"] = res["bytes"] * 8 / res["secs"] / 1e6
    return res


def test_stream(r, sid, seconds=STREAM_TEST_SECONDS):
    """Prueba un canal en directo probando los formatos permitidos."""
    errors, last = [], None
    for fmt, url in stream_urls(r, sid):
        res = _test_one(url, fmt, seconds)
        if res["bytes"] > 0:
            return res
        errors.append(f"{fmt}: {res['error'] or 'sin datos'}")
        last = res
    last["error"] = "; ".join(errors)
    return last


def verdict(t, seconds=STREAM_TEST_SECONDS):
    """0 = fino, 1 = algún corte, 2 = se corta mucho, 3 = no se pudo probar."""
    if t["bytes"] == 0:
        return 3
    if (t["aborted"] and t["secs"] < seconds * 0.8) or t["stalls"] >= 3 or t["mbps"] < 0.3:
        return 2
    if t["stalls"] >= 1:
        return 1
    return 0


VERDICT_TXT = {
    0: "✔ Va fino, sin cortes",
    1: "~ Algún corte puntual",
    2: "✘ Se corta mucho",
    3: "? No se pudo probar (¿conexiones ocupadas o canal caído?)",
}


def show_quality(r):
    print("-" * 60)
    print("Comprobando calidad y estabilidad (unos segundos)...")
    streams = get_live_streams(r)
    if not streams:
        print("No se pudo obtener la lista de canales para analizar la calidad.\n")
        return

    counts = {q: 0 for q in QUALITY_ORDER}
    tagged = []
    for st in streams:
        if not isinstance(st, dict) or not st.get("stream_id"):
            continue
        q = detect_quality(str(st.get("name", "")))
        if q:
            counts[q] += 1
        tagged.append((q, st))

    best_q = next((q for q in QUALITY_ORDER if counts[q]), None)
    print(f"\nCalidad máxima: {best_q or 'no indicada en los nombres de canal'}")
    print("  (según las etiquetas de los canales: "
          + "  ".join(f"{q}: {counts[q]}" for q in QUALITY_ORDER) + ")")

    if not tagged:
        print()
        return

    # canal de mejor calidad + uno al azar
    rank = {q: i for i, q in enumerate(QUALITY_ORDER)}
    tagged.sort(key=lambda x: rank.get(x[0], 99))
    picks = [tagged[0]]
    n_tests = 1 if str(r["max_cons"]) == "1" else 2
    if n_tests > 1 and len(tagged) > 1:
        picks.append(random.choice(tagged[1:]))

    res_real = probe_resolution(stream_urls(r, picks[0][1]["stream_id"])[0][1])
    if res_real:
        print(f"Resolución real del canal de prueba: {res_real}")

    print(f"\nPrueba de estabilidad ({STREAM_TEST_SECONDS} s por canal):")
    worst = 0
    for q, st in picks:
        t = test_stream(r, st["stream_id"])
        v = verdict(t)
        worst = max(worst, v)
        name = str(st.get("name", "?"))[:40]
        extra = f"{t['mbps']:.1f} Mbps, {t['stalls']} cortes ({t['fmt']})"
        if t["error"] and t["bytes"] == 0:
            extra = f"error: {t['error']}"
            print(f"    URL probada: {stream_urls(r, st['stream_id'])[0][1]}")
        print(f"  • {name} [{q or '-'}]: {extra} → {VERDICT_TXT[v]}")

    print(f"\nVeredicto: {VERDICT_TXT[worst]}")
    print("(Es una muestra corta; la estabilidad real varía según la hora y tu conexión.)\n")


def show_detail(i, r):
    _show_detail_base(i, r)
    show_quality(r)


# ------------------------------------- opción 3: comprobar categorías enteras ---
def parse_selection(text, n):
    """'1 4 7-26' -> ([1, 4, 7, ..., 26], [tokens inválidos])."""
    chosen, bad = set(), []
    for tok in text.replace(",", " ").split():
        tok = tok.lstrip("#")
        m = re.fullmatch(r"(\d+)-(\d+)", tok)
        if m:
            a, b = sorted((int(m.group(1)), int(m.group(2))))
            if a >= 1 and b <= n:
                chosen.update(range(a, b + 1))
            else:
                bad.append(tok)
        elif tok.isdigit() and 1 <= int(tok) <= n:
            chosen.add(int(tok))
        else:
            bad.append(tok)
    return sorted(chosen), bad


def test_channel(r, st):
    """Comprueba que un canal da señal y su calidad (con reintentos si hay límite de peticiones)."""
    name = str(st.get("name", "?"))
    out = {"name": name, "ok": False, "tag": detect_quality(name), "res": None,
           "err": None, "url": None, "fmt": None, "rate": False}
    errors = []
    for attempt in range(RETRIES_429 + 1):
        errors, limited, wait = [], False, 0
        for fmt, url in stream_urls(r, st["stream_id"]):
            out["url"] = out["url"] or url
            got = 0
            try:
                for chunk in iter_chunks(url, fmt, time.time() + 7):
                    got += len(chunk)
                    if got >= 65536:
                        break
                if got >= 16384:
                    out.update(ok=True, fmt=fmt, url=url)
                    break
                errors.append(f"{fmt}: sin datos")
            except Exception as e:
                errors.append(f"{fmt}: {err_name(e)}")
                if getattr(e, "code", None) == 429:
                    limited = True
                    try:
                        wait = int(e.headers.get("Retry-After", 0))
                    except Exception:
                        wait = 0
                    break          # no es un problema de formato: no probar el otro
        if out["ok"] or not limited or attempt == RETRIES_429:
            out["rate"] = limited and not out["ok"]
            break
        time.sleep(max(wait, 3 * (attempt + 1)))   # esperar y reintentar
    if out["ok"]:
        out["res"] = probe_resolution(out["url"])
    else:
        out["err"] = ("límite de peticiones del servidor (HTTP 429)" if out["rate"]
                      else "; ".join(errors))
    time.sleep(CHANNEL_DELAY)
    return out


def category_check(alive):
    n = input(f"Número de la lista (sin #, del 1 al {len(alive)}): ").strip().lstrip("#")
    if not (n.isdigit() and 1 <= int(n) <= len(alive)):
        print("Número no válido.")
        return
    r = alive[int(n) - 1]

    try:
        cats = api(r, "get_live_categories", timeout=30, retries=3)
    except Exception:
        cats = None
    if not isinstance(cats, list) or not cats:
        print("No se pudieron obtener las categorías de TV de esta lista.")
        return
    cats = [c for c in cats if isinstance(c, dict)]

    print(f"\nCategorías de TV de la lista {n}:\n")
    for i, c in enumerate(cats, 1):
        print(f"  {i:>3}. {c.get('category_name', '?')}")
    print("\nEscribe las categorías que quieras separadas por espacios.")
    print("Para un rango usa guion, por ejemplo:  3 5 7-26")
    sel, bad = parse_selection(input("> "), len(cats))
    if bad:
        print(f"Ignorado (no válido): {' '.join(bad)}")
    if not sel:
        print("No has elegido ninguna categoría.")
        return

    # canales de cada categoría
    plan = []   # (nombre categoría, [streams])
    for i in sel:
        c = cats[i - 1]
        try:
            streams = api(r, "get_live_streams", timeout=40, retries=2,
                          extra=f"&category_id={quote(str(c.get('category_id')))}")
        except Exception:
            streams = []
        streams = [st for st in streams if isinstance(st, dict) and st.get("stream_id")] \
            if isinstance(streams, list) else []
        plan.append((f"{i}. {c.get('category_name', '?')}", streams))

    total = sum(len(s) for _, s in plan)
    if total == 0:
        print("Las categorías elegidas no tienen canales.")
        return

    # conexiones libres AHORA (otras personas pueden estar usando la lista)
    try:
        ui = api(r, timeout=15, retries=2).get("user_info", {})
        mx = int(ui.get("max_connections", 1))
        used = int(ui.get("active_cons", 0))
    except Exception:
        mx, used = 1, 0
    free = mx - used
    workers = max(1, min(free, 10))
    print(f"\nConexiones de la lista: {used} en uso de {mx}.")
    if free < 1:
        print("⚠ No quedan conexiones libres: puede que los canales fallen. "
              "Prueba más tarde o cuando nadie la use.")
    print(f"{total} canales en {len(plan)} categorías. Se comprueban {workers} a la vez "
          f"para no pasarse del límite.")
    if not shutil.which("ffprobe"):
        print("(Sin ffprobe: la calidad sale de las etiquetas del nombre del canal.)")
    if total > 100 and input("Puede tardar bastante. ¿Continuar? (s/n): ").strip().lower() != "s":
        return
    print()

    results = {}   # (idx categoría, idx canal) -> resultado
    progress(0, total)
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(test_channel, r, st): (ci, si)
                for ci, (_, streams) in enumerate(plan) for si, st in enumerate(streams)}
        for done, fut in enumerate(as_completed(futs), 1):
            results[futs[fut]] = fut.result()
            progress(done, total)
    print("\n")

    lines = []
    example_fail = None
    for ci, (cname, streams) in enumerate(plan):
        res = [results[(ci, si)] for si in range(len(streams))]
        n_ok = sum(1 for x in res if x["ok"])
        state = "✔ TODOS funcionan" if n_ok == len(res) and res else (f"{len(res) - n_ok} falla" if len(res) - n_ok == 1 else f"{len(res) - n_ok} fallan")
        lines.append(f"=== {cname} — {n_ok}/{len(res)} funcionan ({state}) ===")
        if n_ok < len(res):
            motivos = Counter(x["err"] for x in res if not x["ok"])
            lines.append("  Motivos: " + ", ".join(f"{k} ×{v}" for k, v in motivos.most_common(3)))
        for x in res:
            if x["ok"]:
                q = x["res"] or x["tag"] or "?"
                tag = f" [{x['tag']}]" if x["tag"] and x["res"] else ""
                lines.append(f"  ✔ {x['name']}  →  {q}{tag}")
            else:
                lines.append(f"  ✘ {x['name']}  →  sin señal ({x['err']})")
                if example_fail is None:
                    example_fail = x["url"]
        lines.append("")

    n_rate = sum(1 for ci in range(len(plan)) for si in range(len(plan[ci][1]))
                 if results[(ci, si)].get("rate"))
    if n_rate:
        lines.append(f"⚠ {n_rate} canales fallaron por límite de peticiones (HTTP 429), no porque no existan.")
        lines.append("  Espera unos minutos sin usar la lista y repite con pocas categorías.\n")
    if example_fail:
        lines.append("Si fallan todos, copia esta URL en VLC para ver si el problema es del servidor:")
        lines.append(f"  {example_fail}\n")
    report = "\n".join(lines)
    print(report)
    path = save_text("ranking_iptv_categorias.txt", report)
    print(f"✔ Informe guardado y verificado en:\n  {path}" if path else "✘ No se pudo guardar el informe.")


# --------------------------------------------------- carpeta de descargas ---
def downloads_dir():
    home = Path.home()
    candidates = []

    if sys.platform == "win32":
        try:
            import ctypes

            class GUID(ctypes.Structure):
                _fields_ = [("a", ctypes.c_ulong), ("b", ctypes.c_ushort),
                            ("c", ctypes.c_ushort), ("d", ctypes.c_ubyte * 8)]

            guid = GUID(0x374DE290, 0x123F, 0x4565,
                        (ctypes.c_ubyte * 8)(0x91, 0x64, 0x39, 0xC4, 0x92, 0x5E, 0x46, 0x7B))
            buf = ctypes.c_wchar_p()
            if ctypes.windll.shell32.SHGetKnownFolderPath(
                    ctypes.byref(guid), 0, None, ctypes.byref(buf)) == 0:
                candidates.append(Path(buf.value))
        except Exception:
            pass
    else:
        try:
            out = subprocess.run(["xdg-user-dir", "DOWNLOAD"], capture_output=True,
                                 text=True, timeout=3).stdout.strip()
            if out:
                candidates.append(Path(out))
        except Exception:
            pass

    candidates += [home / "Downloads", home / "Descargas"]

    for c in candidates:
        if c.is_dir():
            return c
    return None


def save_ranking(alive):
    return save_text(OUTPUT_NAME, "\n".join(describe(i, r) for i, r in enumerate(alive, 1)))


def save_text(name, content):
    """Guarda un texto en Descargas y verifica que el archivo existe. Devuelve la ruta o None."""
    folder = downloads_dir()
    if folder is None:
        folder = Path.home() / "Downloads"
        try:
            folder.mkdir(parents=True, exist_ok=True)
        except Exception:
            folder = Path.cwd()

    path = folder / name
    try:
        path.write_text(content, encoding="utf-8")
        if path.is_file() and path.stat().st_size > 0:
            return path.resolve()
        print("⚠ El archivo se escribió pero aparece vacío.")
    except Exception as e:
        print(f"⚠ No se pudo guardar en '{folder}': {e}")
        # último recurso: carpeta actual
        try:
            path = Path.cwd() / name
            path.write_text(content, encoding="utf-8")
            if path.is_file() and path.stat().st_size > 0:
                return path.resolve()
        except Exception as e2:
            print(f"⚠ Tampoco se pudo guardar en la carpeta actual: {e2}")
    return None


# ------------------------------------------------------------------ banner ---
FONT = {
    "m": ["█   █", "██ ██", "█ █ █", "█   █", "█   █"],
    "e": ["█████", "█    ", "████ ", "█    ", "█████"],
    "3": ["█████", "    █", " ████", "    █", "█████"],
    "s": [" ████", "█    ", " ███ ", "    █", "████ "],
    "t": ["█████", "  █  ", "  █  ", "  █  ", "  █  "],
}


def show_banner(text="me3st"):
    rows = ["  ".join(FONT[c][i] for c in text) for i in range(5)]
    inner = 43
    wave = "▁▂▃▄▅▆▇█▇▆▅▄▃▂▁▂▃▄▅▆▇█▇▆▅▄▃▂▁"
    lines = [
        "",
        "   ✦ ˖ ✧   ·   ⋆   ✦   ˖   ·   ✧   ⋆   ✦",
        "        \\  |  /            \\  |  /",
        "         \\ | /              \\ | /",
        "   ╭" + "─" * inner + "╮",
        "   │" + " " * inner + "│",
    ]
    lines += ["   │" + r.center(inner) + "│" for r in rows]
    lines += [
        "   │" + " " * inner + "│",
        "   │" + wave.center(inner) + "│",
        "   ╰" + "─" * inner + "╯",
        "      📺  IPTV · Ranking · Categorías · Calidad  ⚡",
        "      ✦ ──────── comprueba antes de ver ──────── ✦",
        "",
    ]
    for ln in lines:
        print(ln)
        time.sleep(0.04)


# ============================================================ me3st extras ===
STATE_NAME = "me3st_estado.json"
STREAM_CACHE = {}
SPAIN_RE = re.compile(r"(\bes\b|\besp\b|espana|spain|spanish|castellano|🇪🇸)")
SPAIN_KEYS = ["la 1", "la 2", "antena 3", "telecinco", "la sexta", "cuatro", "movistar",
              "dazn", "teledeporte", "laliga", "tve", "telemadrid", "canal sur", "24h", "gol"]


# ---------------------------------------------------------------- colores ---
def _auto_color():
    try:
        if not sys.stdout.isatty():
            return False
    except Exception:
        return False
    if "pydroid" in str(Path.home()).lower() or os.environ.get("NO_COLOR"):
        return False
    return True


COLOR = _auto_color()
if COLOR and sys.platform == "win32":
    os.system("")          # activa los colores ANSI en la consola de Windows
_CODES = {"ok": "32", "bad": "31", "warn": "33", "hi": "36;1", "dim": "2", "star": "33;1", "bold": "1"}


def col(text, kind):
    return f"\033[{_CODES[kind]}m{text}\033[0m" if COLOR else text


def toggle_color():
    global COLOR
    COLOR = not COLOR
    print("Colores " + ("ACTIVADOS (si ves códigos raros, desactívalos)" if COLOR else "desactivados") + ".")
    if COLOR:
        print(col("Prueba: ✔ verde  ✘ rojo  ★ amarillo", "ok"))


# ------------------------------------------------------------- utilidades ---
def norm(t):
    t = unicodedata.normalize("NFKD", str(t))
    return "".join(c for c in t if not unicodedata.combining(c)).lower()


def fav_key(r):
    return f"{r['base']}|{r['user']}"


def host_of(r):
    return urlparse(r["base"]).netloc


def conn_free(r):
    try:
        return int(r["max_cons"]) - int(r["cons"])
    except Exception:
        return None


def n_cat(cats):
    return "?" if cats is None else ("-" if not cats else str(len(cats)))


def ask_idx(S, prompt="Número de la lista (sin #): "):
    n = input(prompt).strip().lstrip("#")
    if n.isdigit() and 1 <= int(n) <= len(S["alive"]):
        return int(n)
    print("Número no válido.")
    return None


def pick_lists(S, default_n=15):
    alive = S["alive"]
    t = input(f"¿En qué listas? Enter = las {default_n} primeras · 'todas' · 'fav' · "
              f"o números (ej: 1 5 8-12): ").strip().lower()
    if t == "":
        idx = list(range(1, min(default_n, len(alive)) + 1))
    elif t in ("todas", "todo", "all"):
        idx = list(range(1, len(alive) + 1))
    elif t in ("fav", "favs", "favoritas"):
        idx = [i for i, r in enumerate(alive, 1) if fav_key(r) in S["favs"]]
    else:
        idx, bad = parse_selection(t, len(alive))
        if bad:
            print(f"Ignorado (no válido): {' '.join(bad)}")
    if not idx:
        print("No hay listas elegidas.")
    return [(i, alive[i - 1]) for i in idx]


def run_parallel(fn, items, workers=8, show=True):
    """Ejecuta fn(i, r) para cada (i, r) en paralelo. Devuelve {i: resultado o None}."""
    out = {}
    if show:
        progress(0, len(items))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fn, i, r): i for i, r in items}
        for done, f in enumerate(as_completed(futs), 1):
            try:
                out[futs[f]] = f.result()
            except Exception:
                out[futs[f]] = None
            if show:
                progress(done, len(items))
    if show:
        print("\n")
    return out


# ----------------------------------------------------------------- tablas ---
def render_table(items, favs, color=True):
    def paint(text, kind):
        return col(text, kind) if color else text

    now = time.time()
    head = f"{'#':>4}   {'Caduca':<10} {'Días':>5} {'Conx':>7} {'TV':>4} {'VOD':>4} {'Ser':>4}  Servidor"
    lines = [paint(head, "hi")]
    for i, r in items:
        star = paint("★", "star") if fav_key(r) in favs else " "
        days = fmt_days(r["exp"])
        dn = (r["exp"] - now) / 86400
        dk = "bad" if dn <= 7 else "warn" if dn <= 30 else "ok"
        fr = conn_free(r)
        ck = "ok" if (fr is None or fr > 0) else "warn"
        cons = str(r["cons"]) + "/" + str(r["max_cons"])
        cells = []
        for c in (r["live"], r["vod"], r["series"]):
            t = n_cat(c)
            k = "warn" if t == "?" else "dim" if t == "-" else None
            cells.append(paint(f"{t:>4}", k) if k else f"{t:>4}")
        line = (f"{i:>4} {star} {fmt_date(r['exp']):<10} {paint(f'{days:>5}', dk)} "
                f"{paint(f'{cons:>7}', ck)} {cells[0]} {cells[1]} {cells[2]}  {host_of(r)[:28]}")
        lines.append(line)
        lines.append(paint("       ↳ " + full_link(r), "dim"))
    return lines


def summary_lines(S):
    alive, now = S["alive"], time.time()
    rows = [
        ("Listas analizadas:", f"{S['total']}  (+{S['ignored']} ilimitadas ignoradas)"),
        ("Funcionan:", str(len(alive))),
        ("No funcionan:", str(len(S["failed"]))),
    ]
    if alive:
        b = alive[0]
        rows.append(("Caduca más tarde:", f"#1 · {host_of(b)} · {fmt_days(b['exp'])} días"))
        k = max(range(len(alive)), key=lambda x: len(alive[x]["live"] or []))
        rows.append(("Más categorías de TV:", f"#{k + 1} · {host_of(alive[k])} · {len(alive[k]['live'] or [])}"))
        rows.append(("Con TV + VOD + Series:", str(sum(1 for r in alive if r["live"] and r["vod"] and r["series"]))))
        rows.append(("Con conexiones libres:", str(sum(1 for r in alive if (conn_free(r) or 0) > 0))))
        rows.append(("Caducan en < 30 días:", str(sum(1 for r in alive if r["exp"] - now < 30 * 86400))))
        rows.append(("Favoritas ★:", str(sum(1 for r in alive if fav_key(r) in S["favs"]))))
    body = [f"  {k:<26}{v}" for k, v in rows]
    w = max(len(x) for x in body) + 2
    return ["╭" + " RESUMEN ".center(w, "─") + "╮"] + ["│" + x.ljust(w) + "│" for x in body] + ["╰" + "─" * w + "╯"]


def show_ranking(S, save=True):
    alive, favs = S["alive"], S["favs"]
    items = list(enumerate(alive, 1))
    print(f"\n=== RANKING: {len(alive)} listas que funcionan ===")
    print("(1 = tarda más en caducar · ★ = favorita)\n")
    fav_items = [(i, r) for i, r in items if fav_key(r) in favs]
    if fav_items:
        print(col("★ FAVORITAS", "star"))
        print("\n".join(render_table(fav_items, favs)) + "\n")
    print("\n".join(render_table(items, favs)))
    print()
    print("\n".join(summary_lines(S)))
    if save and alive:
        text = "\n".join(render_table(items, favs, False)) + "\n\n" + "\n".join(summary_lines(S)) + "\n"
        path = save_text(OUTPUT_NAME, text)
        print(f"\n✔ Ranking guardado y verificado en:\n  {path}" if path else "\n✘ No se pudo guardar el ranking.")
        if save_state(S):
            print("✔ Análisis guardado: la próxima vez puedes abrirlo sin volver a comprobar.")


# ----------------------------------------------------------------- estado ---
def state_path():
    folder = downloads_dir() or (Path.home() / "Downloads")
    try:
        folder.mkdir(parents=True, exist_ok=True)
    except Exception:
        folder = Path.cwd()
    return folder / STATE_NAME


def save_state(S):
    data = {"saved_at": time.time(), "alive": S["alive"], "failed": S["failed"],
            "favs": sorted(S["favs"]), "ignored": S["ignored"], "total": S["total"]}
    try:
        state_path().write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        return True
    except Exception as e:
        print(f"⚠ No se pudo guardar el estado: {e}")
        return False


def load_state():
    p = state_path()
    if not p.is_file():
        return None
    try:
        d = json.loads(p.read_text(encoding="utf-8"))
        return {"alive": d["alive"], "failed": d.get("failed", []), "favs": set(d.get("favs", [])),
                "ignored": d.get("ignored", 0), "total": d.get("total", len(d["alive"])),
                "saved_at": d.get("saved_at")}
    except Exception as e:
        print(f"⚠ El análisis guardado no se pudo leer: {e}")
        return None


# ------------------------------------------------------------ opción 1: info ---
def opt_detail(S):
    i = ask_idx(S, f"Número de la lista (sin #, del 1 al {len(S['alive'])}): ")
    if i:
        r = S["alive"][i - 1]
        show_detail(i, r)
        if fav_key(r) in S["favs"]:
            print(col("★ Está en tus favoritas.", "star"))


# ------------------------------------------------------- opción 4: buscar canal ---
def fetch_streams(r):
    key = fav_key(r)
    if key in STREAM_CACHE:
        return STREAM_CACHE[key]
    try:
        data = api(r, "get_live_streams", timeout=60, retries=2)
    except Exception:
        return None
    out = []
    if isinstance(data, list):
        out = [(str(s.get("stream_id")), str(s.get("name", "")), str(s.get("category_id", "")))
               for s in data if isinstance(s, dict) and s.get("stream_id")]
    if len(STREAM_CACHE) >= 30:
        STREAM_CACHE.pop(next(iter(STREAM_CACHE)))
    STREAM_CACHE[key] = out
    return out


def search_channel(S):
    q = input("Canal a buscar (ej: movistar laliga · antena 3 · dazn): ").strip()
    words = norm(q).split()
    if not words:
        return
    sel = pick_lists(S)
    if not sel:
        return
    print(f"\nBuscando '{q}' en {len(sel)} listas (se descarga la lista de canales de cada una)...\n")

    def work(i, r):
        st = fetch_streams(r)
        return None if st is None else [s for s in st if all(w in norm(s[1]) for w in words)]

    res = run_parallel(work, sel, workers=8)
    found = [(i, r, res[i]) for i, r in sel if res.get(i)]
    failed = [i for i, r in sel if res.get(i) is None]
    for i, r, m in found:
        print(col(f"#{i}  {host_of(r)}", "hi") + f"  — {len(m)} coincidencias")
        for s in m[:5]:
            tag = detect_quality(s[1])
            print(f"     • {s[1]}" + (f"  [{tag}]" if tag else ""))
        if len(m) > 5:
            print(f"     … y {len(m) - 5} más")
    if failed:
        print("No se pudo leer la lista de canales de: " + " ".join(f"#{i}" for i in failed))
    if not found:
        print("Sin resultados en esas listas.")
        return
    if input("\n¿Comprobar si funcionan los 2 primeros canales de cada lista? (s/n): ").strip().lower() != "s":
        return
    matches = {i: m for i, r, m in found}

    def probe(i, r):
        fr = conn_free(r)
        if fr is not None and fr <= 0:
            return "sin conexiones libres"
        return [test_channel(r, {"stream_id": s[0], "name": s[1]}) for s in matches[i][:2]]

    out = run_parallel(probe, [(i, r) for i, r, m in found], workers=4)
    for i, r, m in found:
        o = out.get(i)
        if isinstance(o, str) or o is None:
            print(f"#{i} {host_of(r)}: {o or 'error'}")
            continue
        txt = ", ".join((col("✔", "ok") if t["ok"] else col("✘", "bad")) + " " + t["name"][:30] for t in o)
        print(f"#{i} {host_of(r)}: {txt}")


# ------------------------------------------------ opción 5: mejor lista España ---
def sample_spain(chans, k=6):
    picked, used = [], set()
    for key in SPAIN_KEYS:
        for s in chans:
            if key in norm(s[1]) and s[0] not in used:
                picked.append(s)
                used.add(s[0])
                break
        if len(picked) >= k:
            break
    rest = [s for s in chans if s[0] not in used]
    random.shuffle(rest)
    return (picked + rest[:max(0, k - len(picked))])[:k]


def spain_best(S):
    print("Busca categorías de España (ES, ESP, España, Spain…), prueba 6 canales de cada lista")
    print("y te dice cuál es la mejor. Respeta el límite de conexiones libres.\n")
    sel = pick_lists(S)
    if not sel:
        return
    print(f"\nFase 1/2: buscando canales de España en {len(sel)} listas...\n")

    def scan(i, r):
        cats = api(r, "get_live_categories", timeout=30, retries=3)
        if not isinstance(cats, list):
            return None
        names = {str(c.get("category_id")): str(c.get("category_name", "")) for c in cats if isinstance(c, dict)}
        es_ids = {cid for cid, nm in names.items() if SPAIN_RE.search(norm(nm))}
        if not es_ids:
            return {"n": 0, "chans": []}
        st = fetch_streams(r)
        if st is None:
            return None
        chans = [s for s in st if s[2] in es_ids]
        return {"n": len(chans), "chans": chans}

    scans = run_parallel(scan, sel, workers=8)
    with_es = [(i, r) for i, r in sel if scans.get(i) and scans[i]["n"] > 0]
    print(f"Con canales de España: {len(with_es)} de {len(sel)} listas.")
    if not with_es:
        return
    print(f"\nFase 2/2: probando canales ({len(with_es)} listas)...\n")

    def probe(i, r):
        fr = conn_free(r)
        if fr is not None and fr <= 0:
            return {"skipped": True}
        tested = [test_channel(r, {"stream_id": s[0], "name": s[1]}) for s in sample_spain(scans[i]["chans"])]
        ok = [t for t in tested if t["ok"]]
        qs = [QUALITY_ORDER.index(t["tag"]) for t in ok if t["tag"] in QUALITY_ORDER]
        return {"tested": len(tested), "ok": len(ok), "q": QUALITY_ORDER[min(qs)] if qs else "?"}

    pr = run_parallel(probe, with_es, workers=4)
    rows = []
    for i, r in with_es:
        p = pr.get(i) or {}
        rows.append({"i": i, "r": r, "n": scans[i]["n"], "tested": p.get("tested", 0),
                     "ok": p.get("ok", 0), "q": p.get("q", "?"), "skipped": bool(p.get("skipped"))})
    rows.sort(key=lambda x: ((x["ok"] / x["tested"]) if x["tested"] else -1, x["ok"], x["n"], x["r"]["exp"]),
              reverse=True)
    print(col(f"{'Pos':>3} {'#':>4} {'Canales ES':>10} {'Funcionan':>10} {'Calidad':>8} {'Días':>5}  Servidor", "hi"))
    for pos, x in enumerate(rows, 1):
        func = "sin conexión libre" if x["skipped"] else f"{x['ok']}/{x['tested']}"
        print(f"{pos:>3} {x['i']:>4} {x['n']:>10} {func:>10} {x['q']:>8} {fmt_days(x['r']['exp']):>5}  {host_of(x['r'])[:26]}")
    good = [x for x in rows if x["tested"] and x["ok"] > 0][:3]
    if good:
        print(col("\n★ Recomendadas:", "star"))
        for x in good:
            print(f"  #{x['i']}  {x['ok']}/{x['tested']} canales funcionan · {x['n']} canales de España")
            print(f"     {full_link(x['r'])}")
    else:
        print(col("\nNinguna lista mostró canales de España funcionando ahora mismo.", "warn"))
        print("Prueba más tarde, sin VPN y con listas que tengan conexiones libres.")


# ------------------------------------------------------------ opción 6: filtros ---
def _ask_int(prompt):
    t = input(prompt).strip()
    return int(t) if t.lstrip("-").isdigit() else None


def filter_menu(S):
    print("Deja en blanco (Enter) lo que no quieras filtrar.\n")
    dmin = _ask_int("Días mínimos que le queden: ")
    dmax = _ask_int("Días máximos que le queden: ")
    need_vod = input("¿Solo con VOD? (s/Enter): ").strip().lower() == "s"
    need_ser = input("¿Solo con Series? (s/Enter): ").strip().lower() == "s"
    need_free = input("¿Solo con conexiones libres? (s/Enter): ").strip().lower() == "s"
    min_tv = _ask_int("Mínimo de categorías de TV: ")
    text = norm(input("Categoría que contenga (ej: españa, dazn, 4k): ").strip())
    now, out = time.time(), []
    for i, r in enumerate(S["alive"], 1):
        d = (r["exp"] - now) / 86400
        if dmin is not None and d < dmin:
            continue
        if dmax is not None and d > dmax:
            continue
        if need_vod and not r["vod"]:
            continue
        if need_ser and not r["series"]:
            continue
        if need_free and not ((conn_free(r) or 0) > 0):
            continue
        if min_tv is not None and len(r["live"] or []) < min_tv:
            continue
        if text and not any(text in norm(c) for key in ("live", "vod", "series") for c in (r[key] or [])):
            continue
        out.append((i, r))
    S["view"] = out
    print(f"\n{len(out)} listas cumplen el filtro (los números son los del ranking):\n")
    if out:
        print("\n".join(render_table(out, S["favs"])))
        print("\nPuedes exportar este filtro desde la opción 7.")


# ----------------------------------------------------------- opción 7: exportar ---
def _source(S):
    print("  1) Todas   2) Último filtro   3) Favoritas")
    t = input("¿Qué listas? (Enter = todas): ").strip()
    if t == "2":
        if not S.get("view"):
            print("Todavía no hay filtro aplicado. Uso todas.")
            return list(enumerate(S["alive"], 1))
        return S["view"]
    if t == "3":
        return [(i, r) for i, r in enumerate(S["alive"], 1) if fav_key(r) in S["favs"]]
    return list(enumerate(S["alive"], 1))


def export_menu(S):
    print("  1) CSV del ranking (para Excel)")
    print("  2) TXT con solo los enlaces")
    print("  3) M3U con los canales de una lista")
    op = input("> ").strip()
    if op in ("1", "2"):
        items = _source(S)
        if not items:
            print("No hay listas que exportar.")
            return
        if op == "1":
            buf = io.StringIO()
            w = csv.writer(buf, delimiter=";")
            w.writerow(["Pos", "Servidor", "Usuario", "Contraseña", "Caduca", "Días", "Conexiones",
                        "TV", "VOD", "Series", "Enlace"])
            for i, r in items:
                w.writerow([i, host_of(r), r["user"], r["pw"], fmt_date(r["exp"]), fmt_days(r["exp"]),
                            f"{r['cons']}/{r['max_cons']}", n_cat(r["live"]), n_cat(r["vod"]),
                            n_cat(r["series"]), full_link(r)])
            path = save_text("me3st_ranking.csv", "\ufeff" + buf.getvalue())
        else:
            path = save_text("me3st_enlaces.txt", "\n".join(full_link(r) for i, r in items) + "\n")
        print(f"✔ Exportadas {len(items)} listas y verificado en:\n  {path}" if path else "✘ No se pudo guardar.")
    elif op == "3":
        i = ask_idx(S)
        if not i:
            return
        export_m3u(S["alive"][i - 1])


def export_m3u(r):
    try:
        cats = api(r, "get_live_categories", timeout=30, retries=3)
    except Exception:
        print("No se pudieron leer las categorías de esa lista.")
        return
    cats = [c for c in cats if isinstance(c, dict)] if isinstance(cats, list) else []
    for n, c in enumerate(cats, 1):
        print(f"  {n:>3}. {c.get('category_name', '?')}")
    t = input("\nCategorías (ej: 3 5 7-26) · Enter = TODAS: ").strip()
    if t:
        sel, bad = parse_selection(t, len(cats))
        if bad:
            print(f"Ignorado (no válido): {' '.join(bad)}")
        if not sel:
            return
    else:
        sel = list(range(1, len(cats) + 1))
    names = {}
    for n in sel:
        names[str(cats[n - 1].get("category_id"))] = str(cats[n - 1].get("category_name", ""))
    st = fetch_streams(r)
    if st is None:
        print("No se pudo leer la lista de canales.")
        return
    lines = ["#EXTM3U"]
    count = 0
    for sid, name, cid in st:
        if cid in names:
            url = stream_urls(r, sid)[0][1]
            grp = names[cid].replace('"', "'")
            lines.append(f'#EXTINF:-1 tvg-name="{name.replace(chr(34), chr(39))}" group-title="{grp}",{name}')
            lines.append(url)
            count += 1
    fname = "me3st_" + re.sub(r"[^A-Za-z0-9._-]+", "_", f"{host_of(r)}_{r['user']}")[:60] + ".m3u"
    path = save_text(fname, "\n".join(lines) + "\n")
    print(f"✔ M3U con {count} canales y verificado en:\n  {path}" if path else "✘ No se pudo guardar.")


# ------------------------------------------------------------ opción 8: favoritas ---
def favorites_menu(S):
    print("  1) Ver favoritas   2) Añadir   3) Quitar   4) Vaciar")
    op = input("> ").strip()
    alive = S["alive"]
    if op == "1":
        items = [(i, r) for i, r in enumerate(alive, 1) if fav_key(r) in S["favs"]]
        print("\n".join(render_table(items, S["favs"])) if items else "Aún no tienes favoritas.")
    elif op in ("2", "3"):
        sel, bad = parse_selection(input("Números (ej: 1 4 7-9): "), len(alive))
        if bad:
            print(f"Ignorado (no válido): {' '.join(bad)}")
        for i in sel:
            (S["favs"].add if op == "2" else S["favs"].discard)(fav_key(alive[i - 1]))
        print(f"✔ {'Añadidas' if op == '2' else 'Quitadas'}: {len(sel)}")
        save_state(S)
    elif op == "4":
        S["favs"].clear()
        save_state(S)
        print("Favoritas vaciadas.")


# --------------------------------------------------------- opción 9: portapapeles ---
def copy_clipboard(text):
    cmds = [["pbcopy"], ["clip"], ["termux-clipboard-set"], ["wl-copy"],
            ["xclip", "-selection", "clipboard"], ["xsel", "--clipboard", "--input"]]
    for c in cmds:
        if shutil.which(c[0]):
            try:
                subprocess.run(c, input=text.encode("utf-8"), check=True, timeout=5)
                return True
            except Exception:
                continue
    try:
        import tkinter
        t = tkinter.Tk()
        t.withdraw()
        t.clipboard_clear()
        t.clipboard_append(text)
        t.update()
        t.destroy()
        return True
    except Exception:
        return False


def copy_menu(S):
    i = ask_idx(S)
    if not i:
        return
    link = full_link(S["alive"][i - 1])
    if copy_clipboard(link):
        print("✔ Enlace copiado al portapapeles.")
    else:
        print("No pude acceder al portapapeles en este dispositivo. Cópialo de aquí (mantén pulsado):")
    print(link)


# ----------------------------------------------- opción 10: reintentar fallidas ---
def retry_failed(S):
    fl = S["failed"]
    if not fl:
        print("No hay listas fallidas guardadas.")
        return
    print(f"Reintentando {len(fl)} listas que no funcionaban...\n")
    for st in _hosts.values():
        st["fail"] = 0            # no saltarse servidores marcados como caídos antes
    entries = [{k: f[k] for k in ("base", "user", "pw", "raw")} for f in fl]
    results = second_pass(check_all(entries))
    S["ignored"] += sum(1 for r in results if r.get("ignored"))
    have = {fav_key(r) for r in S["alive"]}
    added = [r for r in results if r["ok"] and fav_key(r) not in have]
    S["alive"] += added
    S["alive"].sort(key=lambda r: r["exp"], reverse=True)
    S["failed"] = [{k: r.get(k) for k in ("base", "user", "pw", "raw", "reason")}
                   for r in results if not r["ok"] and not r.get("ignored")]
    print(f"✔ Recuperadas: {len(added)}   |   Siguen sin funcionar: {len(S['failed'])}")
    if added:
        print("Los números del ranking han cambiado:")
        show_ranking(S)
    else:
        save_state(S)


# ----------------------------------------------------------- opción 11: vigilar ---
def quick_status(r):
    try:
        d = api(r, retries=1)
    except Exception:
        return ("down", "sin conexión", "?")
    ui = d.get("user_info") if isinstance(d, dict) else None
    if not ui or str(ui.get("auth")) != "1":
        return ("down", "credenciales inválidas", "?")
    cons = f"{ui.get('active_cons', '?')}/{ui.get('max_connections', '?')}"
    if str(ui.get("status")) != "Active":
        return ("down", f"estado {ui.get('status')}", cons)
    try:
        exp = ui.get("exp_date")
        if exp not in (None, "", "null") and float(exp) < time.time():
            return ("down", "caducada", cons)
    except ValueError:
        pass
    return ("up", "", cons)


def watch_menu(S):
    print("Vigila listas y avisa cuando una caiga o vuelva a funcionar.\n")
    sel = pick_lists(S, default_n=10)
    if not sel:
        return
    mins = _ask_int("Cada cuántos minutos (Enter = 10): ") or 10
    rounds = _ask_int("Cuántas rondas (Enter = hasta que la pares): ")
    print(f"\nVigilando {len(sel)} listas cada {mins} min. Para parar: botón de parar o Ctrl+C.\n")
    prev, n = {}, 0
    try:
        while True:
            n += 1
            res = run_parallel(lambda i, r: quick_status(r), sel, workers=8, show=False)
            stamp = time.strftime("%H:%M:%S")
            for i, r in sel:
                st = res.get(i) or ("down", "error", "?")
                old = prev.get(i)
                if old is None or old[0] != st[0]:
                    mark = col("✔ activa", "ok") if st[0] == "up" else col(f"✘ CAÍDA ({st[1]})", "bad")
                    extra = "   ⚠ CAMBIO" if old is not None else ""
                    print(f"[{stamp}] #{i} {host_of(r)} — {mark}  conexiones {st[2]}{extra}")
                    if old is not None:
                        print("\a", end="")
                prev[i] = st
            up = sum(1 for v in prev.values() if v[0] == "up")
            print(f"[{stamp}] ronda {n}: {up} activas, {len(sel) - up} caídas")
            if rounds and n >= rounds:
                break
            for _ in range(int(mins * 60)):
                time.sleep(1)
    except KeyboardInterrupt:
        print("\nVigilancia detenida.")



# ------------------------------------------------------------------- main ---
def analyze(use_file):
    entries, seen = [], set()
    for line in read_input(use_file):
        e = parse_line(line)
        if e and (e["base"], e["user"], e["pw"]) not in seen:
            seen.add((e["base"], e["user"], e["pw"]))
            entries.append(e)

    if not entries:
        print("No he encontrado ninguna lista válida.")
        return None

    print(f"\nComprobando {len(entries)} listas...\n")
    results = second_pass(check_all(entries))
    n_ignored = sum(1 for r in results if r.get("ignored"))
    results = [r for r in results if not r.get("ignored")]
    alive = sorted((r for r in results if r["ok"]), key=lambda r: r["exp"], reverse=True)
    failed = [{k: r.get(k) for k in ("base", "user", "pw", "raw", "reason")} for r in results if not r["ok"]]
    S = {"alive": alive, "failed": failed, "favs": set(), "ignored": n_ignored, "total": len(entries)}
    old = load_state()
    if old:
        S["favs"] = old["favs"]          # las favoritas se conservan entre análisis
    show_ranking(S)
    return S


MENU = [
    ("1", "Ver detalle de una lista (fechas, categorías, calidad)"),
    ("2", "Empezar de nuevo"),
    ("3", "Comprobar los canales de categorías"),
    ("4", "Buscar un canal en las listas"),
    ("5", "Mejor lista para España"),
    ("6", "Filtrar el ranking"),
    ("7", "Exportar (CSV · enlaces · M3U)"),
    ("8", "Favoritas ★"),
    ("9", "Copiar un enlace al portapapeles"),
    ("10", "Reintentar solo las listas que fallaron"),
    ("11", "Vigilar listas (avisa si caen)"),
    ("c", "Colores: activar / desactivar"),
]


def menu(S):
    """Devuelve True si hay que volver al inicio, False para salir."""
    actions = {"1": opt_detail, "3": lambda S: category_check(S["alive"]), "4": search_channel,
               "5": spain_best, "6": filter_menu, "7": export_menu, "8": favorites_menu,
               "9": copy_menu, "10": retry_failed, "11": watch_menu}
    while True:
        print("\n" + col("─" * 60, "dim"))
        for k, txt in MENU:
            print(f" {col(f'{k:>2}', 'hi')}) {txt}")
        print("  (Enter vacío = salir)")
        op = input("> ").strip().lower()
        if op == "":
            return False
        if op == "2":
            return True
        if op == "c":
            toggle_color()
        elif op in actions:
            if not S["alive"] and op != "10":
                print("No hay listas que funcionen.")
                continue
            try:
                actions[op](S)
            except KeyboardInterrupt:
                print("\nCancelado.")
            except Exception as e:
                print(f"⚠ Algo falló: {type(e).__name__}: {e}")
        else:
            print("Opción no válida.")


def main():
    show_banner()
    use_file = True
    while True:
        S = None
        if use_file and len(sys.argv) > 1:
            S = analyze(True)
        else:
            saved = state_path().is_file()
            print("\n¿Qué quieres hacer?")
            print(f" {col('1', 'hi')}) Analizar listas nuevas")
            if saved:
                when = time.strftime("%d/%m/%Y %H:%M", time.localtime(state_path().stat().st_mtime))
                print(f" {col('2', 'hi')}) Abrir el último análisis guardado ({when}) — sin volver a comprobar")
            print("  (Enter vacío = salir)")
            op = input("> ").strip()
            if op == "1":
                S = analyze(False)
            elif op == "2" and saved:
                S = load_state()
                if S:
                    show_ranking(S, save=False)
            else:
                break
        use_file = False
        if S is None:
            continue
        if not menu(S):
            break
        print()


if __name__ == "__main__":
    main()
