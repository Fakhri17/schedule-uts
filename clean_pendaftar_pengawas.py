import csv
import re
from pathlib import Path
from datetime import datetime


BASE = Path(__file__).parent
SRC = BASE / "pendaftar-pengawas.csv"
OUT = BASE / "pendaftar-pengawas-clean.csv"


DAY_KEYS = ["SENIN", "SELASA", "RABU", "KAMIS", "JUMAT"]
DAY_ALIASES = {
    "SENIN": "SENIN",
    "SEN": "SENIN",
    "MONDAY": "SENIN",
    "MON": "SENIN",
    "SELASA": "SELASA",
    "SEL": "SELASA",
    "TUESDAY": "SELASA",
    "TUE": "SELASA",
    "RABU": "RABU",
    "RAB": "RABU",
    "WEDNESDAY": "RABU",
    "WED": "RABU",
    "KAMIS": "KAMIS",
    "KAM": "KAMIS",
    "THURSDAY": "KAMIS",
    "THU": "KAMIS",
    "JUMAT": "JUMAT",
    "JUM'AT": "JUMAT",
    "JUM\u2019AT": "JUMAT",
    "JUM": "JUMAT",
    "FRIDAY": "JUMAT",
    "FRI": "JUMAT",
}


def titlecase_name(name: str) -> str:
    if not name:
        return ""
    # Lower then title, but keep common connectors uppercased properly
    parts = [p.strip() for p in re.split(r"\s+", name)]
    def fix(part: str) -> str:
        up = part.upper()
        if up in {"OF", "AND", "THE", "DA", "DE", "VAN", "BIN", "BINTI"}:
            return up.lower()
        # Handle initials like M., S., etc.
        if re.fullmatch(r"[A-Za-z]\.", part):
            return part.upper()
        return part.capitalize()
    return " ".join(fix(p) for p in parts if p)


def normalize_nim(nim: str) -> str:
    return re.sub(r"\D", "", nim or "")


def normalize_wa(phone: str) -> str:
    s = re.sub(r"\D", "", phone or "")
    if not s:
        return ""
    # If starts with 0 -> +62
    if s.startswith("0"):
        s = "62" + s[1:]
    # If already 62...
    if s.startswith("62"):
        return "+" + s
    # If already has country code 62 without plus
    if s.startswith("+62"):
        return s
    # Fallback: assume Indonesian
    return "+" + s


def parse_datetime_idn(dt_str: str) -> datetime | None:
    if not dt_str:
        return None
    # Example: 30/10/2025 19.33
    patterns = [
        "%d/%m/%Y %H.%M",
        "%d/%m/%Y %H:%M",
        "%Y-%m-%d %H:%M:%S",
    ]
    for p in patterns:
        try:
            return datetime.strptime(dt_str.strip(), p)
        except Exception:
            continue
    return None


def normalize_time_token(tok: str) -> str | None:
    if not tok:
        return None
    t = tok.strip()
    if not t:
        return None
    # Replace colon with dot, remove spaces
    t = t.replace(":", ".").replace(" ", "")
    # Accept forms like 7.0, 07.00, 700 -> convert to HH.MM
    m = re.fullmatch(r"(\d{1,2})(?:[\.:]?(\d{1,2}))?", t)
    if not m:
        return None
    h = int(m.group(1))
    mnt = int(m.group(2)) if m.group(2) is not None else 0
    if not (0 <= h <= 23 and 0 <= mnt <= 59):
        return None
    return f"{h:02d}.{mnt:02d}"


def normalize_range(range_str: str) -> str | None:
    if not range_str:
        return None
    s = range_str.strip()
    # remove parens
    s = re.sub(r"[()\[\]]", "", s)
    # split by dash
    parts = re.split(r"\s*[-–]\s*", s)
    if len(parts) != 2:
        return None
    a = normalize_time_token(parts[0])
    b = normalize_time_token(parts[1])
    if not a or not b:
        return None
    return f"{a}-{b}"


def normalize_availability(text: str) -> dict:
    """Return mapping day->comma-separated ranges (HH.MM-HH.MM)."""
    res = {d: "" for d in DAY_KEYS}
    if not text:
        return res
    raw = (text or "").replace("\r", "\n")
    # unify separators: newlines and commas both separate chunks
    chunks = []
    for line in raw.split("\n"):
        line = line.strip()
        if not line:
            continue
        parts = [p.strip() for p in line.split(",") if p.strip()]
        chunks.extend(parts)

    # Group by detected day prefix
    # Examples: "Senin 13.00-15.00", "Rabu: 07.00 - 16.00", "Selasa Kosong"
    cur_day = None
    for ch in chunks:
        # Try extract day token
        m = re.match(r"^([A-Za-z'\u2019]+)\s*:?\s*(.*)$", ch)
        if m:
            day_tok = m.group(1).strip().upper()
            rest = m.group(2).strip()
            day = DAY_ALIASES.get(day_tok)
            if day:
                cur_day = day
                # If indicates 'Kosong' => full working window
                if rest and re.search(r"KOSONG", rest, flags=re.IGNORECASE):
                    res[cur_day] = "07.30-17.30"
                    continue
                # Otherwise parse ranges possibly with internal commas already split
                if rest:
                    rng = normalize_range(rest)
                    if rng:
                        res[cur_day] = ", ".join([x for x in [res[cur_day], rng] if x])
                continue
        # If no new day but continuing ranges, append to last day
        if cur_day:
            rng = normalize_range(ch)
            if rng:
                res[cur_day] = ", ".join([x for x in [res[cur_day], rng] if x])

    # Final tidy: collapse multiple spaces
    for d in list(res.keys()):
        res[d] = re.sub(r"\s+", " ", res[d]).strip()
    return res


def normalize_agreement(text: str) -> str:
    t = (text or "").strip().lower()
    if not t:
        return ""
    if "setuju" in t or "agree" in t:
        return "YES"
    return "NO"


def read_rows(src: Path):
    with src.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        rows = list(reader)
    return rows


def main():
    rows = read_rows(SRC)
    if not rows:
        print("Sumber kosong")
        return
    header = rows[0]
    idx = {h.strip(): i for i, h in enumerate(header)}

    def get(row, name, default=""):
        i = idx.get(name, None)
        if i is None or i >= len(row):
            return default
        return row[i].strip()

    # Dedupe by (NIM or Email), keep latest by Completion time
    by_key: dict[str, dict] = {}
    for r in rows[1:]:
        if not any(r):
            continue
        email = get(r, "Email").lower()
        nim = normalize_nim(get(r, "NIM"))
        key = nim or email
        if not key:
            continue
        comp = parse_datetime_idn(get(r, "Completion time")) or datetime.min
        if key not in by_key or comp >= by_key[key]["_comp"]:
            by_key[key] = {
                "email": email,
                "nama": titlecase_name(get(r, "Nama Lengkap") or get(r, "Name")),
                "nim": nim,
                "wa": normalize_wa(get(r, "Nomor WA")),
                "agree": normalize_agreement(get(r, "Saya bersedia, memahami, dan menyetujui tugas sebagai pengawas ujian yang sesuai dengan ketentuan dan jadwal yang telah ditetapkan. Saya bersedia melaksanakan tugas sebagai pengawas ujian dengan penuh")),
                "avail": normalize_availability(get(r, "Isikan jadwal kosong Ex: (Senin: 07.00-10.00, 13.00-15.00 , Selasa: 08.00-13.00)")),
                "_comp": comp,
            }

    # Write cleaned CSV (semicolon-delimited, UTF-8)
    out_cols = ["EMAIL", "NAMA_LENGKAP", "NIM", "WA", "SETUJU"] + DAY_KEYS
    with OUT.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(out_cols)
        for k in sorted(by_key.keys()):
            rec = by_key[k]
            row = [
                rec["email"],
                rec["nama"],
                rec["nim"],
                rec["wa"],
                rec["agree"],
            ]
            for d in DAY_KEYS:
                row.append(rec["avail"].get(d, ""))
            w.writerow(row)

    print(f"OK: {len(by_key)} baris -> {OUT.name}")


if __name__ == "__main__":
    main()


