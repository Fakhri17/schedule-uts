import csv
from pathlib import Path
from datetime import datetime, time


def sniff_reader(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        sample = f.read(4096)
        f.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample)
        except Exception:
            # default ke koma
            dialect = csv.excel
        reader = csv.reader(f, dialect)
        rows = list(reader)
    return rows


def parse_time_range(date_str: str, shift_str: str):
    if not date_str or not shift_str:
        return None
    try:
        date_dt = datetime.strptime(date_str.strip(), "%d-%b-%y")
    except Exception:
        return None
    parts = shift_str.split("-")
    if len(parts) != 2:
        return None
    s1 = parts[0].strip().replace(" ", "")
    s2 = parts[1].strip()
    try:
        h1, m1 = map(int, s1.split("."))
        h2, m2 = map(int, s2.split("."))
    except Exception:
        return None
    start_dt = datetime.combine(date_dt.date(), time(h1, m1))
    end_dt = datetime.combine(date_dt.date(), time(h2, m2))
    return start_dt, end_dt


def read_schedule(path: Path):
    rows = sniff_reader(path)
    if not rows:
        return [], []
    header = [h.strip().upper() for h in rows[0]]
    idx = {name: i for i, name in enumerate(header)}

    def get(row, key):
        i = idx.get(key, None)
        if i is None or i >= len(row):
            return ""
        return row[i].strip()

    # Deteksi nama kolom dari output baru
    required = [
        "HARI", "TANGGAL", "SHIFT", "RUANGAN",
        "KODE MATA KULIAH", "NAMA MATA KULIAH", "NAMA DOSEN", "KELAS",
    ]
    missing = [c for c in required if c not in idx]
    if missing:
        print("Peringatan: Kolom tidak ditemukan:", ", ".join(missing))

    records = []
    for r in rows[1:]:
        if not any(r):
            continue
        rec = {
            "HARI": get(r, "HARI"),
            "TANGGAL": get(r, "TANGGAL"),
            "SHIFT": get(r, "SHIFT"),
            "RUANGAN": get(r, "RUANGAN"),
            "KODE MATA KULIAH": get(r, "KODE MATA KULIAH"),
            "NAMA MATA KULIAH": get(r, "NAMA MATA KULIAH"),
            "NAMA DOSEN": get(r, "NAMA DOSEN"),
            "KELAS": get(r, "KELAS"),
        }
        rec["_INTERVAL"] = parse_time_range(rec["TANGGAL"], rec["SHIFT"])  # tuple atau None
        records.append(rec)
    return header, records


def find_class_conflicts(records):
    # Group by (KELAS, TANGGAL)
    from collections import defaultdict
    by_key = defaultdict(list)
    for rec in records:
        kelas = rec.get("KELAS", "")
        tanggal = rec.get("TANGGAL", "")
        if not kelas or not tanggal:
            continue
        if rec.get("_INTERVAL") is None:
            continue
        by_key[(kelas, tanggal)].append(rec)

    conflicts = []
    for (kelas, tanggal), items in by_key.items():
        # cek pairwise overlap
        for i in range(len(items)):
            s1, e1 = items[i]["_INTERVAL"]
            for j in range(i + 1, len(items)):
                s2, e2 = items[j]["_INTERVAL"]
                overlap = not (e1 <= s2 or s1 >= e2)
                if overlap:
                    conflicts.append({
                        "KELAS": kelas,
                        "TANGGAL": tanggal,
                        "SHIFT_1": items[i]["SHIFT"],
                        "MATA KULIAH 1": items[i]["NAMA MATA KULIAH"],
                        "KODE 1": items[i]["KODE MATA KULIAH"],
                        "RUANGAN 1": items[i]["RUANGAN"],
                        "SHIFT_2": items[j]["SHIFT"],
                        "MATA KULIAH 2": items[j]["NAMA MATA KULIAH"],
                        "KODE 2": items[j]["KODE MATA KULIAH"],
                        "RUANGAN 2": items[j]["RUANGAN"],
                    })
    return conflicts


def write_conflicts(conflicts, out_path: Path):
    if not conflicts:
        # tulis file kosong dengan header
        cols = [
            "KELAS","TANGGAL","SHIFT_1","MATA KULIAH 1","KODE 1","RUANGAN 1",
            "SHIFT_2","MATA KULIAH 2","KODE 2","RUANGAN 2",
        ]
        with out_path.open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            w.writerow(cols)
        return
    cols = list(conflicts[0].keys())
    with out_path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for c in conflicts:
            w.writerow([c.get(k, "") for k in cols])


def main():
    base = Path(__file__).parent
    # Prioritas cek hasil generate
    candidates = [
        base / "jadwal-uts-output.csv",
    ]
    src = None
    for p in candidates:
        if p.exists():
            src = p
            break
    if src is None:
        print("Tidak menemukan file jadwal untuk dicek.")
        return

    header, records = read_schedule(src)
    conflicts = find_class_conflicts(records)
    out_path = base / "kelas-conflicts.csv"
    write_conflicts(conflicts, out_path)

    if conflicts:
        print(f"Ditemukan {len(conflicts)} konflik kelas. Detail ditulis ke {out_path.name}")
    else:
        print(f"Tidak ada konflik kelas. Laporan kosong ditulis ke {out_path.name}")


if __name__ == "__main__":
    main()


