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
    # Coba beberapa format tanggal yang mungkin muncul di output/CSV input
    date_formats = ["%d-%b-%y", "%d/%m/%Y", "%d-%m-%Y"]
    date_dt = None
    for fmt in date_formats:
        try:
            date_dt = datetime.strptime(date_str.strip(), fmt)
            break
        except Exception:
            continue
    if date_dt is None:
        return None
    parts = shift_str.split("-")
    if len(parts) != 2:
        return None
    # Bersihkan whitespace termasuk tab
    s1 = parts[0].strip().replace(" ", "").replace("\t", "")
    s2 = parts[1].strip().replace(" ", "").replace("\t", "")
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


def deduplicate_records(records):
    """Hilangkan duplikasi exact berdasarkan (KELAS, TANGGAL, SHIFT, KODE MATA KULIAH).
    Jika ada multi baris identik (meski beda RUANGAN), simpan satu saja (baris pertama).
    """
    seen = set()
    deduped = []
    for rec in records:
        key = (
            rec.get("KELAS", "").strip(),
            rec.get("TANGGAL", "").strip(),
            rec.get("SHIFT", "").strip(),
            rec.get("KODE MATA KULIAH", "").strip(),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(rec)
    return deduped


def add_keys(records):
    """Tambahkan kolom bantu kunci ke setiap record untuk analisis manual di Excel."""
    for r in records:
        tanggal = r.get("TANGGAL", "").strip().upper()
        shift = r.get("SHIFT", "").strip().upper()
        ruangan = r.get("RUANGAN", "").strip().upper()
        kelas = r.get("KELAS", "").strip().upper()
        dosen = r.get("NAMA DOSEN", "").strip().upper()
        r["KEY_RUANGAN"] = f"{tanggal}|{shift}|{ruangan}"
        r["KEY_KELAS"] = f"{tanggal}|{shift}|{kelas}"
        r["KEY_DOSEN"] = f"{tanggal}|{shift}|{dosen}"
    return records


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
        # cek limit maksimum 2 ujian per hari per kelas
        if len(items) > 2:
            conflicts.append({
                "KELAS": kelas,
                "TANGGAL": tanggal,
                "JENIS": "LIMIT > 2/HARI",
                "TOTAL": str(len(items)),
                "DETAIL": "; ".join(f"{it['SHIFT']} - {it['NAMA MATA KULIAH']}" for it in items),
            })
    return conflicts


# ====== Sinkron dengan generate_schedule.py untuk blacklist tanggal-ruangan ======
START_DATE = datetime(2025, 11, 3)
END_DATE = datetime(2025, 11, 7)
BLACKLIST_MON_WED_SUFFIXES = {"KTT 2.08", "KTT 2.07", "KTT 2.06", "KTT 2.05", "KTT 2.04"}
BLACKLIST_MON_FRI_SUFFIXES = {"KTT 2.09"}


def is_within_uts_week(date_dt: datetime) -> bool:
    return START_DATE.date() <= date_dt.date() <= END_DATE.date()


def is_room_blacklisted_on_date(room: str, date_dt: datetime) -> bool:
    if not is_within_uts_week(date_dt):
        return False
    weekday = date_dt.weekday()
    for suf in BLACKLIST_MON_FRI_SUFFIXES:
        if room.endswith(suf) and weekday <= 4:
            return True
    for suf in BLACKLIST_MON_WED_SUFFIXES:
        if room.endswith(suf) and weekday <= 2:
            return True
    return False


def find_room_conflicts(records):
    # Ruangan tidak boleh dipakai lebih dari 1 kelas pada (tanggal, shift) yang sama
    # Kecuali AULA yang boleh sampai 2 kelas per shift
    from collections import defaultdict
    by_key = defaultdict(list)  # (TANGGAL, SHIFT, RUANGAN) -> list recs
    for rec in records:
        tgl = rec.get("TANGGAL", "")
        shf = rec.get("SHIFT", "")
        room = rec.get("RUANGAN", "")
        if not tgl or not shf or not room:
            continue
        by_key[(tgl, shf, room)].append(rec)

    conflicts = []
    AULA_NAME = "AULA"
    for (tgl, shf, room), items in by_key.items():
        # Khusus AULA: izinkan hingga 2 kelas dalam 1 shift
        if room.strip().upper() == AULA_NAME:
            if len(items) <= 2:
                continue
        # Untuk ruangan lain, maksimal 1 kelas per shift
        if len(items) > 1:
            for it in items:
                conflicts.append({
                    "TANGGAL": tgl,
                    "SHIFT": shf,
                    "RUANGAN": room,
                    "KELAS": it.get("KELAS", ""),
                    "MATA KULIAH": it.get("NAMA MATA KULIAH", ""),
                    "KODE": it.get("KODE MATA KULIAH", ""),
                })
    return conflicts


def find_dosen_conflicts(records):
    # Dosen tidak boleh mengawasi 2 ujian pada waktu yang sama (overlap)
    from collections import defaultdict
    by_dosen = defaultdict(list)  # DOSEN -> list recs
    
    for rec in records:
        dosen = rec.get("NAMA DOSEN", "").strip()
        if not dosen:
            continue
        interval = rec.get("_INTERVAL")
        if interval is None:
            continue
        by_dosen[dosen].append(rec)

    conflicts = []
    for dosen, items in by_dosen.items():
        # Cek pairwise overlap waktu untuk dosen yang sama
        for i in range(len(items)):
            s1, e1 = items[i]["_INTERVAL"]
            tgl1 = items[i].get("TANGGAL", "")
            for j in range(i + 1, len(items)):
                s2, e2 = items[j]["_INTERVAL"]
                # Cek apakah waktu overlap
                overlap = not (e1 <= s2 or s1 >= e2)
                if overlap:
                    conflicts.append({
                        "TANGGAL_1": items[i].get("TANGGAL", ""),
                        "SHIFT_1": items[i]["SHIFT"],
                        "DOSEN": dosen,
                        "KELAS_1": items[i].get("KELAS", ""),
                        "MATA KULIAH 1": items[i].get("NAMA MATA KULIAH", ""),
                        "RUANGAN_1": items[i].get("RUANGAN", ""),
                        "TANGGAL_2": items[j].get("TANGGAL", ""),
                        "SHIFT_2": items[j]["SHIFT"],
                        "KELAS_2": items[j].get("KELAS", ""),
                        "MATA KULIAH 2": items[j].get("NAMA MATA KULIAH", ""),
                        "RUANGAN_2": items[j].get("RUANGAN", ""),
                    })
    return conflicts


def find_blacklist_violations(records):
    violations = []
    for rec in records:
        room = rec.get("RUANGAN", "").strip()
        interval = rec.get("_INTERVAL")
        if not room or interval is None:
            continue
        start_dt, _ = interval
        if is_room_blacklisted_on_date(room, start_dt):
            violations.append({
                "TANGGAL": rec.get("TANGGAL", ""),
                "SHIFT": rec.get("SHIFT", ""),
                "RUANGAN": room,
                "KELAS": rec.get("KELAS", ""),
                "MATA KULIAH": rec.get("NAMA MATA KULIAH", ""),
            })
    return violations


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


def write_key_summary(records, out_path):
    """Ekspor summary pivot count untuk KEY_RUANGAN, KEY_KELAS, KEY_DOSEN (bisa difilter di Excel)."""
    from collections import Counter
    keys = [
        ("KEY_RUANGAN", "Tanggal+Shift+Ruangan"),
        ("KEY_KELAS", "Tanggal+Shift+Kelas"),
        ("KEY_DOSEN", "Tanggal+Shift+Dosen"),
    ]
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["JENIS_KEY", "KEY", "JUMLAH"])
        for key_col, nama_col in keys:
            c = Counter([r.get(key_col, "") for r in records if r.get(key_col,"")])
            for k, v in c.most_common():
                writer.writerow([nama_col, k, v])


def main():
    base = Path(__file__).parent
    # Prioritas cek hasil generate (cek jadwal-uts-fix.csv dulu jika ada)
    candidates = [
        base / "jadwal-uts-fix.csv",
        base / "jadwal-uts-output.csv",
        base / "jadwal-uts.csv",
    ]
    src = None
    for p in candidates:
        if p.exists():
            src = p
            break
    if src is None:
        print("Tidak menemukan file jadwal untuk dicek.")
        print(f"Mencari file di: {[str(p) for p in candidates]}")
        return
    
    print(f"Mengecek konflik pada file: {src.name}")

    header, records = read_schedule(src)
    print(f"Total baris data: {len(records)}")
    
    # Deduplicate sebelum pengecekan
    records_before = len(records)
    records = deduplicate_records(records)
    records_after = len(records)
    if records_before != records_after:
        print(f"Deduplikasi: {records_before} -> {records_after} baris")
    
    # Tambahkan KEY ke records untuk manual audit
    add_keys(records)
    
    # Kelas: overlap + limit >2/hari
    print("\n=== Mengecek Konflik Kelas ===")
    class_conflicts = find_class_conflicts(records)
    out_path_class = base / "kelas-conflicts.csv"
    write_conflicts(class_conflicts, out_path_class)
    print(f"Konflik kelas ditemukan: {len(class_conflicts)}")
    if class_conflicts:
        print(f"  -> Detail disimpan di: {out_path_class.name}")

    # Ruangan: double-booking
    print("\n=== Mengecek Konflik Ruangan ===")
    room_conflicts = find_room_conflicts(records)
    out_path_room = base / "ruangan-conflicts.csv"
    write_conflicts(room_conflicts, out_path_room)
    print(f"Konflik ruangan ditemukan: {len(room_conflicts)}")
    if room_conflicts:
        print(f"  -> Detail disimpan di: {out_path_room.name}")

    # Blacklist: pelanggaran aturan tanggal-ruangan
    print("\n=== Mengecek Pelanggaran Blacklist Ruangan ===")
    blacklist_violations = find_blacklist_violations(records)
    out_path_blk = base / "ruangan-blacklist-violations.csv"
    write_conflicts(blacklist_violations, out_path_blk)
    print(f"Pelanggaran blacklist ditemukan: {len(blacklist_violations)}")
    if blacklist_violations:
        print(f"  -> Detail disimpan di: {out_path_blk.name}")

    # Dosen: konflik waktu mengawasi ujian
    print("\n=== Mengecek Konflik Dosen ===")
    dosen_conflicts = find_dosen_conflicts(records)
    out_path_dosen = base / "dosen-conflicts.csv"
    write_conflicts(dosen_conflicts, out_path_dosen)
    print(f"Konflik dosen ditemukan: {len(dosen_conflicts)}")
    if dosen_conflicts:
        print(f"  -> Detail disimpan di: {out_path_dosen.name}")

    # Tambahkan fitur ekspor rekapitulasi key (TANGGAL+SHIFT+RUANGAN, dsb) untuk manual cek cepat
    print("\n=== Membuat Rekapitulasi Key ===")
    write_key_summary(records, base / "rekap_kombinasi_key.csv")
    print(f"  -> Rekapitulasi disimpan di: rekap_kombinasi_key.csv")

    # Ringkasan
    print("\n" + "="*50)
    print("RINGKASAN HASIL PENGECEKAN:")
    print(f"  Total data: {len(records)} baris")
    print(f"  Konflik kelas: {len(class_conflicts)}")
    print(f"  Konflik ruangan: {len(room_conflicts)}")
    print(f"  Konflik dosen: {len(dosen_conflicts)}")
    print(f"  Pelanggaran blacklist: {len(blacklist_violations)}")
    total_issues = len(class_conflicts) + len(room_conflicts) + len(dosen_conflicts) + len(blacklist_violations)
    if total_issues == 0:
        print("\n✓ Tidak ada konflik atau pelanggaran yang ditemukan!")
    else:
        print(f"\n⚠ Total masalah ditemukan: {total_issues}")
    print("="*50)


if __name__ == "__main__":
    main()


