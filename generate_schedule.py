import csv
import random
from datetime import datetime, timedelta, time
from collections import defaultdict
from pathlib import Path

try:
    import pandas as pd  # optional for Excel output
except Exception:  # pragma: no cover
    pd = None


# ============================ Konfigurasi ============================
START_DATE = datetime(2025, 11, 3)  # mulai 3 Nov 2025
END_DATE = datetime(2025, 11, 7)    # berakhir 7 Nov 2025 (weekday only)
DAY_START = time(7, 30)
DAY_END = time(17, 30)
SHIFT_DURATION_MIN = 120  # 2 jam
GAP_BETWEEN_SHIFTS_MIN = 30  # jeda antar shift
LUNCH_BREAK_START = time(12, 0)
LUNCH_BREAK_END = time(13, 0)

# Daftar ruangan yang tersedia
ALL_ROOMS = [
    "KTT1.02","KTT1.08","KTT1.09","KTT1.16","KTT1.17","KTT1.18","KTT1.19",
    "KTT1.20","KTT1.21","KTT1.03","KTT1.04","KTT1.05","KTT1.06","KTT1.07",
    "KTT1.22","KTT2.07","KTT2.08","KTT2.09","KTT2.15","KTT2.16","KTT2.17",
    "KTT2.18","KTT2.19","KTT2.04","KTT2.05","KTT2.03","KTT2.06","KTT2.02",
    "KTT2.23","KTT2.25","KTT2.26","KTT2.27","KTT2.28","KTT2.35","KTT2.36","KTT2.20",
]

# Blacklist ruangan (mudah diedit)
BLACKLIST_SUFFIXES = {"KTT2.09", "KTT2.08", "KTT2.07", "KTT2.06", "KTT2.05", "KTT2.04"}


def is_room_allowed(room: str) -> bool:
    for suf in BLACKLIST_SUFFIXES:
        if room.endswith(suf):
            return False
    return True


def parse_csv(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        rows = list(reader)
    # Header pada file ini bukan standar, gunakan indeks kolom berdasarkan baris 2
    header = rows[1]
    # Buat mapping kolom ke index
    col_idx = {name.strip().upper(): i for i, name in enumerate(header)}

    def get(row, key, default=""):
        i = col_idx.get(key, None)
        if i is None or i >= len(row):
            return default
        return row[i].strip()

    items = []
    for r in rows[2:]:
        if not any(r):
            continue
        kode_mk = get(r, "KODE MATA KULIAH")
        nama_mk = get(r, "NAMA MATA KULIAH")
        nama_dosen = get(r, "NAMA DOSEN")
        kelas = get(r, "KELAS")
        if not kode_mk and not nama_mk:
            continue
        hari = get(r, "HARI")
        tanggal = get(r, "TANGGAL")
        shift = get(r, "SHIFT")  # contoh: "07.30 - 09.30"
        ruangan = get(r, "RUANGAN")

        items.append({
            "kode_mk": kode_mk,
            "nama_mk": nama_mk,
            "nama_dosen": nama_dosen,
            "kelas": kelas,
            "hari": hari,
            "tanggal": tanggal,
            "shift": shift,
            "ruangan": ruangan,
        })
    return items


def format_time_range(start_dt: datetime, end_dt: datetime) -> str:
    return f"{start_dt:%H.%M} - {end_dt:%H.%M}"


def generate_daily_shifts(start_date: datetime) -> list[tuple[datetime, datetime]]:
    """
    Generate shift intervals for satu hari sesuai aturan:
    - mulai 07.30 sampai 17.30
    - durasi 2 jam per shift, jeda 30 menit
    - istirahat 12.00 - 13.00 (lewati rentang ini)
    """
    day_start = datetime.combine(start_date.date(), DAY_START)
    day_end = datetime.combine(start_date.date(), DAY_END)

    shifts: list[tuple[datetime, datetime]] = []
    t = day_start
    while True:
        end_t = t + timedelta(minutes=SHIFT_DURATION_MIN)
        # Jika shift melampaui akhir hari, stop
        if end_t > day_end:
            break
        # Skip jika overlap dengan jam istirahat
        lunch_start_dt = datetime.combine(start_date.date(), LUNCH_BREAK_START)
        lunch_end_dt = datetime.combine(start_date.date(), LUNCH_BREAK_END)
        overlap_lunch = not (end_t <= lunch_start_dt or t >= lunch_end_dt)
        if overlap_lunch:
            # lompat ke setelah istirahat
            t = lunch_end_dt
            end_t = t + timedelta(minutes=SHIFT_DURATION_MIN)
            if end_t > day_end:
                break
            shifts.append((t, end_t))
            t = end_t + timedelta(minutes=GAP_BETWEEN_SHIFTS_MIN)
            continue

        shifts.append((t, end_t))
        t = end_t + timedelta(minutes=GAP_BETWEEN_SHIFTS_MIN)
        if t >= day_end:
            break

    return shifts


def weekday_name(dt: datetime) -> str:
    mapping = {
        0: "SENIN",
        1: "SELASA",
        2: "RABU",
        3: "KAMIS",
        4: "JUM'AT",
        5: "SABTU",
        6: "MINGGU",
    }
    return mapping[dt.weekday()]


def iter_allowed_dates():
    """Iterasi tanggal dari START_DATE s.d END_DATE, hanya Senin-Jumat."""
    cur = START_DATE
    while cur.date() <= END_DATE.date():
        if cur.weekday() < 5:  # 0-4: Mon-Fri
            yield cur
        cur += timedelta(days=1)


def parse_existing_datetime(hari: str, tanggal: str, shift: str) -> tuple[datetime, datetime] | None:
    if not tanggal or not shift:
        return None
    # tanggal contoh: 05-Nov-25
    try:
        date_dt = datetime.strptime(tanggal, "%d-%b-%y")
    except Exception:
        return None
    # shift contoh: 07.30 - 09.30
    parts = shift.split("-")
    if len(parts) != 2:
        return None
    start_s = parts[0].strip().replace(" ", "")
    end_s = parts[1].strip()
    try:
        h1, m1 = map(int, start_s.split("."))
        h2, m2 = map(int, end_s.split("."))
    except Exception:
        return None
    start_dt = datetime.combine(date_dt.date(), time(h1, m1))
    end_dt = datetime.combine(date_dt.date(), time(h2, m2))
    return start_dt, end_dt


def build_schedule(items: list[dict]):
    # State pemakaian: per (tanggal, shift_str) -> set(room), dan kelas-> list times
    room_usage = defaultdict(lambda: defaultdict(set))  # date_str -> shift_str -> set(rooms)
    class_usage = defaultdict(list)  # kelas -> list[(start_dt,end_dt)]

    # Kumpulkan existing jadwal bila ada
    for it in items:
        if it["tanggal"] and it["shift"]:
            parsed = parse_existing_datetime(it["hari"], it["tanggal"], it["shift"]) 
            if parsed is None:
                continue
            start_dt, end_dt = parsed
            date_key = start_dt.strftime("%Y-%m-%d")
            shift_key = format_time_range(start_dt, end_dt)
            cls = it["kelas"]
            if cls:
                class_usage[cls].append((start_dt, end_dt))
            room = it["ruangan"].strip() if it["ruangan"] else ""
            if room:
                room_usage[date_key][shift_key].add(room)

    # Siapkan daftar shifts harian (dipakai untuk mengisi yang kosong)
    # Kita generate untuk beberapa hari ke depan (misal 14 hari) sampai kebutuhan terpenuhi
    generated_assignments: list[dict] = []

    def is_class_conflict(kelas: str, start_dt: datetime, end_dt: datetime) -> bool:
        if not kelas:
            return False
        for s, e in class_usage.get(kelas, []):
            if not (end_dt <= s or start_dt >= e):
                return True
        return False

    # Filter rooms allowed
    allowed_rooms = [r for r in ALL_ROOMS if is_room_allowed(r)]

    # Fungsi memilih ruangan kosong pada tanggal+shift tertentu
    def pick_free_room(date_key: str, shift_key: str) -> str | None:
        used = room_usage[date_key][shift_key]
        free = [r for r in allowed_rooms if r not in used]
        if not free:
            return None
        return random.choice(free)

    # Helper: iterate allowed dates and shifts until assignable

    for it in items:
        kode = it["kode_mk"]
        nama = it["nama_mk"]
        kelas = it["kelas"]
        hari = it["hari"].strip().upper() if it["hari"] else ""
        tanggal = it["tanggal"].strip() if it["tanggal"] else ""
        shift = it["shift"].strip() if it["shift"] else ""
        ruangan = it["ruangan"].strip() if it["ruangan"] else ""

        # Jika hari, tanggal, shift sudah ada: hanya carikan ruangan jika kosong
        parsed = parse_existing_datetime(hari, tanggal, shift)
        if parsed is not None:
            start_dt, end_dt = parsed
            if is_class_conflict(kelas, start_dt, end_dt):
                # kelas bentrok, cari hari/shift lain dalam rentang diizinkan
                assigned = False
                for day_dt in iter_allowed_dates():
                    for s_start, s_end in generate_daily_shifts(day_dt):
                        if is_class_conflict(kelas, s_start, s_end):
                            continue
                        date_key = s_start.strftime("%Y-%m-%d")
                        shift_key = format_time_range(s_start, s_end)
                        room = pick_free_room(date_key, shift_key)
                        if room:
                            class_usage[kelas].append((s_start, s_end))
                            room_usage[date_key][shift_key].add(room)
                            generated_assignments.append({
                                "HARI": weekday_name(s_start),
                                "TANGGAL": s_start.strftime("%d-%b-%y"),
                                "SHIFT": shift_key,
                                "RUANGAN": room,
                                "KODE MATA KULIAH": kode,
                                "NAMA MATA KULIAH": nama,
                                "NAMA DOSEN": it.get("nama_dosen", ""),
                                "KELAS": kelas,
                            })
                            assigned = True
                            break
                    if assigned:
                        break
                if not assigned:
                    generated_assignments.append({
                        "HARI": weekday_name(start_dt),
                        "TANGGAL": start_dt.strftime("%d-%b-%y"),
                        "SHIFT": format_time_range(start_dt, end_dt),
                        "RUANGAN": "",
                        "KODE MATA KULIAH": kode,
                        "NAMA MATA KULIAH": nama,
                        "NAMA DOSEN": it.get("nama_dosen", ""),
                        "KELAS": kelas,
                    })
            else:
                date_key = start_dt.strftime("%Y-%m-%d")
                shift_key = format_time_range(start_dt, end_dt)
                # carikan ruangan jika kosong atau bentrok
                room = ruangan if (ruangan and ruangan not in room_usage[date_key][shift_key] and is_room_allowed(ruangan)) else pick_free_room(date_key, shift_key)
                if room is None:
                    room = ""
                if kelas:
                    class_usage[kelas].append((start_dt, end_dt))
                if room:
                    room_usage[date_key][shift_key].add(room)
                generated_assignments.append({
                    "HARI": weekday_name(start_dt),
                    "TANGGAL": start_dt.strftime("%d-%b-%y"),
                    "SHIFT": shift_key,
                    "RUANGAN": room,
                    "KODE MATA KULIAH": kode,
                    "NAMA MATA KULIAH": nama,
                    "NAMA DOSEN": it.get("nama_dosen", ""),
                    "KELAS": kelas,
                })
            continue

        # Jika belum ada hari/tanggal/shift, generate baru
        assigned = False
        for day_dt in iter_allowed_dates():
            for s_start, s_end in generate_daily_shifts(day_dt):
                if is_class_conflict(kelas, s_start, s_end):
                    continue
                date_key = s_start.strftime("%Y-%m-%d")
                shift_key = format_time_range(s_start, s_end)
                room = pick_free_room(date_key, shift_key)
                if room:
                    class_usage[kelas].append((s_start, s_end))
                    room_usage[date_key][shift_key].add(room)
                    generated_assignments.append({
                        "HARI": weekday_name(s_start),
                        "TANGGAL": s_start.strftime("%d-%b-%y"),
                        "SHIFT": shift_key,
                        "RUANGAN": room,
                        "KODE MATA KULIAH": kode,
                        "NAMA MATA KULIAH": nama,
                        "NAMA DOSEN": it.get("nama_dosen", ""),
                        "KELAS": kelas,
                    })
                    assigned = True
                    break
            if assigned:
                break
        if not assigned:
            # gagal assign, tetap keluarkan tanpa ruangan
            generated_assignments.append({
                "HARI": "",
                "TANGGAL": "",
                "SHIFT": "",
                "RUANGAN": "",
                "KODE MATA KULIAH": kode,
                "NAMA MATA KULIAH": nama,
                "NAMA DOSEN": it.get("nama_dosen", ""),
                "KELAS": kelas,
            })

    return generated_assignments


def write_outputs(assignments, out_csv: Path, out_xlsx: Path | None):
    # Kolom output sesuai permintaan
    cols = [
        "HARI",
        "TANGGAL",
        "SHIFT",
        "RUANGAN",
        "KODE MATA KULIAH",
        "NAMA MATA KULIAH",
        "NAMA DOSEN",
        "KELAS",
    ]
    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(cols)
        for row in assignments:
            w.writerow([row.get(c, "") for c in cols])

    if pd is not None and out_xlsx is not None:
        try:
            # Tuliskan menggunakan pandas
            df = pd.DataFrame(assignments, columns=cols)
            # Coba gunakan openpyxl lebih dulu agar bisa set auto filter
            try:
                from openpyxl import load_workbook  # type: ignore
                with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:  # type: ignore
                    df.to_excel(writer, index=False, sheet_name="Sheet1")
                    ws = writer.book["Sheet1"]
                    # Set auto filter untuk seluruh area data
                    ws.auto_filter.ref = ws.dimensions
                    # Freeze header baris pertama
                    ws.freeze_panes = "A2"
                    # Auto-resize kolom berdasarkan panjang data
                    try:
                        from openpyxl.utils import get_column_letter  # type: ignore
                        for idx, col_name in enumerate(df.columns, start=1):
                            series = df[col_name].astype(str)
                            max_len = max([len(col_name)] + [len(x) for x in series.tolist()])
                            # Faktor lebar ~1 char = 1 unit, tambahkan margin
                            width = max(10, min(60, max_len + 2))
                            ws.column_dimensions[get_column_letter(idx)].width = width
                    except Exception:
                        pass
            except Exception:
                # Fallback ke xlsxwriter jika openpyxl tidak ada
                try:
                    with pd.ExcelWriter(out_xlsx, engine="xlsxwriter") as writer:  # type: ignore
                        df.to_excel(writer, index=False, sheet_name="Sheet1")
                        workbook  = writer.book
                        worksheet = writer.sheets["Sheet1"]
                        # Hitung dimensi data
                        last_row = len(df)
                        last_col = len(df.columns) - 1
                        # Terapkan autofilter (baris 0 adalah header)
                        worksheet.autofilter(0, 0, last_row, last_col)
                        # Freeze header
                        worksheet.freeze_panes(1, 0)
                        # Auto-resize kolom
                        for c, col_name in enumerate(df.columns):
                            series = df[col_name].astype(str)
                            max_len = max([len(col_name)] + [len(x) for x in series.tolist()])
                            width = max(10, min(60, max_len + 2))
                            worksheet.set_column(c, c, width)
                except Exception:
                    # Jika kedua engine tidak tersedia, tulis tanpa fitur tambahan
                    df.to_excel(out_xlsx, index=False)
        except Exception as e:
            # Jika engine Excel (mis. openpyxl/xlsxwriter) belum terpasang, lanjutkan tanpa XLSX
            print(
                "Gagal menulis Excel (", e, ") -> Melewatkan XLSX. "
                "Install salah satu: 'pip install openpyxl' atau 'pip install xlsxwriter' untuk mengaktifkan ekspor Excel.")


def main():
    base = Path(__file__).parent
    input_csv = base / "jadwal-uts.csv"
    items = parse_csv(input_csv)
    assignments = build_schedule(items)
    out_csv = base / "jadwal-uts-output.csv"
    out_xlsx = base / "jadwal-uts-output.xlsx"
    write_outputs(assignments, out_csv, out_xlsx)
    print(f"Selesai. Output: {out_csv.name} dan {out_xlsx.name}")


if __name__ == "__main__":
    main()


