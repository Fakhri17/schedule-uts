import csv
import random
from datetime import datetime, timedelta, time
from collections import defaultdict
from pathlib import Path

try:
    import pandas as pd
except Exception:
    pd = None

# ============================ Konfigurasi ============================
START_DATE = datetime(2025, 11, 3)
END_DATE = datetime(2025, 11, 7)
ALLOWED_SHIFT_STARTS = [time(7, 30), time(10, 0), time(13, 0), time(15, 30)]
SHIFT_DURATION_MIN = 120

# Blacklist ruangan berdasarkan hari
BLACKLIST_MON_WED_SUFFIXES = {"KTT 2.08", "KTT 2.07", "KTT 2.06", "KTT 2.05", "KTT 2.04"}
BLACKLIST_MON_FRI_SUFFIXES = {"KTT 2.09"}

ALL_ROOMS = []

def is_within_uts_week(date_dt: datetime) -> bool:
    return START_DATE.date() <= date_dt.date() <= END_DATE.date()

def is_room_blacklisted_on_date(room: str, date_dt: datetime) -> bool:
    """Return True jika ruangan diblacklist pada tanggal tersebut."""
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

def load_rooms_from_csv(rooms_csv_path: Path) -> list[str]:
    rooms = []
    try:
        with rooms_csv_path.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.reader(f, delimiter=";")
            rows = list(reader)
        for row in rows[1:]:
            if len(row) > 0 and row[0].strip():
                room_name = row[0].strip()
                if room_name:
                    rooms.append(room_name)
    except Exception as e:
        print(f"Error loading rooms: {e}")
    return rooms

def parse_existing_datetime(hari: str, tanggal: str, shift: str) -> tuple[datetime, datetime] | None:
    if not tanggal or not shift:
        return None
    date_formats = ["%d-%b-%y", "%d/%m/%Y", "%d-%m-%Y"]
    date_dt = None
    for fmt in date_formats:
        try:
            date_dt = datetime.strptime(tanggal.strip(), fmt)
            break
        except Exception:
            continue
    if date_dt is None:
        return None
    parts = shift.split("-")
    if len(parts) != 2:
        return None
    start_s = parts[0].strip().replace(" ", "").replace("\t", "")
    end_s = parts[1].strip().replace(" ", "").replace("\t", "")
    try:
        h1, m1 = map(int, start_s.split("."))
        h2, m2 = map(int, end_s.split("."))
    except Exception:
        return None
    start_dt = datetime.combine(date_dt.date(), time(h1, m1))
    end_dt = datetime.combine(date_dt.date(), time(h2, m2))
    return start_dt, end_dt

def format_time_range(start_dt: datetime, end_dt: datetime) -> str:
    return f"{start_dt:%H.%M} - {end_dt:%H.%M}"

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

def generate_daily_shifts(start_date: datetime) -> list[tuple[datetime, datetime]]:
    shifts = []
    for s in ALLOWED_SHIFT_STARTS:
        start_dt = datetime.combine(start_date.date(), s)
        end_dt = start_dt + timedelta(minutes=SHIFT_DURATION_MIN)
        shifts.append((start_dt, end_dt))
    return shifts

def iter_allowed_dates():
    cur = START_DATE
    while cur.date() <= END_DATE.date():
        if cur.weekday() < 5:
            yield cur
        cur += timedelta(days=1)

def pick_free_room(date_dt: datetime, date_key: str, shift_key: str, start_dt: datetime, end_dt: datetime, 
                   room_usage: dict, allow_aula: bool = False, bentuk_ujian: str = "", jumlah_mhs: int = 0) -> str | None:
    used_counts = room_usage.get(date_key, {}).get(shift_key, {})
    normal_candidates = []
    aula_candidates = []
    
    for r in ALL_ROOMS:
        if is_room_blacklisted_on_date(r, date_dt):
            continue
        
        used_count = used_counts.get(r, 0)
        
        if r.strip().upper() == "AULA":
            if not allow_aula:
                continue
            bentuk = (bentuk_ujian or "").strip().lower()
            if bentuk != "ujian tulis" or jumlah_mhs <= 0 or jumlah_mhs < 40:
                continue
            if used_count < 2:
                aula_candidates.append(r)
        else:
            if used_count == 0:
                normal_candidates.append(r)
    
    # Prioritize normal rooms first, then AULA if allowed
    if normal_candidates:
        return random.choice(normal_candidates)
    if aula_candidates:
        return random.choice(aula_candidates)
    return None

def read_conflict_files(base: Path):
    """Baca semua file konflik dan kumpulkan row indices yang konflik."""
    conflict_indices = set()
    
    # Baca konflik kelas
    class_conflict_file = base / "kelas-conflicts.csv"
    if class_conflict_file.exists():
        with class_conflict_file.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                kelas = row.get("KELAS", "").strip()
                tanggal = row.get("TANGGAL", "").strip()
                shift1 = row.get("SHIFT_1", "").strip()
                shift2 = row.get("SHIFT_2", "").strip()
                kode1 = row.get("KODE 1", "").strip()
                kode2 = row.get("KODE 2", "").strip()
                if kelas and tanggal:
                    # Mark both entries as conflicted
                    conflict_indices.add((kelas, tanggal, shift1, kode1))
                    conflict_indices.add((kelas, tanggal, shift2, kode2))
    
    # Baca konflik ruangan
    room_conflict_file = base / "ruangan-conflicts.csv"
    if room_conflict_file.exists():
        with room_conflict_file.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                kelas = row.get("KELAS", "").strip()
                tanggal = row.get("TANGGAL", "").strip()
                shift = row.get("SHIFT", "").strip()
                kode = row.get("KODE", "").strip()
                if kelas and tanggal and shift:
                    conflict_indices.add((kelas, tanggal, shift, kode))
    
    # Baca konflik dosen
    dosen_conflict_file = base / "dosen-conflicts.csv"
    if dosen_conflict_file.exists():
        with dosen_conflict_file.open("r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                kelas1 = row.get("KELAS_1", "").strip()
                kelas2 = row.get("KELAS_2", "").strip()
                tanggal1 = row.get("TANGGAL_1", "").strip()
                tanggal2 = row.get("TANGGAL_2", "").strip()
                shift1 = row.get("SHIFT_1", "").strip()
                shift2 = row.get("SHIFT_2", "").strip()
                # Ambil kode dari nama mata kuliah jika ada
                mk1 = row.get("MATA KULIAH 1", "").strip()
                mk2 = row.get("MATA KULIAH 2", "").strip()
                if kelas1 and tanggal1 and shift1:
                    conflict_indices.add((kelas1, tanggal1, shift1, ""))
                if kelas2 and tanggal2 and shift2:
                    conflict_indices.add((kelas2, tanggal2, shift2, ""))
    
    return conflict_indices

def main():
    base = Path(__file__).parent
    input_csv = base / "jadwal-uts-fix.csv"
    rooms_csv = base / "ruangan-kampus.csv"
    
    # Load rooms
    global ALL_ROOMS
    ALL_ROOMS = load_rooms_from_csv(rooms_csv)
    print(f"Loaded {len(ALL_ROOMS)} rooms")
    
    # Baca conflict indices
    conflict_keys = read_conflict_files(base)
    print(f"Found {len(conflict_keys)} conflicted entries to fix")
    
    if not conflict_keys:
        print("No conflicts found. Exiting.")
        return
    
    # Read all rows from CSV
    rows = []
    with input_csv.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f, delimiter=";")
        header = next(reader)
        rows = list(reader)
    
    col_idx = {name.strip().upper(): i for i, name in enumerate(header)}
    
    def get(row, key, default=""):
        i = col_idx.get(key, None)
        if i is None or i >= len(row):
            return default
        return row[i].strip()
    
    # Identifikasi row indices yang konflik
    conflicted_row_indices = []
    for idx, row in enumerate(rows):
        if not any(row):
            continue
        
        kelas = get(row, "KELAS")
        tanggal = get(row, "TANGGAL")
        shift = get(row, "SHIFT")
        kode = get(row, "KODE MATA KULIAH")
        
        if not kelas or not tanggal or not shift:
            continue
        
        # Check if this row is in conflict list
        key = (kelas, tanggal, shift, kode)
        # Try with empty kode too (for dosen conflicts where kode might not be available)
        key_no_kode = (kelas, tanggal, shift, "")
        
        if key in conflict_keys or key_no_kode in conflict_keys:
            conflicted_row_indices.append(idx)
            print(f"Row {idx+1}: CONFLICT - {kelas} {tanggal} {shift}")
    
    print(f"\nTotal {len(conflicted_row_indices)} rows need to be regenerated")
    
    # Build usage map from all NON-conflicted entries
    room_usage = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    class_usage = defaultdict(list)
    class_daily_count = defaultdict(lambda: defaultdict(int))
    dosen_usage = defaultdict(list)
    
    for idx, row in enumerate(rows):
        if idx in conflicted_row_indices:
            continue
        if not any(row):
            continue
        
        hari = get(row, "HARI")
        tanggal = get(row, "TANGGAL")
        shift = get(row, "SHIFT")
        ruangan = get(row, "RUANGAN")
        kelas = get(row, "KELAS")
        dosen = get(row, "NAMA DOSEN").strip()
        
        if not hari or not tanggal or not shift:
            continue
        
        parsed = parse_existing_datetime(hari, tanggal, shift)
        if parsed is None:
            continue
        
        start_dt, end_dt = parsed
        date_key = start_dt.strftime("%Y-%m-%d")
        shift_key = format_time_range(start_dt, end_dt)
        
        if ruangan:
            room_usage[date_key][shift_key][ruangan] += 1
        if kelas:
            class_usage[kelas].append((start_dt, end_dt))
            class_daily_count[kelas][date_key] += 1
        if dosen:
            dosen_usage[dosen].append((start_dt, end_dt))
    
    # Conflict checking functions
    def is_class_conflict(kelas: str, start_dt: datetime, end_dt: datetime) -> bool:
        if not kelas:
            return False
        for s, e in class_usage.get(kelas, []):
            if not (end_dt <= s or start_dt >= e):
                return True
        date_key = start_dt.strftime("%Y-%m-%d")
        if class_daily_count[kelas][date_key] >= 2:
            return True
        return False
    
    def is_dosen_conflict(dosen: str, start_dt: datetime, end_dt: datetime) -> bool:
        if not dosen:
            return False
        for s, e in dosen_usage.get(dosen, []):
            if not (end_dt <= s or start_dt >= e):
                return True
        return False
    
    def is_room_conflict(ruangan: str, date_key: str, shift_key: str) -> bool:
        used_counts = room_usage.get(date_key, {}).get(shift_key, {})
        count = used_counts.get(ruangan, 0)
        if ruangan.strip().upper() == "AULA":
            return count >= 2
        else:
            return count >= 1
    
    # Regenerate conflicted entries
    fixed_count = 0
    for idx in conflicted_row_indices:
        row = rows[idx]
        kelas = get(row, "KELAS")
        bentuk_ujian = get(row, "BENTUK UJIAN")
        jumlah_mhs_str = get(row, "JUMLAH MAHASISWA")
        dosen = get(row, "NAMA DOSEN").strip()
        
        try:
            jumlah_mhs = int(jumlah_mhs_str) if jumlah_mhs_str else 0
        except:
            jumlah_mhs = 0
        
        allow_aula = (bentuk_ujian.strip().lower() == "ujian tulis" and jumlah_mhs >= 40)
        
        found_new_slot = False
        
        # Try to find new slot: first same date different time, then different dates
        # Start from the original date if possible
        original_hari = get(row, "HARI")
        original_tanggal = get(row, "TANGGAL")
        
        # Try dates in order: original date first, then others
        dates_to_try = []
        if original_hari and original_tanggal:
            parsed_orig = parse_existing_datetime(original_hari, original_tanggal, "07.30 - 09.30")
            if parsed_orig:
                dates_to_try.append(parsed_orig[0])
        
        # Add other allowed dates
        for day_dt in iter_allowed_dates():
            if not dates_to_try or day_dt.date() != dates_to_try[0].date():
                dates_to_try.append(day_dt)
        
        for day_dt in dates_to_try:
            for s_start, s_end in generate_daily_shifts(day_dt):
                if is_class_conflict(kelas, s_start, s_end):
                    continue
                if is_dosen_conflict(dosen, s_start, s_end):
                    continue
                
                date_key_new = s_start.strftime("%Y-%m-%d")
                shift_key_new = format_time_range(s_start, s_end)
                
                new_room = pick_free_room(s_start, date_key_new, shift_key_new, s_start, s_end,
                                         room_usage, allow_aula, bentuk_ujian, jumlah_mhs)
                
                if new_room and not is_room_conflict(new_room, date_key_new, shift_key_new):
                    # Update row
                    hari_col = col_idx.get("HARI")
                    tanggal_col = col_idx.get("TANGGAL")
                    shift_col = col_idx.get("SHIFT")
                    ruangan_col = col_idx.get("RUANGAN")
                    
                    max_col = max([c for c in [hari_col, tanggal_col, shift_col, ruangan_col] if c is not None])
                    while len(row) <= max_col:
                        row.append("")
                    
                    if hari_col is not None:
                        row[hari_col] = weekday_name(s_start)
                    if tanggal_col is not None:
                        row[tanggal_col] = s_start.strftime("%d-%b-%y")
                    if shift_col is not None:
                        row[shift_col] = shift_key_new
                    if ruangan_col is not None:
                        row[ruangan_col] = new_room
                    
                    # Update usage maps
                    room_usage[date_key_new][shift_key_new][new_room] += 1
                    if kelas:
                        class_usage[kelas].append((s_start, s_end))
                        class_daily_count[kelas][date_key_new] += 1
                    if dosen:
                        dosen_usage[dosen].append((s_start, s_end))
                    
                    fixed_count += 1
                    found_new_slot = True
                    print(f"Fixed row {idx+1}: {kelas} -> {date_key_new} {shift_key_new} {new_room}")
                    break
            
            if found_new_slot:
                break
        
        if not found_new_slot:
            print(f"Warning: Could not fix row {idx+1} (kelas: {kelas})")
    
    print(f"\nFixed {fixed_count} out of {len(conflicted_row_indices)} conflicted entries")
    
    # Write output
    output_csv = base / "jadwal-uts-fix.csv"
    with output_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(header)
        for row in rows:
            writer.writerow(row)
    
    print(f"Output written to {output_csv.name}")
    print("\nPlease run check_conflicts.py again to verify no conflicts remain.")

if __name__ == "__main__":
    main()

