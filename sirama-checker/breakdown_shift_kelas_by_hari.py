"""
Breakdown shift per slot — cek ruang kelas belum terisi.
**Urutan: Hari dulu, baru Ruang Kelas, lalu Shift** (bedanya dengan
breakdown_shift_kelas.py yang urut Ruang Kelas dulu).

Dataset: 180226 Jadwal SIRAMA - baru.xlsx
Output: Ruang Kelas, Hari, Shift, MK, Kelas, Dosen (kosong = slot belum terisi).
"""
from __future__ import annotations

from datetime import datetime, timedelta, time
from pathlib import Path

import pandas as pd

# -----------------------------------------------------------------------------
# Konfigurasi
# -----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
EXCEL_PATH = BASE_DIR / "180226 Jadwal SIRAMA - baru.xlsx"
OUTPUT_PATH = BASE_DIR / "hasil_breakdown_shift_kelas_by_hari.xlsx"

HARI_ORDER = ["SENIN", "SELASA", "RABU", "KAMIS", "JUMAT", "SABTU"]
# Slot per jam: 06.30 s/d 18.30
SLOT_HOURS = list(range(6, 19))


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def parse_shift(s: str) -> tuple[time | None, time | None]:
    """Parse '13:30:00 - 15:30:00' -> (time(13,30), time(15,30))."""
    if pd.isna(s) or not isinstance(s, str):
        return None, None
    parts = s.split("-", 1)
    if len(parts) != 2:
        return None, None
    start_s = parts[0].strip().replace(" ", "")
    end_s = parts[1].strip().replace(" ", "")
    for fmt in ("%H:%M:%S", "%H:%M"):
        try:
            t1 = datetime.strptime(start_s, fmt).time()
            t2 = datetime.strptime(end_s, fmt).time()
            return t1, t2
        except ValueError:
            continue
    return None, None


def time_to_shift_str(t: time | None) -> str | None:
    """time(6, 30) -> '06.30'."""
    if t is None:
        return None
    return f"{t.hour:02d}.{t.minute:02d}"


def range_to_slots(start_t: time | None, end_t: time | None) -> list[str]:
    """Generate slot strings per jam dari start sampai sebelum end.
    06:30-09:30 -> ['06.30','07.30','08.30'].
    """
    if start_t is None or end_t is None:
        return []
    slots = []
    dt = datetime.combine(datetime.today().date(), start_t)
    end_dt = datetime.combine(datetime.today().date(), end_t)
    while dt < end_dt:
        slots.append(time_to_shift_str(dt.time()) or "")
        dt += timedelta(hours=1)
    return slots


# -----------------------------------------------------------------------------
# Load & process
# -----------------------------------------------------------------------------
def load_data(path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Load sheet Master Jadwal SIRAMA dan Master Ruangan TUS."""
    xl = pd.ExcelFile(path)
    jadwal = pd.read_excel(xl, sheet_name="Master Jadwal SIRAMA")
    ruang = pd.read_excel(xl, sheet_name="Master Ruangan TUS")
    return jadwal, ruang


def build_filled_slots(jadwal: pd.DataFrame) -> pd.DataFrame:
    """Breakdown setiap baris jadwal (range SHIFT) menjadi baris per slot jam."""
    rows = []
    for _, r in jadwal.iterrows():
        start_t, end_t = parse_shift(r["SHIFT"])
        slot_strs = range_to_slots(start_t, end_t)
        for slot in slot_strs:
            rows.append(
                {
                    "Ruang Kelas": r["RUANGAN"],
                    "Hari": r["HARI"],
                    "Shift": slot,
                    "MK": r["NAMA MATA KULIAH"],
                    "Kelas": r["KELAS"],
                    "Dosen": r["DOSEN"],
                }
            )
    return pd.DataFrame(rows)


def build_full_grid(
    ruang_df: pd.DataFrame, all_slots: list[str]
) -> pd.DataFrame:
    """Semua kombinasi (Ruang Kelas, Hari, Shift)."""
    ruang_list = (
        ruang_df["Nama Ruang"].dropna().astype(str).str.strip().unique().tolist()
    )
    grid_rows = []
    for ruang in ruang_list:
        for hari in HARI_ORDER:
            for shift in all_slots:
                grid_rows.append(
                    {"Ruang Kelas": ruang, "Hari": hari, "Shift": shift}
                )
    return pd.DataFrame(grid_rows)


def run_breakdown(jadwal: pd.DataFrame, ruang: pd.DataFrame) -> pd.DataFrame:
    """Gabung grid penuh dengan slot terisi; MK kosong = belum terisi.
    Urutan: Hari -> Ruang Kelas -> Shift.
    """
    all_slots = [f"{h:02d}.30" for h in SLOT_HOURS]
    filled = build_filled_slots(jadwal)
    grid = build_full_grid(ruang, all_slots)
    merged = grid.merge(
        filled.drop_duplicates(["Ruang Kelas", "Hari", "Shift"]),
        on=["Ruang Kelas", "Hari", "Shift"],
        how="left",
    )
    merged["MK"] = merged["MK"].fillna("")
    merged["Kelas"] = merged["Kelas"].fillna("")
    merged["Dosen"] = merged["Dosen"].fillna("")
    merged["Hari"] = pd.Categorical(
        merged["Hari"], categories=HARI_ORDER, ordered=True
    )
    # Urut Hari dulu, baru Ruang Kelas, lalu Shift
    merged = merged.sort_values(["Hari", "Ruang Kelas", "Shift"]).reset_index(
        drop=True
    )
    return merged


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main() -> None:
    print("=" * 50)
    print("  Breakdown Shift per Slot — by Hari")
    print("  (urut: Hari -> Ruang Kelas -> Shift)")
    print("=" * 50)

    if not EXCEL_PATH.exists():
        print(f"File tidak ditemukan: {EXCEL_PATH}")
        return

    print("Memuat data...")
    jadwal, ruang = load_data(EXCEL_PATH)
    print(f"  Jadwal: {len(jadwal)} baris")
    print(f"  Ruang: {len(ruang)} ruang")

    print("Breakdown shift per slot (urut Hari -> Ruang Kelas -> Shift)...")
    result = run_breakdown(jadwal, ruang)

    kosong = (result["MK"] == "").sum()
    print(f"  Total baris: {len(result)}")
    print(f"  Slot kosong (belum terisi): {kosong}")

    print(f"Menulis ke {OUTPUT_PATH}...")
    result.to_excel(OUTPUT_PATH, sheet_name="Breakdown Shift", index=False)
    print("Selesai.")


if __name__ == "__main__":
    main()
