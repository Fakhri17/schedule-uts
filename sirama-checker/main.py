"""
Croscheck komparasi data jadwal SIRAMA.
7 aturan: belum jadwal, SKS vs jam, maghrib, bentrok dosen, bentrok ruangan, bentrok angkatan, ruangan kosong.
Output: file Excel (satu sheet per aturan + ringkasan).
"""
from __future__ import annotations

from datetime import time
from pathlib import Path

import pandas as pd

# -----------------------------------------------------------------------------
# Konfigurasi
# -----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
EXCEL_PATH = BASE_DIR / "030226 MK SIRAMA V2.xlsx"
OUTPUT_PATH = BASE_DIR / "hasil_croscheck_sirama.xlsx"

MAGHRIB_START = time(17, 30)
MAGHRIB_END = time(19, 30)


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def parse_shift_to_duration(shift_series: pd.Series) -> pd.Series:
    """Parse kolom SHIFT (e.g. '13:30:00 - 16:30:00') -> durasi dalam jam (float)."""
    def _parse(s):
        if pd.isna(s) or not isinstance(s, str):
            return None
        parts = s.split("-", 1)
        if len(parts) != 2:
            return None
        start_s = parts[0].strip().replace(" ", "")
        end_s = parts[1].strip().replace(" ", "")
        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                from datetime import datetime
                t1 = datetime.strptime(start_s, fmt).time()
                t2 = datetime.strptime(end_s, fmt).time()
                # duration in hours
                h1, m1 = t1.hour, t1.minute
                h2, m2 = t2.hour, t2.minute
                return (h2 - h1) + (m2 - m1) / 60.0
            except ValueError:
                continue
        return None

    return shift_series.map(_parse)


def parse_shift_times(shift_series: pd.Series) -> tuple[pd.Series, pd.Series]:
    """Return (start_time, end_time) as time objects (or None)."""
    def _parse(s):
        if pd.isna(s) or not isinstance(s, str):
            return None, None
        parts = s.split("-", 1)
        if len(parts) != 2:
            return None, None
        start_s = parts[0].strip().replace(" ", "")
        end_s = parts[1].strip().replace(" ", "")
        from datetime import datetime
        t1 = t2 = None
        for fmt in ("%H:%M:%S", "%H:%M"):
            try:
                t1 = datetime.strptime(start_s, fmt).time()
                t2 = datetime.strptime(end_s, fmt).time()
                break
            except ValueError:
                continue
        return t1, t2

    starts = shift_series.map(lambda s: _parse(s)[0])
    ends = shift_series.map(lambda s: _parse(s)[1])
    return starts, ends


def overlaps_maghrib(start_t: time | None, end_t: time | None) -> bool:
    if start_t is None or end_t is None:
        return False
    return start_t < MAGHRIB_END and end_t > MAGHRIB_START


def normalize_ruangan(ruangan_series: pd.Series) -> pd.Series:
    """Normalisasi nama ruangan: strip, uppercase, collapse spasi."""
    return (
        ruangan_series.astype(str).str.strip().str.upper().str.replace(r"\s+", " ", regex=True)
    )


def extract_angkatan(kelas: str) -> str:
    """IT-06-GAB -> IT-06, DB-04-05 -> DB-04."""
    if pd.isna(kelas):
        return ""
    parts = str(kelas).strip().split("-")
    return "-".join(parts[:2]) if len(parts) >= 2 else str(kelas)


# -----------------------------------------------------------------------------
# Load data
# -----------------------------------------------------------------------------
def load_sirama(path: Path) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    xl = pd.ExcelFile(path)
    course = pd.read_excel(xl, sheet_name="Course")
    jadwal = pd.read_excel(xl, sheet_name="Jadwal")
    dosen_all = pd.read_excel(xl, sheet_name="Dosen ALL")
    return course, jadwal, dosen_all


# -----------------------------------------------------------------------------
# 7 Aturan croscheck
# -----------------------------------------------------------------------------
def check_1_belum_jadwal(course: pd.DataFrame, jadwal: pd.DataFrame) -> pd.DataFrame:
    """Mata kuliah yang belum dijadwalkan (UID di Course tidak ada di Jadwal)."""
    uid_jadwal = set(jadwal["UID"].dropna().astype(str))
    mask = ~course["UID"].astype(str).isin(uid_jadwal)
    out = course.loc[mask, ["UID", "MATA KULIAH", "KODE KULIAH", "KELAS", "DOSEN/TIM DOSEN", "PROGRAM STUDI"]].copy()
    out.insert(0, "NO", range(1, len(out) + 1))
    return out


def check_2_tidak_match_sks(course: pd.DataFrame, jadwal: pd.DataFrame) -> pd.DataFrame:
    """Jadwal yang durasi jam tidak sama dengan SKS (1 SKS = 1 jam)."""
    j = jadwal[["UID", "HARI", "SHIFT", "RUANGAN", "NAMA MATA KULIAH", "KELAS"]].copy()
    j["DURASI_JAM"] = parse_shift_to_duration(jadwal["SHIFT"])
    c = course[["UID", "SKS"]].copy()
    merged = j.merge(c, on="UID", how="left")
    merged["SKS"] = pd.to_numeric(merged["SKS"], errors="coerce").fillna(0).astype(int)
    mask = merged["DURASI_JAM"].notna() & (merged["DURASI_JAM"] != merged["SKS"])
    out = merged.loc[mask, ["UID", "NAMA MATA KULIAH", "KELAS", "SKS", "DURASI_JAM", "HARI", "SHIFT", "RUANGAN"]].copy()
    out = out.rename(columns={"DURASI_JAM": "DURASI_JAM_AKTUAL"})
    out.insert(0, "NO", range(1, len(out) + 1))
    return out


def check_3_jadwal_maghrib(jadwal: pd.DataFrame) -> pd.DataFrame:
    """Jadwal yang overlap dengan waktu maghrib (17:30 - 19:30)."""
    starts, ends = parse_shift_times(jadwal["SHIFT"])
    mask = pd.Series(
        [overlaps_maghrib(starts.iloc[i], ends.iloc[i]) for i in range(len(jadwal))],
        index=jadwal.index,
    )
    out = jadwal.loc[mask, ["UID", "HARI", "SHIFT", "RUANGAN", "NAMA MATA KULIAH", "KELAS", "DOSEN"]].copy()
    out.insert(0, "NO", range(1, len(out) + 1))
    return out


def check_4_bentrok_dosen(jadwal: pd.DataFrame) -> pd.DataFrame:
    """Dosen yang double booking (HARI, SHIFT, DOSEN) muncul > 1 kali."""
    g = jadwal.groupby(["HARI", "SHIFT", "DOSEN"], dropna=False)
    bentrok = g.filter(lambda x: len(x) > 1)
    if bentrok.empty:
        return pd.DataFrame(columns=["NO", "HARI", "SHIFT", "DOSEN", "UID", "NAMA MATA KULIAH", "KELAS", "RUANGAN"])
    out = bentrok[["HARI", "SHIFT", "DOSEN", "UID", "NAMA MATA KULIAH", "KELAS", "RUANGAN"]].copy()
    out = out.sort_values(["HARI", "SHIFT", "DOSEN"])
    out.insert(0, "NO", range(1, len(out) + 1))
    return out


def check_5_bentrok_ruangan(jadwal: pd.DataFrame) -> pd.DataFrame:
    """Ruangan double booking (HARI, SHIFT, RUANGAN normalisasi) > 1."""
    j = jadwal.copy()
    j["RUANGAN_NORM"] = normalize_ruangan(j["RUANGAN"])
    g = j.groupby(["HARI", "SHIFT", "RUANGAN_NORM"], dropna=False)
    bentrok = g.filter(lambda x: len(x) > 1)
    if bentrok.empty:
        return pd.DataFrame(columns=["NO", "HARI", "SHIFT", "RUANGAN", "UID", "NAMA MATA KULIAH", "KELAS", "DOSEN"])
    out = bentrok[["HARI", "SHIFT", "RUANGAN", "UID", "NAMA MATA KULIAH", "KELAS", "DOSEN"]].copy()
    out = out.sort_values(["HARI", "SHIFT", "RUANGAN"])
    out.insert(0, "NO", range(1, len(out) + 1))
    return out


def check_6_bentrok_angkatan(jadwal: pd.DataFrame) -> pd.DataFrame:
    """Satu angkatan (IT-06, DB-04, ...) dapat 2+ kelas di (HARI, SHIFT) yang sama."""
    j = jadwal.copy()
    j["ANGKATAN"] = j["KELAS"].apply(extract_angkatan)
    g = j.groupby(["HARI", "SHIFT", "ANGKATAN"], dropna=False)
    bentrok = g.filter(lambda x: len(x) > 1)
    if bentrok.empty:
        return pd.DataFrame(columns=["NO", "HARI", "SHIFT", "ANGKATAN", "UID", "NAMA MATA KULIAH", "KELAS", "DOSEN"])
    out = bentrok[["HARI", "SHIFT", "ANGKATAN", "UID", "NAMA MATA KULIAH", "KELAS", "DOSEN"]].copy()
    out = out.sort_values(["HARI", "SHIFT", "ANGKATAN"])
    out.insert(0, "NO", range(1, len(out) + 1))
    return out


def check_7_ruangan_kosong(jadwal: pd.DataFrame) -> pd.DataFrame:
    """Per slot (HARI, SHIFT): ruangan yang ada di Jadwal tapi tidak dipakai di slot itu (sementara dari Jadwal)."""
    j = jadwal.copy()
    j["RUANGAN_NORM"] = normalize_ruangan(j["RUANGAN"])
    semua_ruangan = set(j["RUANGAN_NORM"].dropna().unique())
    slots = j.groupby(["HARI", "SHIFT"], dropna=False)
    rows = []
    for (hari, shift), grp in slots:
        terpakai = set(grp["RUANGAN_NORM"].dropna().unique())
        kosong = sorted(semua_ruangan - terpakai)
        if kosong:
            rows.append({"HARI": hari, "SHIFT": shift, "RUANGAN_KOSONG": ", ".join(kosong), "JUMLAH": len(kosong)})
    if not rows:
        return pd.DataFrame(columns=["NO", "HARI", "SHIFT", "RUANGAN_KOSONG", "JUMLAH"])
    out = pd.DataFrame(rows)
    out = out.sort_values(["HARI", "SHIFT"])
    out.insert(0, "NO", range(1, len(out) + 1))
    return out


def build_summary(results: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Sheet ringkasan: nama cek + jumlah baris."""
    rows = [
        {"NO": 1, "PENGECEKAN": "1. Mata kuliah belum dijadwalkan", "JUMLAH": len(results["1_belum_jadwal"])},
        {"NO": 2, "PENGECEKAN": "2. Jadwal tidak match SKS (1 SKS = 1 jam)", "JUMLAH": len(results["2_tidak_match_sks"])},
        {"NO": 3, "PENGECEKAN": "3. Jadwal overlap maghrib (17:30-19:30)", "JUMLAH": len(results["3_jadwal_maghrib"])},
        {"NO": 4, "PENGECEKAN": "4. Bentrok jadwal dosen", "JUMLAH": len(results["4_bentrok_dosen"])},
        {"NO": 5, "PENGECEKAN": "5. Bentrok jadwal ruangan", "JUMLAH": len(results["5_bentrok_ruangan"])},
        {"NO": 6, "PENGECEKAN": "6. Bentrok jadwal per angkatan", "JUMLAH": len(results["6_bentrok_angkatan"])},
        {"NO": 7, "PENGECEKAN": "7. Ruangan kosong per slot (sementara dari Jadwal)", "JUMLAH": len(results["7_ruangan_kosong"])},
    ]
    return pd.DataFrame(rows)


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def main() -> None:
    if not EXCEL_PATH.exists():
        print(f"File tidak ditemukan: {EXCEL_PATH}")
        return

    print("Memuat data SIRAMA...")
    course, jadwal, dosen_all = load_sirama(EXCEL_PATH)

    print("Menjalankan 7 croscheck...")
    results = {
        "1_belum_jadwal": check_1_belum_jadwal(course, jadwal),
        "2_tidak_match_sks": check_2_tidak_match_sks(course, jadwal),
        "3_jadwal_maghrib": check_3_jadwal_maghrib(jadwal),
        "4_bentrok_dosen": check_4_bentrok_dosen(jadwal),
        "5_bentrok_ruangan": check_5_bentrok_ruangan(jadwal),
        "6_bentrok_angkatan": check_6_bentrok_angkatan(jadwal),
        "7_ruangan_kosong": check_7_ruangan_kosong(jadwal),
    }

    summary = build_summary(results)

    print(f"Menulis hasil ke {OUTPUT_PATH}...")
    with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as writer:
        summary.to_excel(writer, sheet_name="0_ringkasan", index=False)
        for name, df in results.items():
            df.to_excel(writer, sheet_name=name, index=False)

    print("Selesai.")
    print("\nRingkasan:")
    for _, row in summary.iterrows():
        print(f"  {row['PENGECEKAN']}: {row['JUMLAH']} baris")


if __name__ == "__main__":
    main()
