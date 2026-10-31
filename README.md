# data-pipeline-project

ELT per jam dari PostgreSQL (Neon) ke BigQuery dengan medallion architecture. Airflow 3.3.2 menjalankan orkestrasi di Docker, dan dbt Core membangun layer silver dan gold.

```
Neon Postgres ──Airflow + pipeline/──▶ bronze ──dbt──▶ silver ──dbt──▶ gold
```

| Layer | Isi | Dibuat oleh |
|---|---|---|
| Bronze | Salinan mentah source, nama tabel sama dengan source. Tabel incremental append-only, tabel kecil di-snapshot | `pipeline/extract_load.py` |
| Silver | Sudah dibersihkan: `std_*` untuk standardisasi per entitas, `scd_*` untuk slowly changing dimension, `cdc_*` untuk change data capture | `dbt/models/silver` |
| Gold | Star schema dan agregat untuk analisis: `dim_*`, `fct_*`, `agg_*`, dan `mrt_*` untuk mart table | `dbt/models/gold` |

## Setup

Yang dibutuhkan dari BigQuery:

1. Project GCP dengan billing aktif. Mode sandbox tidak bisa dipakai karena melarang DML, sedangkan dbt memakai `MERGE`.
2. Service account dengan dua role: **BigQuery Data Editor** dan **BigQuery Job User**.
3. Key JSON service account, simpan di `secrets/gcp-service-account.json`.

Lalu isi `.env`:

| Variabel | Isi |
|---|---|
| `BQ_PROJECT_ID` | Project ID GCP |
| `BQ_LOCATION` | Lokasi dataset, harus sama persis dengan dataset, misalnya `asia-southeast2` |
| `BQ_DATASET_BRONZE`, `BQ_DATASET_SILVER`, `BQ_DATASET_GOLD` | Nama dataset per layer. Kalau hanya ada satu dataset, isi ketiganya dengan nama yang sama |

Jalankan:

```bash
docker compose up -d --build
```

Buka http://localhost:8080. Username dan password ada di `AIRFLOW_ADMIN_USERNAME` dan `AIRFLOW_ADMIN_PASSWORD` di `.env`. DAG `media_ingestion_hourly` dan `media_dbt` awalnya dalam keadaan pause, jadi aktifkan keduanya setelah `.env` dan key terisi.

## Alur DAG

`media_ingestion_hourly` berjalan setiap jam dengan satu run aktif pada satu waktu:

1. `extract_load`: satu task per tabel source, maksimal tiga berjalan paralel.
2. `snapshot_article_ids`: menyalin semua id artikel dari source, lalu meng-update Asset `media_bronze_ready`.

`media_dbt` tidak punya jadwal sendiri. DAG ini jalan setiap Asset `media_bronze_ready` ter-update:

1. `dbt_silver`: memuat seeds, membangun model silver dan snapshot, lalu menjalankan test yang hanya membaca layer ini.
2. `dbt_gold`: membangun model gold, lalu menjalankan test gold, termasuk test yang juga membaca silver.

Kalau ingestion gagal, Asset tidak ter-update dan `media_dbt` tidak jalan. Kalau `dbt_silver` gagal, `dbt_gold` tidak jalan, jadi gold tetap memakai data run sebelumnya. Satu layer bisa diulang lewat clear task-nya di Airflow.

## Keputusan desain

**Watermark.** Watermark diambil dari `MAX(_source_change_ts)` di tabel bronze, jadi hanya maju kalau load job BigQuery sudah commit. Setiap run membaca ulang 15 menit terakhir untuk menangkap transaksi yang commit terlambat, dan silver menghilangkan duplikatnya.

**Soft delete.** `updated_at` tidak berubah saat artikel dihapus, jadi change timestamp-nya `GREATEST(updated_at, deleted_at)`.

**Bonus 1: data sejak 2016.** Run pertama mendeteksi tabel bronze yang kosong, lalu memuat data dalam potongan 30 hari dari baris tertua. Kalau gagal di tengah, run berikutnya lanjut dari potongan terakhir yang berhasil. Tidak perlu DAG backfill terpisah, dan `catchup=False` karena satu run sudah mengejar semua yang tertinggal. Di production, baca dari read replica dan jalankan di luar jam sibuk.

**Bonus 2: hard delete.** Baris yang terhapus tidak meninggalkan jejak untuk watermark, jadi setiap jam semua id artikel disalin ke `articles_source_ids`. `dim_articles` melakukan inner join ke daftar itu, dan id yang hilang tercatat di `cdc_article_hard_deletes`.
- Snapshot id berjalan setelah extract, supaya artikel baru tidak terlihat seolah terhapus.
- Kalau jumlah id turun lebih dari 10%, snapshot ditolak dan task gagal, supaya source yang bermasalah tidak mengosongkan gold.
- `fct_page_views` tetap menyimpan riwayat view artikel yang terhapus. `agg_article_daily` mengikuti `dim_articles`.
- Menyalin semua id tidak cocok untuk jutaan baris per jam. Pada skala itu sebaiknya memakai CDC dari WAL, misalnya Debezium.

**Skala.** Kolom `GREATEST(updated_at, deleted_at)` belum punya index. Dengan 50.000 baris ini tidak masalah, tapi di production perlu expression index di source.

## Layout

```
dags/       DAG Airflow per folder: bigquery_ingestion_dags/ dan dbt_dags/
pipeline/   Extract dari Postgres, load ke bronze, Asset yang dipakai bersama
dbt/        Model silver dan gold, seeds, snapshots, tests, analyses
secrets/    Key service account (tidak di-commit)
```
