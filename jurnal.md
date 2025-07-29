# Jurnal Pengembangan Chronos

Dokumen ini mencatat masalah-masalah penting yang dihadapi selama pengembangan, akar penyebabnya, dan pembelajaran yang didapat untuk referensi di masa depan.

---

### 1. Masalah: SQL Gagal Mem-parsing Tipe `STRING`

- **Symptom:** Perintah `CREATE TABLE users (id INT, name STRING);` gagal dengan error parsing.
- **Akar Penyebab:**
    1. Grammar Pest (`src/parser/sql.pest`) tidak mengenali `STRING` sebagai `data_type` yang valid.
    2. Enum `DataType` di AST (`src/parser/ast.rs`) tidak memiliki varian `String`.
    3. Logika penyimpanan (`src/storage/csv_storage.rs`) tidak menangani `DataType::String` dalam *match statements*, menyebabkan error kompilasi `non-exhaustive patterns`.
- **Solusi:** Menambahkan `STRING` ke semua tiga lapisan secara konsisten: parser, AST, dan penyimpanan.
- **Pembelajaran:** Saat menambahkan fitur baru ke bahasa SQL (seperti tipe data), pastikan untuk memperbarui semua lapisan yang relevan (Parsing -> AST -> Eksekusi -> Penyimpanan) untuk memastikan konsistensi.

---

### 2. Masalah: Keadaan Tidak Konsisten (`Table already exists` & `Schema not found`)

- **Symptom:** Error yang kontradiktif. `CREATE TABLE` gagal karena tabel dianggap sudah ada, tetapi `INSERT`/`SELECT` gagal karena skema tidak ditemukan.
- **Akar Penyebab:** Proses **klien** secara keliru memiliki logika untuk berinteraksi dengan sistem file. Ia membuat file `data/users.csv` di direktori yang salah, yang berbeda dari direktori kerja **node** (`data/node1/`). Hal ini menyebabkan node mendeteksi file CSV tanpa skema yang sesuai.
- **Solusi:** Merombak arsitektur secara fundamental. Menghapus semua logika `Executor` dan manajemen file dari klien. Klien sekarang menjadi antarmuka jaringan murni, dan hanya node yang bertanggung jawab atas status dan penyimpanan.
- **Pembelajaran:** Patuhi pemisahan tanggung jawab (separation of concerns) yang ketat dalam arsitektur client-server. Klien tidak boleh mengelola atau memodifikasi keadaan di sisi server.

---

### 3. Masalah: Aplikasi Klien Panic (`Cannot start a runtime from within a runtime`)

- **Symptom:** Klien mengalami *panic* saat mencoba menjalankan perintah SQL apa pun setelah refactoring.
- **Akar Penyebab:** Mencoba membuat runtime Tokio baru (`tokio::runtime::Runtime::new().block_on(...)`) di dalam fungsi (`Repl::run`) yang sudah berjalan di dalam konteks runtime Tokio utama (`#[tokio::main]` di `main.rs`).
- **Solusi:** Merangkul sifat asinkron sepenuhnya. Mengubah `Repl::run` menjadi `async fn` dan menggunakan `.await` secara langsung pada panggilan jaringan, alih-alih membuat runtime baru.
- **Pembelajaran:** Dalam aplikasi yang sudah asinkron, jangan membuat runtime baru. Gunakan `.await` untuk menunggu *futures* selesai. Propagasi `async` ke atas tumpukan panggilan jika perlu.
