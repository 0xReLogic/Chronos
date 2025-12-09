use std::fs::{self, File};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};

/// Binary snapshot format (v1)
/// Header:
/// - magic: b"CHRONOSSNAP\0" (12 bytes)
/// - version: u32 (1)
/// - created_at_secs: u64
/// - db_count: u32
///   For each DB (e.g. "main", "agg_state"):
///   - name_len: u16, name bytes (utf-8)
///   - tree_count: u32
///     For each tree:
///     - tree_name_len: u16, tree_name bytes
///     - entry_count: u64
///       For each entry:
///       - key_len: u32, val_len: u32, key bytes, val bytes
/// - file_count: u32
///   For each file (e.g. raft/log.bin):
///   - path_len: u16, path bytes (utf-8, relative)
///   - content_len: u64
///   - content bytes
pub const SNAPSHOT_MAGIC: &[u8] = b"CHRONOSSNAP\0"; // 12 bytes
pub const SNAPSHOT_VERSION: u32 = 1;

fn write_u16<W: Write>(w: &mut W, v: u16) -> Result<()> {
    w.write_all(&v.to_be_bytes()).map_err(Into::into)
}
fn write_u32<W: Write>(w: &mut W, v: u32) -> Result<()> {
    w.write_all(&v.to_be_bytes()).map_err(Into::into)
}
fn write_u64<W: Write>(w: &mut W, v: u64) -> Result<()> {
    w.write_all(&v.to_be_bytes()).map_err(Into::into)
}

fn read_exact<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<()> {
    r.read_exact(buf).map_err(Into::into)
}
fn read_u16<R: Read>(r: &mut R) -> Result<u16> {
    let mut b = [0; 2];
    read_exact(r, &mut b)?;
    Ok(u16::from_be_bytes(b))
}
fn read_u32<R: Read>(r: &mut R) -> Result<u32> {
    let mut b = [0; 4];
    read_exact(r, &mut b)?;
    Ok(u32::from_be_bytes(b))
}
fn read_u64<R: Read>(r: &mut R) -> Result<u64> {
    let mut b = [0; 8];
    read_exact(r, &mut b)?;
    Ok(u64::from_be_bytes(b))
}

fn open_main_db(data_dir: &str) -> Result<sled::Db> {
    let path = Path::new(data_dir);
    if !path.exists() {
        fs::create_dir_all(path)?;
    }
    sled::open(path).map_err(|e| anyhow!("Sled open error: {}", e))
}

fn open_agg_db(data_dir: &str) -> Result<Option<sled::Db>> {
    let path = Path::new(data_dir).join("agg_state");
    if !path.exists() {
        return Ok(None);
    }
    let db = sled::open(path).map_err(|e| anyhow!("Sled open error: {}", e))?;
    Ok(Some(db))
}

pub fn create_snapshot(data_dir: &str, output_path: &str) -> Result<()> {
    // Flush best-effort: open DBs and call flush
    let main_db = open_main_db(data_dir)?;
    main_db.flush().map_err(|e| anyhow!("flush error: {}", e))?;
    if let Ok(Some(agg_db)) = open_agg_db(data_dir) {
        let _ = agg_db.flush();
    }

    let file = File::create(output_path)?;
    let mut w = BufWriter::new(file);

    // Header
    w.write_all(SNAPSHOT_MAGIC)?;
    write_u32(&mut w, SNAPSHOT_VERSION)?;
    let created_at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    write_u64(&mut w, created_at)?;

    // Databases: main + optional agg_state
    let mut dbs: Vec<(&str, sled::Db)> = Vec::new();
    dbs.push(("main", main_db));
    if let Ok(Some(agg_db)) = open_agg_db(data_dir) {
        dbs.push(("agg_state", agg_db));
    }
    write_u32(&mut w, dbs.len() as u32)?;

    for (db_name, db) in dbs.iter() {
        let name_bytes = db_name.as_bytes();
        write_u16(&mut w, name_bytes.len() as u16)?;
        w.write_all(name_bytes)?;

        let tree_names = db.tree_names();
        // Filter out empty names (defensive)
        let tree_names: Vec<_> = tree_names.into_iter().filter(|n| !n.is_empty()).collect();
        write_u32(&mut w, tree_names.len() as u32)?;

        for tn in tree_names {
            write_u16(&mut w, tn.len() as u16)?;
            w.write_all(tn.as_ref())?;
            let tree = db
                .open_tree(&tn)
                .map_err(|e| anyhow!("open_tree error: {}", e))?;

            // Count entries (iterating once); alternatively stream with placeholder but keep simple
            let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
            for item in tree.iter() {
                let (k, v) = item.map_err(|e| anyhow!("tree iter error: {}", e))?;
                entries.push((k.to_vec(), v.to_vec()));
            }
            write_u64(&mut w, entries.len() as u64)?;
            for (k, v) in entries {
                write_u32(&mut w, k.len() as u32)?;
                write_u32(&mut w, v.len() as u32)?;
                w.write_all(&k)?;
                w.write_all(&v)?;
            }
        }
    }

    // Files (raft log)
    let raft_path = Path::new(data_dir).join("raft").join("log.bin");
    let mut file_count: u32 = 0;
    if raft_path.exists() {
        file_count += 1;
    }
    write_u32(&mut w, file_count)?;
    if raft_path.exists() {
        let rel = PathBuf::from("raft/log.bin");
        let rel_str = rel.to_string_lossy();
        let bytes = std::fs::read(&raft_path)?;
        write_u16(&mut w, rel_str.len() as u16)?;
        w.write_all(rel_str.as_bytes())?;
        write_u64(&mut w, bytes.len() as u64)?;
        w.write_all(&bytes)?;
    }

    w.flush()?;
    Ok(())
}

pub fn restore_snapshot(data_dir: &str, input_path: &str, force: bool) -> Result<()> {
    let data_path = Path::new(data_dir);
    if data_path.exists() {
        // Check emptiness
        let mut is_empty = true;
        if let Ok(mut rd) = fs::read_dir(data_path) {
            if rd.next().is_some() {
                is_empty = false;
            }
        }
        if !is_empty {
            if !force {
                return Err(anyhow!("data_dir is not empty (use --force to overwrite)"));
            }
            fs::remove_dir_all(data_path)?;
        }
    }
    fs::create_dir_all(data_path)?;

    let file = File::open(input_path)?;
    let mut r = BufReader::new(file);

    // Header
    let mut magic = [0u8; 12];
    read_exact(&mut r, &mut magic)?;
    if magic != SNAPSHOT_MAGIC {
        return Err(anyhow!("invalid snapshot magic"));
    }
    let version = read_u32(&mut r)?;
    if version != SNAPSHOT_VERSION {
        return Err(anyhow!("unsupported snapshot version: {}", version));
    }
    let _created_at = read_u64(&mut r)?;

    // Databases
    let db_count = read_u32(&mut r)?;
    for _ in 0..db_count {
        let name_len = read_u16(&mut r)? as usize;
        let mut name_buf = vec![0u8; name_len];
        read_exact(&mut r, &mut name_buf)?;
        let db_name = String::from_utf8_lossy(&name_buf).to_string();
        let db = if db_name == "main" {
            open_main_db(data_dir)?
        } else if db_name == "agg_state" {
            // ensure directory exists
            fs::create_dir_all(Path::new(data_dir).join("agg_state"))?;
            sled::open(Path::new(data_dir).join("agg_state"))
                .map_err(|e| anyhow!("Sled open error: {}", e))?
        } else {
            // Unknown DB label; create a subdir with same name
            fs::create_dir_all(Path::new(data_dir).join(&db_name))?;
            sled::open(Path::new(data_dir).join(&db_name))
                .map_err(|e| anyhow!("Sled open error: {}", e))?
        };

        let tree_count = read_u32(&mut r)? as usize;
        for _ in 0..tree_count {
            let tn_len = read_u16(&mut r)? as usize;
            let mut tn_buf = vec![0u8; tn_len];
            read_exact(&mut r, &mut tn_buf)?;
            let tree = db
                .open_tree(&tn_buf)
                .map_err(|e| anyhow!("open_tree error: {}", e))?;

            let entry_count = read_u64(&mut r)? as usize;
            for _ in 0..entry_count {
                let k_len = read_u32(&mut r)? as usize;
                let v_len = read_u32(&mut r)? as usize;
                let mut k = vec![0u8; k_len];
                let mut v = vec![0u8; v_len];
                read_exact(&mut r, &mut k)?;
                read_exact(&mut r, &mut v)?;
                tree.insert(k, v)
                    .map_err(|e| anyhow!("tree insert error: {}", e))?;
            }
            tree.flush()
                .map_err(|e| anyhow!("tree flush error: {}", e))?;
        }
        db.flush().map_err(|e| anyhow!("db flush error: {}", e))?;
    }

    // Files
    let file_count = read_u32(&mut r)? as usize;
    for _ in 0..file_count {
        let p_len = read_u16(&mut r)? as usize;
        let mut p_buf = vec![0u8; p_len];
        read_exact(&mut r, &mut p_buf)?;
        let rel_path = String::from_utf8_lossy(&p_buf).to_string();
        let len = read_u64(&mut r)? as usize;
        let mut content = vec![0u8; len];
        read_exact(&mut r, &mut content)?;

        let target = Path::new(data_dir).join(rel_path);
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut f = File::create(&target)?;
        f.write_all(&content)?;
    }

    // Write storage version marker for on-disk compatibility checks
    let ver_path = Path::new(data_dir).join("STORAGE_VERSION");
    let _ = fs::write(ver_path, b"1\n");

    Ok(())
}
