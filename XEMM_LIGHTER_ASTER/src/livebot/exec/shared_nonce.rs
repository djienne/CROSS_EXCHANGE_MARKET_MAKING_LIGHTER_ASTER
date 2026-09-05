//! Local signer-domain nonce allocation. Initialization is cold; each reservation is
//! one shared-memory CAS, with no filesystem I/O, process lock, or REST round trip.

use std::fs::OpenOptions;
use std::path::Path;
use std::sync::atomic::{AtomicI64, Ordering};

use anyhow::{bail, Context, Result};
use memmap2::{MmapMut, MmapOptions};
use tiny_keccak::{Hasher, Keccak};

const FILE_LEN: usize = 128;
const COUNTER_OFFSET: usize = 64;
const MAGIC: &[u8; 8] = b"ASTERN01";
const MAX_CLOCK_LEAD_US: i64 = 1_000_000;

pub struct AsterNonce {
    mapping: MmapMut,
}

impl AsterNonce {
    pub fn for_signer(base_url: &str, signer: &str) -> Result<Self> {
        let directory = std::env::var_os("ASTER_NONCE_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join("lighter-aster-nonces"));
        Self::open_at(&directory, base_url, signer)
    }

    fn open_at(directory: &Path, base_url: &str, signer: &str) -> Result<Self> {
        let origin = reqwest::Url::parse(base_url)?.origin().ascii_serialization();
        let identity = format!("{}\n{}", origin.to_ascii_lowercase(), signer.trim().to_ascii_lowercase());
        let mut hash = Keccak::v256();
        let mut digest = [0u8; 32];
        hash.update(identity.as_bytes());
        hash.finalize(&mut digest);
        std::fs::create_dir_all(directory).context("create Aster nonce directory")?;
        let path = directory.join(format!("{}.nonce", hex::encode(digest)));
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(false)
            .open(&path).context("open Aster nonce counter")?;
        fs2::FileExt::lock_exclusive(&file).context("initialize Aster nonce counter")?;
        let length = file.metadata()?.len();
        if length != 0 && length != FILE_LEN as u64 {
            bail!("Aster nonce file has incompatible length");
        }
        if length == 0 {
            file.set_len(FILE_LEN as u64)?;
        }
        // File length never changes once initialized. Both crates use this exact layout.
        // The mmap base is page-aligned; offset 64 satisfies AtomicI64 alignment on our
        // supported 64-bit targets. All accesses to the counter after initialization are atomic.
        let mut mapping = unsafe { MmapOptions::new().len(FILE_LEN).map_mut(&file)? };
        if length == 0 {
            unsafe {
                std::ptr::write(mapping.as_mut_ptr().add(COUNTER_OFFSET).cast::<AtomicI64>(), AtomicI64::new(0));
            }
            mapping[..MAGIC.len()].copy_from_slice(MAGIC);
            mapping.flush().context("initialize durable Aster nonce header")?;
        } else if &mapping[..MAGIC.len()] != MAGIC {
            bail!("Aster nonce file has incompatible header");
        }
        fs2::FileExt::unlock(&file)?;
        Ok(Self { mapping })
    }

    pub fn next(&self) -> Result<i64> {
        self.next_at(chrono::Utc::now().timestamp_micros())
    }

    fn next_at(&self, now_us: i64) -> Result<i64> {
        let counter = unsafe { &*self.mapping.as_ptr().add(COUNTER_OFFSET).cast::<AtomicI64>() };
        let mut previous = counter.load(Ordering::Acquire);
        loop {
            let candidate = now_us.max(previous.checked_add(1).context("Aster nonce overflow")?);
            // A backwards clock step must not make us sign arbitrarily future requests.
            if candidate.saturating_sub(now_us) > MAX_CLOCK_LEAD_US {
                bail!("Aster nonce clock is more than one second behind the shared counter");
            }
            match counter.compare_exchange_weak(previous, candidate, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return Ok(candidate),
                Err(observed) => previous = observed,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    const NOW: i64 = 1_700_000_000_000_000;

    #[test]
    #[ignore = "subprocess helper; invoked with isolated directory by the cross-process test"]
    fn nonce_process_child() {
        let directory = std::env::var_os("ASTER_NONCE_TEST_DIR").expect("test child directory");
        let nonce = AsterNonce::open_at(Path::new(&directory), "https://example.test", "public-test-signer").unwrap();
        for _ in 0..64 {
            println!("NONCE_TEST={}", nonce.next_at(NOW).unwrap());
        }
    }

    #[test]
    fn concurrent_processes_with_the_same_clock_never_repeat_a_nonce() {
        let directory = std::env::temp_dir().join(format!("aster-nonce-test-{}-{}",
            std::process::id(), chrono::Utc::now().timestamp_nanos_opt().unwrap()));
        let test_name = format!("{}::nonce_process_child", module_path!().split_once("::").unwrap().1);
        let spawn = || std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", &test_name, "--ignored", "--nocapture"])
            .env("ASTER_NONCE_TEST_DIR", &directory)
            .stdout(std::process::Stdio::piped()).spawn().unwrap();
        let first = spawn();
        let second = spawn();
        let mut nonces = Vec::new();
        for child in [first, second] {
            let output = child.wait_with_output().unwrap();
            assert!(output.status.success());
            for line in String::from_utf8(output.stdout).unwrap().lines() {
                if let Some((_, value)) = line.split_once("NONCE_TEST=") {
                    nonces.push(value.trim().parse::<i64>().unwrap());
                }
            }
        }
        nonces.sort_unstable();
        assert_eq!(nonces, (NOW..NOW + 128).collect::<Vec<_>>());
        let reopened = AsterNonce::open_at(&directory, "https://EXAMPLE.test/", "PUBLIC-TEST-SIGNER").unwrap();
        assert_eq!(reopened.next_at(NOW).unwrap(), NOW + 128);
        assert!(reopened.next_at(NOW - 2_000_000).is_err());
        drop(reopened);
        for entry in std::fs::read_dir(&directory).unwrap() {
            std::fs::remove_file(entry.unwrap().path()).unwrap();
        }
        std::fs::remove_dir(&directory).unwrap();
    }
}
