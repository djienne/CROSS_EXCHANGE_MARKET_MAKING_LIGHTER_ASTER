//! FFI bindings to the official Lighter native signer shared library
//! (`lighter-signer-<os>-<arch>.so/.dylib/.dll`), the exact same binary the Python
//! SDK loads via `ctypes`. We do NOT reimplement Lighter's signing scheme — we call
//! into the vetted Go/cgo library so signatures are byte-identical to the live bot.
//!
//! ABI validated against lighter-python `signer_client.py`:
//!   * structs `CreateOrderTxReq`, `SignedTxResponse`, `StrOrErr`, `ApiKeyResponse`
//!   * `CreateClient` returns an *error-string pointer* (NULL on success); client state
//!     is held inside the library keyed by (api_key_index, account_index).
//!   * every returned `char*` is malloc'd by the library and must be freed with libc
//!     `free` after copying (mirrors Python `decode_and_free`).
//!   * mainnet `chain_id = 304` (url contains "mainnet" or "api"), testnet = 300.

use anyhow::{bail, Context, Result};
use libloading::{Library, Symbol};
use std::ffi::{c_char, c_int, c_longlong, c_void, CStr, CString};
use std::path::Path;

// ---- Order / TIF constants (from SignerClient) ----
pub const ORDER_TYPE_LIMIT: i32 = 0;
pub const ORDER_TYPE_MARKET: i32 = 1;
pub const TIF_IMMEDIATE_OR_CANCEL: i32 = 0;
pub const TIF_GOOD_TILL_TIME: i32 = 1;
pub const TIF_POST_ONLY: i32 = 2;
pub const CANCEL_ALL_TIF_IMMEDIATE: i32 = 0;
pub const CANCEL_ALL_TIF_SCHEDULED: i32 = 1;
pub const CANCEL_ALL_TIF_ABORT: i32 = 2;
pub const NIL_TRIGGER_PRICE: i32 = 0;
pub const DEFAULT_28_DAY_ORDER_EXPIRY: i64 = -1;
pub const DEFAULT_IOC_EXPIRY: i64 = 0;
pub const MARGIN_MODE_CROSS: i32 = 0;
pub const MARGIN_MODE_ISOLATED: i32 = 1;

// ---- repr(C) structs mirroring ctypes.Structure layouts ----
#[repr(C)]
struct SignedTxResponse {
    tx_type: u8,
    tx_info: *mut c_char,
    tx_hash: *mut c_char,
    message_to_sign: *mut c_char,
    err: *mut c_char,
}

#[repr(C)]
struct StrOrErr {
    s: *mut c_char,
    err: *mut c_char,
}

// ABI guards (codex review): on 64-bit, SignedTxResponse = u8 + 4*ptr = 40 bytes
// (u8 padded to 8 for pointer alignment), StrOrErr = 2*ptr = 16 bytes. If these ever
// fail the layout no longer matches the ctypes.Structure the Python SDK relies on.
const _: () = assert!(std::mem::size_of::<SignedTxResponse>() == 40);
const _: () = assert!(std::mem::align_of::<SignedTxResponse>() == 8);
const _: () = assert!(std::mem::size_of::<StrOrErr>() == 16);

// ---- extern fn signatures (argtypes verified in signer_client.py) ----
type CreateClientFn =
    unsafe extern "C" fn(*const c_char, *const c_char, c_int, c_int, c_longlong) -> *mut c_char;
type CheckClientFn = unsafe extern "C" fn(c_int, c_longlong) -> *mut c_char;
type SignCreateOrderFn = unsafe extern "C" fn(
    c_int,      // market_index
    c_longlong, // client_order_index
    c_longlong, // base_amount
    c_int,      // price
    c_int,      // is_ask
    c_int,      // order_type
    c_int,      // time_in_force
    c_int,      // reduce_only
    c_int,      // trigger_price
    c_longlong, // order_expiry
    c_longlong, // nonce
    c_int,      // api_key_index
    c_longlong, // account_index
) -> SignedTxResponse;
type SignCancelOrderFn =
    unsafe extern "C" fn(c_int, c_longlong, c_longlong, c_int, c_longlong) -> SignedTxResponse;
type SignCancelAllOrdersFn =
    unsafe extern "C" fn(c_int, c_longlong, c_longlong, c_int, c_longlong) -> SignedTxResponse;
type SignModifyOrderFn = unsafe extern "C" fn(
    c_int,      // market_index
    c_longlong, // order_index
    c_longlong, // base_amount
    c_longlong, // price
    c_longlong, // trigger_price
    c_longlong, // nonce
    c_int,      // api_key_index
    c_longlong, // account_index
) -> SignedTxResponse;
type SignUpdateLeverageFn = unsafe extern "C" fn(
    c_int,      // market_index
    c_int,      // fraction
    c_int,      // margin_mode
    c_longlong, // nonce
    c_int,      // api_key_index
    c_longlong, // account_index
) -> SignedTxResponse;
type CreateAuthTokenFn = unsafe extern "C" fn(c_longlong, c_int, c_longlong) -> StrOrErr;

/// A signed transaction ready to send: (tx_type, tx_info json, tx_hash).
#[derive(Debug, Clone)]
pub struct SignedTx {
    pub tx_type: u8,
    pub tx_info: String,
    pub tx_hash: String,
}

/// Safe wrapper around the native signer. Holds raw fn pointers into a leaked
/// (process-lifetime) `Library`, so it is `Send + Sync` (fn pointers are both).
pub struct Signer {
    account_index: i64,
    create_client: CreateClientFn,
    check_client: CheckClientFn,
    sign_create_order: SignCreateOrderFn,
    sign_cancel_order: SignCancelOrderFn,
    sign_cancel_all_orders: SignCancelAllOrdersFn,
    sign_modify_order: SignModifyOrderFn,
    sign_update_leverage: SignUpdateLeverageFn,
    create_auth_token: CreateAuthTokenFn,
}

/// Free a library-allocated C string after copying it into an owned `String`.
/// Mirrors Python `decode_and_free`.
unsafe fn take_cstring(ptr: *mut c_char) -> Option<String> {
    if ptr.is_null() {
        return None;
    }
    let s = CStr::from_ptr(ptr).to_string_lossy().into_owned();
    libc::free(ptr as *mut c_void);
    Some(s)
}

#[inline]
fn nonempty(o: Option<String>) -> Option<String> {
    o.filter(|s| !s.is_empty())
}

impl Signer {
    /// Load the signer library for the current platform and register the API key.
    /// `signers_dir` is the directory containing the `lighter-signer-*` binaries.
    /// `api_private_key` is the API key private key hex (with or without 0x).
    pub fn load(
        signers_dir: &Path,
        url: &str,
        api_private_key: &str,
        api_key_index: i32,
        account_index: i64,
    ) -> Result<Self> {
        let file = signer_filename();
        let path = signers_dir.join(file);
        // SAFETY: loading a trusted, first-party shared library shipped with the project.
        let lib: &'static Library = Box::leak(Box::new(unsafe {
            Library::new(&path).with_context(|| format!("loading signer lib {}", path.display()))?
        }));

        macro_rules! sym {
            ($name:literal, $ty:ty) => {{
                let s: Symbol<$ty> = unsafe {
                    lib.get($name)
                        .with_context(|| format!("missing symbol {}", String::from_utf8_lossy($name)))?
                };
                *s // copy out the fn pointer (points into the 'static library)
            }};
        }

        let signer = Signer {
            account_index,
            create_client: sym!(b"CreateClient\0", CreateClientFn),
            check_client: sym!(b"CheckClient\0", CheckClientFn),
            sign_create_order: sym!(b"SignCreateOrder\0", SignCreateOrderFn),
            sign_cancel_order: sym!(b"SignCancelOrder\0", SignCancelOrderFn),
            sign_cancel_all_orders: sym!(b"SignCancelAllOrders\0", SignCancelAllOrdersFn),
            sign_modify_order: sym!(b"SignModifyOrder\0", SignModifyOrderFn),
            sign_update_leverage: sym!(b"SignUpdateLeverage\0", SignUpdateLeverageFn),
            create_auth_token: sym!(b"CreateAuthToken\0", CreateAuthTokenFn),
        };

        let chain_id = chain_id_for_url(url);
        let key = api_private_key.strip_prefix("0x").unwrap_or(api_private_key);
        let c_url = CString::new(url)?;
        let c_key = CString::new(key)?;
        // SAFETY: valid C strings, fn pointer from the loaded library.
        let err = unsafe {
            take_cstring((signer.create_client)(
                c_url.as_ptr(),
                c_key.as_ptr(),
                chain_id,
                api_key_index,
                account_index,
            ))
        };
        if let Some(e) = nonempty(err) {
            bail!("CreateClient failed: {e}");
        }
        Ok(signer)
    }

    /// Verify the API key matches the one registered on Lighter (network call inside lib).
    pub fn check_client(&self, api_key_index: i32) -> Result<()> {
        let err = unsafe { take_cstring((self.check_client)(api_key_index, self.account_index)) };
        match nonempty(err) {
            Some(e) => bail!("CheckClient failed: {e}"),
            None => Ok(()),
        }
    }

    fn decode(&self, r: SignedTxResponse) -> Result<SignedTx> {
        // Free all four library-allocated pointers (matches __decode_tx_info order).
        let err = unsafe { take_cstring(r.err) };
        let tx_info = unsafe { take_cstring(r.tx_info) };
        let tx_hash = unsafe { take_cstring(r.tx_hash) };
        let _ = unsafe { take_cstring(r.message_to_sign) };
        if let Some(e) = nonempty(err) {
            bail!("sign error: {e}");
        }
        Ok(SignedTx {
            tx_type: r.tx_type,
            tx_info: tx_info.unwrap_or_default(),
            tx_hash: tx_hash.unwrap_or_default(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sign_create_order(
        &self,
        market_index: i32,
        client_order_index: i64,
        base_amount: i64,
        price: i32,
        is_ask: bool,
        order_type: i32,
        time_in_force: i32,
        reduce_only: bool,
        trigger_price: i32,
        order_expiry: i64,
        nonce: i64,
        api_key_index: i32,
    ) -> Result<SignedTx> {
        let r = unsafe {
            (self.sign_create_order)(
                market_index,
                client_order_index,
                base_amount,
                price,
                is_ask as c_int,
                order_type,
                time_in_force,
                reduce_only as c_int,
                trigger_price,
                order_expiry,
                nonce,
                api_key_index,
                self.account_index,
            )
        };
        self.decode(r)
    }

    pub fn sign_cancel_order(
        &self,
        market_index: i32,
        order_index: i64,
        nonce: i64,
        api_key_index: i32,
    ) -> Result<SignedTx> {
        let r = unsafe {
            (self.sign_cancel_order)(market_index, order_index, nonce, api_key_index, self.account_index)
        };
        self.decode(r)
    }

    pub fn sign_cancel_all_orders(
        &self,
        time_in_force: i32,
        timestamp_ms: i64,
        nonce: i64,
        api_key_index: i32,
    ) -> Result<SignedTx> {
        let r = unsafe {
            (self.sign_cancel_all_orders)(
                time_in_force,
                timestamp_ms,
                nonce,
                api_key_index,
                self.account_index,
            )
        };
        self.decode(r)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn sign_modify_order(
        &self,
        market_index: i32,
        order_index: i64,
        base_amount: i64,
        price: i64,
        trigger_price: i64,
        nonce: i64,
        api_key_index: i32,
    ) -> Result<SignedTx> {
        let r = unsafe {
            (self.sign_modify_order)(
                market_index,
                order_index,
                base_amount,
                price,
                trigger_price,
                nonce,
                api_key_index,
                self.account_index,
            )
        };
        self.decode(r)
    }

    pub fn sign_update_leverage(
        &self,
        market_index: i32,
        fraction: i32,
        margin_mode: i32,
        nonce: i64,
        api_key_index: i32,
    ) -> Result<SignedTx> {
        let r = unsafe {
            (self.sign_update_leverage)(
                market_index,
                fraction,
                margin_mode,
                nonce,
                api_key_index,
                self.account_index,
            )
        };
        self.decode(r)
    }

    /// Create a short-lived WS auth token. `deadline_unix` is an absolute unix-seconds
    /// expiry (Python passes `timestamp + ttl`).
    pub fn create_auth_token(&self, deadline_unix: i64, api_key_index: i32) -> Result<String> {
        let r = unsafe { (self.create_auth_token)(deadline_unix, api_key_index, self.account_index) };
        let s = unsafe { take_cstring(r.s) };
        let err = unsafe { take_cstring(r.err) };
        if let Some(e) = nonempty(err) {
            bail!("CreateAuthToken failed: {e}");
        }
        s.context("CreateAuthToken returned no token")
    }
}

// fn pointers + i64 are Send/Sync; the underlying lib is leaked ('static).
unsafe impl Send for Signer {}
unsafe impl Sync for Signer {}

pub fn chain_id_for_url(url: &str) -> i32 {
    if url.contains("mainnet") || url.contains("api") {
        304
    } else {
        300
    }
}

fn signer_filename() -> &'static str {
    match (std::env::consts::OS, std::env::consts::ARCH) {
        ("linux", "x86_64") => "lighter-signer-linux-amd64.so",
        ("linux", "aarch64") => "lighter-signer-linux-arm64.so",
        ("macos", "aarch64") => "lighter-signer-darwin-arm64.dylib",
        ("windows", "x86_64") => "lighter-signer-windows-amd64.dll",
        (os, arch) => panic!("unsupported platform for lighter signer: {os}/{arch}"),
    }
}
