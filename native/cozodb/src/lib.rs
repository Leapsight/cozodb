// =============================================================================
// cozodb.erl -
//
// Copyright (c) 2023-2025 Leapsight. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// =============================================================================

// =============================================================================
// GLOBAL ALLOCATOR CONFIGURATION
// =============================================================================
//
// We support two allocator modes:
//
// 1. nif_alloc (default): Use Erlang's allocator via rustler::EnifAllocator
//    - Rust allocations are tracked by the BEAM
//    - Memory is returned when Erlang GCs
//    - Better integration with Erlang's memory management
//
// 2. jemalloc (default): Use jemalloc for all Rust allocations
//    - Best performance under high allocation rates
//    - Unified with RocksDB's allocator (via rocksdb-jemalloc feature)
//    - Configured with 0ms decay for immediate memory return to OS
//    - Override via COZODB_JEMALLOC_DIRTY_DECAY_MS / COZODB_JEMALLOC_MUZZY_DECAY_MS
//
// When nif_alloc is enabled, it takes precedence over jemalloc for Rust.
// RocksDB (C++) can still use jemalloc independently via rocksdb-jemalloc.
// =============================================================================

// Option 1: Use Erlang's allocator for Rust (nif_alloc feature, default)
// This forwards all Rust allocations to Erlang's enif_alloc/enif_free
#[cfg(feature = "nif_alloc")]
#[global_allocator]
static GLOBAL: rustler::EnifAllocator = rustler::EnifAllocator;

// Option 2: Use jemalloc for Rust (only when jemalloc is enabled AND nif_alloc is disabled)
#[cfg(all(feature = "jemalloc", not(feature = "nif_alloc"), not(target_env = "msvc")))]
use tikv_jemallocator::Jemalloc;

#[cfg(all(feature = "jemalloc", not(feature = "nif_alloc"), not(target_env = "msvc")))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

// =============================================================================
// JEMALLOC COMPILE-TIME CONFIGURATION
// =============================================================================
// This static variable is read by jemalloc BEFORE main() is entered.
// It provides safe defaults for containerized environments (Docker, ECS, K8s).
//
// Key settings:
//   - background_thread:false - Prevents crashes after fork() in containers
//   - dirty_decay_ms:1000     - Balanced memory return to OS (1 second)
//   - muzzy_decay_ms:1000     - Balanced memory return to OS (1 second)
//
// These can be overridden at runtime via MALLOC_CONF environment variable.
// =============================================================================
#[cfg(all(feature = "jemalloc", not(feature = "nif_alloc"), not(target_env = "msvc")))]
#[allow(non_upper_case_globals)]
#[export_name = "malloc_conf"]
pub static malloc_conf: &[u8] = b"background_thread:false,dirty_decay_ms:1000,muzzy_decay_ms:1000\0";

// Rust std libs

use core::hash::Hash;
use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::hash::Hasher;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

// Rustler
use rustler::types::LocalPid;
use rustler::Encoder;
use rustler::Env;
use rustler::ListIterator;
use rustler::MapIterator;
use rustler::NifResult;
use rustler::OwnedEnv;
use rustler::ResourceArc;
use rustler::Term;
// Raw NIF functions for efficient list building (avoids intermediate Vec allocations)
use rustler::sys::{enif_make_list_from_array, ERL_NIF_TERM};

// Used for global state
use lazy_static::lazy_static;

// Used for CALLBACKS feature
use crossbeam::channel::*;
use once_cell::sync::Lazy;
use threadpool::ThreadPool;

// Cozo
use cozo::*;
use ndarray::Array1; // used by Array32Wrapper
use serde_json::json;

// =============================================================================
// RUSTLER SETUP
// =============================================================================

// We define atoms in Rustler
mod atoms {
    rustler::atoms! {
        ok,
        undefined,
        true_ = "true",
        false_ = "false",
        error,
        null,
        json,
        count,
        cozo_named_rows,
        engine,
        path,
        rows,
        headers,
        next,
        took,
        updated,
        removed,
        cozodb,
        relation,
        message,
        code,
        severity,
        help,
        url,
        labels,
        // Error Reasons
        badarg,
        invalid_engine
    }
}

// Define erlang module and functions
// Note: rustler 0.36+ auto-discovers NIF functions via #[rustler::nif] proc macro
rustler::init!("cozodb", load = on_load);

/// Define NIF Resources using rustler::resource! macro
fn on_load(env: Env, _: Term) -> bool {
    rustler::resource!(DbHandleResource, env);

    // Configure jemalloc: background threads, decay times, and optional arena
    // limits
    // Only when jemalloc is the global allocator (not when using nif_alloc)
    #[cfg(all(feature = "jemalloc", not(feature = "nif_alloc"), not(target_env = "msvc")))]
    {
        // Force jemalloc's global initialization (arena creation, pthread key
        // setup)
        // BEFORE configure_jemalloc() and before any dirty scheduler thread
        // enters.
        // This ensures the global state is fully set up when dirty schedulers
        // later call warmup_jemalloc_tls() for per-thread TSD initialization.
        unsafe {
            let ptr = tikv_jemalloc_sys::malloc(1);
            if !ptr.is_null() {
                tikv_jemalloc_sys::free(ptr);
            }
        }

        configure_jemalloc();
    }

    true
}

/// Configure jemalloc for optimal memory behavior.
///
/// This configures:
/// 1. Background thread for async purging (reduces latency impact)
/// 2. Decay times for returning memory to the OS
/// 3. Optional arena count limiting
///
/// Environment variables:
/// - COZODB_JEMALLOC_BACKGROUND_THREAD: "true" (default) or "false"
/// - COZODB_JEMALLOC_NARENAS: number of arenas (optional, reduces RSS with many threads)
/// - COZODB_JEMALLOC_DIRTY_DECAY_MS: dirty page decay time (default: 1000ms)
/// - COZODB_JEMALLOC_MUZZY_DECAY_MS: muzzy page decay time (default: 1000ms)
///
/// Decay values:
/// - 0 = immediate return (aggressive, may impact latency)
/// - 1000-5000 = balanced (good RSS with minimal latency impact)
/// - -1 = disable decay (jemalloc default 10s, holds memory longer)
#[cfg(all(feature = "jemalloc", not(feature = "nif_alloc"), not(target_env = "msvc")))]
fn configure_jemalloc() {
    use std::env;
    use std::ffi::CString;

    // Enable background thread for async purging (reduces latency impact of
    // decay)
    // COZODB_JEMALLOC_BACKGROUND_THREAD: "true" (default) or "false"
    // Default to false to match compile-time malloc_conf setting.
    // background_thread:true causes segfaults in container environments (
    // Docker, ECS, K8s)
    // due to signal handling conflicts between jemalloc's background thread and
    // the BEAM VM.
    // Users can opt-in via COZODB_JEMALLOC_BACKGROUND_THREAD=true if not
    // running in containers.
    let enable_bg_thread = env::var("COZODB_JEMALLOC_BACKGROUND_THREAD")
        .map(|v| v == "true" || v == "1")
        .unwrap_or(false);

    if enable_bg_thread {
        // background_thread requires jemalloc compiled with
        // --enable-background-thread
        // It's optional - if not available, decay still works but synchronously
        unsafe {
            let key = CString::new("background_thread").unwrap();
            let value: bool = true;
            let result = tikv_jemalloc_sys::mallctl(
                key.as_ptr(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &value as *const bool as *mut std::ffi::c_void,
                std::mem::size_of::<bool>(),
            );
            // Error 2 (ENOENT) means not compiled in - this is fine, just skip
            if result != 0 && result != 2 {
                eprintln!("Warning: Failed to enable jemalloc background_thread: error {}", result);
            }
        }
    }

    // Optionally limit the number of arenas to reduce memory overhead
    // COZODB_JEMALLOC_NARENAS: number of arenas (default: jemalloc auto,
    // typically 4*ncpus)
    // Lower values (4-8) can reduce RSS spikes with many threads
    if let Ok(narenas_str) = env::var("COZODB_JEMALLOC_NARENAS") {
        if let Ok(narenas) = narenas_str.parse::<u32>() {
            unsafe {
                let key = CString::new("narenas").unwrap();
                let result = tikv_jemalloc_sys::mallctl(
                    key.as_ptr(),
                    std::ptr::null_mut(),
                    std::ptr::null_mut(),
                    &narenas as *const u32 as *mut std::ffi::c_void,
                    std::mem::size_of::<u32>(),
                );
                if result != 0 {
                    eprintln!("Warning: Failed to set jemalloc narenas to {}: error {}", narenas, result);
                }
            }
        }
    }

    // Balanced defaults: 1000ms decay with background thread gives good RSS
    // while minimizing latency impact. Use 0 for aggressive memory return,
    // or higher values (5000-10000) if you see performance issues.
    //
    // COZODB_JEMALLOC_DIRTY_DECAY_MS: dirty page decay time (default: 1000ms)
    // COZODB_JEMALLOC_MUZZY_DECAY_MS: muzzy page decay time (default: 1000ms)
    let dirty_decay_ms: i64 = env::var("COZODB_JEMALLOC_DIRTY_DECAY_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);

    let muzzy_decay_ms: i64 = env::var("COZODB_JEMALLOC_MUZZY_DECAY_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1000);

    // Apply decay settings to all arenas
    if let Err(e) = set_jemalloc_decay(dirty_decay_ms, muzzy_decay_ms) {
        eprintln!("Warning: Failed to configure jemalloc decay: {}", e);
    }
}

/// Set jemalloc decay times for all arenas
/// Only available when jemalloc is the global allocator
///
/// According to jemalloc docs, `arenas.dirty_decay_ms` sets the decay time
/// for ALL arenas (not just new ones). The value is ssize_t.
#[cfg(all(feature = "jemalloc", not(feature = "nif_alloc"), not(target_env = "msvc")))]
fn set_jemalloc_decay(dirty_decay_ms: i64, muzzy_decay_ms: i64) -> Result<(), String> {
    use std::ffi::CString;

    // Set dirty decay time for ALL arenas
    // jemalloc uses ssize_t which maps to isize in Rust
    let dirty_val: isize = dirty_decay_ms as isize;
    unsafe {
        let key = CString::new("arenas.dirty_decay_ms").unwrap();
        let result = tikv_jemalloc_sys::mallctl(
            key.as_ptr(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &dirty_val as *const isize as *mut std::ffi::c_void,
            std::mem::size_of::<isize>(),
        );
        if result != 0 {
            return Err(format!("Failed to set dirty_decay_ms: error {}", result));
        }
    }

    // Set muzzy decay time for ALL arenas
    let muzzy_val: isize = muzzy_decay_ms as isize;
    unsafe {
        let key = CString::new("arenas.muzzy_decay_ms").unwrap();
        let result = tikv_jemalloc_sys::mallctl(
            key.as_ptr(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            &muzzy_val as *const isize as *mut std::ffi::c_void,
            std::mem::size_of::<isize>(),
        );
        if result != 0 {
            return Err(format!("Failed to set muzzy_decay_ms: error {}", result));
        }
    }

    Ok(())
}

// =============================================================================
// JEMALLOC PER-THREAD TLS WARMUP
// =============================================================================
//
// When cozodb.so is loaded via dlopen (as Erlang does for NIFs), jemalloc's
// thread-specific data (TSD) — including per-thread tcaches — is initialized
// lazily on each thread's first allocation. With `disable_initial_exec_tls`,
// jemalloc uses pthread_getspecific for TSD, which requires proper pthread key
// setup.
//
// On BEAM dirty IO scheduler threads (configured with -SDio 256), the first
// NIF call triggers jemalloc TSD initialization concurrently across many
// threads.
// If this happens during a heavy allocation path (e.g., RocksDB open/recovery),
// the TSD may not be fully initialized before jemalloc's internal structures
// are accessed, causing SIGSEGV in _rjem_je_free_default or
// _rjem_je_tcache_bin_flush_small.
//
// The fix: force a small malloc+free on each thread BEFORE any heavy work.
// This ensures jemalloc's TSD, tcache, and arena binding are fully initialized
// in a controlled context.
// =============================================================================

#[cfg(all(feature = "jemalloc", not(feature = "nif_alloc"), not(target_env = "msvc")))]
thread_local! {
    static JEMALLOC_TLS_WARM: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Force jemalloc TSD initialization for the current thread.
///
/// Must be called at the entry of every dirty-scheduled NIF function to prevent
/// SIGSEGV during RocksDB operations on BEAM dirty IO/CPU scheduler threads.
/// After the first call per thread, this is effectively a no-op (single TLS read).
#[cfg(all(feature = "jemalloc", not(feature = "nif_alloc"), not(target_env = "msvc")))]
#[inline]
fn warmup_jemalloc_tls() {
    JEMALLOC_TLS_WARM.with(|warm| {
        if !warm.get() {
            // Small alloc+free through jemalloc forces full TSD initialization:
            // pthread key lookup, tcache creation, arena binding.
            // Both prefixed (_rjem_je_*) and unprefixed (malloc/free) APIs share
            // the same TSD, so warming up via either path covers both.
            unsafe {
                let ptr = tikv_jemalloc_sys::malloc(1);
                if !ptr.is_null() {
                    tikv_jemalloc_sys::free(ptr);
                }
            }
            warm.set(true);
        }
    });
}

#[cfg(not(all(feature = "jemalloc", not(feature = "nif_alloc"), not(target_env = "msvc"))))]
#[inline]
fn warmup_jemalloc_tls() {}

// =============================================================================
// STRUCTS REQUIRED FOR NIF
// =============================================================================

struct Registration {
    receiver: Receiver<(CallbackOp, NamedRows, NamedRows)>,
    relname: String,
    pid: LocalPid,
}

// id -> (channel, relname, pid)
type Registrations = Arc<Mutex<HashMap<u32, Registration>>>;

// Static variables are allocated for the duration of a program's run and are
// not specific to any thread.
// This macro lazily initializes the variable on its first access.
lazy_static! {
    // Required for Callback feature.
    // We use THREAD_POOL to shard the callback handlers based on relation name.
    static ref THREAD_POOL: Lazy<ThreadPool> =
        Lazy::new(|| {
            ThreadPool::new(*NUM_THREADS)
        });

    // Required for Callback feature.
    static ref NUM_THREADS: usize = {
        if let Ok(val) = std::env::var("COZODB_CALLBACK_THREADPOOL_SIZE") {
            let mut num = val.parse::<usize>().unwrap_or_default();
            if num == 0 {
                num = num_cpus::get();
            };
            num
        } else {
            num_cpus::get()
        }
    };

    // Required for Callback feature.
    static ref REGISTRATIONS: Lazy<Registrations> =
        Lazy::new(|| {
            Arc::new(Mutex::new(HashMap::new()))
        });
}

/// A NIF Resource wrapping the CozoDB DbInstance.
/// Resources are reference-counted by Erlang - when all references are
/// garbage collected, the resource (and DbInstance) is automatically dropped.
struct DbHandleResource {
    db_instance: DbInstance,
}

/// Wrapper required to serialise Cozo's NamedRows value as Erlang map
struct NamedRowsWrapper<'a>(&'a NamedRows);

impl<'a> Encoder for NamedRowsWrapper<'_> {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        let headers = self.0.headers.encode(env);

        // OPTIMIZED: Use buffer reuse to reduce allocator churn
        // Instead of allocating Vec<Term> per row, we reuse a single buffer
        // and build Erlang lists directly using raw NIF functions.
        // This reduces allocations from 2*M+2 to just 2 for M rows.
        let rows_term = encode_rows_optimized(env, &self.0.rows);
        let count = self.0.rows.len();

        let next = match &self.0.next {
            Some(more_ref) => {
                // Dereference `more` before encoding
                let more = &**more_ref;
                NamedRowsWrapper(more).encode(env)
            }
            None => atoms::null().encode(env),
        };

        // Create and return an Erlang map with atom keys headers, rows and next
        let mut map = rustler::types::map::map_new(env);
        map = map.map_put(atoms::headers(), headers).unwrap();
        map = map.map_put(atoms::rows(), rows_term).unwrap();
        map = map.map_put(atoms::next(), next).unwrap();
        map = map.map_put(atoms::count(), count.encode(env)).unwrap();
        map
    }
}

/// Encode rows efficiently by reusing buffers instead of allocating per-row.
///
/// Previous implementation allocated:
/// - 1 Vec<Term> per row for collecting encoded column values
/// - 1 Vec<NIF_TERM> per row inside .encode() for the Erlang list
/// - 1 Vec<Term> for all rows
/// - 1 Vec<NIF_TERM> for the outer list
/// Total: 2*M + 2 allocations for M rows
///
/// This implementation allocates:
/// - 1 Vec<NIF_TERM> for column buffer (reused across all rows)
/// - 1 Vec<NIF_TERM> for row terms
/// Total: 2 allocations regardless of row count
fn encode_rows_optimized<'b>(env: Env<'b>, rows: &[Vec<DataValue>]) -> Term<'b> {
    if rows.is_empty() {
        // Return empty list
        return unsafe {
            Term::new(env, enif_make_list_from_array(env.as_c_arg(), std::ptr::null(), 0))
        };
    }

    // Pre-allocate buffer for outer list (row terms)
    let mut row_terms: Vec<ERL_NIF_TERM> = Vec::with_capacity(rows.len());

    // Pre-allocate buffer for inner lists - reused across all rows
    // Use the max column count to avoid reallocations
    let max_cols = rows.iter().map(|r| r.len()).max().unwrap_or(0);
    let mut col_buffer: Vec<ERL_NIF_TERM> = Vec::with_capacity(max_cols);

    for row in rows {
        // Clear and reuse the column buffer
        col_buffer.clear();

        // Encode each column value directly into the buffer
        for val in row {
            col_buffer.push(encode_data_value_ref(env, val).as_c_arg());
        }

        // Create Erlang list for this row directly from the buffer
        let row_list = unsafe {
            enif_make_list_from_array(
                env.as_c_arg(),
                col_buffer.as_ptr(),
                col_buffer.len() as u32,
            )
        };
        row_terms.push(row_list);
    }

    // Create outer list from row terms
    unsafe {
        Term::new(
            env,
            enif_make_list_from_array(env.as_c_arg(), row_terms.as_ptr(), row_terms.len() as u32),
        )
    }
}

/// Wrapper required to serialise Cozo's BTreeMap<String, NamedRows>) value as
/// Erlang Term.
/// Used by export_relations()
struct BTreeMapWrapper(BTreeMap<String, NamedRows>);

impl<'a> Encoder for BTreeMapWrapper {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        let mut map = rustler::types::map::map_new(env);
        for (key, value) in &self.0 {
            let key_term = key.encode(env);
            let value_term = NamedRowsWrapper(value).encode(env);
            map = map.map_put(key_term, value_term).unwrap();
        }
        map
    }
}

/// Wrapper required to serialise Cozo's DataValue value as Erlang Term
struct DataValueWrapper(DataValue);

impl<'a> Encoder for DataValueWrapper {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        encode_data_value_ref(env, &self.0)
    }
}

/// Encode a DataValue by reference without cloning the value itself.
/// This is more memory efficient than wrapping in DataValueWrapper which
/// requires ownership or cloning.
fn encode_data_value_ref<'b>(env: Env<'b>, value: &DataValue) -> Term<'b> {
    match value {
        DataValue::Null => atoms::null().encode(env),
        DataValue::Bool(i) => i.encode(env),
        DataValue::Num(i) => encode_num_ref(env, i),
        DataValue::Str(i) => i.as_str().encode(env),
        DataValue::Bytes(i) => i.as_slice().encode(env),
        DataValue::Uuid(w) => w.0.hyphenated().to_string().encode(env),
        DataValue::List(i) => {
            // OPTIMIZED: Build Erlang list directly without
            // intermediate Vec<Term>
            let mut terms: Vec<ERL_NIF_TERM> = Vec::with_capacity(i.len());
            for val in i.iter() {
                terms.push(encode_data_value_ref(env, val).as_c_arg());
            }
            unsafe {
                Term::new(
                    env,
                    enif_make_list_from_array(env.as_c_arg(), terms.as_ptr(), terms.len() as u32),
                )
            }
        }
        DataValue::Json(i) => match serde_json::to_string(&i) {
            Ok(json_str) => (atoms::json(), json_str).encode(env),
            Err(_) => "Failed to serialize JsonValue".encode(env),
        },
        DataValue::Vec(i) => encode_vector_ref(env, i),
        DataValue::Validity(i) => {
            let ts = i.timestamp.0 .0.encode(env);
            let assert = i.is_assert.0.encode(env);
            (ts, assert).encode(env)
        }
        DataValue::Regex(_) | DataValue::Set(_) | DataValue::Bot => {
            atoms::null().encode(env)
        }
    }
}

/// Encode Num by reference
fn encode_num_ref<'b>(env: Env<'b>, num: &Num) -> Term<'b> {
    match num {
        Num::Int(i) => i.encode(env),
        Num::Float(f) => f.encode(env),
    }
}

/// Encode Vector by reference
fn encode_vector_ref<'b>(env: Env<'b>, vec: &Vector) -> Term<'b> {
    match vec {
        Vector::F32(arr) => arr.to_vec().encode(env),
        Vector::F64(arr) => arr.to_vec().encode(env),
    }
}

// DataValue does not provide a decode function so we create a new trait and
// implement it. This is used to convert the options Erlang map to a BTreeMap
// expected by run_script
trait Decoder<'a> {
    fn decode(term: Term<'a>) -> NifResult<Self>
    where
        Self: Sized;
}

impl<'a> DataValueWrapper {
    pub fn decode(term: Term<'a>) -> NifResult<Self> {
        if term == atoms::null().encode(term.get_env()) {
            return Ok(DataValueWrapper(DataValue::Null));
        }

        if term == atoms::true_().encode(term.get_env()) {
            return Ok(DataValueWrapper(DataValue::Bool(true)));
        }

        if term == atoms::false_().encode(term.get_env()) {
            return Ok(DataValueWrapper(DataValue::Bool(false)));
        }

        // All other atoms converted to strings
        if term.is_atom() {
            let string = term.atom_to_string()?;
            return Ok(DataValueWrapper(DataValue::Str(string.into())));
        }

        if let Ok(num) = term.decode::<i64>() {
            return Ok(DataValueWrapper(DataValue::Num(cozo::Num::Int(num))));
        } else if let Ok(num) = term.decode::<f64>() {
            return Ok(DataValueWrapper(DataValue::Num(cozo::Num::Float(num))));
        }

        // Handle lists
        if let Ok(list_iterator) = term.decode::<ListIterator>() {
            // Collect the list into a vector to process multiple times
            let list_terms: Vec<Term<'a>> = list_iterator.collect();

            let is_list_of_integers = list_terms.iter().all(|t| t.decode::<i64>().is_ok());

            if is_list_of_integers {
                // Decode as a list of integers
                let decoded_list: NifResult<Vec<DataValue>> = list_terms
                    .iter()
                    .map(|term| {
                        let num = term.decode::<i64>()?;
                        Ok(DataValue::Num(cozo::Num::Int(num)))
                    })
                    .collect();
                return Ok(DataValueWrapper(DataValue::List(decoded_list?)));
            }

            let decoded_list: NifResult<Vec<DataValue>> = list_terms
                .iter()
                .map(|term| DataValueWrapper::decode(*term).map(|wrapper| wrapper.0))
                .collect();
            return Ok(DataValueWrapper(DataValue::List(decoded_list?)));
        }

        if let Ok(string) = term.decode::<String>() {
            return Ok(DataValueWrapper(DataValue::Str(string.into())));
        }

        // Handle JSON
        if let Ok((json_atom, json_str)) = term.decode::<(Term, String)>() {
            if json_atom == atoms::json().encode(term.get_env()) {
                if let Ok(json_value) = serde_json::from_str(&json_str) {
                    return Ok(DataValueWrapper(DataValue::Json(json_value)));
                }
            }
        }

        // TODO Handle validity

        // Default case for unrecognized or unsupported terms
        Err(rustler::Error::Term(Box::new(
            "Unsupported Erlang term".to_string(),
        )))
    }
}

/// Wrapper required to serialise Cozo's Num value as Erlang Term
struct NumWrapper(Num);

impl<'a> Encoder for NumWrapper {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        match &self.0 {
            Num::Int(i) => i.encode(env),
            Num::Float(f) => f.encode(env),
        }
    }
}

/// Wrapper required to serialise Cozo's Vector value as Erlang Term
struct VectorWrapper(Vector);

impl<'a> Encoder for VectorWrapper {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        match &self.0 {
            Vector::F32(i) => Array32Wrapper(i.clone()).encode(env),
            Vector::F64(i) => Array64Wrapper(i.clone()).encode(env),
        }
    }
}

/// Wrapper required to serialise Cozo's Array1<f32> value as Erlang Term
struct Array32Wrapper(Array1<f32>); // Used by Vector

impl<'a> Encoder for Array32Wrapper {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        // Convert ndarray::Array1 to a Vec<f32>
        let vec: Vec<f32> = self.0.to_vec();
        // Encode the Vec<f32> as an Elixir list
        vec.encode(env)
    }
}

/// Wrapper required to serialise Cozo's Array1<f64> value as Erlang Term
struct Array64Wrapper(Array1<f64>); // Used by Vector

impl<'a> Encoder for Array64Wrapper {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        // Convert ndarray::Array1 to a Vec<f64>
        let vec: Vec<f64> = self.0.to_vec();
        // Encode the Vec<f64> as an Elixir list
        vec.encode(env)
    }
}

// =============================================================================
// ERROR HANDLING
// =============================================================================

/// Convert a CozoDB error to a structured Erlang term
/// CozoDB uses miette::Report for errors, which provides rich error information
fn cozo_error_to_term<'a, E: std::fmt::Display + std::fmt::Debug>(
    env: Env<'a>,
    err: &E,
) -> Term<'a> {
    let mut error_map = Term::map_new(env);

    // Use the alternate Display format which may include more error chain information
    // format!("{:#}") uses the alternate Display format
    let message = format!("{:#}", err);

    error_map = error_map
        .map_put(atoms::message(), message.encode(env))
        .unwrap();

    (atoms::error(), error_map).encode(env)
}

// =============================================================================
// OPERATIONS (RESOURCE-BASED, LOCK-FREE)
// =============================================================================
//
// All operations work with ResourceArc<DbHandleResource> directly.
// Resources are reference-counted by Erlang - when all references are
// garbage collected, the DbInstance is automatically dropped.
//
// Use `open_res/2` or `open_res/3` from Erlang to open a database.
//
// These functions work with ResourceArc<DbHandleResource> directly, completely
// bypassing the global HANDLES mutex. This provides:
// - Lock-free access to the database instance
// - Better performance under high concurrency
// - Reduced contention between worker processes
//
// Use `open_res/2` or `open_res/3` to open a database and get a ResourceArc
// directly. This is the recommended path for high-performance applications.

/// Opens/creates a database with options, returning a ResourceArc directly.
/// This is the lock-free version that bypasses the global HANDLES mutex entirely.
#[rustler::nif(schedule = "DirtyIo", name = "open_res_opts_nif")]
fn open_res_with_options<'a>(
    env: Env<'a>,
    engine: String,
    path: String,
    options: String,
) -> NifResult<Term<'a>> {
    warmup_jemalloc_tls();
    // Validate engine name and obtain DBInstance
    // Supported engines:
    //   - "mem": In-memory storage (no persistence)
    //   - "sqlite": SQLite backend (good for small datasets, single-writer)
    //   - "rocksdb": RocksDB via cozorocks (C++ FFI, current default)
    //   - "newrocksdb": RocksDB via rust-rocksdb crate (comprehensive env var config)
    //
    // The "newrocksdb" engine requires the "new-rocksdb" feature to be enabled.
    // It reads configuration from COZO_ROCKSDB_* environment variables.
    let result = match engine.as_str() {
        "mem" | "sqlite" | "rocksdb" | "newrocksdb" => {
            DbInstance::new_with_str(engine.as_str(), path.as_str(), &options)
        }
        _ => return Err(rustler::Error::Term(Box::new(atoms::invalid_engine()))),
    };

    // Validate we have a DBInstance and return error if not
    let db = match result {
        Ok(db) => db,
        Err(err) => {
            return Err(rustler::Error::Term(Box::new(format!("{:#?}", err))))
        }
    };

    // Wrap directly in ResourceArc - no HANDLES, no mutex!
    let resource = ResourceArc::new(DbHandleResource { db_instance: db });
    Ok((atoms::ok(), resource.encode(env)).encode(env))
}

/// Returns the result of running script using a ResourceArc (lock-free).
/// This is the high-performance version of run_script that avoids mutex contention.
#[rustler::nif(schedule = "DirtyIo", name = "run_script_res_nif")]
fn run_script_res<'a>(
    env: Env<'a>,
    db_res: ResourceArc<DbHandleResource>,
    script: String,
    params: Term,
    read_only: Term,
) -> NifResult<Term<'a>> {
    warmup_jemalloc_tls();
    let db = &db_res.db_instance;

    let read_only = if atoms::true_() == read_only {
        ScriptMutability::Immutable
    } else {
        ScriptMutability::Mutable
    };

    // Convert the Erlang map to a BTreeMap using the helper function
    let btree = convert_to_btreemap(params)?;

    let start = Instant::now();

    match db.run_script(&script, btree, read_only) {
        Ok(named_rows) => {
            let took = start.elapsed().as_secs_f64();
            let mut map = NamedRowsWrapper(&named_rows).encode(env);
            map = map.map_put(atoms::took(), took).unwrap();
            let result = (atoms::ok(), map);
            Ok(result.encode(env))
        }
        Err(err) => Ok(cozo_error_to_term(env, &err)),
    }
}

/// Run the CozoScript passed in folding any error into the returned JSON (lock-free).
/// This is the high-performance version of run_script_str that avoids mutex contention.
#[rustler::nif(schedule = "DirtyIo", name = "run_script_str_res_nif")]
fn run_script_str_res<'a>(
    env: Env<'a>,
    db_res: ResourceArc<DbHandleResource>,
    script: String,
    params: String,
    read_only: Term,
) -> NifResult<Term<'a>> {
    warmup_jemalloc_tls();
    let db = &db_res.db_instance;

    let read_only = atoms::true_() == read_only;
    let json_str = db.run_script_str(&script, &params, read_only);
    let result = (atoms::ok().encode(env), json_str.encode(env));
    Ok(result.encode(env))
}

/// Same as run_script_json but using ResourceArc (lock-free).
#[rustler::nif(schedule = "DirtyIo", name = "run_script_json_res_nif")]
fn run_script_json_res<'a>(
    env: Env<'a>,
    db_res: ResourceArc<DbHandleResource>,
    script: String,
    params: String,
    read_only: Term,
) -> NifResult<Term<'a>> {
    warmup_jemalloc_tls();
    let db = &db_res.db_instance;

    let read_only = if atoms::true_() == read_only {
        ScriptMutability::Immutable
    } else {
        ScriptMutability::Mutable
    };

    let params_json = match params_to_btree(&params) {
        Ok(value) => value,
        Err(err) => return Err(rustler::Error::Term(Box::new(format!("{:#?}", err)))),
    };

    match db.run_script(&script, params_json, read_only) {
        Ok(result) => {
            let json = result.into_json();
            match serde_json::to_string(&json) {
                Ok(json_str) => {
                    let result = (atoms::ok().encode(env), json_str.encode(env));
                    Ok(result.encode(env))
                }
                Err(_) => Err(rustler::Error::Atom("json_encode_error")),
            }
        }
        Err(err) => Ok(cozo_error_to_term(env, &err)),
    }
}

/// Imports relations using ResourceArc (lock-free).
#[rustler::nif(schedule = "DirtyIo", name = "import_relations_res_nif")]
fn import_relations_res<'a>(
    env: Env<'a>,
    db_res: ResourceArc<DbHandleResource>,
    data: String,
) -> NifResult<Term<'a>> {
    warmup_jemalloc_tls();
    let db = &db_res.db_instance;

    match db.import_relations_str_with_err(&data) {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(err) => Err(rustler::Error::Term(Box::new(format!("{:#?}", err)))),
    }
}

/// Exports relations using ResourceArc (lock-free).
#[rustler::nif(schedule = "DirtyIo", name = "export_relations_res_nif")]
fn export_relations_res<'a>(
    env: Env<'a>,
    db_res: ResourceArc<DbHandleResource>,
    relations: Vec<String>,
) -> NifResult<Term<'a>> {
    warmup_jemalloc_tls();
    let db = &db_res.db_instance;

    match db.export_relations(relations.iter().map(|s| s as &str)) {
        Ok(btreemap) => {
            let mut data = rustler::types::map::map_new(env);
            for (key, value) in btreemap {
                let key_term = key.encode(env);
                let value_term = NamedRowsWrapper(&value).encode(env);
                data = data.map_put(key_term, value_term).unwrap();
            }
            Ok((atoms::ok().encode(env), data.encode(env)).encode(env))
        }
        Err(err) => Err(rustler::Error::Term(Box::new(format!("{:#?}", err)))),
    }
}

/// Export relations as JSON using ResourceArc (lock-free).
#[rustler::nif(schedule = "DirtyIo", name = "export_relations_json_res_nif")]
fn export_relations_json_res<'a>(
    env: Env<'a>,
    db_res: ResourceArc<DbHandleResource>,
    relations: Vec<String>,
) -> NifResult<Term<'a>> {
    warmup_jemalloc_tls();
    let db = &db_res.db_instance;

    match db.export_relations(relations.iter().map(|s| s as &str)) {
        Ok(btreemap) => {
            let data: Vec<_> = btreemap
                .into_iter()
                .map(|(k, v)| (k, v.into_json()))
                .collect();
            let json = json!(data).to_string();
            Ok((atoms::ok().encode(env), json.encode(env)).encode(env))
        }
        Err(err) => Err(rustler::Error::Term(Box::new(format!("{:#?}", err)))),
    }
}

/// Backs up the database using ResourceArc (lock-free).
#[rustler::nif(schedule = "DirtyIo", name = "backup_res_nif")]
fn backup_res<'a>(
    env: Env<'a>,
    db_res: ResourceArc<DbHandleResource>,
    path: String,
) -> NifResult<Term<'a>> {
    warmup_jemalloc_tls();
    let db = &db_res.db_instance;

    match db.backup_db(path) {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(err) => Err(rustler::Error::Term(Box::new(format!("{:#?}", err)))),
    }
}

/// Restores the database from backup using ResourceArc (lock-free).
#[rustler::nif(schedule = "DirtyIo", name = "restore_res_nif")]
fn restore_res<'a>(
    env: Env<'a>,
    db_res: ResourceArc<DbHandleResource>,
    path: String,
) -> NifResult<Term<'a>> {
    warmup_jemalloc_tls();
    let db = &db_res.db_instance;

    match db.restore_backup(path) {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(err) => Err(rustler::Error::Term(Box::new(format!("{:#?}", err)))),
    }
}

/// Import from backup using ResourceArc (lock-free).
#[rustler::nif(schedule = "DirtyIo", name = "import_from_backup_res_nif")]
fn import_from_backup_res<'a>(
    env: Env<'a>,
    db_res: ResourceArc<DbHandleResource>,
    path: String,
    relations: Vec<String>,
) -> NifResult<Term<'a>> {
    warmup_jemalloc_tls();
    let db = &db_res.db_instance;

    match db.import_from_backup(path, &relations) {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(err) => Err(rustler::Error::Term(Box::new(format!("{:#?}", err)))),
    }
}

/// Register callback using ResourceArc (lock-free).
#[rustler::nif(schedule = "DirtyCpu", name = "register_callback_res_nif")]
fn register_callback_res<'a>(
    env: Env<'a>,
    db_res: ResourceArc<DbHandleResource>,
    rel: String,
) -> NifResult<Term<'a>> {
    warmup_jemalloc_tls();
    let db = &db_res.db_instance;

    // Register the callback in cozodb
    let (reg_id, receiver) = db.register_callback(rel.as_str(), None);

    // This is a "threaded NIF": it spawns a thread that sends a message back
    // to the calling thread later.
    let local_pid = env.pid();

    // Store receiver and PID in the global maps
    {
        let mut regs = REGISTRATIONS.lock().unwrap();
        regs.insert(
            reg_id,
            Registration {
                receiver: receiver,
                relname: rel.clone(),
                pid: local_pid,
            },
        );
    }

    // Distribute across threads in pool
    let pool = THREAD_POOL.clone();
    let worker_count = pool.max_count();
    let regs_clone = REGISTRATIONS.clone();

    // Hash the metadata to determine the worker index
    let hash = calculate_hash(&rel);
    let worker_index = (hash % pool.max_count() as u64) as usize;

    pool.execute(move || {
        worker_thread(regs_clone, worker_count, worker_index);
    });

    Ok((atoms::ok(), reg_id).encode(env))
}

/// Unregister callback using ResourceArc (lock-free).
#[rustler::nif(schedule = "DirtyCpu", name = "unregister_callback_res_nif")]
fn unregister_callback_res<'a>(
    env: Env<'a>,
    db_res: ResourceArc<DbHandleResource>,
    reg_id: u32,
) -> NifResult<Term<'a>> {
    warmup_jemalloc_tls();
    let db = &db_res.db_instance;

    let result: bool = db.unregister_callback(reg_id);

    // We remove even if result == false
    {
        let mut regs = REGISTRATIONS.lock().unwrap();
        regs.remove(&reg_id);
    }

    Ok(result.encode(env))
}

/// Flush all RocksDB memtables to disk using ResourceArc (lock-free).
#[rustler::nif(schedule = "DirtyIo", name = "flush_memtables_res_nif")]
fn flush_memtables_res<'a>(
    env: Env<'a>,
    db_res: ResourceArc<DbHandleResource>,
) -> NifResult<Term<'a>> {
    warmup_jemalloc_tls();
    let db = &db_res.db_instance;

    match db.flush() {
        Ok(()) => Ok(atoms::ok().encode(env)),
        Err(err) => Err(rustler::Error::Term(Box::new(format!("{:#?}", err)))),
    }
}

/// Get RocksDB memory statistics using ResourceArc (lock-free).
#[rustler::nif(name = "rocksdb_memory_stats_res_nif")]
fn rocksdb_memory_stats_res<'a>(
    env: Env<'a>,
    db_res: ResourceArc<DbHandleResource>,
) -> NifResult<Term<'a>> {
    let db = &db_res.db_instance;

    match db.get_rocksdb_memory_stats() {
        Some(stats) => {
            let mut map = rustler::types::map::map_new(env);
            map = map.map_put("memtable_size".encode(env), stats.memtable_size.encode(env)).unwrap();
            map = map.map_put("block_cache_usage".encode(env), stats.block_cache_usage.encode(env)).unwrap();
            map = map.map_put("block_cache_pinned".encode(env), stats.block_cache_pinned.encode(env)).unwrap();
            map = map.map_put("table_readers_mem".encode(env), stats.table_readers_mem.encode(env)).unwrap();
            let total = stats.memtable_size + stats.block_cache_usage + stats.table_readers_mem;
            map = map.map_put("total".encode(env), total.encode(env)).unwrap();
            Ok((atoms::ok(), map).encode(env))
        }
        None => {
            Ok((atoms::error(), "not_rocksdb".encode(env)).encode(env))
        }
    }
}

/// Returns memory statistics.
/// Reports the Rust allocator type and jemalloc stats when available.
#[rustler::nif(name = "memory_stats_nif")]
fn memory_stats<'a>(env: Env<'a>) -> NifResult<Term<'a>> {
    let registrations_count = {
        let regs = REGISTRATIONS.lock().unwrap();
        regs.len()
    };

    let mut map = rustler::types::map::map_new(env);
    map = map.map_put("callback_registrations".encode(env), registrations_count.encode(env)).unwrap();

    // Report which allocator Rust is using
    #[cfg(feature = "nif_alloc")]
    {
        // Rust uses Erlang's allocator (EnifAllocator)
        map = map.map_put("rust_allocator".encode(env), "erlang".encode(env)).unwrap();
    }

    #[cfg(all(feature = "jemalloc", not(feature = "nif_alloc"), not(target_env = "msvc")))]
    {
        // Rust uses jemalloc
        map = map.map_put("rust_allocator".encode(env), "jemalloc".encode(env)).unwrap();
    }

    #[cfg(all(not(feature = "nif_alloc"), not(all(feature = "jemalloc", not(target_env = "msvc")))))]
    {
        // Rust uses system allocator
        map = map.map_put("rust_allocator".encode(env), "system".encode(env)).unwrap();
    }

    // For backwards compatibility, also set "allocator" key
    #[cfg(feature = "nif_alloc")]
    {
        map = map.map_put("allocator".encode(env), "erlang".encode(env)).unwrap();
    }

    #[cfg(all(feature = "jemalloc", not(feature = "nif_alloc"), not(target_env = "msvc")))]
    {
        map = map.map_put("allocator".encode(env), "jemalloc".encode(env)).unwrap();
    }

    #[cfg(all(not(feature = "nif_alloc"), not(all(feature = "jemalloc", not(target_env = "msvc")))))]
    {
        map = map.map_put("allocator".encode(env), "system".encode(env)).unwrap();
    }

    Ok((atoms::ok(), map).encode(env))
}

/// Dump a jemalloc heap profile to the specified file path.
///
/// IMPORTANT: For this to work, the application MUST be started with:
///   MALLOC_CONF="prof:true,prof_prefix:jeprof.out"
///
/// The profile can then be analyzed with:
///   jeprof --svg /path/to/beam.smp /path/to/profile.heap > heap.svg
///
/// Returns:
///   {:ok, path} - Profile dumped successfully to the given path
///   {:error, :profiling_not_enabled} - jemalloc profiling not enabled (need MALLOC_CONF=prof:true)
///   {:error, :not_jemalloc} - Not using jemalloc allocator
///   {:error, reason} - Other error
///
/// Note: Only available when jemalloc is the global allocator (not when using nif_alloc).
#[rustler::nif(name = "dump_heap_profile_nif")]
fn dump_heap_profile<'a>(env: Env<'a>, path: String) -> NifResult<Term<'a>> {
    // Only available when jemalloc is the global allocator AND profiling feature is enabled
    #[cfg(all(feature = "jemalloc", feature = "jemalloc-profiling", not(feature = "nif_alloc"), not(target_env = "msvc")))]
    {
        use std::ffi::CString;

        // Check if profiling is enabled
        let prof_active: bool = match tikv_jemalloc_ctl::profiling::prof::read() {
            Ok(active) => active,
            Err(_) => {
                return Ok((atoms::error(), "profiling_not_available".encode(env)).encode(env));
            }
        };

        if !prof_active {
            return Ok((atoms::error(), "profiling_not_enabled".encode(env)).encode(env));
        }

        // Dump the profile to the specified path
        // We need to use the raw mallctl interface for prof.dump
        let path_cstr = match CString::new(path.clone()) {
            Ok(s) => s,
            Err(_) => {
                return Ok((atoms::error(), "invalid_path".encode(env)).encode(env));
            }
        };

        // Use the prof.dump mallctl to dump to a specific file
        let name = CString::new("prof.dump").unwrap();
        let path_ptr = path_cstr.as_ptr();

        let result = unsafe {
            tikv_jemalloc_sys::mallctl(
                name.as_ptr(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &path_ptr as *const _ as *mut std::ffi::c_void,
                std::mem::size_of::<*const i8>(),
            )
        };

        if result == 0 {
            Ok((atoms::ok(), path.encode(env)).encode(env))
        } else {
            let error_msg = format!("mallctl failed with code {}", result);
            Ok((atoms::error(), error_msg.encode(env)).encode(env))
        }
    }

    // When nif_alloc is enabled, jemalloc isn't the global allocator
    #[cfg(feature = "nif_alloc")]
    {
        let _ = path; // suppress unused warning
        Ok((atoms::error(), "using_erlang_allocator".encode(env)).encode(env))
    }

    // Fallback: jemalloc without profiling, or no jemalloc at all
    #[cfg(all(not(feature = "nif_alloc"), not(all(feature = "jemalloc", feature = "jemalloc-profiling", not(target_env = "msvc")))))]
    {
        let _ = path; // suppress unused warning
        Ok((atoms::error(), "profiling_not_compiled".encode(env)).encode(env))
    }
}

// =============================================================================
// BLOCK CACHE CONTROL (PROCESS-GLOBAL)
// =============================================================================

/// Clear all entries from the shared RocksDB block cache.
/// This releases memory but keeps the cache structure intact.
/// New reads will repopulate the cache as needed.
///
/// Returns: :ok
#[rustler::nif(name = "clear_block_cache_nif")]
fn clear_block_cache<'a>(env: Env<'a>) -> NifResult<Term<'a>> {
    cozo::clear_block_cache();
    Ok(atoms::ok().encode(env))
}

/// Set the capacity of the shared RocksDB block cache in MB.
/// Setting to 0 effectively disables caching.
/// Setting to a smaller value will trigger eviction of excess entries.
///
/// Returns: :ok
#[rustler::nif(name = "set_block_cache_capacity_nif")]
fn set_block_cache_capacity<'a>(env: Env<'a>, capacity_mb: u64) -> NifResult<Term<'a>> {
    cozo::set_block_cache_capacity_mb(capacity_mb as usize);
    Ok(atoms::ok().encode(env))
}

/// Get statistics about the shared RocksDB block cache.
///
/// Returns: {:ok, %{capacity: int, usage: int, pinned_usage: int}}
/// All values are in bytes.
#[rustler::nif(name = "get_block_cache_stats_nif")]
fn get_block_cache_stats<'a>(env: Env<'a>) -> NifResult<Term<'a>> {
    let stats = cozo::get_block_cache_stats();

    let map = Term::map_new(env);
    let map = map.map_put(
        "capacity".encode(env),
        (stats.capacity as u64).encode(env)
    ).map_err(|_| rustler::Error::Term(Box::new("Failed to build map")))?;
    let map = map.map_put(
        "usage".encode(env),
        (stats.usage as u64).encode(env)
    ).map_err(|_| rustler::Error::Term(Box::new("Failed to build map")))?;
    let map = map.map_put(
        "pinned_usage".encode(env),
        (stats.pinned_usage as u64).encode(env)
    ).map_err(|_| rustler::Error::Term(Box::new("Failed to build map")))?;

    Ok((atoms::ok(), map).encode(env))
}

/// Force jemalloc to return unused memory to the operating system.
/// This purges dirty pages from all arenas, making them available to the OS.
///
/// Returns: {:ok, purged_bytes} on success, {:error, reason} on failure.
///
/// Note: Only available when jemalloc is the global allocator (not when using nif_alloc).
/// When using Erlang's allocator, memory is managed by the BEAM and returned via GC.
#[rustler::nif(name = "purge_jemalloc_nif")]
fn purge_jemalloc<'a>(env: Env<'a>) -> NifResult<Term<'a>> {
    // Only available when jemalloc is the global allocator
    #[cfg(all(feature = "jemalloc", not(feature = "nif_alloc"), not(target_env = "msvc")))]
    {
        use std::ffi::CString;

        // First, get current stats for comparison
        let epoch = tikv_jemalloc_ctl::epoch::mib().unwrap();
        epoch.advance().unwrap();

        let retained_before: usize = tikv_jemalloc_ctl::stats::retained::read().unwrap_or(0);

        // Purge all arenas using "arena.<i>.purge" mallctl
        // The special arena index MALLCTL_ARENAS_ALL (4096) purges all arenas
        let purge_cmd = CString::new("arena.4096.purge").unwrap();
        let result = unsafe {
            tikv_jemalloc_sys::mallctl(
                purge_cmd.as_ptr(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                0,
            )
        };

        if result == 0 {
            // Advance epoch again and get new stats
            epoch.advance().unwrap();
            let retained_after: usize = tikv_jemalloc_ctl::stats::retained::read().unwrap_or(0);
            let purged = if retained_before > retained_after {
                retained_before - retained_after
            } else {
                0
            };

            Ok((atoms::ok(), (purged as u64).encode(env)).encode(env))
        } else {
            let error_msg = format!("mallctl purge failed with code {}", result);
            Ok((atoms::error(), error_msg.encode(env)).encode(env))
        }
    }

    // When nif_alloc is enabled, jemalloc isn't the global allocator
    #[cfg(feature = "nif_alloc")]
    {
        Ok((atoms::error(), "using_erlang_allocator".encode(env)).encode(env))
    }

    // Fallback for other cases (no jemalloc at all)
    #[cfg(all(not(feature = "nif_alloc"), not(all(feature = "jemalloc", not(target_env = "msvc")))))]
    {
        Ok((atoms::error(), "not_jemalloc".encode(env)).encode(env))
    }
}

/// Configure jemalloc decay times at runtime.
///
/// Arguments:
/// - dirty_decay_ms: Time in milliseconds before dirty pages are purged (0 = immediate, -1 = disable)
/// - muzzy_decay_ms: Time in milliseconds before muzzy pages are purged (0 = immediate, -1 = disable)
///
/// Returns: :ok on success, {:error, reason} on failure.
///
/// Lower values = more aggressive memory return to OS (may impact latency)
/// Higher values = better latency but higher memory usage
/// Default jemalloc is 10000ms (10 seconds), we default to 1000ms (balanced)
///
/// Note: Only available when jemalloc is the global allocator (not when using nif_alloc).
#[rustler::nif(name = "set_jemalloc_decay_nif")]
fn set_jemalloc_decay_nif<'a>(
    env: Env<'a>,
    dirty_decay_ms: i64,
    muzzy_decay_ms: i64,
) -> NifResult<Term<'a>> {
    // Only available when jemalloc is the global allocator
    #[cfg(all(feature = "jemalloc", not(feature = "nif_alloc"), not(target_env = "msvc")))]
    {
        match set_jemalloc_decay(dirty_decay_ms, muzzy_decay_ms) {
            Ok(()) => Ok(atoms::ok().encode(env)),
            Err(e) => Ok((atoms::error(), e.encode(env)).encode(env)),
        }
    }

    // When nif_alloc is enabled, jemalloc isn't the global allocator
    #[cfg(feature = "nif_alloc")]
    {
        let _ = (dirty_decay_ms, muzzy_decay_ms); // Suppress unused warnings
        Ok((atoms::error(), "using_erlang_allocator".encode(env)).encode(env))
    }

    // Fallback for other cases (no jemalloc at all)
    #[cfg(all(not(feature = "nif_alloc"), not(all(feature = "jemalloc", not(target_env = "msvc")))))]
    {
        let _ = (dirty_decay_ms, muzzy_decay_ms); // Suppress unused warnings
        Ok((atoms::error(), "not_jemalloc".encode(env)).encode(env))
    }
}

/// Get current jemalloc decay settings.
///
/// Returns: {:ok, #{dirty_decay_ms => integer(), muzzy_decay_ms => integer()}}
///
/// Note: Only available when jemalloc is the global allocator (not when using nif_alloc).
#[rustler::nif(name = "get_jemalloc_decay_nif")]
fn get_jemalloc_decay_nif<'a>(env: Env<'a>) -> NifResult<Term<'a>> {
    // Only available when jemalloc is the global allocator
    #[cfg(all(feature = "jemalloc", not(feature = "nif_alloc"), not(target_env = "msvc")))]
    {
        use tikv_jemalloc_ctl::raw;

        let dirty_decay_ms: isize = unsafe {
            let key = b"arenas.dirty_decay_ms\0";
            raw::read(key).unwrap_or(-999)
        };

        let muzzy_decay_ms: isize = unsafe {
            let key = b"arenas.muzzy_decay_ms\0";
            raw::read(key).unwrap_or(-999)
        };

        let mut map = rustler::types::map::map_new(env);
        map = map.map_put("dirty_decay_ms".encode(env), (dirty_decay_ms as i64).encode(env)).unwrap();
        map = map.map_put("muzzy_decay_ms".encode(env), (muzzy_decay_ms as i64).encode(env)).unwrap();

        Ok((atoms::ok(), map).encode(env))
    }

    // When nif_alloc is enabled, jemalloc isn't the global allocator
    #[cfg(feature = "nif_alloc")]
    {
        Ok((atoms::error(), "using_erlang_allocator".encode(env)).encode(env))
    }

    // Fallback for other cases (no jemalloc at all)
    #[cfg(all(not(feature = "nif_alloc"), not(all(feature = "jemalloc", not(target_env = "msvc")))))]
    {
        Ok((atoms::error(), "not_jemalloc".encode(env)).encode(env))
    }
}

// =============================================================================
// UTILS
// =============================================================================

/// Helper function to convert Erlang map to BTreeMap.
/// Atom keys are converted to strings.
fn convert_to_btreemap(map_term: Term) -> NifResult<BTreeMap<String, DataValue>> {
    let map_iterator: MapIterator = map_term.decode()?;

    let mut btree: BTreeMap<String, DataValue> = BTreeMap::new();

    for (key, value) in map_iterator {
        // Check if the key is an Atom or a String
        let key_str: String = if key.is_atom() {
            key.atom_to_string()?
        } else if let Ok(string_key) = key.decode::<String>() {
            string_key
        } else {
            return Err(rustler::Error::BadArg);
        };

        // Decode the value into DataValue
        let data_value = DataValueWrapper::decode(value)?.0;

        // Insert into the BTreeMap
        btree.insert(key_str, data_value);
    }

    Ok(btree)
}

fn params_to_btree(params: &String) -> Result<BTreeMap<String, DataValue>, &'static str> {
    if params.is_empty() {
        Ok(BTreeMap::new()) // Wrap in Ok
    } else {
        match serde_json::from_str::<BTreeMap<String, DataValue>>(params) {
            Ok(map) => Ok(map
                .into_iter()
                .map(|(k, v)| (k, DataValue::from(v)))
                .collect()),
            Err(_) => Err("params argument is not a JSON map"),
        }
    }
}

fn worker_thread(registrations: Registrations, worker_count: usize, worker_index: usize) -> bool {
    loop {
        let registrations = registrations.lock().unwrap();

        // Filter receivers for this worker thread
        for (&reg_id, registration) in registrations.iter() {
            let rel = &registration.relname;
            if should_handle(&rel, worker_count, worker_index) {
                {
                    let receiver = &registration.receiver;
                    let pid = registration.pid;
                    for (op, new_rows, old_rows) in receiver.try_iter() {
                        handle_event(rel, op, new_rows, old_rows, reg_id, pid);
                    }
                }
            }

            // Sleep or yield the thread to prevent busy-waiting
            // std::thread::sleep(std::time::Duration::from_millis(100));
        }
    }
}

/// Returns true if relname hashes to the worker_index
fn should_handle(relname: &String, worker_count: usize, worker_index: usize) -> bool {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    relname.hash(&mut hasher);
    let hash = hasher.finish();
    // let size : usize = *NUM_THREADS;
    (hash % worker_count as u64) as usize == worker_index
}

fn handle_event(
    rel: &String,
    op: CallbackOp,
    new_rows: NamedRows,
    old_rows: NamedRows,
    reg_id: u32,
    pid: LocalPid,
) {
    let _ = OwnedEnv::new().send_and_clear(&pid, |env| {
        let result: NifResult<Term> = (|| {
            let reg_id = reg_id.encode(env);
            let rel = rel.encode(env);
            let op = match op {
                CallbackOp::Put => atoms::updated(),
                CallbackOp::Rm => atoms::removed(),
            };
            let event_name = vec![atoms::cozodb(), atoms::relation(), op];
            let new_rows = NamedRowsWrapper(&new_rows).encode(env);
            let old_rows = NamedRowsWrapper(&old_rows).encode(env);
            let event = (event_name, reg_id, rel, new_rows, old_rows);
            Ok(event.encode(env))
        })();

        match result {
            Ok(term) => term,
            Err(_err) => env.error_tuple("failed".encode(env)),
        }
    });
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    t.hash(&mut hasher);
    hasher.finish()
}
