// =============================================================================
// cozodb.erl -
//
// Copyright (c) 2020 Leapsight Holdings Limited. All rights reserved.
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

use core::hash::Hash;
use lazy_static::lazy_static;
use std::collections::BTreeMap;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::Hasher;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Mutex;
use std::time::Instant;

// Used for CALLBACKS feature
// use std::thread;
use once_cell::sync::Lazy;
use std::sync::Arc;
use threadpool::ThreadPool;
use crossbeam::channel::*;

use cozo::*;
use ndarray::Array1; // used by cozo



use rustler::Encoder;
use rustler::Env;
use rustler::NifResult;
use rustler::OwnedEnv;
use rustler::ResourceArc;
use rustler::Term;
use rustler::types::Pid;




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
        nil,
        json,
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
        // Error Reasons
        badarg,
        invalid_engine
    }
}


// Define erlang module and functions
rustler::init!("cozodb",
    [
      new,
      close,
      info,
      run_script,
      run_script_str,
      run_script_json,
      import_relations,
      export_relations,
      export_relations_json,
      register_callback
    ],
    load = on_load
);


/// Define NIF Resources using rustler::resource! macro
fn on_load(env: Env, _: Term) -> bool {
    rustler::resource!(DbResource, env);
    true
}


// =============================================================================
// STRUCTS REQUIRED FOR NIF
// =============================================================================


/// Struct used to globally manage database handles
/// This is combined with lazy_static!macro to create a static variable
/// containing this struct.
struct Handles {
    current: AtomicI32, // thread safe counter
    dbs: Mutex<BTreeMap<i32, DbInstance>>, // mapping of Id -> DbInstance handle
}


struct Registration {
    receiver: Receiver<(CallbackOp, NamedRows, NamedRows)>,
    relname: String,
    pid: Pid
}

// id -> (channel, relname, pid)
type Registrations = Arc<Mutex<HashMap<u32, Registration>>>;


// Static variables are allocated for the duration of a program's run and are
// not specific to any thread.
// lazy_static is used because Rust's standard library doesn't support static
// variables with non-constant initializers directly. It lazily initializes the
// variable on its first access.
lazy_static! {
    // Required so that we can close a database from Erlang
    static ref HANDLES: Handles =
        Handles {
            // sets current to 0
            current: Default::default(),
            // empty BTreeMap guarded by mutex
            dbs: Mutex::new(Default::default())
        };

    static ref NUM_THREADS: usize = num_cpus::get();

    // Required for Callback feature
    static ref THREAD_POOL: Lazy<ThreadPool> =
        Lazy::new(|| {
            // Adjust the number of threads in the pool as needed from config
            ThreadPool::new(*NUM_THREADS)
        });

    static ref REGISTRATIONS: Lazy<Registrations> =
        Lazy::new(|| {
            Arc::new(Mutex::new(HashMap::new()))
        });

}

/// A NIF Resource representing the identifier for a DbInstance handle
struct DbResource {
    db_id: i32,
    engine: String,
    path: String
}

/// Wrapper required to serialise NamedRows value as Erlang map
struct NamedRowsWrapper<'a>(&'a NamedRows);

impl<'a> Encoder for NamedRowsWrapper<'_> {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        let headers = self.0.headers.encode(env);
        let rows: Vec<Vec<DataValueWrapper>> =
            self.0.rows
                .clone()
                .into_iter()
                .map(|inner_vec|
                    inner_vec.into_iter().map(DataValueWrapper).collect())
                .collect();
        let next = match &self.0.next {
            Some(more_ref) => {
                // Dereference `more` before encoding
                let more = &**more_ref;
                NamedRowsWrapper(more).encode(env)
            },
            None => atoms::undefined().encode(env)
        };

        // Create and return the Erlang term {ok, map()}
        let mut map = rustler::types::map::map_new(env);
        map = map.map_put(atoms::headers(), headers).unwrap();
        map = map.map_put(atoms::rows(), rows).unwrap();
        map.map_put(atoms::next(), next).unwrap()
    }
}

struct BTreeMapWrapper(BTreeMap<String, NamedRows>);

impl<'a> Encoder for BTreeMapWrapper {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        let map = rustler::types::map::map_new(env);
        for (key, value) in &self.0 {
            let key_term = key.encode(env);
            let value_term = NamedRowsWrapper(&value).encode(env);
            map.map_put(key_term, value_term).unwrap();
        }
        map
    }
}


/// Wrapper required to serialise DataValue value as Erlang Term
struct DataValueWrapper(DataValue);

impl<'a> Encoder for DataValueWrapper {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        match &self.0 {
            DataValue::Null => atoms::nil().encode(env),
            DataValue::Bool(i) => i.encode(env),
            DataValue::Num(i) => NumWrapper(i.clone()).encode(env),
            DataValue::Str(i) => i.encode(env),
            DataValue::Bytes(i) => i.encode(env),
            DataValue::Uuid(w) => w.0.hyphenated().to_string().encode(env),
            DataValue::List(i) => {
                let encoded_values: Vec<Term<'b>> = i
                    .iter()
                    .map(|val| DataValueWrapper(val.clone()).encode(env))
                    .collect();

                encoded_values.encode(env)
            },
            DataValue::Json(i) => {
                match serde_json::to_string(&i) {
                    Ok(json_str) =>
                        (atoms::json(), json_str).encode(env),
                    Err(_) =>
                        "error: failed to serialize JsonValue".encode(env),
                }
            }
            DataValue::Vec(i) => VectorWrapper(i.clone()).encode(env),
            DataValue::Validity(i) => {
                let ts = i.timestamp.0.0.encode(env);
                let assert = i.is_assert.0.encode(env);
                // (float, bool)
                (ts, assert).encode(env)
            },
            DataValue::Regex(_) | DataValue::Set(_) | DataValue::Bot =>
                // This types are only used internally so we shoul never receive
                // one of this as a result of running a script, but we match
                // them to avoid a compiler error.
                // Just in case we do get them, we return the atom 'nil'
                atoms::nil().encode(env)
        }
    }
}

/// Wrapper required to serialise Num value as Erlang Term
struct NumWrapper(Num);

impl<'a> Encoder for NumWrapper {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        match &self.0 {
            Num::Int(i) => i.encode(env),
            Num::Float(f) => f.encode(env),
        }
    }}

/// Wrapper required to serialise Vector value as Erlang Term
struct VectorWrapper(Vector);

impl<'a> Encoder for VectorWrapper {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        match &self.0 {
            Vector::F32(i) => Array32Wrapper(i.clone()).encode(env),
            Vector::F64(i) => Array64Wrapper(i.clone()).encode(env),
        }
    }
}


/// Wrapper required to serialise Vector value as Erlang Term
struct Array32Wrapper(Array1<f32>);  // Used by Vector

impl<'a> Encoder for Array32Wrapper {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        // Convert ndarray::Array1 to a Vec<f32>
        let vec: Vec<f32> = self.0.to_vec();
        // Encode the Vec<f32> as an Elixir list
        vec.encode(env)
    }
}

/// Wrapper required to serialise Vector value as Erlang Term
struct Array64Wrapper(Array1<f64>);  // Used by Vector

impl<'a> Encoder for Array64Wrapper {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        // Convert ndarray::Array1 to a Vec<f64>
        let vec: Vec<f64> = self.0.to_vec();
        // Encode the Vec<f64> as an Elixir list
        vec.encode(env)
    }
}


// =============================================================================
// OPERATIONS
// =============================================================================



/// Returns a new cozo engine
#[rustler::nif(schedule = "DirtyIo", name="new_nif")]

fn new<'a>(env: Env<'a>, engine: String, path: String, options:&str) ->
    NifResult<Term<'a>> {
    // Validate engine
    let result =
        match engine.as_str() {
            "mem" | "sqlite" | "rocksdb" =>
                DbInstance::new_with_str(
                    engine.as_str(), path.as_str(), options
                ),
            _ =>
                return Err(rustler::Error::Term(
                    Box::new(atoms::invalid_engine())
                ))
        };

    let db =
        match result {
            Ok(db) => {
                db
            },
            Err(err) =>
                // TODO catch error and pritty format
                // RocksDB error: IO error: lock hold by current process,
                return Err(rustler::Error::Term(Box::new(err.to_string())))
        };

    let id = HANDLES.current.fetch_add(1, Ordering::AcqRel);
    let mut dbs = HANDLES.dbs.lock().unwrap();
    dbs.insert(id, db);

    let resource = ResourceArc::new(DbResource {
        db_id: id,
        engine: engine,
        path: path
    });
    Ok((atoms::ok().encode(env), resource.encode(env)).encode(env))
}


/// Returns the result of running script
#[rustler::nif(schedule = "DirtyIo", name="close_nif")]

fn close<'a>(env: Env<'a>, resource: ResourceArc<DbResource>) ->
    NifResult<Term<'a>> {
    let db = {
        let mut dbs = HANDLES.dbs.lock().unwrap();
        dbs.remove(&resource.db_id)
    };
    if db.is_some() {
        Ok(atoms::ok().encode(env))
    } else {
        Err(rustler::Error::Term(
            Box::new("Failed to close database".to_string())
        ))
    }
}

/// Returns the result of running script
#[rustler::nif(schedule = "DirtyIo", name="info_nif")]

fn info<'a>(env: Env<'a>, resource: ResourceArc<DbResource>) ->
    NifResult<Term<'a>> {

    let mut map = rustler::types::map::map_new(env);
    map = map.map_put(
        atoms::engine().encode(env),
        resource.engine.encode(env)
    ).unwrap();
    map = map.map_put(
        atoms::path().encode(env),
        resource.path.encode(env)
    ).unwrap();
    Ok(map)

}


/// Returns the result of running script
#[rustler::nif(schedule = "DirtyIo", name="run_script_nif")]

fn run_script<'a>(
    env: Env<'a>,
    resource: ResourceArc<DbResource>,
    script: String,
    params: String,
    mutability: Term
    ) -> NifResult<Term<'a>> {

    let db =
        match get_db(resource.db_id) {
            Some(db) => db,
            None => {
                return Err(
                    rustler::Error::Term(
                        Box::new("invalid reference".to_string())
                    )
                )
            }
        };

    let mutability =
        if atoms::true_() == mutability {
            ScriptMutability::Mutable
        } else {
            ScriptMutability::Mutable

        };

    let params_json =
        match params_to_btree(&params) {
            Ok(value) => value,
            Err(err) => return Err(rustler::Error::Term(
                Box::new(err.to_string())
            ))
        };

    let start = Instant::now();

    match db.run_script(&script, params_json, mutability) {
        Ok(named_rows) => {
            let took = start.elapsed().as_secs_f64();
            let mut map = NamedRowsWrapper(&named_rows).encode(env);
            map = map.map_put(atoms::took(), took).unwrap();
            let result = (atoms::ok(), map);
            Ok(result.encode(env))
        }
        Err(err) =>
            Err(rustler::Error::Term(Box::new(err.to_string())))
    }
}

/// Sames as
#[rustler::nif(schedule = "DirtyIo", name="run_script_json_nif")]
fn run_script_json<'a>(
    env: Env<'a>,
    resource: ResourceArc<DbResource>,
    script: String,
    params: String,
    mutability: Term
    ) -> NifResult<Term<'a>>  {

    let db =
        match get_db(resource.db_id) {
            Some(db) => db,
            None => {
                return Err(
                    rustler::Error::Term(
                        Box::new("invalid reference".to_string())
                    )
                )
            }
        };

    let mutability =
        if atoms::true_() == mutability {
            ScriptMutability::Mutable
        } else {
            ScriptMutability::Mutable

        };

    let params_json =
        match params_to_btree(&params) {
            Ok(value) => value,
            Err(err) => return Err(rustler::Error::Term(
                Box::new(err.to_string())
            ))
        };

    let result = db.run_script(
        &script,
        params_json,
        mutability
    ).unwrap();

    let json = result.into_json();

    match serde_json::to_string(&json) {
        Ok(json_str) => {
            let result = (atoms::ok().encode(env), json_str.encode(env));
            Ok(result.encode(env))
        }
        Err(_) => Err(rustler::Error::Atom("json_encode_error"))
    }
}

/// Run the CozoScript passed in folding any error into the returned JSON
#[rustler::nif(schedule = "DirtyIo", name="run_script_str_nif")]
fn run_script_str<'a>(
    env: Env<'a>,
    resource: ResourceArc<DbResource>,
    script: String,
    params: String,
    mutability: Term
    ) -> NifResult<Term<'a>>  {
    let db = match get_db(resource.db_id) {
        Some(db) => db,
        None => {
            return Err(
                rustler::Error::Term(
                    Box::new("invalid reference".to_string())
                )
            )
        }
    };

    let mutability = atoms::true_() == mutability;
    let json_str = db.run_script_str(&script, &params, mutability);
    let result = (atoms::ok().encode(env), json_str.encode(env));
    Ok(result.encode(env))
}


/// Imports relations
#[rustler::nif(schedule = "DirtyIo", name="import_relations_nif")]

fn import_relations<'a>(
    env: Env<'a>, resource: ResourceArc<DbResource>, data: String
    ) -> NifResult<Term<'a>> {

    let db =
        match get_db(resource.db_id) {
            Some(db) => db,
            None => {
                return Err(
                    rustler::Error::Term(
                        Box::new("invalid reference".to_string())
                    )
                )
            }
        };

    match db.import_relations_str_with_err(&data) {
        Ok(()) =>
            Ok(atoms::ok().encode(env)),
        Err(err) =>
            Err(rustler::Error::Term(Box::new(err.to_string())))
    }
}

// Exports relations defined by `relations`.
#[rustler::nif(schedule = "DirtyIo", name="export_relations_nif")]

fn export_relations<'a>(
    env: Env<'a>, resource: ResourceArc<DbResource>, relations: Vec<Term<'a>>
    ) -> NifResult<Term<'a>> {

    let db =
        match get_db(resource.db_id) {
            Some(db) => db,
            None => {
                return Err(
                    rustler::Error::Term(
                        Box::new("invalid reference".to_string())
                    )
                )
            }
        };

    let mut strings = Vec::new();
    for term in relations {
        let string: Result<String, _> = term.decode();
        match string {
            Ok(s) =>
                strings.push(s),
            Err(_) =>
                return Err(
                    rustler::Error::Term(
                        Box::new("invalid relations".to_string())
                    )
                )
        }
    };

    // let results = db.export_relations(strings.iter().map(|s| s as &str));
    match db.export_relations(strings.iter()) {
        Ok(btreemap) =>{
            let data = BTreeMapWrapper(btreemap).encode(env);
            Ok((atoms::ok().encode(env), data.encode(env)).encode(env))
        },
        Err(err) =>
            Err(rustler::Error::Term(Box::new(err.to_string())))
    }
}



/// Imports relations
#[rustler::nif(schedule = "DirtyIo", name="export_relations_json_nif")]

fn export_relations_json<'a>(
    env: Env<'a>, resource: ResourceArc<DbResource>, relations: Vec<Term<'a>>
    ) -> NifResult<Term<'a>> {

    let db =
        match get_db(resource.db_id) {
            Some(db) => db,
            None => {
                return Err(
                    rustler::Error::Term(
                        Box::new("invalid reference".to_string())
                    )
                )
            }
        };

    let mut strings = Vec::new();
    for term in relations {
        let string: Result<String, _> = term.decode();
        match string {
            Ok(s) =>
                strings.push(s),
            Err(_) =>
                return Err(
                    rustler::Error::Term(
                        Box::new("invalid relations".to_string())
                    )
                )
        }
    };

    // let results = db.export_relations(strings.iter().map(|s| s as &str));
    match db.export_relations(strings.iter()) {
        Ok(btreemap) =>{
            let data = BTreeMapWrapper(btreemap).encode(env);
            Ok((atoms::ok().encode(env), data.encode(env)).encode(env))
        },
        Err(err) =>
            Err(rustler::Error::Term(Box::new(err.to_string())))
    }
}



#[rustler::nif(schedule = "DirtyCpu", name = "register_callback_nif",)]

fn register_callback<'a>(
    env: Env<'a>,
    resource: ResourceArc<DbResource>,
    rel: String) -> NifResult<Term<'a>>  {

    // Get db handle and fail is invalid
    let db =
        match get_db(resource.db_id) {
            Some(db) => db,
            None => {
                return Err(
                    rustler::Error::Term(
                        Box::new("invalid reference".to_string())
                    )
                )
            }
        };

    // Register the callback in cozodb
    let (reg_id, receiver) = db.register_callback(rel.as_str(), None);

    // This is a "threaded NIF": it spawns a thread that sends a message back
    // to the calling thread later.
    let local_pid = env.pid();

    // Store receiver and PID in the global maps
    {
        let mut regs = REGISTRATIONS.lock().unwrap();
        regs.insert(reg_id, Registration{
            receiver: receiver, relname: rel.clone(), pid: local_pid
        });
    }

    // Distribute across threads in pool
    // Add a new receiver to the shared state and schedules a worker thread to
    // process messages from it. Receivers are distributed across threads in a
    // round-robin fashion using an atomic counter (next_worker).
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


fn worker_thread(
    registrations: Registrations, worker_count: usize, worker_index: usize) -> bool {
        loop {
            let registrations = registrations.lock().unwrap();

            // Filter receivers for this worker thread
            for (&reg_id, registration) in registrations.iter() {
                let rel = &registration.relname;
                if should_handle(&rel, worker_count, worker_index) { {
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
    pid: Pid) {
    let _ = OwnedEnv::new().send_and_clear(&pid, |env| {
        let result: NifResult<Term> = (|| {
            let reg_id = reg_id.encode(env);
            let rel = rel.encode(env);
            let op = match op {
                CallbackOp::Put => atoms::updated(),
                CallbackOp::Rm => atoms::removed()
            };
            let event_name = vec![
                atoms::cozodb(),
                atoms::relation(),
                op
            ];
            let new_rows = NamedRowsWrapper(&new_rows).encode(env);
            let old_rows = NamedRowsWrapper(&old_rows).encode(env);
            let event = (
                event_name,
                reg_id,
                rel,
                new_rows,
                old_rows
            );
            Ok(event.encode(env))
        })();

        match result {
            Ok(term) => term,
            Err(_err) => env.error_tuple("failed".encode(env))
        }
    });
}



// =============================================================================
// UTILS
// =============================================================================



fn params_to_btree(params: &String) ->
    Result<BTreeMap<String, DataValue>, &'static str> {
    if params.is_empty() {
        Ok(BTreeMap::new()) // Wrap in Ok
    } else {
        match serde_json::from_str::<BTreeMap<String, DataValue>>(params) {
            Ok(map) => Ok(
                map.into_iter()
                    .map(|(k, v)| (k, DataValue::from(v)))
                    .collect()
            ),
            Err(_) => Err("params argument is not a JSON map")
        }
    }
}

fn get_db(db_id: i32) -> Option<DbInstance> {
    let dbs = HANDLES.dbs.lock().unwrap();
    dbs.get(&db_id).cloned()
}


fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    t.hash(&mut hasher);
    hasher.finish()
}