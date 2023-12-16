// =============================================================================
// cozodb.erl -
//
// Copyright (c) 2023 Leapsight Holdings Limited. All rights reserved.
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

// Rust std libs
use core::hash::Hash;
use std::collections::BTreeMap;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::Hasher;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Mutex;
use std::time::Instant;

// Rustler
use rustler::Encoder;
use rustler::Env;
use rustler::NifResult;
use rustler::OwnedEnv;
use rustler::NifMap;
// use rustler::ResourceArc;
use rustler::Term;
use rustler::types::Pid;

// Used for global state
use lazy_static::lazy_static;

// Used for CALLBACKS feature
use once_cell::sync::Lazy;
use threadpool::ThreadPool;
use crossbeam::channel::*;

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
      import_from_backup,
      export_relations,
      export_relations_json,
      backup,
      restore,
      register_callback,
      unregister_callback
    ],
    load = on_load
);


/// Define NIF Resources using rustler::resource! macro
fn on_load(_env: Env, _: Term) -> bool {
    // rustler::resource!(DbHandle, env);
    true
}


// =============================================================================
// STRUCTS REQUIRED FOR NIF
// =============================================================================


/// Struct used to globally manage database handles
/// This is combined with lazy_static! macro to create a static variable
/// containing this struct.
/// We do this so that DbHandle contains the Id as opposed to the DBinstance
/// handle, required so that we can close a database from Erlang.
///
/// The most common alternative would have been to return the wrapped
/// DBInstance as a NifResource, but unless we control all Erlang processes
/// with a copy of the reference, the Rust DbInstance cannot be destroyed. As
/// long as any process in Erlang has the reference, Rust will keep the
/// DBInstance alive.
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
// This macro lazily initializes the variable on its first access.
// We use it because Rust's standard library doesn't support static
// variables with non-constant initializers directly.
lazy_static! {

    // Required so that we can close a database
    static ref HANDLES: Handles =
        Handles {
            // sets current to 0
            current: Default::default(),
            // empty BTreeMap guarded by mutex
            dbs: Mutex::new(Default::default())
        };

    // Required for Callback feature.
    // We use THREAD_POOL to shard the callback handlers based on relation name.
    // This means each thread will be responsible for handling the events
    // published on each callback registration channel. This is a more robust
    // alternative than spawning thread for each subscription that is used in
    // other binding libraries, where there is no limit to the number of
    // subscriptions. This obvisoulsy comes at the cost of extra
    // synchronisation.
    static ref THREAD_POOL: Lazy<ThreadPool> =
        Lazy::new(|| {
            ThreadPool::new(*NUM_THREADS)
        });

    // Required for Callback feature.
    // See THREAD_POOL comments.
    static ref NUM_THREADS: usize = {
        // Attempt to get the value from an environment variable
        if let Ok(val) = std::env::var("COZODB_CALLBACK_THREADPOOL_SIZE") {
            let mut num = val.parse::<usize>().unwrap_or_default();
            if num == 0 {
                // unwrap_or_default() will return 0 if invalid.
                // If 0 we default to the number of cores in the host.
                num = num_cpus::get();
            };
            num
        }
        // Uncomment if using a config file
        // else if let Ok(mut settings) = Config::builder()
        //     .add_source(File::with_name("Config"))
        //     .build()
        // {
        //     settings.get::<usize>("config_key").unwrap_or_default()
        // }
        else {
            // Default value if neither is available
            num_cpus::get()
        }
    };

    // Required for Callback feature.
    // We use REGISTRATIONS to keep track of the metadata associated with it,
    // including the relation name, channel and the caller's LocalPid which we
    // need in order to match events and send the event to the Erlang caller.
    static ref REGISTRATIONS: Lazy<Registrations> =
        Lazy::new(|| {
            Arc::new(Mutex::new(HashMap::new()))
        });

}

/// A NIF Resource representing the identifier for a DbInstance handle.
/// We use HANDLES to associate identifiers with Cozo's DbInstance Handles
/// so that we can implement close().
#[derive(NifMap)]
struct DbHandle {
    db_id: i32,
    engine: String,
    path: String
}

/// Wrapper required to serialise Cozo's NamedRows value as Erlang map
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
            None => atoms::null().encode(env)
        };

        // Create and return an Erlang map with atom keys headers, rows and next
        let mut map = rustler::types::map::map_new(env);
        map = map.map_put(atoms::headers(), headers).unwrap();
        map = map.map_put(atoms::rows(), rows).unwrap();
        map.map_put(atoms::next(), next).unwrap()
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
        match &self.0 {
            DataValue::Null => atoms::null().encode(env),
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
                        "Failed to serialize JsonValue".encode(env),
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
                // Just in case we do get them, we return the atom 'null'
                atoms::null().encode(env)
        }
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
    }}

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
struct Array32Wrapper(Array1<f32>);  // Used by Vector

impl<'a> Encoder for Array32Wrapper {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        // Convert ndarray::Array1 to a Vec<f32>
        let vec: Vec<f32> = self.0.to_vec();
        // Encode the Vec<f32> as an Elixir list
        vec.encode(env)
    }
}

/// Wrapper required to serialise Cozo's Array1<f64> value as Erlang Term
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



/// Opens/creates a database returning an Erlang NIF Resource (reference).
#[rustler::nif(schedule = "DirtyIo", name="new_nif")]

fn new<'a>(env: Env<'a>, engine: String, path: String, options:&str) ->
    NifResult<Term<'a>> {
    // Validate engine name and obtain DBInstance
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

    // Validate we have a DBInstance and return error if not
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

    // Store the DBInstance handle in a global hashmap using id as key
    let id = HANDLES.current.fetch_add(1, Ordering::AcqRel);
    let mut dbs = HANDLES.dbs.lock().unwrap();
    dbs.insert(id, db);

    // Finally create and return a Nif Resource with the id and metadata
    let resource = DbHandle {
        db_id: id,
        engine: engine,
        path: path
    };
    Ok((atoms::ok().encode(env), resource).encode(env))
}


/// Returns the result of running a script
#[rustler::nif(schedule = "DirtyIo", name="close_nif")]

fn close<'a>(env: Env<'a>, resource: Term<'a>) ->
    NifResult<Term<'a>> {

    let resource: DbHandle = resource.decode()?;

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

/// Returns the metadata associated with the resource
#[rustler::nif(schedule = "DirtyIo", name="info_nif")]

fn info<'a>(env: Env<'a>, resource: Term<'a>) ->
    NifResult<Term<'a>> {
    let resource: DbHandle = resource.decode()?;

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
    resource: Term<'a>,
    script: String,
    params: String,
    read_only: Term
    ) -> NifResult<Term<'a>> {

    let resource: DbHandle = resource.decode()?;

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

    let read_only =
        if atoms::true_() == read_only {
            ScriptMutability::Immutable
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

    match db.run_script(&script, params_json, read_only) {
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

/// Sames as run_script but encodes as JSON
#[rustler::nif(schedule = "DirtyIo", name="run_script_json_nif")]
fn run_script_json<'a>(
    env: Env<'a>,
    resource: Term<'a>,
    script: String,
    params: String,
    read_only: Term
    ) -> NifResult<Term<'a>>  {

    let resource: DbHandle = resource.decode()?;

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

    let read_only =
        if atoms::true_() == read_only {
            ScriptMutability::Immutable
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
        read_only
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
    resource: Term<'a>,
    script: String,
    params: String,
    read_only: Term
    ) -> NifResult<Term<'a>>  {

    let resource: DbHandle = resource.decode()?;

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

    let read_only = atoms::true_() == read_only;
    let json_str = db.run_script_str(&script, &params, read_only);
    let result = (atoms::ok().encode(env), json_str.encode(env));
    Ok(result.encode(env))
}


/// Imports relations
#[rustler::nif(schedule = "DirtyIo", name="import_relations_nif")]

fn import_relations<'a>(
    env: Env<'a>, resource: Term<'a>, data: String
    ) -> NifResult<Term<'a>> {

    let resource: DbHandle = resource.decode()?;

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
    env: Env<'a>, resource: Term<'a>, relations: Vec<String>
    ) -> NifResult<Term<'a>> {

    let resource: DbHandle = resource.decode()?;

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

    match db.export_relations(relations.iter().map(|s| s as &str)) {
        Ok(btreemap) =>{
            let mut data = rustler::types::map::map_new(env);
            for (key, value) in btreemap {
                let key_term = key.encode(env);
                let value_term = NamedRowsWrapper(&value).encode(env);
                data = data.map_put(key_term, value_term).unwrap();
            }
            Ok((atoms::ok().encode(env), data.encode(env)).encode(env))
        },
        Err(err) =>
            Err(rustler::Error::Term(Box::new(err.to_string())))
    }

}

/// Export relations as JSON
#[rustler::nif(schedule = "DirtyIo", name="export_relations_json_nif")]

fn export_relations_json<'a>(
    env: Env<'a>, resource: Term<'a>, relations: Vec<String>
    ) -> NifResult<Term<'a>> {

    let resource: DbHandle = resource.decode()?;

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

    match db.export_relations(relations.iter().map(|s| s as &str)) {
        Ok(btreemap) =>{
            let data: Vec<_> = btreemap
                .into_iter()
                .map(|(k, v)| (k, v.into_json()))
                .collect();
            let json = json!(data).to_string();
            Ok((atoms::ok().encode(env), json.encode(env)).encode(env))
        },
        Err(err) =>
            Err(rustler::Error::Term(Box::new(err.to_string())))
    }
}



/// Backs up the database at path
#[rustler::nif(schedule = "DirtyIo", name="backup_nif")]

fn backup<'a>(env: Env<'a>, resource: Term<'a>, path: String
    ) -> NifResult<Term<'a>> {

    let resource: DbHandle = resource.decode()?;

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

    match db.backup_db(path) {
        Ok(()) => {
            Ok(atoms::ok().encode(env))
        }
        Err(err) =>
            Err(rustler::Error::Term(Box::new(err.to_string())))
    }
}

/// Backs up the database at path
#[rustler::nif(schedule = "DirtyIo", name="restore_nif")]

fn restore<'a>(env: Env<'a>, resource: Term<'a>, path: String
    ) -> NifResult<Term<'a>> {

    let resource: DbHandle = resource.decode()?;

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

    match db.restore_backup(path) {
        Ok(()) => {
            Ok(atoms::ok().encode(env))
        }
        Err(err) =>
            Err(rustler::Error::Term(Box::new(err.to_string())))
    }
}

/// Backs up the database at path
#[rustler::nif(schedule = "DirtyIo", name="import_from_backup_nif")]

fn import_from_backup<'a>(
    env: Env<'a>,
    resource: Term<'a>,
    path: String,
    relations: Vec<String>
    ) -> NifResult<Term<'a>> {

    let resource: DbHandle = resource.decode()?;

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

    match db.import_from_backup(path, &relations) {
        Ok(()) => {
            Ok(atoms::ok().encode(env))
        }
        Err(err) =>
            Err(rustler::Error::Term(Box::new(err.to_string())))
    }
}


#[rustler::nif(schedule = "DirtyCpu", name = "register_callback_nif",)]

fn register_callback<'a>(
    env: Env<'a>,
    resource: Term<'a>,
    rel: String) -> NifResult<Term<'a>>  {

    let resource: DbHandle = resource.decode()?;

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


#[rustler::nif(schedule = "DirtyCpu", name = "unregister_callback_nif",)]

fn unregister_callback<'a>(
    env: Env<'a>,
    resource: Term<'a>,
    reg_id: u32) -> NifResult<Term<'a>>  {

    let resource: DbHandle = resource.decode()?;

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

    let result: bool = db.unregister_callback(reg_id);

    // We remove even if result == false
    {
        let mut regs = REGISTRATIONS.lock().unwrap();
        regs.remove(&reg_id);
    }

    Ok(result.encode(env))
}




// =============================================================================
// UTILS
// =============================================================================



fn get_db(db_id: i32) -> Option<DbInstance> {
    let dbs = HANDLES.dbs.lock().unwrap();
    dbs.get(&db_id).cloned()
}


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


fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    t.hash(&mut hasher);
    hasher.finish()
}