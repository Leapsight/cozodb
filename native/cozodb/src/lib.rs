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

use std::collections::BTreeMap;
use std::time::Instant;
use cozo::*;
use ndarray::Array1; // used by cozo

use rustler::Encoder;
use rustler::Env;
use rustler::NifResult;
use rustler::ResourceArc;
use rustler::Term;

use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::Mutex;
use lazy_static::lazy_static;


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
        cozo_named_rows,
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
      run_script,
      run_script_str,
      run_script_json
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

// Static instance of Handles
// Static variables are allocated for the duration of a program's run and are
// not specific to any thread.
// lazy_static is used because Rust's standard library doesn't support static
// variables with non-constant initializers directly. It lazily initializes the
// variable on its first access.
lazy_static! {
    static ref HANDLES: Handles = Handles {
        current: Default::default(), // sets current to 0
        dbs: Mutex::new(Default::default()) // empty BTreeMap guarded by mutex
    };
}

/// A NIF Resource representing the identifier for a DbInstance handle
struct DbResource {
    pub db_id: i32
}

/// Wrapper required to serialise NamedRows value as Erlang Term
struct NamedRowsWrapper<'a>(&'a NamedRows, f64);

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
                NamedRowsWrapper(more, 0.0).encode(env)
            },
            None => atoms::undefined().encode(env)
        };

        let took = self.1;

        // Create a cozo_named_rows record in Erlang
        (atoms::cozo_named_rows(), headers, rows, next, took).encode(env)
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
                        json_str.encode(env),
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
#[rustler::nif(schedule = "DirtyIo")]

fn new<'a>(env: Env<'a>, engine: String, path: String, options:&str) ->
    NifResult<Term<'a>> {
    // Validate engine
    let engine = match engine.as_str() {
        "mem" | "sqlite" | "rocksdb" =>
            engine.as_str(),
        _ =>
            return Err(rustler::Error::Term(Box::new(atoms::invalid_engine())))
    };

    let result = DbInstance::new_with_str(engine, path.as_str(), options);

    let db = match result {
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

    let resource = ResourceArc::new(DbResource {db_id: id});
    Ok((atoms::ok().encode(env), resource.encode(env)).encode(env))
}


/// Returns the result of running script
#[rustler::nif(schedule = "DirtyIo")]

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
#[rustler::nif(schedule = "DirtyIo")]

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
            let record = NamedRowsWrapper(&named_rows, took).encode(env);

            let result = (atoms::ok().encode(env), record);
            Ok(result.encode(env))
        }
        Err(err) =>
            Err(rustler::Error::Term(Box::new(err.to_string())))
    }
}

/// Sames as
#[rustler::nif(schedule = "DirtyIo")]
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
#[rustler::nif(schedule = "DirtyIo")]
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
