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

use rustler::NifResult;
use rustler::Env;
use rustler::Error;
use rustler::Encoder;
use rustler::ResourceArc;
use rustler::Term;
use cozo::*;
use ndarray::Array1; // used by cozo



// =============================================================================
// RUSTLER SETUP
// =============================================================================


// We define atoms in Rustler
mod atoms {
    rustler::atoms! {
        ok,
        undefined,
        nil,
        error,
        mem,
        sqlite,
        rocksdb,
        invalid_engine
    }
}


// INIT
rustler::init!("cozodb",
    [
      new,
      run_script,
      run_script_json
    ],
    load = on_load
);


fn on_load(env: Env, _: Term) -> bool {
    rustler::resource!(DbResource, env);
    true
}



// =============================================================================
// STRUCTS
// =============================================================================


struct DbResource {
    pub db: DbInstance
}

/// DataValueWrapper wrapps the DataValue struct defined in Cozo as:
/// ```
/// pub enum DataValue {
///     /// null
///     Null,
///     /// boolean
///     Bool(bool),
///     /// number, may be int or float
///     Num(Num),
///     /// string
///     Str(SmartString<LazyCompact>),
///     /// bytes
///     #[serde(with = "serde_bytes")]
///     Bytes(Vec<u8>),
///     /// UUID
///     Uuid(UuidWrapper),
///     /// Regex, used internally only  => we don't need to encode
///     Regex(RegexWrapper),
///     /// list
///     List(Vec<DataValue>),
///     /// set, used internally only => we don't need to encode
///     Set(BTreeSet<DataValue>),
///     /// Array, mainly for proximity search
///     Vec(Vector),
///     /// Json
///     Json(JsonData),
///     /// validity,
///     Validity(Validity),
///     /// bottom type, used internally only => we don't need to encode
///     Bot,
/// }
/// ```
struct DataValueWrapper(DataValue);

impl<'a> Encoder for DataValueWrapper {
    fn encode<'b>(&self, env: Env<'b>) -> Term<'b> {
        match &self.0 {
            DataValue::Null => atoms::nil().encode(env),
            DataValue::Bool(i) => i.encode(env),
            // number, may be int or float
            DataValue::Num(i) => NumWrapper(i.clone()).encode(env),
            DataValue::Str(i) => i.encode(env),
            DataValue::Bytes(i) => i.encode(env),
            DataValue::Uuid(w) => w.0.hyphenated().to_string().encode(env),
            DataValue::List(i) => {
                let encoded_values: Vec<Term<'b>> = i
                    .iter()                    .map(|val| DataValueWrapper(val.clone()).encode(env))
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
    }
}

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
        // Encode the Vec<f64> as an Elixir list        vec.encode(env)
    }
}


// =============================================================================
// OPERATIONS
// =============================================================================



/// Returns a new cozo engine
#[rustler::nif(schedule = "DirtyIo")]
fn new<'a>(env: Env<'a>, engine: Term, path: String, _options:&str) ->
    Result<Term<'a>, Error> {
    let engine_str = engine.atom_to_string().unwrap();
    let db = DbInstance::new(
        &engine_str,
        path.to_string(),
        Default::default()).unwrap();
    let resource = ResourceArc::new(DbResource {db: db});
    Ok((atoms::ok().encode(env), resource.encode(env)).encode(env))
}


/// Returns the result of running script
#[rustler::nif(schedule = "DirtyIo")]
fn run_script<'a>(
    env: Env<'a>, resource: ResourceArc<DbResource>, script: String
    ) -> NifResult<Term<'a>> {

    let named_rows = resource.db.run_script(
        &script,
        Default::default(),
        ScriptMutability::Immutable
    ).unwrap();

    // let nxt = match named_rows.next {
    //         None => atoms::nil(),
    //         Some(more) => more.into_json(),
    //     };
    // let rows = self
    //     .rows
    //     .into_iter()
    //     .map(|row| row.into_iter().map(JsonValue::from).collect::<JsonValue>())
    //     .collect::<JsonValue>();

    let wrapped_data: Vec<Vec<DataValueWrapper>> =
        named_rows.rows
            .into_iter()
            .map(|inner_vec|
                inner_vec.into_iter().map(DataValueWrapper).collect())
            .collect();

    let result = (
        atoms::ok().encode(env),
        named_rows.headers,
        wrapped_data
    );
    Ok(result.encode(env))

}

#[rustler::nif(schedule = "DirtyIo")]
fn run_script_json<'a>(
    env: Env<'a>, resource: ResourceArc<DbResource>, script: String
    ) -> NifResult<Term<'a>>  {
    let result = resource.db.run_script(
        &script,
        Default::default(),
        ScriptMutability::Immutable
    ).unwrap();

    let json = result.into_json();

    match serde_json::to_string(&json) {
        Ok(json_str) => Ok(json_str.encode(env)),
        Err(_) => Err(rustler::Error::Atom("json_encode_error"))
    }

}




// =============================================================================
// UTILS
// =============================================================================





