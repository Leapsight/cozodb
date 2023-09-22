// use std::collections::BTreeMap;
use serde_rustler::to_term;
use rustler::NifResult;
use rustler::Env;
use rustler::Error;
// use rustler::ErlOption;
use rustler::NifMap;
use rustler::NifTuple;
use rustler::NifUnitEnum;
use rustler::NifTaggedEnum;
use rustler::NifUntaggedEnum;
use rustler::ResourceArc;
use rustler::Term;
use cozo::*;


mod atoms {
    rustler::atoms! {
        ok,
        undefined,
        error,
        mem,
        sqlite,
        rocksdb,
        sled,
        tikv,
        result_set
    }
}


struct DbResource {
    pub db: DbInstance
}



// API

fn load(env: Env, _: Term) -> bool {
    rustler::resource!(DbResource, env);
    true
}


#[rustler::nif(schedule = "DirtyCpu")]
fn new(engine: Term, path: String, _options:&str) ->
    Result<ResourceArc<DbResource>, Error> {
    let engine_str = engine.atom_to_string().unwrap();
            let db = DbInstance::new(
        &engine_str,
        path.to_string(),
        Default::default()).unwrap();
    let resource = ResourceArc::new(DbResource {
        db: db
    });
    Ok(resource)
}


// #[rustler::nif(schedule = "DirtyCpu")]
// fn run_script(env: Env, resource: ResourceArc<DbResource>, script: String) -> NifResult<Term<>>
//  {
//     let result = resource.db.run_script(
//         &script,
//         Default::default(),
//         ScriptMutability::Immutable
//     ).unwrap();
//     to_term(env, ResultSet::new(result)).map_err(|err| err.into())

// }

#[rustler::nif(schedule = "DirtyCpu")]
fn run_script_json(env: Env, resource: ResourceArc<DbResource>, script: String) -> NifResult<Term<>>{
    let result = resource.db.run_script(
        &script,
        Default::default(),
        ScriptMutability::Immutable
    ).unwrap();
    to_term(env, result.into_json()).map_err(|err| err.into())

}

#[rustler::nif]
fn debug_run_script(resource: ResourceArc<DbResource>, script: String)
 {
    let result = resource.db.run_script(
        &script,
        Default::default(),
        ScriptMutability::Immutable
    ).unwrap();
    println!("{:?}", result);

}

// SUPPORT



// INIT

rustler::init!("ecozodb",
    [
      new
    // , run_script
    , run_script_json
    , debug_run_script
    // Demo
    , add_nif
    , my_map_nif
    , my_list
    , my_maps
    , my_tuple
    , unit_enum_echo
    , tagged_enum_echo
    , untagged_enum_echo
    , my_string
    ],
    load = load
);


// ========================================================================
// RUSTLER TESTS
// See Derive Macros docs at https://docs.rs/rustler/0.26.0/rustler/index.html#derives

#[derive(NifMap)]
struct MyMap {
    lhs: i32,
    rhs: i32,
}

#[derive(NifTuple)]
struct MyTuple {
    lhs: i32,
    rhs: i32
}

#[derive(NifUnitEnum)]
enum UnitEnum {
    FooBar,
    Baz,
}

#[derive(NifTaggedEnum)]
enum TaggedEnum {
    Foo,
    Bar(String),
    Baz{ a: i32, b: i32 },
}

#[derive(NifUntaggedEnum)]
enum UntaggedEnum {
    Foo(u32),
    Bar(String),
}

#[rustler::nif(name = "add")]
fn add_nif(a: i64, b: i64) -> i64 {
    add(a, b)
}

fn add(a: i64, b: i64) -> i64 {
    a + b
}

#[rustler::nif(name = "my_map")]
fn my_map_nif() -> MyMap {
    my_map()
}

#[rustler::nif]
fn my_maps() -> Vec<MyMap> {
    vec![ my_map(), my_map()]
}

fn my_map() -> MyMap {
    MyMap { lhs: 33, rhs: 21 }
}

#[rustler::nif]
fn my_string(val: String) -> String {
    val
}

#[rustler::nif]
fn my_tuple() -> MyTuple {
    MyTuple { lhs: 33, rhs: 21 }
}

#[rustler::nif]
fn my_list() -> Vec<f64> {
    vec![1.0, 3.0]
}

#[rustler::nif]
fn unit_enum_echo(unit_enum: UnitEnum) -> UnitEnum {
    unit_enum
}

#[rustler::nif]
fn tagged_enum_echo(tagged_enum: TaggedEnum) -> TaggedEnum {
    tagged_enum
}

#[rustler::nif]
fn untagged_enum_echo(untagged_enum: UntaggedEnum) -> UntaggedEnum {
    untagged_enum
}



#[cfg(test)]
mod tests {
    use crate::add;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}