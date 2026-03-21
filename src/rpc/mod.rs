pub mod client;
pub mod server;

pub mod barka_capnp {
    include!(concat!(env!("OUT_DIR"), "/barka_capnp.rs"));
}
