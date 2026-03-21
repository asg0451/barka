fn main() {
    capnpc::CompilerCommand::new()
        .src_prefix("schema")
        .file("schema/barka.capnp")
        .default_parent_module(vec!["rpc".into()])
        .run()
        .expect("capnp schema compilation");
}
