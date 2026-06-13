fn main() {
    std::env::set_var("PROTOC", protobuf_src::protoc());
    tonic_build::configure()
        .compile_protos(&["proto/shuffle.proto"], &["proto"])
        .unwrap();
}
