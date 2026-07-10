fn main() {
    let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc is available");
    std::env::set_var("PROTOC", protoc);
    tonic_build::configure()
        .compile_protos(&["proto/shuffle.proto"], &["proto"])
        .unwrap();
}
