use protobuf_codegen::Codegen;

fn main() {
    Codegen::new()
        .pure()
        .cargo_out_dir("generated")
        .include("src/pipeline/lookup/feathr_online_store/protos")
        .input("src/pipeline/lookup/feathr_online_store/protos/feathr.proto")
        .run_from_script();
}
