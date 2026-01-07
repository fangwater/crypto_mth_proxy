fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=period.proto");
    prost_build::compile_protos(&["period.proto"], &["."])?;
    Ok(())
}
