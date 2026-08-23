fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .server_mod_attribute(
            "logex",
            "#[allow(clippy::double_must_use, clippy::mixed_attributes_style)]",
        )
        .compile_protos(&["proto/logex.proto"], &["proto"])?;
    Ok(())
}
