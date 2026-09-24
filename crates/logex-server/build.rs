fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(false)
        .compile_protos(&["proto/logex.proto"], &["proto"])?;

    // Preserve the generated wire messages/client. The server owns query
    // messages together with their memory reservations until encoding completes.
    let mut service = tonic_build::manual::Service::builder()
        .name("LogExService")
        .package("logex");
    for (name, route, input, output, streaming) in [
        ("query", "Query", "QueryRequest", "QueryResponse", false),
        (
            "get_head_block",
            "GetHeadBlock",
            "Empty",
            "HeadBlockResponse",
            false,
        ),
        (
            "get_logs",
            "GetLogs",
            "GetLogsRequest",
            "GetLogsResponse",
            false,
        ),
        (
            "stream_logs",
            "StreamLogs",
            "GetLogsRequest",
            "LogEntry",
            true,
        ),
    ] {
        let metadata = route == "GetHeadBlock";
        let output = if metadata {
            format!("super::{output}")
        } else {
            format!("crate::grpc::protocol::OwnedResponse<super::{output}>")
        };
        let mut method = tonic_build::manual::Method::builder()
            .name(name)
            .route_name(route)
            .input_type(format!("super::{input}"))
            .output_type(output)
            .codec_path(if metadata {
                "tonic::codec::ProstCodec"
            } else {
                "crate::grpc::protocol::OwnedProstCodec"
            });
        if streaming {
            method = method.server_streaming();
        }
        service = service.method(method.build());
    }
    tonic_build::manual::Builder::new()
        .build_client(false)
        .compile(&[service.build()]);
    Ok(())
}
