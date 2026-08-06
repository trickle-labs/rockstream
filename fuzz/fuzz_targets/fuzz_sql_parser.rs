#![no_main]
use libfuzzer_sys::fuzz_target;
use rockstream_sql::frontend::SqlFrontend;

fuzz_target!(|data: &[u8]| {
    if let Ok(s) = std::str::from_utf8(data) {
        let frontend = SqlFrontend::new();
        let _ = frontend.parse_ddl(s);
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build();
        if let Ok(rt) = rt {
            rt.block_on(async {
                let _ = frontend.sql_to_plan_node(s).await;
            });
        }
    }
});
