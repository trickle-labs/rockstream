#![no_main]
use bytes::{Bytes, BytesMut};
use libfuzzer_sys::fuzz_target;
use pgwire::messages::{
    extendedquery::{Bind, Execute, Parse},
    simplequery::Query,
    startup::Startup,
    Message, PgWireFrontendMessage,
};

fuzz_target!(|data: &[u8]| {
    let _ = Startup::decode(&mut BytesMut::from(data));
    let _ = PgWireFrontendMessage::decode(&mut BytesMut::from(data));

    let Some(kind) = data.first() else { return };
    let text: String = data
        .iter()
        .skip(1)
        .take(32)
        .map(|byte| char::from(byte % 95 + 32))
        .collect();
    let mut frame = BytesMut::new();
    let expected = if kind % 5 == 0 {
        let mut message = Startup::new();
        message.parameters.insert("user".into(), text);
        message.encode(&mut frame).unwrap();
        format!("{message:?}")
    } else {
        let message = match kind % 5 {
            1 => PgWireFrontendMessage::Query(Query::new(text)),
            2 => PgWireFrontendMessage::Parse(Parse::new(Some("stmt".into()), text, vec![23])),
            3 => PgWireFrontendMessage::Bind(Bind::new(
                Some("portal".into()),
                Some("stmt".into()),
                vec![0],
                vec![
                    Some(Bytes::copy_from_slice(&data[1..data.len().min(17)])),
                    None,
                ],
                vec![0],
            )),
            _ => PgWireFrontendMessage::Execute(Execute::new(
                Some("portal".into()),
                i32::from(*data.get(1).unwrap_or(&0)),
            )),
        };
        message.encode(&mut frame).unwrap();
        format!("{message:?}")
    };
    let split = usize::from(*data.get(2).unwrap_or(&0)) % frame.len();
    let mut input = BytesMut::from(&frame[..split]);
    let before = input.clone();
    if kind % 5 == 0 {
        assert!(Startup::decode(&mut input).unwrap().is_none());
    } else {
        assert!(PgWireFrontendMessage::decode(&mut input).unwrap().is_none());
    }
    assert_eq!(input, before);
    input.extend_from_slice(&frame[split..]);
    let actual = if kind % 5 == 0 {
        format!("{:?}", Startup::decode(&mut input).unwrap().unwrap())
    } else {
        format!(
            "{:?}",
            PgWireFrontendMessage::decode(&mut input).unwrap().unwrap()
        )
    };
    assert_eq!(actual, expected);
    assert!(input.is_empty());
});
