use super::*;

#[test]
fn externally_supplied_limits_cannot_overflow_runtime_budgets() {
    Limits::default().validate().unwrap();
    for invalid in [
        Limits {
            queue_messages: usize::MAX,
            ..Limits::default()
        },
        Limits {
            total_queue_bytes: usize::MAX,
            ..Limits::default()
        },
        Limits {
            callback_ms: u64::MAX,
            ..Limits::default()
        },
        Limits {
            recovery_ms: u64::MAX,
            ..Limits::default()
        },
        Limits {
            effects: 0,
            ..Limits::default()
        },
    ] {
        assert_eq!(
            invalid.validate().unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }
}

#[test]
fn socket_messages_preserve_text_binary_and_size_boundaries() {
    let limits = Limits {
        frame_bytes: 3,
        ..Limits::default()
    };
    limits
        .frame(&pb::SocketFrame {
            data: vec![255; 3],
            text: false,
        })
        .unwrap();
    assert_eq!(
        limits
            .frame(&pb::SocketFrame {
                data: vec![255],
                text: true
            })
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        limits
            .frame(&pb::SocketFrame {
                data: vec![0; 4],
                text: false
            })
            .unwrap_err()
            .code(),
        tonic::Code::ResourceExhausted
    );
    close(&pb::SocketClose {
        code: 1000,
        reason: "done".into(),
    })
    .unwrap();
    assert!(
        close(&pb::SocketClose {
            code: 1006,
            reason: String::new()
        })
        .is_err()
    );
}
