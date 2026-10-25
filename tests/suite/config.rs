use std::time::Duration;

use rekka::{EventSource, JobHandler, Queues, RekkaError, RekkaQueue, RekkaData};
use serde::{Deserialize, Serialize};

use crate::common::TestEnv;

#[derive(Serialize, Deserialize, RekkaQueue)]
#[rekka(name = "cfg")]
struct CfgJob {
    task_id: String,
}

struct NotRegistered;

async fn needs_data(_h: JobHandler, _job: CfgJob, _state: RekkaData<NotRegistered>) {}

async fn noop(_h: JobHandler, _job: CfgJob) {}

async fn task_needs_data(_state: RekkaData<NotRegistered>) {}

/// Mantém 32 KiB vivos através de um `.await`: passa do `max_future_size` padrão (16 KiB).
async fn heavy(_h: JobHandler, _job: CfgJob) {
    let buf = [0u8; 32 * 1024];
    tokio::task::yield_now().await;
    std::hint::black_box(&buf);
}

async fn task_heavy() {
    let buf = [0u8; 32 * 1024];
    tokio::task::yield_now().await;
    std::hint::black_box(&buf);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_data_fails_at_start() {
    let env = TestEnv::new();
    let res = env.builder().register::<CfgJob>(|w| w.build_fn(needs_data)).start().await;
    assert!(matches!(res, Err(RekkaError::MissingData(name)) if name.contains("NotRegistered")));

    let res = env.builder().register_recursive("t", |w| w.build_fn(task_needs_data)).start().await;
    assert!(matches!(res, Err(RekkaError::MissingData(_))));

    let res = env.builder().with_reconcile(task_needs_data, Duration::from_secs(1)).start().await;
    assert!(matches!(res, Err(RekkaError::MissingData(_))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn invalid_configuration_is_rejected() {
    let env = TestEnv::new();
    let invalid = |res: rekka::Result<Queues>| matches!(res, Err(RekkaError::InvalidConfig(_)));

    let dup = env.builder().register::<CfgJob>(|w| w.build_fn(noop)).register::<CfgJob>(|w| w.build_fn(noop));
    assert!(invalid(dup.start().await), "duplicate queue");

    assert!(invalid(env.builder().register::<CfgJob>(|w| w.concurrency(0).build_fn(noop)).start().await));
    assert!(invalid(env.builder().register::<CfgJob>(|w| w.max_attempts(0).build_fn(noop)).start().await));
    assert!(invalid(env.builder().register::<CfgJob>(|w| w.lease(Duration::from_millis(1)).build_fn(noop)).start().await));
    assert!(invalid(env.builder().register_recursive("bad{name}", |w| w.build_fn(|| async {})).start().await));
    assert!(invalid(
        env.builder()
            .register_recursive("twice", |w| w.build_fn(|| async {}))
            .register_recursive("twice", |w| w.build_fn(|| async {}))
            .start()
            .await
    ));
    assert!(invalid(env.builder().prefix("bad{prefix}").start().await));

    for expr in ["* * *", "*/0 * * * *", "61 * * * * *", "* * * * mon-xyz", "0 0 30 2 *"] {
        let res = env.builder().register_recursive("cron", move |w| w.cron_time(expr).build_fn(|| async {})).start().await;
        assert!(invalid(res), "cron_time {expr:?} should be rejected");
    }
    let ok = env.builder().register_recursive("cron", |w| w.cron_time("*/2 * * * *").build_fn(|| async {})).start().await;
    ok.unwrap().shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unreachable_redis_fails_at_start() {
    let res = tokio::time::timeout(
        Duration::from_secs(60),
        Queues::new("redis://127.0.0.1:1").connection_timeout(Duration::from_millis(200)).disable_buffer().start(),
    )
    .await
    .expect("start must give up");
    assert!(matches!(res, Err(RekkaError::Redis(_))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn handler_future_sizes_are_reported() {
    let env = TestEnv::new();
    let queues = env
        .builder()
        .register::<CfgJob>(|w| w.build_fn(heavy))
        .register_recursive("light", |w| w.build_fn(|| async {}))
        .register_recursive("big", |w| w.max_future_size(64 * 1024).build_fn(task_heavy))
        .with_reconcile(task_heavy, Duration::from_secs(3600))
        .start()
        .await
        .expect("an oversized handler only warns by default");

    // O janitor (tarefa interna) não aparece.
    let sizes = queues.future_sizes();
    let by_name = |name: &str| *sizes.iter().find(|s| s.name() == name).unwrap_or_else(|| panic!("`{name}` reported"));
    assert_eq!(sizes.len(), 4, "{sizes:?}");

    let queue = by_name("cfg");
    assert_eq!(queue.source(), EventSource::Queue);
    assert!(queue.size() >= 32 * 1024, "{queue:?}");
    assert_eq!(queue.limit(), 16 * 1024);
    assert!(queue.exceeds_limit());

    let light = by_name("light");
    assert_eq!(light.source(), EventSource::Recursive);
    assert!(!light.exceeds_limit(), "{light:?}");

    let big = by_name("big");
    assert_eq!(big.limit(), 64 * 1024);
    assert!(big.size() >= 32 * 1024 && !big.exceeds_limit(), "{big:?}");

    let reconcile = by_name("reconcile");
    assert_eq!(reconcile.source(), EventSource::Reconcile);
    assert!(reconcile.exceeds_limit(), "{reconcile:?}");

    queues.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn strict_future_size_fails_at_start() {
    let env = TestEnv::new();
    let rejected = |res: rekka::Result<Queues>, name: &str| match res {
        Err(RekkaError::InvalidConfig(msg)) => {
            assert!(
                msg.contains(&format!("`{name}`")) && msg.contains("max_future_size") && msg.contains("Box::pin"),
                "{msg}"
            );
        }
        Err(e) => panic!("`{name}`: expected InvalidConfig, got {e}"),
        Ok(_) => panic!("`{name}`: expected InvalidConfig, the queues started"),
    };

    let strict = || env.builder().strict_future_size(true);
    rejected(strict().register::<CfgJob>(|w| w.build_fn(heavy)).start().await, "cfg");
    rejected(strict().register_recursive("big", |w| w.build_fn(task_heavy)).start().await, "big");
    rejected(strict().with_reconcile(task_heavy, Duration::from_secs(3600)).start().await, "reconcile");
    // O limite do worker vale mais que o global.
    rejected(
        strict()
            .max_future_size(64 * 1024)
            .register::<CfgJob>(|w| w.max_future_size(16).build_fn(noop))
            .start()
            .await,
        "cfg",
    );

    let ok = strict()
        .max_future_size(64 * 1024)
        .register::<CfgJob>(|w| w.build_fn(heavy))
        .register_recursive("big", |w| w.build_fn(task_heavy))
        .start()
        .await;
    ok.unwrap().shutdown().await.unwrap();
    let ok = strict().register::<CfgJob>(|w| w.max_future_size(64 * 1024).build_fn(heavy)).start().await;
    ok.unwrap().shutdown().await.unwrap();
}
