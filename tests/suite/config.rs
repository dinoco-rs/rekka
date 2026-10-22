use std::time::Duration;

use rekka::{JobHandler, Queues, RekkaError, RekkaQueue, RekkaData};
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
