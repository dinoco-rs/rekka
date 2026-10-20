use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use rekka::{EventSource, JobEvent, JobEventKind, JobHandler, RekkaQueue, RekkaData};
use serde::{Deserialize, Serialize};

use crate::common::{Events, Probe, TestEnv, eventually};

#[derive(Serialize, Deserialize, RekkaQueue, Debug, PartialEq, Clone)]
#[rekka(name = "mail")]
struct MailJob {
    task_id: String,
    to: String,
}

#[derive(Serialize, Deserialize, RekkaQueue, Debug)]
#[rekka(name = "other")]
struct OtherJob {
    task_id: String,
}

async fn send(h: JobHandler, _job: MailJob, RekkaData(probe): RekkaData<Probe>) {
    probe.record(h.task_id(), h.attempt(), 0, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn events_expose_the_job_and_metadata() {
    let env = TestEnv::new();
    let (probe, events) = (Probe::new(), Events::default());
    let q = env
        .builder()
        .data_arc(probe.clone())
        .on_event(events.handler())
        .register::<MailJob>(|w| w.build_fn(send))
        .start()
        .await
        .unwrap();

    let job = MailJob { task_id: "e1".into(), to: "a@b.c".into() };
    q.immediate(job.clone()).await.unwrap();
    assert!(eventually(Duration::from_secs(3), || events.kinds("e1").len() == 2).await);

    let all = events.all();
    let started = &all[0];
    assert!(started.is_started() && !started.is_final());
    let done = &all[1];
    assert!(done.is_success() && done.is_final() && !done.is_failure() && !done.is_retry());
    assert_eq!(done.source(), EventSource::Queue);
    assert_eq!(done.job_name(), "mail");
    assert_eq!(done.task_id(), Some("e1"));
    assert_eq!(done.attempt(), 1);
    assert!(done.elapsed().is_some());
    assert!(done.is::<MailJob>() && !done.is::<OtherJob>());
    assert_eq!(done.get_job::<MailJob>(), Some(job));
    assert!(done.get_job::<OtherJob>().is_none());
    q.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn panicking_event_handler_does_not_break_workers() {
    let env = TestEnv::new();
    let probe = Probe::new();
    let seen = Arc::new(AtomicUsize::new(0));
    let q = env
        .builder()
        .data_arc(probe.clone())
        .on_event({
            let seen = seen.clone();
            move |e: JobEvent| {
                seen.fetch_add(1, Ordering::SeqCst);
                if e.kind() == JobEventKind::Succeeded {
                    panic!("event handler bug");
                }
            }
        })
        .register::<MailJob>(|w| w.concurrency(2).build_fn(send))
        .start()
        .await
        .unwrap();

    for i in 0..5 {
        q.immediate(MailJob { task_id: format!("p{i}"), to: String::new() }).await.unwrap();
    }
    assert!(eventually(Duration::from_secs(5), || probe.count() == 5).await);
    assert!(eventually(Duration::from_secs(2), || seen.load(Ordering::SeqCst) == 10).await);
    assert_eq!(q.stats::<MailJob>().await.unwrap().running, 0);
    q.shutdown().await.unwrap();
}
