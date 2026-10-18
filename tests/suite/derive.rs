use rekka::{RekkaQueue, ToTaskId};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Serialize, Deserialize, RekkaQueue)]
struct MailJob {
    task_id: String,
}

#[derive(Serialize, Deserialize, RekkaQueue)]
#[rekka(name = "payments")]
struct NamedJob {
    task_id: i64,
}

#[derive(Serialize, Deserialize, RekkaQueue)]
struct HTTPRequestJob {
    task_id: u64,
}

#[derive(Serialize, Deserialize, RekkaQueue)]
struct UsizeJob {
    task_id: usize,
}

#[derive(Serialize, Deserialize, RekkaQueue)]
struct UuidJob {
    task_id: Uuid,
}

#[derive(Serialize, Deserialize, RekkaQueue)]
struct CustomIdJob {
    #[rekka(task_id)]
    id: i32,
    // Ignorado: o campo marcado tem prioridade.
    task_id: String,
}

#[derive(Serialize, Deserialize, RekkaQueue)]
#[rekka(name = "generic")]
struct GenericJob<T> {
    task_id: String,
    value: T,
}

#[test]
fn default_name_is_snake_case() {
    assert_eq!(MailJob::NAME, "mail_job");
    assert_eq!(HTTPRequestJob::NAME, "http_request_job");
    assert_eq!(UsizeJob::NAME, "usize_job");
}

#[test]
fn explicit_name() {
    assert_eq!(NamedJob::NAME, "payments");
    assert_eq!(GenericJob::<u8>::NAME, "generic");
}

#[test]
fn task_id_types() {
    assert_eq!(MailJob { task_id: "abc".into() }.task_id(), "abc");
    assert_eq!(NamedJob { task_id: -42 }.task_id(), "-42");
    assert_eq!(HTTPRequestJob { task_id: u64::MAX }.task_id(), u64::MAX.to_string());
    assert_eq!(UsizeJob { task_id: 7 }.task_id(), "7");

    let id = Uuid::new_v4();
    assert_eq!(UuidJob { task_id: id }.task_id(), id.to_string());

    assert_eq!(GenericJob { task_id: "g".into(), value: 1u8 }.task_id(), "g");
}

#[test]
fn custom_task_id_field() {
    let job = CustomIdJob { id: 99, task_id: "ignored".into() };
    assert_eq!(job.task_id(), "99");
}

#[test]
fn to_task_id_accepts_common_types() {
    assert_eq!("x".to_task_id(), "x");
    assert_eq!(String::from("y").to_task_id(), "y");
    assert_eq!(5i64.to_task_id(), "5");
    assert_eq!(5usize.to_task_id(), "5");
    let id = Uuid::nil();
    assert_eq!(id.to_task_id(), id.to_string());
}
