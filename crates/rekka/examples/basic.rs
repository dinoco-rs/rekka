//! `cargo run -p rekka --example basic` (precisa de um Redis em `REDIS_URL`).

use std::time::Duration;

use rekka::chrono::Utc;
use rekka::{
    Cache, JobEvent, JobHandler, Queues, ReconcileHandler, RekkaCache, RekkaData, RekkaExecutionResult, RekkaQueue,
    RetryPolicy, StepResult,
};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, RekkaQueue)]
#[rekka(name = "mail")]
pub struct MailJob {
    // Obrigatório: String, inteiros, usize, Uuid... (qualquer `Display`).
    task_id: String,
    business_id: i64,
    to: String,
}

#[derive(Serialize, Deserialize, RekkaQueue)]
#[rekka(name = "other")]
pub struct OtherJob {
    task_id: u64,
}

/// Entrada do workflow: o fluxo só começa enfileirando este job.
#[derive(Serialize, Deserialize, RekkaQueue)]
#[rekka(name = "create_customer")]
pub struct CreateCustomerJob {
    task_id: String,
    name: String,
    email: String,
}

/// Passa de um passo do workflow para o próximo (não precisa de `RekkaQueue`).
#[derive(Serialize, Deserialize)]
pub struct Customer {
    id: i64,
    name: String,
    email: String,
}

#[derive(Serialize, Deserialize, RekkaCache)]
#[rekka(key = id)]
#[rekka(ttl = "10m")] // sem `ttl` a entrada é permanente
pub struct Business {
    id: i64,
    name: String,
}

struct MyData {
    sender: String,
}

#[tokio::main]
async fn main() -> rekka::Result<()> {
    let redis_url = std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let policy = RetryPolicy::RetryAfterRange(Duration::from_secs(1), Duration::from_secs(10));

    let queues = Queues::new(redis_url)
        .data(MyData { sender: "no-reply@example.com".into() })
        .with_reconcile(reconcile_function, Duration::from_secs(300))
        .on_event(event_handler)
        .register::<MailJob>(|w| {
            w.retry(policy.clone())
                .concurrency(5)
                .concurrency_by_key(|job| job.business_id)
                .build_fn(execute_mail_job)
        })
        .register::<OtherJob>(|w| w.retry(policy).concurrency(3).build_fn(execute_other_job))
        // Cada passo é uma fila própria com estas opções; a saída de um passo é a entrada do próximo.
        .register_workflow::<CreateCustomerJob>(|w| {
            w.concurrency(4)
                .retry(RetryPolicy::RetryAfterRange(Duration::from_secs(2), Duration::from_secs(60)))
                .max_attempts(8)
                .timeout(Duration::from_secs(30))
                .start_with(create_customer)
                .then(validate_customer)
                .then(send_welcome_email)
        })
        .register_recursive("reconcile_payments", |w| {
            w.interval(Duration::from_secs(60)).build_fn(reconcile_payments)
        })
        // Expressão cron (formato do node-cron, segundos opcionais): 09:00 de segunda a sexta, UTC.
        .register_recursive("weekday_report", |w| w.cron_time("0 9 * * mon-fri").build_fn(reconcile_payments))
        .start()
        .await?;

    queues.immediate(MailJob { task_id: "mail-1".into(), business_id: 1, to: "a@b.c".into() }).await?;
    queues
        .schedule(MailJob { task_id: "mail-2".into(), business_id: 1, to: "d@e.f".into() }, Utc::now())
        .await?;
    queues.remove::<MailJob>("mail-2").await?;
    queues.immediate(OtherJob { task_id: 1 }).await?;
    queues
        .immediate(CreateCustomerJob {
            task_id: "customer-1".into(),
            name: "Ada".into(),
            email: "ada@example.com".into(),
        })
        .await?;

    // Cache: standalone (`Cache::new(url).await?`) ou compartilhando a conexão das filas.
    let cache = queues.cache();
    let businesses = vec![Business { id: 1, name: "Acme".into() }, Business { id: 2, name: "Globex".into() }];
    cache.insert(&businesses[0]).await?;
    cache.insert_many(&businesses).await?;
    let _one: Option<Business> = cache.get::<Business>(1).await;
    let _many: Vec<Option<Business>> = cache.get_many::<Business>([1, 2, 3]).await?;
    cache.remove::<Business>(1).await?;
    cache.remove_many::<Business>([2, 3]).await?;

    // Limpeza manual de uma fila inteira (ex.: tipo removido do código).
    queues.purge_queue("old_queue").await?;

    // Bloqueia até SIGINT/SIGTERM; para de pegar jobs e espera os que estão rodando.
    queues.join().await
}

fn event_handler(event: JobEvent) {
    if event.is_success()
        && let Some(job) = event.get_job::<MailJob>()
    {
        println!("mail {} sent to {}", job.task_id, job.to);
    }
    if event.is_dead() {
        eprintln!("{} {:?} gave up: {:?}", event.job_name(), event.task_id(), event.error());
    }
}

/// Roda a cada intervalo em uma única instância por ciclo (na primeira subida, roda na hora).
async fn reconcile_function(handler: ReconcileHandler) -> rekka::Result<()> {
    // Busca as tasks pendentes em algum lugar (ex.: banco de dados).
    // `add` só cria o que não existe: nunca duplica nem interrompe um job em andamento.
    handler.add(MailJob { task_id: "mail-from-db".into(), business_id: 2, to: "g@h.i".into() }).await?;
    Ok(())
}

// O job vem sempre primeiro; depois dele, extractors em qualquer ordem (inclusive `JobHandler`).
async fn execute_mail_job(
    job: MailJob,
    RekkaData(state): RekkaData<MyData>,
    cache: Cache,
    handler: JobHandler,
) -> RekkaExecutionResult {
    // `get` nunca falha: ausente, formato incompatível ou Redis fora do ar viram `None`.
    let business = cache.get::<Business>(job.business_id).await;
    let teste = false;
    if teste {
        return handler.err("falhou"); // entra na regra de retry
    }
    if job.to.is_empty() {
        return handler.cancel();
    }
    if handler.attempt() > 3 {
        return handler.retry_after(Duration::from_secs(30));
    }
    let name = business.map_or_else(|| "?".to_owned(), |b| b.name);
    println!("sending {} from {} ({name})", job.task_id, state.sender);
    handler.success()
}

async fn execute_other_job(_job: OtherJob) -> Result<(), std::io::Error> {
    Ok(())
}

/// Primeiro passo do workflow: o `Ok` vira a entrada do próximo passo.
async fn create_customer(job: CreateCustomerJob) -> Result<Customer, std::io::Error> {
    Ok(Customer { id: 1, name: job.name, email: job.email })
}

async fn validate_customer(customer: Customer, handler: JobHandler) -> StepResult<Customer> {
    if !customer.email.contains('@') {
        // Encerra a execução: os próximos passos não executam.
        return handler.cancel().into();
    }
    StepResult::Next(customer)
}

async fn send_welcome_email(customer: Customer, RekkaData(state): RekkaData<MyData>) {
    println!("welcome {} ({}) <{}>, from {}", customer.name, customer.id, customer.email, state.sender);
}

async fn reconcile_payments(RekkaData(_state): RekkaData<MyData>) {}
