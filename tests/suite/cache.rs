use std::time::Duration;

use rekka::{Cache, RekkaCache, RekkaError, RekkaQueue};
use serde::{Deserialize, Serialize};

use crate::common::{TestEnv, eventually_async, ms};

#[derive(Serialize, Deserialize, RekkaCache, Debug, PartialEq, Clone)]
#[rekka(key = sku)]
#[rekka(ttl = 300)]
struct Product {
    sku: String,
    price: u32,
}

/// Sem `key` (usa o campo `id`) e sem `ttl` (permanente).
#[derive(Serialize, Deserialize, RekkaCache, Debug, PartialEq, Clone)]
#[rekka(name = "users")]
struct User {
    id: u64,
    name: String,
}

#[derive(Serialize, Deserialize, RekkaCache)]
#[rekka(key = "code", ttl = "1h30m")]
struct Coupon {
    code: String,
}

/// Os dois derives na mesma struct, compartilhando `#[rekka(...)]`.
#[derive(Serialize, Deserialize, RekkaCache, RekkaQueue, Debug, PartialEq)]
#[rekka(name = "both", key = task_id, ttl = "2s")]
struct Both {
    task_id: String,
}

/// Chave dinâmica montada a partir de um campo da própria struct.
#[derive(Serialize, Deserialize, RekkaCache, Debug, PartialEq, Clone)]
#[rekka(name = "payments", key = "payments-{business_id}", ttl = "1m")]
struct Payment {
    business_id: i64,
    amount: u32,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
struct Owner {
    id: u32,
}

/// Exemplo do `get` hidratado: basta passar o `session_id`.
#[derive(Serialize, Deserialize, RekkaCache, Debug, PartialEq, Clone)]
#[rekka(key = "session:{session_id}", ttl = "1m")]
struct Session {
    session_id: i64,
    user: String,
}

/// Template só com o campo e um especificador de formato: o valor é sempre hidratado.
#[derive(Serialize, Deserialize, RekkaCache, Debug, PartialEq, Clone)]
#[rekka(key = "{id:05}")]
struct Padded {
    id: u32,
}

/// Especificador de tipo (`x`): não dá para hidratar a partir de qualquer valor, só a chave completa.
#[derive(Serialize, Deserialize, RekkaCache, Debug, PartialEq, Clone)]
#[rekka(key = "hex-{id:x}")]
struct Hex {
    id: u32,
}

/// Vários campos, campo aninhado, especificador de formato e chaves literais.
#[derive(Serialize, Deserialize, RekkaCache, Debug, PartialEq, Clone)]
#[rekka(key = "{tenant}:{owner.id:05}:{{v1}}")]
struct Report {
    tenant: String,
    owner: Owner,
}

/// Template em struct genérica: o campo do template não precisa ser o tipo genérico.
#[derive(Serialize, Deserialize, RekkaCache, Debug, PartialEq)]
#[rekka(name = "wrapped", key = "w-{id}")]
struct Wrapped<T> {
    id: u8,
    value: T,
}

/// TTL com unidade sem aspas: para o Rust, `10m` é o inteiro `10` com sufixo `m`.
#[derive(Serialize, Deserialize, RekkaCache)]
#[rekka(ttl = 1d)]
struct DailyTtl {
    id: u8,
}

#[derive(Serialize, Deserialize, RekkaCache)]
#[rekka(ttl = 2h)]
struct HourlyTtl {
    id: u8,
}

#[derive(Serialize, Deserialize, RekkaCache)]
#[rekka(ttl = 10m)]
struct MinutesTtl {
    id: u8,
}

#[derive(Serialize, Deserialize, RekkaCache)]
#[rekka(ttl = 30s)]
struct SecondsTtl {
    id: u8,
}

#[derive(Serialize, Deserialize, RekkaCache)]
#[rekka(ttl = 1h30m)]
struct CombinedTtl {
    id: u8,
}

#[derive(Serialize, Deserialize, RekkaCache)]
#[rekka(ttl = "1d 12h")]
struct SpacedTtl {
    id: u8,
}

fn user(id: u64) -> User {
    User { id, name: format!("user {id}") }
}

#[test]
fn ttl_accepts_units_with_and_without_quotes() {
    let hour = Duration::from_secs(3600);
    assert_eq!(DailyTtl::TTL, Some(hour * 24));
    assert_eq!(HourlyTtl::TTL, Some(hour * 2));
    assert_eq!(MinutesTtl::TTL, Some(Duration::from_secs(600)));
    assert_eq!(SecondsTtl::TTL, Some(Duration::from_secs(30)));
    assert_eq!(CombinedTtl::TTL, Some(Duration::from_secs(90 * 60)));
    assert_eq!(SpacedTtl::TTL, Some(hour * 36));
}

#[test]
fn derive_builds_dynamic_keys_from_fields() {
    assert_eq!(Payment { business_id: 42, amount: 10 }.cache_key(), "payments-42");
    assert_eq!(Payment { business_id: -7, amount: 10 }.cache_key(), "payments--7");
    assert_eq!(Payment::NAMESPACE, "payments");
    assert_eq!(Payment::TTL, Some(Duration::from_secs(60)));

    let report = Report { tenant: "acme".into(), owner: Owner { id: 42 } };
    assert_eq!(report.cache_key(), "acme:00042:{v1}");
    assert_eq!(Report::NAMESPACE, "report");

    assert_eq!(Wrapped { id: 3, value: "x".to_owned() }.cache_key(), "w-3");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dynamic_keys_round_trip() {
    let env = TestEnv::new();
    let cache = env.cache().await;
    let payment = Payment { business_id: 42, amount: 990 };

    cache.insert(&payment).await.unwrap();
    assert_eq!(env.keys("cache:payments:*").await, vec![format!("{}:cache:payments:payments-42", env.prefix)]);
    assert_eq!(cache.get::<Payment>("payments-42").await, Some(payment.clone()));
    assert_eq!(cache.get::<Payment>(format!("payments-{}", payment.business_id)).await, Some(payment));
    assert!((1..=60_000).contains(&pttl(&env, "payments:payments-42").await));

    let reports = [
        Report { tenant: "acme".into(), owner: Owner { id: 1 } },
        Report { tenant: "acme".into(), owner: Owner { id: 2 } },
    ];
    cache.insert_many(&reports).await.unwrap();
    let found = cache.get_many::<Report>(["acme:00002:{v1}", "acme:00003:{v1}", "acme:00001:{v1}"]).await.unwrap();
    assert_eq!(found, vec![Some(reports[1].clone()), None, Some(reports[0].clone())]);

    assert!(cache.remove::<Payment>("payments-42").await.unwrap());
    assert_eq!(cache.get::<Payment>("payments-42").await, None);
}

#[test]
fn resolve_key_hydrates_single_field_templates() {
    assert_eq!(Session::resolve_key(42), "session:42");
    assert_eq!(Session::resolve_key(-7), "session:-7");
    assert_eq!(Session::resolve_key("session:42"), "session:42");
    assert_eq!(Session::resolve_key("abc"), "session:abc");
    // Só o prefixo não é uma chave completa: o campo precisa de pelo menos um caractere.
    assert_eq!(Session::resolve_key("session:"), "session:session:");
    assert_eq!(Session::resolve_key(""), "");

    assert_eq!(Payment::resolve_key(42), "payments-42");
    assert_eq!(Payment::resolve_key("payments-42"), "payments-42");
    assert_eq!(Wrapped::<String>::resolve_key(3), "w-3");

    assert_eq!(Padded::resolve_key(42), "00042");
    assert_eq!(Padded::resolve_key("00042"), "00042");

    assert_eq!(Hex::resolve_key("hex-ff"), "hex-ff");
    assert_eq!(Report::resolve_key("acme:00042:{v1}"), "acme:00042:{v1}");
    assert_eq!(User::resolve_key(7), "7");
}

#[test]
fn key_template_matching() {
    use rekka::__private::matches_key_template as m;
    assert!(m("session:42", &["session:", ""]));
    assert!(!m("session:", &["session:", ""]));
    assert!(!m("42", &["session:", ""]));
    assert!(m("acme:00042:{v1}", &["", ":", ":{v1}"]));
    assert!(!m("acme:{v1}", &["", ":", ":{v1}"]));
    assert!(m("ab", &["", "", ""]));
    assert!(!m("a", &["", "", ""]));
    assert!(m("é-1-x", &["", "-", "-x"]));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn get_hydrates_the_key_from_the_field_value() {
    let env = TestEnv::new();
    let cache = env.cache().await;
    let session = Session { session_id: 42, user: "ana".into() };

    cache.insert(&session).await.unwrap();
    assert_eq!(env.keys("cache:session:*").await, vec![format!("{}:cache:session:session:42", env.prefix)]);
    assert_eq!(cache.get::<Session>(42).await, Some(session.clone()));
    assert_eq!(cache.get::<Session>(42i64).await, Some(session.clone()));
    assert_eq!(cache.get::<Session>("session:42").await, Some(session.clone()));
    assert_eq!(cache.get::<Session>(43).await, None);
    assert_eq!(cache.get_many::<Session>([42, 43]).await.unwrap(), vec![Some(session.clone()), None]);
    assert!(cache.contains::<Session>(42).await.unwrap());

    cache.insert(&Padded { id: 7 }).await.unwrap();
    assert_eq!(cache.get::<Padded>(7).await, Some(Padded { id: 7 }));

    assert!(cache.remove::<Session>(42).await.unwrap());
    assert_eq!(cache.get::<Session>("session:42").await, None);
}

#[test]
fn derive_sets_namespace_ttl_and_key() {
    assert_eq!(Product::NAMESPACE, "product");
    assert_eq!(Product::TTL, Some(ms(300)));
    assert_eq!(Product { sku: "A-1".into(), price: 1 }.cache_key(), "A-1");

    assert_eq!(User::NAMESPACE, "users");
    assert_eq!(User::TTL, None);
    assert_eq!(user(42).cache_key(), "42");

    assert_eq!(Coupon::TTL, Some(Duration::from_secs(90 * 60)));
    assert_eq!(Coupon { code: "X".into() }.cache_key(), "X");

    assert_eq!(<Both as RekkaCache>::NAMESPACE, "both");
    assert_eq!(<Both as RekkaQueue>::NAME, "both");
    assert_eq!(Both::TTL, Some(Duration::from_secs(2)));
}

async fn pttl(env: &TestEnv, key: &str) -> i64 {
    redis::cmd("PTTL").arg(format!("{}:cache:{key}", env.prefix)).query_async(&mut env.redis().await).await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn insert_get_contains_remove() {
    let env = TestEnv::new();
    let cache = env.cache().await;

    cache.insert(&user(1)).await.unwrap();
    assert_eq!(cache.get::<User>(1).await, Some(user(1)));
    assert!(cache.contains::<User>(1).await.unwrap());
    assert_eq!(pttl(&env, "users:1").await, -1, "no ttl attribute means permanent");

    let renamed = User { id: 1, name: "renamed".into() };
    cache.insert(&renamed).await.unwrap();
    assert_eq!(cache.get::<User>(1).await, Some(renamed));

    assert!(cache.remove::<User>(1).await.unwrap());
    assert!(!cache.remove::<User>(1).await.unwrap());
    assert_eq!(cache.get::<User>(1).await, None);
    assert!(!cache.contains::<User>(1).await.unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ttl_is_applied_and_entries_expire() {
    let env = TestEnv::new();
    let cache = env.cache().await;

    cache.insert(&Product { sku: "p".into(), price: 10 }).await.unwrap();
    let ttl = pttl(&env, "product:p").await;
    assert!((1..=300).contains(&ttl), "pttl {ttl}");

    cache.insert_with_ttl(&user(7), ms(200)).await.unwrap();
    cache.insert_many(&[Product { sku: "q".into(), price: 1 }]).await.unwrap();
    assert!((1..=300).contains(&pttl(&env, "product:q").await));

    assert!(
        eventually_async(Duration::from_secs(3), || async {
            cache.get::<Product>("p").await.is_none()
                && cache.get::<Product>("q").await.is_none()
                && cache.get::<User>(7).await.is_none()
        })
        .await
    );
    assert!(env.keys("cache:*").await.is_empty(), "expired entries must be gone");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn many_operations_keep_order_and_handle_big_batches() {
    let env = TestEnv::new();
    let cache = env.cache().await;

    let users: Vec<User> = (0..2500).map(user).collect();
    cache.insert_many(&users).await.unwrap();

    // Chaves inexistentes intercaladas: o resultado segue a ordem pedida.
    let keys: Vec<u64> = (0..2600).rev().collect();
    let got = cache.get_many::<User>(&keys).await.unwrap();
    assert_eq!(got.len(), 2600);
    for (key, value) in keys.iter().zip(&got) {
        if *key < 2500 {
            assert_eq!(value.as_ref(), Some(&user(*key)));
        } else {
            assert!(value.is_none());
        }
    }

    assert_eq!(cache.remove_many::<User>(&keys).await.unwrap(), 2500);
    assert!(cache.get_many::<User>(0..2500).await.unwrap().iter().all(Option::is_none));
    assert!(env.keys("cache:*").await.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_inputs_and_invalid_keys() {
    let env = TestEnv::new();
    let cache = env.cache().await;

    cache.insert_many::<User>([]).await.unwrap();
    assert!(cache.get_many::<User>(Vec::<u64>::new()).await.unwrap().is_empty());
    assert_eq!(cache.remove_many::<User>(Vec::<u64>::new()).await.unwrap(), 0);

    assert!(matches!(cache.insert(&Product { sku: String::new(), price: 0 }).await, Err(RekkaError::InvalidKey)));
    assert!(cache.get::<Product>("").await.is_none());
    assert!(matches!(cache.contains::<Product>("").await, Err(RekkaError::InvalidKey)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incompatible_entries_are_misses() {
    let env = TestEnv::new();
    let cache = env.cache().await;
    let _: () = redis::cmd("SET")
        .arg(format!("{}:cache:users:9", env.prefix))
        .arg("{\"not\": \"a user\"}")
        .query_async(&mut env.redis().await)
        .await
        .unwrap();

    assert_eq!(cache.get::<User>(9).await, None);
    assert_eq!(cache.get_many::<User>([9, 10]).await.unwrap(), vec![None, None]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clear_removes_only_its_namespace() {
    let env = TestEnv::new();
    let cache = env.cache().await;
    cache.insert_many(&(0..1500).map(user).collect::<Vec<_>>()).await.unwrap();
    cache.insert(&Product { sku: "keep".into(), price: 1 }).await.unwrap();

    assert_eq!(cache.clear::<User>().await.unwrap(), 1500);
    assert!(cache.get_many::<User>(0..1500).await.unwrap().iter().all(Option::is_none));
    assert!(cache.get::<Product>("keep").await.is_some());
}

#[derive(Serialize, Deserialize, RekkaQueue)]
#[rekka(name = "warm")]
struct WarmJob {
    task_id: u64,
}

/// O cache também é um extractor e compartilha a conexão das filas.
async fn warm(job: WarmJob, cache: Cache) -> rekka::Result<()> {
    cache.insert(&user(job.task_id)).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cache_inside_jobs_and_from_queues() {
    let env = TestEnv::new();
    let q = env.builder().register::<WarmJob>(|w| w.build_fn(warm)).start().await.unwrap();

    q.immediate(WarmJob { task_id: 77 }).await.unwrap();
    let cache = q.cache();
    assert!(eventually_async(Duration::from_secs(3), || async { cache.get::<User>(77).await.is_some() }).await);

    // Mesmo prefixo: uma instância separada enxerga o mesmo dado.
    assert_eq!(env.cache().await.get::<User>(77).await, Some(user(77)));
    q.shutdown().await.unwrap();
}
