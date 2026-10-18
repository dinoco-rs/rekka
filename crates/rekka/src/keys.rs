/// Chaves de uma fila. Todas usam a hash tag `{nome}` (mesmo slot em Redis Cluster).
pub(crate) struct QueueKeys {
    /// `prefix:{nome}:` — base das chaves dinâmicas montadas dentro dos scripts.
    pub(crate) base: String,
    pub(crate) queue: String,
    pub(crate) active: String,
    pub(crate) locks: String,
}

impl QueueKeys {
    pub(crate) fn new(prefix: &str, name: &str) -> Self {
        let base = format!("{prefix}:{{{name}}}:");
        Self { queue: format!("{base}queue"), active: format!("{base}active"), locks: format!("{base}locks"), base }
    }

    pub(crate) fn notify_channel(&self) -> String {
        format!("{}notify", self.base)
    }

    pub(crate) fn invocation<'a>(&self, script: &'a redis::Script) -> redis::ScriptInvocation<'a> {
        let mut inv = script.prepare_invoke();
        inv.key(&self.queue).key(&self.active).key(&self.locks).arg(&self.base);
        inv
    }
}

pub(crate) struct RecursiveKeys {
    pub(crate) next: String,
    pub(crate) lock: String,
}

impl RecursiveKeys {
    pub(crate) fn new(prefix: &str, name: &str) -> Self {
        Self { next: format!("{prefix}:{{rec:{name}}}:next"), lock: format!("{prefix}:{{rec:{name}}}:lock") }
    }

    pub(crate) fn invocation<'a>(&self, script: &'a redis::Script) -> redis::ScriptInvocation<'a> {
        let mut inv = script.prepare_invoke();
        inv.key(&self.next).key(&self.lock);
        inv
    }
}

pub(crate) fn validate_name(kind: &str, name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err(format!("{kind} name must not be empty"));
    }
    if name.contains(['{', '}']) {
        return Err(format!("{kind} name `{name}` must not contain `{{` or `}}`"));
    }
    Ok(())
}
