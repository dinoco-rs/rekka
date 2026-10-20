use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::{ToTokens, quote};
use syn::punctuated::Punctuated;
use syn::{Attribute, Data, DeriveInput, Field, Fields, Generics, Ident, Lit, LitStr, Path, Token, parse_macro_input};

/// Implementa `rekka::RekkaQueue` para uma struct com campos nomeados.
///
/// - `#[rekka(name = "mail")]` define o nome da fila (padrão: nome da struct em snake_case).
/// - `#[rekka(task_id)]` marca o campo identificador (padrão: o campo `task_id`).
/// - `#[rekka(crate = "path")]` muda o caminho do crate `rekka` (útil em re-exports).
#[proc_macro_derive(RekkaQueue, attributes(rekka))]
pub fn derive_rekka_queue(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_queue(input).unwrap_or_else(syn::Error::into_compile_error).into()
}

/// Implementa `rekka::RekkaCache` para uma struct com campos nomeados.
///
/// - `#[rekka(key = campo)]` campo usado como chave (padrão: o campo `key` ou `id`).
/// - `#[rekka(key = "payments-{business_id}")]` chave montada a partir dos campos da struct:
///   cada `{campo}` (ou `{campo.subcampo}`, com especificador opcional como `{id:05}`) vira o
///   valor do campo; `{{` e `}}` são chaves literais. Com um único `{campo}`, `get`/`contains`/
///   `remove` aceitam só o valor (`cache.get::<T>(42)` busca `payments-42`) ou a chave completa.
/// - `#[rekka(ttl = 60000)]` TTL em milissegundos, ou uma duração com unidade, com ou sem aspas:
///   `ttl = 30s`, `ttl = 10m`, `ttl = 2h`, `ttl = 7d`, `ttl = 1h30m`, `ttl = "1h 30m"`
///   (`ms`, `s`, `m`, `h`, `d`).
///   Sem `ttl`, a entrada é permanente.
/// - `#[rekka(name = "items")]` namespace no Redis (padrão: nome da struct em snake_case).
/// - `#[rekka(crate = "path")]` muda o caminho do crate `rekka`.
#[proc_macro_derive(RekkaCache, attributes(rekka))]
pub fn derive_rekka_cache(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_cache(input).unwrap_or_else(syn::Error::into_compile_error).into()
}

/// Atributos de container. Os dois derives aceitam todas as chaves (uma struct pode derivar
/// ambos); cada um usa só as que lhe dizem respeito.
#[derive(Default)]
struct ContainerAttrs {
    name: Option<LitStr>,
    krate: Option<Path>,
    key: Option<KeySpec>,
    ttl_ms: Option<u64>,
}

/// Chave do cache: um campo da struct ou um template com `{campo}`.
enum KeySpec {
    Field(Ident),
    Template(LitStr),
}

impl ContainerAttrs {
    fn parse(attrs: &[Attribute]) -> syn::Result<Self> {
        let mut out = Self::default();
        for attr in rekka_attrs(attrs) {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("name") {
                    out.name = Some(meta.value()?.parse()?);
                } else if meta.path.is_ident("crate") {
                    let lit: LitStr = meta.value()?.parse()?;
                    out.krate = Some(lit.parse()?);
                } else if meta.path.is_ident("key") {
                    let value = meta.value()?;
                    out.key = Some(if value.peek(LitStr) {
                        let lit: LitStr = value.parse()?;
                        // `key = "id"` continua sendo um campo; com `{`/`}` é um template.
                        if lit.value().contains(['{', '}']) {
                            KeySpec::Template(lit)
                        } else {
                            KeySpec::Field(lit.parse()?)
                        }
                    } else {
                        KeySpec::Field(value.parse()?)
                    });
                } else if meta.path.is_ident("ttl") {
                    let lit: Lit = meta.value()?.parse()?;
                    out.ttl_ms = Some(parse_ttl(&lit)?);
                } else {
                    return Err(meta.error("atributo desconhecido; use `name`, `crate`, `key` ou `ttl`"));
                }
                Ok(())
            })?;
        }
        Ok(out)
    }

    fn krate(&self) -> Path {
        self.krate.clone().unwrap_or_else(|| syn::parse_quote!(::rekka))
    }
}

fn rekka_attrs(attrs: &[Attribute]) -> impl Iterator<Item = &Attribute> {
    attrs.iter().filter(|a| a.path().is_ident("rekka"))
}

fn named_fields<'a>(input: &'a DeriveInput, derive: &str) -> syn::Result<&'a Punctuated<Field, Token![,]>> {
    match &input.data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => Ok(&fields.named),
            _ => Err(syn::Error::new_spanned(&input.ident, format!("{derive} exige uma struct com campos nomeados"))),
        },
        _ => Err(syn::Error::new_spanned(&input.ident, format!("{derive} só pode ser derivado em structs"))),
    }
}

/// Generics com `Self: bound` quando a struct é genérica.
fn bounded_generics(generics: &Generics, bound: TokenStream2) -> Generics {
    let mut generics = generics.clone();
    if !generics.params.is_empty() {
        generics.make_where_clause().predicates.push(syn::parse_quote!(Self: #bound));
    }
    generics
}

fn expand_queue(input: DeriveInput) -> syn::Result<TokenStream2> {
    let attrs = ContainerAttrs::parse(&input.attrs)?;
    let fields = named_fields(&input, "RekkaQueue")?;

    let mut task_id: Option<&Ident> = None;
    let mut explicit = false;
    for field in fields {
        let mut marked = false;
        for attr in rekka_attrs(&field.attrs) {
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("task_id") {
                    marked = true;
                    Ok(())
                } else {
                    Err(meta.error("atributo de campo desconhecido; use `#[rekka(task_id)]`"))
                }
            })?;
        }
        if marked {
            if explicit {
                return Err(syn::Error::new_spanned(field, "apenas um campo pode ter `#[rekka(task_id)]`"));
            }
            explicit = true;
            task_id = field.ident.as_ref();
        } else if task_id.is_none() && field.ident.as_ref().is_some_and(|i| i == "task_id") {
            task_id = field.ident.as_ref();
        }
    }
    let Some(task_id) = task_id else {
        return Err(syn::Error::new(
            Span::call_site(),
            format!("`{}` precisa de um campo `task_id` (ou de um campo marcado com `#[rekka(task_id)]`)", input.ident),
        ));
    };

    let queue_name = resolve_name(&attrs, &input.ident, &['{', '}'])?;
    let krate = attrs.krate();
    let ident = &input.ident;
    let generics = bounded_generics(&input.generics, quote!(#krate::__private::QueueBound));
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics #krate::RekkaQueue for #ident #ty_generics #where_clause {
            const NAME: &'static str = #queue_name;

            #[inline]
            fn task_id(&self) -> ::std::string::String {
                #krate::ToTaskId::to_task_id(&self.#task_id)
            }
        }
    })
}

fn expand_cache(input: DeriveInput) -> syn::Result<TokenStream2> {
    let attrs = ContainerAttrs::parse(&input.attrs)?;
    let fields = named_fields(&input, "RekkaCache")?;

    let field_named = |name: &str| fields.iter().filter_map(|f| f.ident.as_ref()).find(|i| *i == name);
    let krate = attrs.krate();
    let mut resolve_key = None;
    let cache_key = match &attrs.key {
        Some(KeySpec::Field(key)) => match field_named(&key.to_string()) {
            Some(field) => quote!(#krate::ToTaskId::to_task_id(&self.#field)),
            None => return Err(syn::Error::new_spanned(key, format!("`{}` não tem o campo `{key}`", input.ident))),
        },
        Some(KeySpec::Template(lit)) => {
            let (cache_key, resolve) = expand_key_template(lit, &input.ident, fields, &krate)?;
            resolve_key = resolve;
            cache_key
        }
        None => match field_named("key").or_else(|| field_named("id")) {
            Some(field) => quote!(#krate::ToTaskId::to_task_id(&self.#field)),
            None => {
                return Err(syn::Error::new(
                    Span::call_site(),
                    format!("`{}` precisa de `#[rekka(key = campo)]` (ou de um campo `key` / `id`)", input.ident),
                ));
            }
        },
    };

    let namespace = resolve_name(&attrs, &input.ident, &[':', '{', '}'])?;
    let ttl = match attrs.ttl_ms {
        Some(ms) => quote!(::core::option::Option::Some(::core::time::Duration::from_millis(#ms))),
        None => quote!(::core::option::Option::None),
    };
    let ident = &input.ident;
    let generics = bounded_generics(&input.generics, quote!(#krate::__private::CacheBound));
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics #krate::RekkaCache for #ident #ty_generics #where_clause {
            const NAMESPACE: &'static str = #namespace;
            const TTL: ::core::option::Option<::core::time::Duration> = #ttl;

            #[inline]
            fn cache_key(&self) -> ::std::string::String {
                #cache_key
            }

            #resolve_key
        }
    })
}

/// `"payments-{business_id}"` -> `format!("payments-{}", &self.business_id)`, mais o
/// `resolve_key` que hidrata a chave a partir do valor do campo (só com um único `{campo}`).
fn expand_key_template(
    lit: &LitStr,
    ident: &Ident,
    fields: &Punctuated<Field, Token![,]>,
    krate: &Path,
) -> syn::Result<(TokenStream2, Option<TokenStream2>)> {
    let template = parse_key_template(&lit.value()).map_err(|message| syn::Error::new_spanned(lit, message))?;
    let mut args = Vec::with_capacity(template.placeholders.len());
    for path in &template.placeholders {
        let field = &path[0];
        if !fields.iter().any(|f| f.ident.as_ref().is_some_and(|i| i == field)) {
            return Err(syn::Error::new_spanned(lit, format!("`{ident}` não tem o campo `{field}`")));
        }
        let segments = path.iter().map(|segment| match segment.parse::<usize>() {
            Ok(index) => {
                let index = syn::Index { index: index as u32, span: lit.span() };
                quote!(#index)
            }
            Err(_) => match segment.strip_prefix("r#") {
                Some(raw) => Ident::new_raw(raw, lit.span()).into_token_stream(),
                None => Ident::new(segment, lit.span()).into_token_stream(),
            },
        });
        args.push(quote!(&self #(. #segments)*));
    }
    let format = LitStr::new(&template.format, lit.span());
    let cache_key = quote!(::std::format!(#format, #(#args),*));
    Ok((cache_key, expand_resolve_key(&template, krate, lit.span())))
}

/// `resolve_key` de um template com um único `{campo}`: a chave completa é usada como está;
/// qualquer outro valor preenche o `{campo}`. Especificadores de tipo (`{id:x}`) só valem para o
/// tipo do campo, então nesse caso o `get` exige a chave completa.
fn expand_resolve_key(template: &KeyTemplate, krate: &Path, span: Span) -> Option<TokenStream2> {
    if template.placeholders.len() != 1 || template.specs[0].ends_with(['x', 'X', 'o', 'b', 'e', 'E', '?']) {
        return None;
    }
    let format = LitStr::new(&template.format, span);
    // Sem texto fixo, qualquer valor "casa" com o template: sempre hidrata.
    let check = if template.literals.iter().all(String::is_empty) {
        quote!(false)
    } else {
        let literals = template.literals.iter().map(|l| LitStr::new(l, span));
        quote!(#krate::__private::matches_key_template(&raw, &[#(#literals),*]))
    };
    Some(quote! {
        fn resolve_key(key: impl ::core::fmt::Display) -> ::std::string::String {
            let raw = ::std::string::ToString::to_string(&key);
            if raw.is_empty() || #check {
                raw
            } else {
                ::std::format!(#format, key)
            }
        }
    })
}

/// Template de chave já validado: a string de formato, o caminho e o especificador de cada
/// `{campo}` e o texto fixo entre eles (`literals.len() == placeholders.len() + 1`).
#[derive(Debug)]
struct KeyTemplate {
    format: String,
    placeholders: Vec<Vec<String>>,
    specs: Vec<String>,
    literals: Vec<String>,
}

fn parse_key_template(template: &str) -> Result<KeyTemplate, String> {
    let mut format = String::with_capacity(template.len());
    let mut placeholders = Vec::new();
    let mut specs = Vec::new();
    let mut literals = vec![String::new()];
    let mut rest = template;
    while let Some(c) = rest.chars().next() {
        if rest.starts_with("{{") || rest.starts_with("}}") {
            format.push_str(&rest[..2]);
            literals.last_mut().unwrap().push(c);
            rest = &rest[2..];
        } else if c == '}' {
            return Err("`}` sem `{` correspondente; use `}}` para um `}` literal".into());
        } else if c == '{' {
            let end = rest.find('}').ok_or("`{` sem `}` correspondente; use `{{` para um `{` literal")?;
            let inner = &rest[1..end];
            let (expr, spec) = match inner.split_once(':') {
                Some((expr, spec)) => (expr, Some(spec)),
                None => (inner, None),
            };
            let path = expr.trim();
            if let Some(field) = path.strip_prefix("self.") {
                return Err(format!("use `{{{field}}}` em vez de `{{self.{field}}}`"));
            }
            let segments: Vec<String> = path.split('.').map(str::to_owned).collect();
            if !segments.iter().all(|segment| is_path_segment(segment)) {
                return Err(format!("`{{{inner}}}` não é válido; use `{{campo}}` ou `{{campo.subcampo}}`"));
            }
            if segments[0].chars().all(|c| c.is_ascii_digit()) {
                return Err(format!("`{{{inner}}}` não é um campo; use o nome de um campo, como `{{id}}`"));
            }
            format.push('{');
            if let Some(spec) = spec {
                format.push(':');
                format.push_str(spec);
            }
            format.push('}');
            placeholders.push(segments);
            specs.push(spec.unwrap_or_default().to_owned());
            literals.push(String::new());
            rest = &rest[end + 1..];
        } else {
            format.push(c);
            literals.last_mut().unwrap().push(c);
            rest = &rest[c.len_utf8()..];
        }
    }
    if placeholders.is_empty() {
        return Err("o template da key precisa de pelo menos um `{campo}`".into());
    }
    Ok(KeyTemplate { format, placeholders, specs, literals })
}

/// Identificador (`campo`, `r#type`) ou índice de tupla (`0`).
fn is_path_segment(segment: &str) -> bool {
    if !segment.is_empty() && segment.chars().all(|c| c.is_ascii_digit()) {
        return true;
    }
    let ident = segment.strip_prefix("r#").unwrap_or(segment);
    let mut chars = ident.chars();
    ident != "_"
        && chars.next().is_some_and(|c| c == '_' || c.is_alphabetic())
        && chars.all(|c| c == '_' || c.is_alphanumeric())
}

fn resolve_name(attrs: &ContainerAttrs, ident: &Ident, forbidden: &[char]) -> syn::Result<String> {
    match &attrs.name {
        Some(lit) => {
            let value = lit.value();
            if value.is_empty() {
                return Err(syn::Error::new_spanned(lit, "o nome não pode ser vazio"));
            }
            if let Some(c) = value.chars().find(|c| forbidden.contains(c)) {
                return Err(syn::Error::new_spanned(lit, format!("o nome não pode conter `{c}`")));
            }
            Ok(value)
        }
        None => Ok(to_snake_case(&ident.to_string())),
    }
}

/// Sufixos de tipo de inteiro: `ttl = 1500u64` continua sendo milissegundos.
const INTEGER_SUFFIXES: [&str; 12] =
    ["u8", "u16", "u32", "u64", "u128", "usize", "i8", "i16", "i32", "i64", "i128", "isize"];

/// `ttl = 1500` (ms), `ttl = 30s` / `10m` / `2h` / `7d` / `1h30m` (sem aspas) ou `ttl = "1h 30m"`.
fn parse_ttl(lit: &Lit) -> syn::Result<u64> {
    const INVALID: &str = "TTL inválido; use milissegundos (`ttl = 1500`) ou uma duração como `500ms`, `30s`, `10m`, `2h`, `7d` ou `\"1h 30m\"`";
    let ms = match lit {
        Lit::Int(int) if int.suffix().is_empty() || INTEGER_SUFFIXES.contains(&int.suffix()) => {
            int.base10_parse::<u64>()?
        }
        // Para o Rust, `10m` sem aspas é o inteiro `10` com o sufixo `m`: o sufixo é a unidade.
        Lit::Int(int) => parse_duration_ms(&format!("{}{}", int.base10_digits(), int.suffix()))
            .ok_or_else(|| syn::Error::new_spanned(int, INVALID))?,
        Lit::Str(s) => parse_duration_ms(&s.value()).ok_or_else(|| syn::Error::new_spanned(s, INVALID))?,
        other => {
            return Err(syn::Error::new_spanned(
                other,
                "TTL deve ser um inteiro (ms) ou uma duração como `10m` ou `\"1h 30m\"`",
            ));
        }
    };
    if ms == 0 {
        return Err(syn::Error::new_spanned(lit, "TTL deve ser maior que zero"));
    }
    Ok(ms)
}

fn parse_duration_ms(s: &str) -> Option<u64> {
    let mut total: u64 = 0;
    let mut rest = s.trim();
    if rest.is_empty() {
        return None;
    }
    while !rest.is_empty() {
        let digits = rest.find(|c: char| !c.is_ascii_digit())?;
        if digits == 0 {
            return None;
        }
        let value: u64 = rest[..digits].parse().ok()?;
        rest = rest[digits..].trim_start();
        let unit_len = rest.find(|c: char| !c.is_ascii_alphabetic()).unwrap_or(rest.len());
        let factor = match &rest[..unit_len] {
            "ms" => 1,
            "s" => 1_000,
            "m" => 60_000,
            "h" => 3_600_000,
            "d" => 86_400_000,
            _ => return None,
        };
        total = total.checked_add(value.checked_mul(factor)?)?;
        rest = rest[unit_len..].trim_start();
    }
    Some(total)
}

fn to_snake_case(ident: &str) -> String {
    let chars: Vec<char> = ident.chars().collect();
    let mut out = String::with_capacity(ident.len() + 4);
    for (i, &c) in chars.iter().enumerate() {
        if c.is_uppercase() {
            let prev = i.checked_sub(1).map(|p| chars[p]);
            let next = chars.get(i + 1);
            let boundary = prev.is_some_and(|p| p.is_lowercase() || p.is_ascii_digit())
                || (prev.is_some_and(char::is_uppercase) && next.is_some_and(|n| n.is_lowercase()));
            if boundary && !out.ends_with('_') {
                out.push('_');
            }
            out.extend(c.to_lowercase());
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{parse_key_template, parse_ttl};

    fn ttl(source: &str) -> syn::Result<u64> {
        parse_ttl(&syn::parse_str(source).unwrap())
    }

    #[test]
    fn ttl_units_with_and_without_quotes() {
        assert_eq!(ttl("1500").unwrap(), 1_500);
        assert_eq!(ttl("1500u64").unwrap(), 1_500);
        assert_eq!(ttl("500ms").unwrap(), 500);
        assert_eq!(ttl("30s").unwrap(), 30_000);
        assert_eq!(ttl("10m").unwrap(), 600_000);
        assert_eq!(ttl("2h").unwrap(), 7_200_000);
        assert_eq!(ttl("7d").unwrap(), 604_800_000);
        assert_eq!(ttl("1h30m").unwrap(), 5_400_000);
        assert_eq!(ttl("1d12h").unwrap(), 129_600_000);
        assert_eq!(ttl("\"1d\"").unwrap(), 86_400_000);
        assert_eq!(ttl("\"1h30m\"").unwrap(), 5_400_000);
        assert_eq!(ttl("\" 1h 30m 15s \"").unwrap(), 5_415_000);
        assert_eq!(ttl("\"2 h\"").unwrap(), 7_200_000);
    }

    #[test]
    fn invalid_ttls() {
        for invalid in ["0", "0s", "10x", "1hour", "\"\"", "\"10\"", "\"h\"", "\"10m extra\"", "1.5", "true"] {
            assert!(ttl(invalid).is_err(), "{invalid} should be rejected");
        }
    }

    #[test]
    fn key_templates() {
        let template = parse_key_template("payments-{business_id}").unwrap();
        assert_eq!(template.format, "payments-{}");
        assert_eq!(template.placeholders, vec![vec!["business_id"]]);
        assert_eq!(template.literals, vec!["payments-", ""]);

        let template = parse_key_template("{tenant}:{ owner.id :05}:{{v1}}").unwrap();
        assert_eq!(template.format, "{}:{:05}:{{v1}}");
        assert_eq!(template.placeholders, vec![vec!["tenant"], vec!["owner", "id"]]);
        assert_eq!(template.specs, vec!["", "05"]);
        assert_eq!(template.literals, vec!["", ":", ":{v1}"]);

        let template = parse_key_template("pagamento-ção-{r#type}-{pair.0}").unwrap();
        assert_eq!(template.format, "pagamento-ção-{}-{}");
        assert_eq!(template.placeholders, vec![vec!["r#type"], vec!["pair", "0"]]);
    }

    #[test]
    fn invalid_key_templates() {
        for invalid in [
            "payments-{self.business_id}",
            "payments-{}",
            "{0}",
            "{.a}",
            "{a b}",
            "{a-b}",
            "{a..b}",
            "x-{id",
            "x}-{id}",
            "{{no placeholder}}",
        ] {
            assert!(parse_key_template(invalid).is_err(), "{invalid} should be rejected");
        }
        assert_eq!(
            parse_key_template("payments-{self.business_id}").unwrap_err(),
            "use `{business_id}` em vez de `{self.business_id}`"
        );
    }
}
