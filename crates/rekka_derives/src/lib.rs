use proc_macro::TokenStream;
use proc_macro2::{Span, TokenStream as TokenStream2};
use quote::quote;
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
/// - `#[rekka(ttl = 60000)]` TTL em milissegundos, ou `ttl = "10m"` (`ms`, `s`, `m`, `h`, `d`).
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
    key: Option<Ident>,
    ttl_ms: Option<u64>,
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
                        lit.parse()?
                    } else {
                        value.parse()?
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
    let key = match &attrs.key {
        Some(key) => match field_named(&key.to_string()) {
            Some(field) => field,
            None => return Err(syn::Error::new_spanned(key, format!("`{}` não tem o campo `{key}`", input.ident))),
        },
        None => match field_named("key").or_else(|| field_named("id")) {
            Some(field) => field,
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
    let krate = attrs.krate();
    let ident = &input.ident;
    let generics = bounded_generics(&input.generics, quote!(#krate::__private::CacheBound));
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics #krate::RekkaCache for #ident #ty_generics #where_clause {
            const NAMESPACE: &'static str = #namespace;
            const TTL: ::core::option::Option<::core::time::Duration> = #ttl;

            #[inline]
            fn cache_key(&self) -> ::std::string::String {
                #krate::ToTaskId::to_task_id(&self.#key)
            }
        }
    })
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

/// `ttl = 1500` (ms) ou `ttl = "1h30m"` (unidades `ms`, `s`, `m`, `h`, `d`).
fn parse_ttl(lit: &Lit) -> syn::Result<u64> {
    let ms = match lit {
        Lit::Int(int) => int.base10_parse::<u64>()?,
        Lit::Str(s) => parse_duration_ms(&s.value()).ok_or_else(|| {
            syn::Error::new_spanned(s, "TTL inválido; use milissegundos (`ttl = 1500`) ou `\"30s\"`, `\"10m\"`, `\"1h30m\"`, `\"7d\"`")
        })?,
        other => return Err(syn::Error::new_spanned(other, "TTL deve ser um inteiro (ms) ou uma string como `\"10m\"`")),
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
        rest = &rest[digits..];
        let unit_len = rest.find(|c: char| c.is_ascii_digit()).unwrap_or(rest.len());
        let factor = match &rest[..unit_len] {
            "ms" => 1,
            "s" => 1_000,
            "m" => 60_000,
            "h" => 3_600_000,
            "d" => 86_400_000,
            _ => return None,
        };
        total = total.checked_add(value.checked_mul(factor)?)?;
        rest = &rest[unit_len..];
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
