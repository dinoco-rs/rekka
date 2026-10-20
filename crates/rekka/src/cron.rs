//! Expressões cron (`cronTime`), no formato do `node-cron`:
//!
//! ```text
//! ┌────────────── segundo (0-59, opcional)
//! │ ┌──────────── minuto (0-59)
//! │ │ ┌────────── hora (0-23)
//! │ │ │ ┌──────── dia do mês (1-31)
//! │ │ │ │ ┌────── mês (1-12 ou jan-dec)
//! │ │ │ │ │ ┌──── dia da semana (0-7 ou sun-sat; 0 e 7 = domingo)
//! * * * * * *
//! ```
//!
//! Cada campo aceita `*`, valores (`5`), intervalos (`1-5`), listas (`1,15,30`) e passos
//! (`*/2`, `10-30/5`, `5/15`). Com 5 campos, o segundo é `0`. Como no cron padrão, quando dia do
//! mês e dia da semana são restritos (nenhum começa com `*`), basta um dos dois casar.

use chrono::{
    DateTime, Datelike, Duration as ChronoDuration, LocalResult, NaiveDate, NaiveDateTime, TimeZone, Timelike, Utc,
};

const MONTHS: [&str; 12] = ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"];
const MONTHS_LONG: [&str; 12] = [
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december",
];
const WEEKDAYS: [&str; 7] = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];
const WEEKDAYS_LONG: [&str; 7] = ["sunday", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday"];

/// Quantos anos procurar a próxima execução (`29 2 *` pode levar 8 anos).
const SEARCH_YEARS: i32 = 10;

/// Expressão cron já validada. Cada campo é um bitmask dos valores aceitos.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CronTime {
    seconds: u64,
    minutes: u64,
    hours: u64,
    days: u64,
    months: u64,
    weekdays: u64,
    /// O campo começa com `*` (decide entre E/OU entre dia do mês e dia da semana).
    days_star: bool,
    weekdays_star: bool,
}

struct FieldSpec {
    name: &'static str,
    min: u32,
    max: u32,
    names: &'static [&'static [&'static str]],
}

const SECOND: FieldSpec = FieldSpec { name: "second", min: 0, max: 59, names: &[] };
const MINUTE: FieldSpec = FieldSpec { name: "minute", min: 0, max: 59, names: &[] };
const HOUR: FieldSpec = FieldSpec { name: "hour", min: 0, max: 23, names: &[] };
const DAY: FieldSpec = FieldSpec { name: "day of month", min: 1, max: 31, names: &[] };
const MONTH: FieldSpec = FieldSpec { name: "month", min: 1, max: 12, names: &[&MONTHS, &MONTHS_LONG] };
const WEEKDAY: FieldSpec = FieldSpec { name: "day of week", min: 0, max: 7, names: &[&WEEKDAYS, &WEEKDAYS_LONG] };

impl CronTime {
    pub(crate) fn parse(expr: &str) -> Result<Self, String> {
        let fields: Vec<&str> = expr.split_whitespace().collect();
        let (second, rest) = match fields.len() {
            6 => (fields[0], &fields[1..]),
            5 => ("0", &fields[..]),
            n => return Err(format!("expected 5 or 6 fields (got {n})")),
        };
        let mut weekdays = parse_field(rest[4], &WEEKDAY)?;
        // 7 também é domingo.
        if weekdays & (1 << 7) != 0 {
            weekdays = (weekdays & !(1 << 7)) | 1;
        }
        Ok(Self {
            seconds: parse_field(second, &SECOND)?,
            minutes: parse_field(rest[0], &MINUTE)?,
            hours: parse_field(rest[1], &HOUR)?,
            days: parse_field(rest[2], &DAY)?,
            months: parse_field(rest[3], &MONTH)?,
            weekdays,
            days_star: rest[2].starts_with('*'),
            weekdays_star: rest[4].starts_with('*'),
        })
    }

    /// Próximo instante estritamente depois de `after`, avaliando a expressão no fuso `tz`.
    /// Horários que não existem no fuso (início do horário de verão) são pulados; horários
    /// repetidos (fim do horário de verão) executam uma vez.
    pub(crate) fn next_after<Tz: TimeZone>(&self, after: DateTime<Utc>, tz: &Tz) -> Option<DateTime<Utc>> {
        let local = after.with_timezone(tz).naive_local();
        let mut t = local.with_nanosecond(0)? + ChronoDuration::seconds(1);
        let limit = t.year() + SEARCH_YEARS;
        while t.year() <= limit {
            if !has(self.months, t.month()) {
                let (y, m) = if t.month() == 12 { (t.year() + 1, 1) } else { (t.year(), t.month() + 1) };
                t = NaiveDate::from_ymd_opt(y, m, 1)?.and_hms_opt(0, 0, 0)?;
                continue;
            }
            if !self.day_matches(t.date()) {
                t = t.date().succ_opt()?.and_hms_opt(0, 0, 0)?;
                continue;
            }
            if !has(self.hours, t.hour()) {
                t = truncate(t, 3600)? + ChronoDuration::hours(1);
                continue;
            }
            if !has(self.minutes, t.minute()) {
                t = truncate(t, 60)? + ChronoDuration::minutes(1);
                continue;
            }
            if !has(self.seconds, t.second()) {
                t += ChronoDuration::seconds(1);
                continue;
            }
            let found = match tz.from_local_datetime(&t) {
                LocalResult::Single(at) => Some(at),
                LocalResult::Ambiguous(first, second) => {
                    [first, second].into_iter().find(|at| at.with_timezone(&Utc) > after)
                }
                LocalResult::None => None,
            };
            if let Some(at) = found.map(|at| at.with_timezone(&Utc))
                && at > after
            {
                return Some(at);
            }
            t += ChronoDuration::seconds(1);
        }
        None
    }

    fn day_matches(&self, date: NaiveDate) -> bool {
        let day = has(self.days, date.day());
        let weekday = has(self.weekdays, date.weekday().num_days_from_sunday());
        if self.days_star || self.weekdays_star { day && weekday } else { day || weekday }
    }
}

fn has(mask: u64, value: u32) -> bool {
    mask & (1 << value) != 0
}

/// Zera o que estiver abaixo de `seconds` (60 = minuto, 3600 = hora).
fn truncate(t: NaiveDateTime, seconds: u32) -> Option<NaiveDateTime> {
    let secs = t.num_seconds_from_midnight() / seconds * seconds;
    t.date().and_hms_opt(secs / 3600, secs % 3600 / 60, secs % 60)
}

fn parse_field(field: &str, spec: &FieldSpec) -> Result<u64, String> {
    let mut mask = 0u64;
    for part in field.split(',') {
        let (range, step) = match part.split_once('/') {
            Some((range, step)) => {
                let step: u32 =
                    step.parse().map_err(|_| format!("invalid step `{step}` in {} `{field}`", spec.name))?;
                if step == 0 {
                    return Err(format!("step must be greater than zero in {} `{field}`", spec.name));
                }
                (range, Some(step))
            }
            None => (part, None),
        };
        let (lo, hi) = if range == "*" {
            (spec.min, spec.max)
        } else if let Some((lo, hi)) = range.split_once('-') {
            (value(lo, spec, field)?, value(hi, spec, field)?)
        } else {
            let v = value(range, spec, field)?;
            // `5/15` = de 5 até o fim, de 15 em 15.
            (v, if step.is_some() { spec.max } else { v })
        };
        if lo > hi {
            return Err(format!("range `{range}` is reversed in {} `{field}`", spec.name));
        }
        let step = step.unwrap_or(1);
        let mut v = lo;
        while v <= hi {
            mask |= 1 << v;
            v += step;
        }
    }
    Ok(mask)
}

fn value(raw: &str, spec: &FieldSpec, field: &str) -> Result<u32, String> {
    let lower = raw.to_ascii_lowercase();
    for names in spec.names {
        if let Some(i) = names.iter().position(|n| *n == lower) {
            return Ok(i as u32 + if spec.min == 1 { 1 } else { 0 });
        }
    }
    let v: u32 = raw.parse().map_err(|_| format!("invalid value `{raw}` in {} `{field}`", spec.name))?;
    if v < spec.min || v > spec.max {
        return Err(format!("{} `{v}` is out of range {}-{}", spec.name, spec.min, spec.max));
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    use chrono::{FixedOffset, TimeZone, Utc};

    use super::CronTime;

    fn next(expr: &str, after: &str) -> String {
        let after = after.parse().unwrap();
        CronTime::parse(expr).unwrap().next_after(after, &Utc).unwrap().format("%Y-%m-%d %H:%M:%S %a").to_string()
    }

    #[test]
    fn five_and_six_fields() {
        assert_eq!(next("* * * * *", "2026-10-20T10:00:00Z"), "2026-10-20 10:01:00 Tue");
        assert_eq!(next("* * * * * *", "2026-10-20T10:00:00Z"), "2026-10-20 10:00:01 Tue");
        assert_eq!(next("* * * * * *", "2026-10-20T10:00:00.900Z"), "2026-10-20 10:00:01 Tue");
        assert_eq!(next("30 9 * * *", "2026-10-20T10:00:00Z"), "2026-10-21 09:30:00 Wed");
        assert_eq!(next("15 30 9 * * *", "2026-10-20T09:30:15Z"), "2026-10-21 09:30:15 Wed");
    }

    #[test]
    fn steps_ranges_and_lists() {
        assert_eq!(next("*/2 * * * *", "2026-10-20T10:00:00Z"), "2026-10-20 10:02:00 Tue");
        assert_eq!(next("*/2 * * * *", "2026-10-20T10:01:59Z"), "2026-10-20 10:02:00 Tue");
        assert_eq!(next("*/15 * * * * *", "2026-10-20T10:00:44Z"), "2026-10-20 10:00:45 Tue");
        assert_eq!(next("10-30/10 * * * *", "2026-10-20T10:30:00Z"), "2026-10-20 11:10:00 Tue");
        assert_eq!(next("5/20 * * * *", "2026-10-20T10:26:00Z"), "2026-10-20 10:45:00 Tue");
        assert_eq!(next("0 8,12,18 * * *", "2026-10-20T12:00:00Z"), "2026-10-20 18:00:00 Tue");
        assert_eq!(next("0 0 1 */3 *", "2026-10-20T00:00:00Z"), "2027-01-01 00:00:00 Fri");
    }

    #[test]
    fn names_and_sunday_as_seven() {
        assert_eq!(next("0 9 * * mon-fri", "2026-10-23T10:00:00Z"), "2026-10-26 09:00:00 Mon");
        assert_eq!(next("0 9 * * 7", "2026-10-20T10:00:00Z"), "2026-10-25 09:00:00 Sun");
        assert_eq!(next("0 9 * * 0", "2026-10-20T10:00:00Z"), "2026-10-25 09:00:00 Sun");
        assert_eq!(next("0 9 * * 5-7", "2026-10-24T10:00:00Z"), "2026-10-25 09:00:00 Sun");
        assert_eq!(next("0 0 1 JAN *", "2026-10-20T10:00:00Z"), "2027-01-01 00:00:00 Fri");
        assert_eq!(next("0 0 1 december Sunday", "2026-10-20T10:00:00Z"), "2026-12-01 00:00:00 Tue");
    }

    #[test]
    fn day_of_month_or_day_of_week() {
        // Dia 13 OU sexta-feira (5): sexta 23/10 vem antes de 13/11.
        assert_eq!(next("0 0 13 * 5", "2026-10-20T00:00:00Z"), "2026-10-23 00:00:00 Fri");
        assert_eq!(next("0 0 13 * 5", "2026-11-12T00:00:00Z"), "2026-11-13 00:00:00 Fri");
        assert_eq!(next("0 0 13 * 5", "2026-12-12T00:00:00Z"), "2026-12-13 00:00:00 Sun");
        // 0 e 7 são domingo.
        assert_eq!(next("0 0 13 * 0", "2026-10-20T00:00:00Z"), "2026-10-25 00:00:00 Sun");
        assert_eq!(next("0 0 13 * 7", "2026-10-20T00:00:00Z"), "2026-10-25 00:00:00 Sun");
        // Com segundos: o primeiro campo é o segundo, o resto não muda.
        assert_eq!(next("30 0 0 13 * 5", "2026-10-20T00:00:00Z"), "2026-10-23 00:00:30 Fri");
        // Os dois restritos: dia 1 OU segunda-feira.
        assert_eq!(next("0 0 1 * mon", "2026-10-20T00:00:00Z"), "2026-10-26 00:00:00 Mon");
        // Dia da semana com `*/…` conta como `*`: precisa casar os dois.
        assert_eq!(next("0 0 13 * */1", "2026-10-20T00:00:00Z"), "2026-11-13 00:00:00 Fri");
        assert_eq!(next("0 0 29 2 *", "2026-10-20T00:00:00Z"), "2028-02-29 00:00:00 Tue");
        assert_eq!(next("0 0 31 * *", "2026-11-01T00:00:00Z"), "2026-12-31 00:00:00 Thu");
    }

    #[test]
    fn evaluates_in_the_given_timezone() {
        let sao_paulo = FixedOffset::west_opt(3 * 3600).unwrap();
        let cron = CronTime::parse("0 9 * * *").unwrap();
        let after = Utc.with_ymd_and_hms(2026, 10, 20, 11, 0, 0).unwrap();
        assert_eq!(cron.next_after(after, &sao_paulo).unwrap(), Utc.with_ymd_and_hms(2026, 10, 20, 12, 0, 0).unwrap());
        let after = Utc.with_ymd_and_hms(2026, 10, 20, 12, 0, 0).unwrap();
        assert_eq!(cron.next_after(after, &sao_paulo).unwrap(), Utc.with_ymd_and_hms(2026, 10, 21, 12, 0, 0).unwrap());
    }

    #[test]
    fn impossible_dates_never_match() {
        let cron = CronTime::parse("0 0 30 2 *").unwrap();
        assert_eq!(cron.next_after(Utc::now(), &Utc), None);
    }

    #[test]
    fn invalid_expressions() {
        for invalid in [
            "",
            "* * * *",
            "* * * * * * *",
            "60 * * * *",
            "* 24 * * *",
            "* * 0 * *",
            "* * 32 * *",
            "* * * 13 *",
            "* * * * 8",
            "*/0 * * * *",
            "5-1 * * * *",
            "a * * * *",
            "* * * foo *",
            "1,,2 * * * *",
            "*/x * * * *",
        ] {
            assert!(CronTime::parse(invalid).is_err(), "{invalid:?} should be rejected");
        }
    }
}
