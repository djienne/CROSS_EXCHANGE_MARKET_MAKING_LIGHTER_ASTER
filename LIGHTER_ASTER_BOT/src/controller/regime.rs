//! Regime switch: which engine holds execution rights for the market. A line-by-line port of
//! the retired orchestrator.py `decide()` and its helpers; `tests::python_decisions_match`
//! replays decisions recorded from the Python original (`tests/fixtures/regime_decisions.json.zst`
//! at the stack root).
//!
//! The inputs are the engines' status reports as JSON (`taker::status::StatusReport` and
//! `livebot::status::StatusReport`), read with the original's lenient semantics:
//! * a number is `Decimal(str(x))` and must be finite; anything else is unknown (`None`);
//! * `dec_or_zero` fields (required edge, margin buffer) read unknown as zero;
//! * an empty report object counts as absent where the original tested truthiness.

use std::collections::HashMap;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde_json::{json, Map, Value};
use tokio::time::{Duration, Instant};

use super::iso;

/// Bot labels kept from the two-process era: persisted state and reports use them.
pub const TAKER_BOT: &str = "LIGHTER_ASTER_TAKER_ARB";
pub const XEMM_BOT: &str = "XEMM_LIGHTER_ASTER";
/// `clip()` fallback when no status names a desired notional.
const DEFAULT_CLIP_USD: Decimal = rust_decimal_macros::dec!(13);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bot {
    Taker,
    Xemm,
}

impl Bot {
    pub fn label(self) -> &'static str {
        match self {
            Bot::Taker => TAKER_BOT,
            Bot::Xemm => XEMM_BOT,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TakerMode {
    Normal,
    Reduce,
}

impl TakerMode {
    pub fn as_str(self) -> &'static str {
        match self {
            TakerMode::Normal => "normal",
            TakerMode::Reduce => "reduce",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Target {
    SafeHalt,
    Bot(Bot),
}

impl Target {
    pub fn label(self) -> &'static str {
        match self {
            Target::SafeHalt => "SAFE_HALT",
            Target::Bot(bot) => bot.label(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Decision {
    pub target: Target,
    pub reason: &'static str,
    /// Set by the branches that name a mode; `resume_taker` leaves it to the switch (normal).
    pub taker_mode: Option<TakerMode>,
    pub details: Map<String, Value>,
}

impl Decision {
    fn new(target: Target, reason: &'static str, taker_mode: Option<TakerMode>, details: Map<String, Value>) -> Self {
        Self { target, reason, taker_mode, details }
    }

    pub fn to_json(&self) -> Value {
        let mut body = Map::new();
        body.insert("target".into(), json!(self.target.label()));
        body.insert("reason".into(), json!(self.reason));
        if let Some(mode) = self.taker_mode {
            body.insert("taker_mode".into(), json!(mode.as_str()));
        }
        body.insert("details".into(), Value::Object(self.details.clone()));
        Value::Object(body)
    }
}

/// Switch thresholds (`[controller]` defaults: 90 s blocked, 45 s ready, 2/3 clips headroom and
/// margin, near-flat at one clip).
#[derive(Debug, Clone)]
pub struct Thresholds {
    pub blocked_confirm: Duration,
    pub resume_confirm: Duration,
    pub switch_headroom_clips: Decimal,
    pub switch_margin_clips: Decimal,
    pub resume_headroom_clips: Decimal,
    pub resume_margin_clips: Decimal,
    /// `<= 0` means one clip.
    pub near_flat_notional_usd: Decimal,
}

/// The reduce-only execution lease held by the promoted observer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReduceLease {
    pub id: String,
    pub started_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub max_expires_at: DateTime<Utc>,
}

/// Which engine is active, the reduce lease, and the confirm-window timers `decide` reads.
#[derive(Debug, Clone)]
pub struct Regime {
    pub thresholds: Thresholds,
    pub active: Option<Bot>,
    pub taker_mode: TakerMode,
    pub lease: Option<ReduceLease>,
    /// Monotonic: an NTP step must neither satisfy nor reset a confirm window. Cleared on
    /// every mode switch — each key is evaluated in one mode only, and a stale start would
    /// collapse the window to zero on re-entry (the 2026-07-07 flap storm).
    condition_since: HashMap<&'static str, Instant>,
}

impl Regime {
    pub fn new(thresholds: Thresholds) -> Self {
        Self { thresholds, active: None, taker_mode: TakerMode::Normal, lease: None, condition_since: HashMap::new() }
    }

    pub fn clear_confirm_windows(&mut self) {
        self.condition_since.clear();
    }

    pub fn lease_active(&self, now_utc: DateTime<Utc>) -> bool {
        self.lease.as_ref().is_some_and(|lease| now_utc < lease.expires_at)
    }

    /// `taker`/`xemm` are the latest status reports (`None` when the poll failed).
    pub fn decide(&mut self, taker: Option<&Value>, xemm: Option<&Value>, now: Instant, now_utc: DateTime<Utc>) -> Decision {
        if let Some(xemm) = truthy(xemm) {
            if xemm.get("reduce_position_only") != Some(&Value::Bool(true)) {
                let mut details = Map::new();
                details.insert("reduce_position_only".into(), xemm.get("reduce_position_only").cloned().unwrap_or(Value::Null));
                return Decision::new(Target::SafeHalt, "xemm_reduce_position_only_disabled", None, details);
            }
        }
        let Some(status) = truthy(taker).or(truthy(xemm)) else {
            return Decision::new(Target::SafeHalt, "no_status", None, Map::new());
        };
        match self.active {
            Some(Bot::Taker) => {
                if self.taker_mode == TakerMode::Reduce {
                    let near_flat = self.near_flat(status);
                    let mut details = position_details(status);
                    details.insert("near_flat".into(), json!(near_flat));
                    let lease = self.lease.as_ref();
                    details.insert("reduce_lease_id".into(), json!(lease.map(|l| l.id.clone())));
                    details.insert("reduce_lease_expires_at".into(), json!(lease.map(|l| iso(l.expires_at))));
                    details.insert("reduce_lease_max_expires_at".into(), json!(lease.map(|l| iso(l.max_expires_at))));
                    if near_flat {
                        return Decision::new(Target::Bot(Bot::Taker), "reduce_lease_near_flat_resume_normal_taker", Some(TakerMode::Normal), details);
                    }
                    if !self.lease_active(now_utc) {
                        return Decision::new(Target::Bot(Bot::Xemm), "reduce_lease_expired_resume_xemm", None, details);
                    }
                    return Decision::new(Target::Bot(Bot::Taker), "reduce_lease_active", Some(TakerMode::Reduce), details);
                }
                let (blocked, details) = self.taker_blocked(taker);
                if self.sustained("taker_margin_limited", blocked, self.thresholds.blocked_confirm, now) {
                    return Decision::new(Target::Bot(Bot::Xemm), "taker_margin_limited", None, details);
                }
                Decision::new(Target::Bot(Bot::Taker), "taker_active", Some(TakerMode::Normal), details)
            }
            Some(Bot::Xemm) => {
                let (ready, details) = self.ready_for_taker(status, taker);
                if self.sustained("ready_for_taker", ready, self.thresholds.resume_confirm, now) {
                    return Decision::new(Target::Bot(Bot::Taker), "resume_taker", None, details);
                }
                Decision::new(Target::Bot(Bot::Xemm), "xemm_reduce_only_active", None, details)
            }
            None => {
                let (ready, details) = self.ready_for_taker(status, taker);
                if ready {
                    return Decision::new(Target::Bot(Bot::Taker), "bootstrap_taker_preferred", Some(TakerMode::Normal), details);
                }
                Decision::new(Target::Bot(Bot::Xemm), "bootstrap_margin_limited_reduce_existing_position", None, details)
            }
        }
    }

    fn taker_blocked(&self, status: Option<&Value>) -> (bool, Map<String, Value>) {
        let (margin_limited, mut details) =
            self.taker_margin_state(status, self.thresholds.switch_headroom_clips, self.thresholds.switch_margin_clips);
        let executable_reduce = details.get("taker_executable_reduce") == Some(&Value::Bool(true));
        let blocked = margin_limited && !executable_reduce;
        details.insert("taker_blocked".into(), json!(blocked));
        (blocked, details)
    }

    /// Margin-limited at the given clip thresholds: blocking uses the switch clips, resuming
    /// the higher resume clips, so headroom hovering at one boundary cannot flip every tick.
    fn taker_margin_state(&self, status: Option<&Value>, headroom_clips: Decimal, margin_clips: Decimal) -> (bool, Map<String, Value>) {
        let Some(status) = truthy(status) else {
            let mut details = Map::new();
            details.insert("status".into(), json!("missing"));
            details.insert("taker_margin_limited".into(), json!(false));
            return (false, details);
        };
        let required = dec_or_zero(status.get("required_gross_edge_bps"));
        let opportunities: &[Value] = match status.get("opportunities") {
            Some(Value::Array(items)) => items,
            _ => &[],
        };
        let str_field = |o: &Value, key: &str| o.get(key).and_then(Value::as_str).map(str::to_owned);
        let profitable: Vec<&Value> = opportunities
            .iter()
            .filter(|o| py_decimal(o.get("gross_edge_bps")).is_some_and(|edge| edge >= required))
            .collect();
        let executable_reduce = opportunities
            .iter()
            .filter(|o| str_field(o, "limiting_reason").as_deref() == Some("ok") && str_field(o, "exposure_effect").as_deref() == Some("reduce"))
            .count();
        let executable_any = opportunities.iter().filter(|o| str_field(o, "limiting_reason").as_deref() == Some("ok")).count();
        let profitable_increase = profitable.iter().filter(|o| str_field(o, "exposure_effect").as_deref() == Some("increase")).count();
        let margin_binding: Vec<Value> = profitable
            .iter()
            .filter(|o| matches!(str_field(o, "limiting_reason").as_deref(), Some("headroom" | "margin")))
            .map(|o| o.get("limiting_reason").cloned().unwrap_or(Value::Null))
            .collect();
        let clip = clip(Some(status));
        let headroom = py_decimal(object(status.get("positions")).and_then(|p| p.get("headroom_notional_usd")));
        let accounts = object(status.get("accounts"));
        let margin_buffer = dec_or_zero(status.get("margin_buffer_usd"));
        let aster_available = py_decimal(accounts.and_then(|a| a.get("aster_available_usd")));
        let lighter_available = py_decimal(accounts.and_then(|a| a.get("lighter_available_usd")));
        let min_available = match (aster_available, lighter_available) {
            (Some(a), Some(l)) => Some(a.min(l)),
            _ => None,
        };
        let low_headroom = headroom.is_some_and(|h| h < clip * headroom_clips);
        let low_margin = min_available.is_some_and(|m| m - margin_buffer < clip * margin_clips);
        let margin_limited = !margin_binding.is_empty() || low_headroom || low_margin;
        let mut details = Map::new();
        details.insert("headroom_clips".into(), dec_json(Some(headroom_clips)));
        details.insert("margin_clips".into(), dec_json(Some(margin_clips)));
        details.insert("profitable".into(), json!(profitable.len()));
        details.insert("profitable_increase".into(), json!(profitable_increase));
        details.insert("executable_any".into(), json!(executable_any));
        details.insert("executable_reduce".into(), json!(executable_reduce));
        details.insert("taker_executable_reduce".into(), json!(executable_reduce > 0));
        details.insert("margin_binding_reasons".into(), Value::Array(margin_binding));
        details.insert("headroom_notional_usd".into(), dec_json(headroom));
        details.insert("min_available_usd".into(), dec_json(min_available));
        details.insert("low_headroom".into(), json!(low_headroom));
        details.insert("low_margin".into(), json!(low_margin));
        details.insert("taker_margin_limited".into(), json!(margin_limited));
        (margin_limited, details)
    }

    /// Never cold-starts the taker without a taker status: near-flat alone says nothing about
    /// taker margin, and a missed poll only delays the resume by a tick.
    fn ready_for_taker(&self, position_status: &Value, taker_status: Option<&Value>) -> (bool, Map<String, Value>) {
        let near_flat = self.near_flat(position_status);
        let (margin_limited, margin_details) = if truthy(taker_status).is_some() {
            self.taker_margin_state(taker_status, self.thresholds.resume_headroom_clips, self.thresholds.resume_margin_clips)
        } else {
            let mut details = Map::new();
            details.insert("status".into(), json!("missing"));
            details.insert("taker_margin_limited".into(), json!(false));
            (false, details)
        };
        let executable_reduce = margin_details.get("taker_executable_reduce") == Some(&Value::Bool(true));
        // An empty (but present) taker report still counts as present, as in the original.
        let ready = taker_status.is_some() && (near_flat || executable_reduce || !margin_limited);
        let ready_reason = if taker_status.is_none() {
            "taker_status_missing"
        } else if near_flat {
            "near_flat"
        } else if executable_reduce {
            "taker_executable_reduce"
        } else if !margin_limited {
            "taker_margin_available"
        } else {
            "taker_margin_limited"
        };
        let mut details = position_details(position_status);
        details.insert("near_flat".into(), json!(near_flat));
        details.insert("taker_executable_reduce".into(), json!(executable_reduce));
        details.insert("taker_margin_limited".into(), json!(margin_limited));
        details.insert("ready_for_taker".into(), json!(ready));
        details.insert("ready_reason".into(), json!(ready_reason));
        details.extend(margin_details);
        (ready, details)
    }

    fn sustained(&mut self, key: &'static str, condition: bool, window: Duration, now: Instant) -> bool {
        if !condition {
            self.condition_since.remove(key);
            return false;
        }
        let first = *self.condition_since.entry(key).or_insert(now);
        now.saturating_duration_since(first) >= window
    }

    pub fn near_flat(&self, status: &Value) -> bool {
        let Some(abs_position) = py_decimal(object(status.get("positions")).and_then(|p| p.get("abs_position_notional_usd"))) else {
            return false;
        };
        let mut threshold = self.thresholds.near_flat_notional_usd;
        if threshold <= Decimal::ZERO {
            threshold = clip(Some(status));
        }
        abs_position <= threshold
    }
}

/// The clip notional a status reports (`desired_notional_usd`, which both engines' reports
/// carry, else a legacy `quote.desired_notional`), else $13. A zero value falls through, like
/// Python's `or`.
pub fn clip(status: Option<&Value>) -> Decimal {
    let Some(status) = truthy(status) else { return DEFAULT_CLIP_USD };
    let nonzero = |d: Option<Decimal>| d.filter(|d| !d.is_zero());
    nonzero(py_decimal(status.get("desired_notional_usd")))
        .or_else(|| nonzero(py_decimal(object(status.get("quote")).and_then(|q| q.get("desired_notional")))))
        .unwrap_or(DEFAULT_CLIP_USD)
}

pub fn position_details(status: &Value) -> Map<String, Value> {
    let positions = object(status.get("positions"));
    let field = |key: &str| positions.and_then(|p| p.get(key)).cloned().unwrap_or(Value::Null);
    let mut details = Map::new();
    for key in ["aster_qty", "lighter_qty", "abs_position_notional_usd", "headroom_notional_usd"] {
        details.insert(key.into(), field(key));
    }
    details
}

/// Python truthiness of a status dict: absent, null or an empty object is falsy.
fn truthy(value: Option<&Value>) -> Option<&Value> {
    value.filter(|v| match v {
        Value::Object(map) => !map.is_empty(),
        Value::Null => false,
        _ => true,
    })
}

/// `x.get(key) or {}`: a falsy or non-object sub-report reads as empty.
fn object(value: Option<&Value>) -> Option<&Map<String, Value>> {
    value.and_then(Value::as_object)
}

/// `Decimal(str(raw))` when finite, else `None` (economics.optional_decimal).
pub fn py_decimal(raw: Option<&Value>) -> Option<Decimal> {
    match raw? {
        Value::String(text) => parse_decimal_text(text),
        Value::Number(number) => parse_decimal_text(&number.to_string()),
        // str(True) / str([..]) / str({..}) are not decimals; None is unknown.
        Value::Null | Value::Bool(_) | Value::Array(_) | Value::Object(_) => None,
    }
}

/// Python `Decimal(text)` for plain and exponent forms. Infinity/NaN are not finite, so they
/// are unknown here as in the original. Ponytail: exotic Python spellings (digit
/// underscores, over-long precision) are not reproduced; engine reports never use them.
fn parse_decimal_text(text: &str) -> Option<Decimal> {
    let text = text.trim();
    if text.is_empty() {
        return None;
    }
    Decimal::from_str(text).ok().or_else(|| {
        let has_exponent = text.contains(['e', 'E']);
        has_exponent.then(|| Decimal::from_scientific(text).ok()).flatten()
    })
}

fn dec_or_zero(raw: Option<&Value>) -> Decimal {
    py_decimal(raw).unwrap_or(Decimal::ZERO)
}

fn dec_json(value: Option<Decimal>) -> Value {
    value.map_or(Value::Null, |d| json!(d.normalize().to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The detail keys the fixture records (`DETAIL_KEYS` in gen_regime_fixture.py).
    const DETAIL_KEYS: [&str; 17] = [
        "near_flat", "ready_for_taker", "ready_reason", "taker_blocked", "taker_margin_limited", "low_headroom",
        "low_margin", "profitable", "profitable_increase", "executable_any", "executable_reduce",
        "taker_executable_reduce", "margin_binding_reasons", "headroom_notional_usd", "min_available_usd", "status",
        "reduce_position_only",
    ];

    fn thresholds(raw: &Value) -> Thresholds {
        let dec = |key: &str| Decimal::from_str(raw[key].as_str().unwrap()).unwrap();
        let secs = |key: &str| Duration::from_secs_f64(raw[key].as_f64().unwrap());
        Thresholds {
            blocked_confirm: secs("blocked"),
            resume_confirm: secs("resume"),
            switch_headroom_clips: dec("sh"),
            switch_margin_clips: dec("sm"),
            resume_headroom_clips: dec("rh"),
            resume_margin_clips: dec("rm"),
            near_flat_notional_usd: dec("nf"),
        }
    }

    /// Replays the decisions recorded from the real orchestrator.py `decide()`
    /// (tests/fixtures/gen_regime_fixture.py, seed 20260923): per step the same explicit state
    /// (active engine, taker mode, reduce lease), the same confirm-window clears, the same
    /// clocks and statuses must give the same target, reason, taker mode and recorded details.
    #[test]
    fn python_decisions_match() {
        let raw = zstd::decode_all(&include_bytes!("../../../tests/fixtures/regime_decisions.json.zst")[..]).unwrap();
        let fixture: Value = serde_json::from_slice(&raw).unwrap();
        let base_utc: DateTime<Utc> = fixture["base_utc"].as_str().unwrap().parse().unwrap();
        let utc_at = |offset: &Value| base_utc + chrono::Duration::microseconds((offset.as_f64().unwrap() * 1e6).round() as i64);
        let (taker_pool, xemm_pool) = (fixture["taker_pool"].as_array().unwrap(), fixture["xemm_pool"].as_array().unwrap());
        let base = Instant::now();
        let (mut steps, mut mismatches) = (0, Vec::new());
        for (s, scenario) in fixture["scenarios"].as_array().unwrap().iter().enumerate() {
            let mut regime = Regime::new(thresholds(&scenario["thresholds"]));
            for (i, step) in scenario["steps"].as_array().unwrap().iter().enumerate() {
                if step["clear"].as_bool().unwrap() {
                    regime.clear_confirm_windows();
                }
                regime.active = match step["active"].as_str().unwrap() {
                    "none" => None,
                    "taker" => Some(Bot::Taker),
                    "xemm" => Some(Bot::Xemm),
                    other => panic!("unknown active {other}"),
                };
                regime.taker_mode = if step["taker_mode"] == "reduce" { TakerMode::Reduce } else { TakerMode::Normal };
                regime.lease = step["lease"].as_object().map(|lease| ReduceLease {
                    id: lease["id"].as_str().unwrap().to_owned(),
                    started_at: base_utc,
                    expires_at: utc_at(&lease["expires_at_offset"]),
                    max_expires_at: utc_at(&lease["max_expires_at_offset"]),
                });
                let taker = step["taker"].as_u64().map(|index| &taker_pool[index as usize]);
                let xemm = step["xemm"].as_u64().map(|index| &xemm_pool[index as usize]);
                let now = base + Duration::from_secs_f64(step["t"].as_f64().unwrap());
                let decision = regime.decide(taker, xemm, now, utc_at(&step["t"]));
                let expected = &step["expected"];
                let same = decision.target.label() == expected["target"]
                    && decision.reason == expected["reason"]
                    && decision.taker_mode.map(TakerMode::as_str) == expected["taker_mode"].as_str()
                    && DETAIL_KEYS.iter().all(|key| decision.details.get(*key) == expected["details"].get(*key));
                if !same {
                    mismatches.push(format!("scenario {s} step {i}: got {} expected {expected}", decision.to_json()));
                }
                steps += 1;
            }
        }
        assert_eq!(steps, 5054, "fixture step count");
        assert!(mismatches.is_empty(), "{} of {steps} decisions differ:\n{}", mismatches.len(), mismatches[..mismatches.len().min(8)].join("\n"));
    }
}
