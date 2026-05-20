use serde_json::Value;

use crate::types::{Event, GroupIdentify, PropertyType, TupleKey};

/// Length cap on `property_key` in Unicode codepoints. Matches Django
/// `PropertyDefinition.name` max_length.
pub const MAX_PROPERTY_KEY_LEN: usize = 400;

/// Length cap on `property_value` in Unicode codepoints. Strictly less than
/// this, 256 codepoints is dropped.
pub const MAX_PROPERTY_VALUE_LEN: usize = 256;

/// Fan one Event out to its constituent property-value tuples.
///
/// For each entry in `properties` / `person_properties` / `groupN_properties`:
///   - drop entries whose JSON value is `null`
///   - coerce non-string values to their JSON string form (numbers, bools, etc.)
///   - drop entries with empty `property_key` or `property_key` longer than 400 chars
///   - drop entries with empty `property_value` or `property_value` 256+ chars
pub fn fan_out(event: &Event) -> Vec<TupleKey> {
    let mut out = Vec::new();

    if let Some(raw) = &event.properties {
        emit_from_blob(event.team_id, PropertyType::Event, raw, &mut out);
    }
    if let Some(raw) = &event.person_properties {
        emit_from_blob(event.team_id, PropertyType::Person, raw, &mut out);
    }
    if let Some(raw) = &event.group0_properties {
        emit_from_blob(event.team_id, PropertyType::Group0, raw, &mut out);
    }
    if let Some(raw) = &event.group1_properties {
        emit_from_blob(event.team_id, PropertyType::Group1, raw, &mut out);
    }
    if let Some(raw) = &event.group2_properties {
        emit_from_blob(event.team_id, PropertyType::Group2, raw, &mut out);
    }
    if let Some(raw) = &event.group3_properties {
        emit_from_blob(event.team_id, PropertyType::Group3, raw, &mut out);
    }
    if let Some(raw) = &event.group4_properties {
        emit_from_blob(event.team_id, PropertyType::Group4, raw, &mut out);
    }

    out
}

/// Fan one $groupidentify message out to its constituent (group_N, key, value)
/// tuples. Same length caps and value coercion as the events fan-out so the
/// produced tuples are guaranteed to match what the storage MV will accept.
pub fn fan_out_group(event: &GroupIdentify) -> Vec<TupleKey> {
    let mut out = Vec::new();
    let property_type = match event.group_type_index {
        0 => PropertyType::Group0,
        1 => PropertyType::Group1,
        2 => PropertyType::Group2,
        3 => PropertyType::Group3,
        4 => PropertyType::Group4,
        _ => return out,
    };
    if let Some(raw) = &event.group_properties {
        emit_from_blob(event.team_id, property_type, raw, &mut out);
    }
    out
}

fn emit_from_blob(team_id: i64, property_type: PropertyType, raw: &str, out: &mut Vec<TupleKey>) {
    // Treat parse errors as an empty object (no emissions).
    let parsed: Value = match serde_json::from_str(raw) {
        Ok(v) => v,
        Err(_) => return,
    };

    let obj = match parsed.as_object() {
        Some(o) => o,
        None => return,
    };

    for (key, value) in obj {
        if value.is_null() {
            continue;
        }

        let property_value = coerce_to_string(value);

        if key.is_empty() || key.chars().count() > MAX_PROPERTY_KEY_LEN {
            continue;
        }
        if property_value.is_empty() || property_value.chars().count() >= MAX_PROPERTY_VALUE_LEN {
            continue;
        }

        out.push(TupleKey {
            team_id,
            property_type,
            property_key: key.clone(),
            property_value,
        });
    }
}

fn coerce_to_string(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        _ => v.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(properties: &str) -> Event {
        Event {
            team_id: 2,
            properties: Some(properties.to_string()),
            person_properties: None,
            group0_properties: None,
            group1_properties: None,
            group2_properties: None,
            group3_properties: None,
            group4_properties: None,
        }
    }

    #[test]
    fn event_property_produces_event_type_tuple() {
        let tuples = fan_out(&event(r#"{"$browser":"Chrome"}"#));
        assert_eq!(tuples.len(), 1);
        assert_eq!(tuples[0].team_id, 2);
        assert_eq!(tuples[0].property_type, PropertyType::Event);
        assert_eq!(tuples[0].property_key, "$browser");
        assert_eq!(tuples[0].property_value, "Chrome");
    }

    #[test]
    fn json_null_value_is_dropped() {
        let tuples = fan_out(&event(r#"{"nullable_field":null}"#));
        assert!(tuples.is_empty());
    }

    #[test]
    fn empty_string_value_is_dropped() {
        let tuples = fan_out(&event(r#"{"blank":""}"#));
        assert!(tuples.is_empty());
    }

    #[test]
    fn property_value_256_chars_is_dropped_and_255_is_kept() {
        let kept = "a".repeat(255);
        let dropped = "a".repeat(256);

        let tuples_kept = fan_out(&event(&format!(r#"{{"k":"{}"}}"#, kept)));
        let tuples_dropped = fan_out(&event(&format!(r#"{{"k":"{}"}}"#, dropped)));

        assert_eq!(tuples_kept.len(), 1);
        assert_eq!(tuples_kept[0].property_value, kept);
        assert!(tuples_dropped.is_empty());
    }

    #[test]
    fn multi_byte_value_under_codepoint_cap_is_kept() {
        // 255 CJK codepoints = 765 bytes. Using .len() would silently drop
        // this; .chars().count() correctly keeps it.
        let value = "あ".repeat(255);
        let tuples = fan_out(&event(&format!(r#"{{"k":"{}"}}"#, value)));
        assert_eq!(tuples.len(), 1);
        assert_eq!(tuples[0].property_value, value);
    }

    #[test]
    fn multi_byte_key_over_codepoint_cap_is_dropped() {
        // 401 CJK codepoints = 1203 bytes. Codepoint-aware check drops it.
        let key = "あ".repeat(401);
        let tuples = fan_out(&event(&format!(r#"{{"{}":"v"}}"#, key)));
        assert!(tuples.is_empty());
    }

    #[test]
    fn property_key_401_chars_is_dropped_and_400_is_kept() {
        let kept = "k".repeat(400);
        let dropped = "k".repeat(401);

        let tuples_kept = fan_out(&event(&format!(r#"{{"{}":"v"}}"#, kept)));
        let tuples_dropped = fan_out(&event(&format!(r#"{{"{}":"v"}}"#, dropped)));

        assert_eq!(tuples_kept.len(), 1);
        assert_eq!(tuples_kept[0].property_key, kept);
        assert!(tuples_dropped.is_empty());
    }

    #[test]
    fn person_properties_emit_person_type() {
        let ev = Event {
            team_id: 2,
            properties: None,
            person_properties: Some(r#"{"email":"foo@bar.com"}"#.to_string()),
            group0_properties: None,
            group1_properties: None,
            group2_properties: None,
            group3_properties: None,
            group4_properties: None,
        };
        let tuples = fan_out(&ev);
        assert_eq!(tuples.len(), 1);
        assert_eq!(tuples[0].property_type, PropertyType::Person);
        assert_eq!(tuples[0].property_key, "email");
        assert_eq!(tuples[0].property_value, "foo@bar.com");
    }

    #[test]
    fn group2_properties_emit_group_2_type() {
        let ev = Event {
            team_id: 2,
            properties: None,
            person_properties: None,
            group0_properties: None,
            group1_properties: None,
            group2_properties: Some(r#"{"project":"posthog"}"#.to_string()),
            group3_properties: None,
            group4_properties: None,
        };
        let tuples = fan_out(&ev);
        assert_eq!(tuples.len(), 1);
        assert_eq!(tuples[0].property_type, PropertyType::Group2);
        assert_eq!(tuples[0].property_value, "posthog");
    }

    #[test]
    fn bool_value_coerces_to_string() {
        let tuples = fan_out(&event(r#"{"a_bool":true}"#));
        assert_eq!(tuples.len(), 1);
        assert_eq!(tuples[0].property_value, "true");
    }

    #[test]
    fn number_value_coerces_to_string() {
        let tuples = fan_out(&event(r#"{"a_number":42}"#));
        assert_eq!(tuples.len(), 1);
        assert_eq!(tuples[0].property_value, "42");
    }

    #[test]
    fn unparseable_blob_emits_nothing() {
        let tuples = fan_out(&event(r#"not valid json"#));
        assert!(tuples.is_empty());
    }

    #[test]
    fn empty_object_emits_nothing() {
        let tuples = fan_out(&event("{}"));
        assert!(tuples.is_empty());
    }

    fn group_identify(group_type_index: u8, properties: &str) -> GroupIdentify {
        GroupIdentify {
            team_id: 2,
            group_type_index,
            group_properties: Some(properties.to_string()),
        }
    }

    #[test]
    fn group_identify_index_0_emits_group_0_type() {
        let tuples = fan_out_group(&group_identify(0, r#"{"plan":"enterprise"}"#));
        assert_eq!(tuples.len(), 1);
        assert_eq!(tuples[0].property_type, PropertyType::Group0);
        assert_eq!(tuples[0].property_key, "plan");
        assert_eq!(tuples[0].property_value, "enterprise");
    }

    #[test]
    fn group_identify_index_4_emits_group_4_type() {
        let tuples = fan_out_group(&group_identify(4, r#"{"region":"us-east"}"#));
        assert_eq!(tuples.len(), 1);
        assert_eq!(tuples[0].property_type, PropertyType::Group4);
    }

    #[test]
    fn group_identify_index_out_of_range_drops() {
        let tuples = fan_out_group(&group_identify(5, r#"{"plan":"enterprise"}"#));
        assert!(tuples.is_empty());
    }

    #[test]
    fn group_identify_missing_properties_drops() {
        let g = GroupIdentify {
            team_id: 2,
            group_type_index: 0,
            group_properties: None,
        };
        assert!(fan_out_group(&g).is_empty());
    }

    #[test]
    fn group_identify_applies_codepoint_caps() {
        // Same caps as fan_out: 401-codepoint key dropped, 256-codepoint
        // value dropped.
        let big_key = "k".repeat(401);
        let big_value = "v".repeat(256);
        let json = format!(
            "{{\"{}\":\"ok\",\"k\":\"{}\",\"ok\":\"yes\"}}",
            big_key, big_value
        );
        let tuples = fan_out_group(&group_identify(0, &json));
        // Only the "ok":"yes" pair survives.
        assert_eq!(tuples.len(), 1);
        assert_eq!(tuples[0].property_key, "ok");
        assert_eq!(tuples[0].property_value, "yes");
    }

    #[test]
    fn group_identify_unparseable_drops() {
        let tuples = fan_out_group(&group_identify(0, "not valid json"));
        assert!(tuples.is_empty());
    }
}
