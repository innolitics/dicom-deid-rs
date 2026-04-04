//! CTP (Clinical Trial Processor) script parser and recipe translator.
//!
//! Parses CTP anonymizer XML scripts, pixel anonymizer scripts, and filter
//! scripts, translating them into the dicom-deid-rs recipe format.

use crate::error::DeidError;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use regex::Regex;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Result of translating CTP scripts into a recipe.
#[derive(Debug, Clone)]
pub struct TranslationResult {
    /// The recipe text (parseable by `Recipe::parse`).
    pub recipe_text: String,
    /// Variables extracted from `<p>` parameters.
    pub variables: HashMap<String, String>,
    /// Whether private tags should be removed (`<r t="privategroups" en="T">`).
    pub remove_private_tags: bool,
    /// Whether unspecified elements should be removed (`<r t="unspecifiedelements" en="T">`).
    pub remove_unspecified_elements: bool,
}

/// Translate CTP scripts into a dicom-deid-rs recipe.
pub fn translate_ctp_scripts(
    anonymizer_xml: Option<&str>,
    pixel_script: Option<&str>,
    filter_script: Option<&str>,
) -> Result<TranslationResult, DeidError> {
    let mut output = String::from("FORMAT dicom\n");
    let mut variables = HashMap::new();
    let mut remove_private_tags = true;
    let mut remove_unspecified_elements = false;

    if let Some(filter_text) = filter_script {
        let filter_lines = translate_filter_script(filter_text);
        if !filter_lines.is_empty() {
            output.push_str("\n%filter whitelist\n\n");
            output.push_str(&filter_lines.join("\n"));
            output.push('\n');
        }
    }

    if let Some(pixel_text) = pixel_script {
        let pixel_lines = translate_pixel_script(pixel_text);
        if !pixel_lines.is_empty() {
            output.push_str("\n%filter graylist\n\n");
            output.push_str(&pixel_lines.join("\n"));
            output.push('\n');
        }
    }

    if let Some(xml_text) = anonymizer_xml {
        let parsed = parse_anonymizer_xml(xml_text)?;
        variables = parsed.params;
        remove_private_tags = parsed.remove_private_tags;
        remove_unspecified_elements = parsed.remove_unspecified_elements;

        let mut header_lines = Vec::new();
        for elem in &parsed.elements {
            if let Some(line) = translate_action(&elem.action, &elem.tag) {
                header_lines.push(line);
            }
        }
        for kg in &parsed.keep_groups {
            header_lines.push(format!("KEEP_GROUP {}", kg));
        }

        if !header_lines.is_empty() {
            output.push_str("\n%header\n\n");
            output.push_str(&header_lines.join("\n"));
            output.push('\n');
        }
    }

    Ok(TranslationResult {
        recipe_text: output,
        variables,
        remove_private_tags,
        remove_unspecified_elements,
    })
}

// ---------------------------------------------------------------------------
// Anonymizer XML parsing
// ---------------------------------------------------------------------------

struct ParsedAnonymizer {
    params: HashMap<String, String>,
    elements: Vec<CtpElement>,
    keep_groups: Vec<String>,
    remove_private_tags: bool,
    remove_unspecified_elements: bool,
}

struct CtpElement {
    tag: String,
    action: String,
}

fn parse_anonymizer_xml(xml_text: &str) -> Result<ParsedAnonymizer, DeidError> {
    let mut reader = Reader::from_str(xml_text);
    let mut params = HashMap::new();
    let mut elements = Vec::new();
    let mut keep_groups = Vec::new();
    let mut remove_private_tags = true;
    let mut remove_unspecified_elements = false;

    let mut buf = Vec::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(ref e) | Event::Empty(ref e)) => {
                let name_bytes = e.name();
                let name = std::str::from_utf8(name_bytes.as_ref()).unwrap_or("");
                match name {
                    "p" => {
                        let param_name = attr_value(e, "t").unwrap_or_default();
                        if !param_name.is_empty() {
                            let text = read_element_text(&mut reader);
                            if !text.is_empty() {
                                params.insert(param_name, text);
                            }
                        }
                    }
                    "e" => {
                        let enabled = attr_value(e, "en").unwrap_or("T".into());
                        if enabled == "F" {
                            // Skip disabled elements, but consume text
                            let _ = read_element_text(&mut reader);
                            continue;
                        }
                        let tag_hex = attr_value(e, "t").unwrap_or_default();
                        let tag = format_tag(&tag_hex);
                        let action = read_element_text(&mut reader);
                        elements.push(CtpElement { tag, action });
                    }
                    "k" => {
                        let enabled = attr_value(e, "en").unwrap_or("T".into());
                        if enabled == "T" {
                            let group = attr_value(e, "t").unwrap_or_default();
                            if !group.is_empty() {
                                keep_groups.push(group);
                            }
                        }
                    }
                    "r" => {
                        let enabled = attr_value(e, "en").unwrap_or("T".into());
                        let rule_type = attr_value(e, "t").unwrap_or_default();
                        match rule_type.as_str() {
                            "privategroups" => remove_private_tags = enabled == "T",
                            "unspecifiedelements" => {
                                remove_unspecified_elements = enabled == "T"
                            }
                            "curves" if enabled == "T" => {
                                elements.push(CtpElement {
                                    tag: "(5000-501e,*)".into(),
                                    action: "@remove()".into(),
                                });
                            }
                            "overlays" if enabled == "T" => {
                                elements.push(CtpElement {
                                    tag: "(6000-601e,*)".into(),
                                    action: "@remove()".into(),
                                });
                            }
                            _ => {}
                        }
                    }
                    _ => {}
                }
            }
            Ok(Event::Eof) => break,
            Err(e) => {
                return Err(DeidError::RecipeParse(format!(
                    "XML parse error: {}",
                    e
                )));
            }
            _ => {}
        }
        buf.clear();
    }

    Ok(ParsedAnonymizer {
        params,
        elements,
        keep_groups,
        remove_private_tags,
        remove_unspecified_elements,
    })
}

fn attr_value(e: &quick_xml::events::BytesStart, name: &str) -> Option<String> {
    for attr in e.attributes().flatten() {
        if attr.key.as_ref() == name.as_bytes() {
            return String::from_utf8(attr.value.to_vec()).ok();
        }
    }
    None
}

fn read_element_text(reader: &mut Reader<&[u8]>) -> String {
    let mut text = String::new();
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Text(e)) => {
                text.push_str(&e.unescape().unwrap_or_default());
            }
            Ok(Event::End(_) | Event::Eof) => break,
            _ => {}
        }
        buf.clear();
    }
    text.trim().to_string()
}

fn format_tag(tag_hex: &str) -> String {
    let hex = tag_hex.trim();
    if hex.len() == 8 && hex.chars().all(|c| c.is_ascii_hexdigit()) {
        format!("({},{})", &hex[..4], &hex[4..])
    } else {
        hex.to_string()
    }
}

// ---------------------------------------------------------------------------
// Action translation
// ---------------------------------------------------------------------------

/// Translate a CTP action script into a recipe line.
///
/// Returns `None` for actions that should be skipped (e.g. `@encrypt()`).
fn translate_action(action_text: &str, tag: &str) -> Option<String> {
    let action = action_text.trim();

    if action.is_empty() {
        return Some(format!("BLANK {}", tag));
    }

    // Simple actions
    if action == "@keep()" {
        return Some(format!("KEEP {}", tag));
    }
    if action == "@remove()" {
        return Some(format!("REMOVE {}", tag));
    }
    if action == "@empty()" {
        return Some(format!("BLANK {}", tag));
    }
    if action == "remove" {
        return Some(format!("REMOVE {}", tag));
    }
    if action == "@skip()" {
        return Some(format!("REMOVE {}", tag)); // skip = abort, approximate as remove
    }
    if action == "@quarantine()" {
        return Some(format!("REMOVE {}", tag));
    }

    // @require() variants
    if action == "@require()" {
        return Some(format!("ADD {}", tag));
    }
    if action.starts_with("@require(") {
        let args = extract_function_args(action)?;
        let parts: Vec<&str> = args.splitn(2, ',').map(|s| s.trim().trim_matches('"')).collect();
        return match parts.len() {
            1 => Some(format!("ADD {} func:contents({})", tag, parts[0])),
            _ => Some(format!("ADD {} func:value({},{})", tag, parts[0], parts[1])),
        };
    }

    // Hash functions — all use REPLACE_ONLY (modify if present, skip if absent)
    if action.starts_with("@hashuid(") {
        return Some(format!("REPLACE_ONLY {} func:hashuid", tag));
    }
    if action.starts_with("@hashname(") {
        return Some(format!("REPLACE_ONLY {} func:hashname", tag));
    }
    if action.starts_with("@hashdate(") {
        return Some(format!("REPLACE_ONLY {} func:hashdate", tag));
    }
    if action.starts_with("@hashptid(") {
        return Some(format!("REPLACE_ONLY {} func:hashptid", tag));
    }
    if action.starts_with("@hash(") {
        return Some(format!("REPLACE_ONLY {} func:hash", tag));
    }

    // Date shifting
    if action.starts_with("@incrementdate(") {
        let var_name = extract_increment_param(action);
        return Some(format!("JITTER {} var:{}", tag, var_name));
    }

    // @always() — create or overwrite (REPLACE)
    if let Some(inner) = action.strip_prefix("@always()") {
        return translate_always(inner, tag);
    }

    // Parameters
    if action.starts_with("@param(") {
        let var_name = extract_param_ref(action)?;
        return Some(format!("REPLACE_ONLY {} var:{}", tag, var_name));
    }

    // Lookup
    if action.starts_with("@lookup(") {
        let args = extract_function_args(action).unwrap_or_default();
        let parts: Vec<&str> = args.split(',').map(|s| s.trim()).collect();
        if parts.len() >= 3 {
            let failure_action = parts[2];
            if parts.len() >= 4 {
                return Some(format!(
                    "REPLACE_ONLY {} func:lookup({},{})",
                    tag, failure_action, parts[3]
                ));
            }
            return Some(format!(
                "REPLACE_ONLY {} func:lookup({})",
                tag, failure_action
            ));
        }
        return Some(format!("REPLACE_ONLY {} func:lookup", tag));
    }

    // Append
    if let Some(value) = action.strip_prefix("@append()") {
        let expanded = expand_inline_functions(value);
        return Some(format!("APPEND {} {}", tag, expanded));
    }

    // Conditionals
    if action.starts_with("@if(") {
        return translate_if_action(action, tag);
    }

    // Sequence processing
    if action.starts_with("@process(") || action == "@process()" {
        return Some(format!("PROCESS {}", tag));
    }

    // @select() — emit a comment (partially supported)
    if action.starts_with("@select(") {
        return Some(format!("# @select() not fully supported: {}", action));
    }

    // @encrypt() — skip per user instruction
    if action.starts_with("@encrypt(") {
        return None;
    }

    // @call() — skip (plugin-specific)
    if action.starts_with("@call(") {
        return None;
    }

    // Date/time functions
    if action.starts_with("@date(") {
        return Some(format!("REPLACE_ONLY {} func:date", tag));
    }
    if action.starts_with("@time(") {
        return Some(format!("REPLACE_ONLY {} func:time", tag));
    }

    // Identifier functions
    if action.starts_with("@integer(") {
        return Some(format!("REPLACE {} func:integer", tag));
    }
    if action.starts_with("@initials(") {
        return Some(format!("REPLACE_ONLY {} func:initials", tag));
    }

    // Content retrieval
    if action.starts_with("@contents(") {
        let args = extract_function_args(action).unwrap_or_default();
        return Some(format!("REPLACE_ONLY {} func:contents({})", tag, args));
    }
    if action.starts_with("@value(") {
        let args = extract_function_args(action).unwrap_or_default();
        return Some(format!("REPLACE_ONLY {} func:value({})", tag, args));
    }
    if action.starts_with("@truncate(") {
        let args = extract_function_args(action).unwrap_or_default();
        return Some(format!("REPLACE_ONLY {} func:truncate({})", tag, args));
    }

    // Case conversion
    if action.starts_with("@uppercase(") {
        let args = extract_function_args(action).unwrap_or_default();
        return Some(format!("REPLACE_ONLY {} func:uppercase({})", tag, args));
    }
    if action.starts_with("@lowercase(") {
        let args = extract_function_args(action).unwrap_or_default();
        return Some(format!("REPLACE_ONLY {} func:lowercase({})", tag, args));
    }

    // Date modification
    if action.starts_with("@modifydate(") {
        let args = extract_function_args(action).unwrap_or_default();
        return Some(format!("REPLACE_ONLY {} func:modifydate({})", tag, args));
    }

    // Blank with count
    if action.starts_with("@blank(") {
        let args = extract_function_args(action).unwrap_or_default();
        return Some(format!("REPLACE_ONLY {} func:blank({})", tag, args));
    }

    // Round
    if action.starts_with("@round(") {
        let args = extract_function_args(action).unwrap_or_default();
        return Some(format!("REPLACE_ONLY {} func:round({})", tag, args));
    }

    // Bare literal value
    Some(format!("REPLACE_ONLY {} {}", tag, action))
}

fn translate_always(inner: &str, tag: &str) -> Option<String> {
    if inner.starts_with('@') {
        // Recursively translate the inner function, but use REPLACE (create or overwrite)
        let inner_result = translate_action(inner, tag)?;
        // Convert REPLACE_ONLY → REPLACE in the result
        let line = inner_result.replacen("REPLACE_ONLY ", "REPLACE ", 1);
        Some(line)
    } else {
        let expanded = expand_inline_functions(inner);
        Some(format!("REPLACE {} {}", tag, expanded))
    }
}

fn translate_if_action(action_text: &str, tag: &str) -> Option<String> {
    let re = Regex::new(r"@if\(([^)]+)\)\{([^}]*)\}(?:\{([^}]*)\})?").ok()?;
    let caps = re.captures(action_text)?;
    let condition = caps.get(1)?.as_str();
    let true_branch = caps.get(2)?.as_str().trim();
    let false_branch = caps.get(3).map(|m| m.as_str().trim()).unwrap_or("");

    let parts: Vec<&str> = condition.splitn(3, ',').map(|s| s.trim()).collect();
    if parts.len() < 2 {
        return Some(format!("# UNSUPPORTED @if: {}", action_text));
    }

    let element = parts[0];
    let cond_type = parts[1];

    match cond_type {
        "isblank" => {
            if true_branch == "@remove()" {
                let mut lines = format!("REMOVE_IF IsBlank({}) {}", element, tag);
                if !false_branch.is_empty() && false_branch != "@keep()" {
                    lines.push_str(&format!(
                        "\nREPLACE_ONLY_IF IsNotBlank({}) {} {}",
                        element, tag, false_branch
                    ));
                }
                return Some(lines);
            }
            if true_branch == "@keep()" && false_branch == "@keep()" {
                return Some(format!("KEEP {}", tag));
            }
        }
        "exists" => {
            if true_branch == "@remove()" {
                return Some(format!("REMOVE_IF Exists({}) {}", element, tag));
            }
            if true_branch == "@keep()" {
                return Some(format!("KEEP_IF Exists({}) {}", element, tag));
            }
        }
        "contains" => {
            let value = parts.get(2).unwrap_or(&"").trim_matches('"');
            if true_branch == "@quarantine()" {
                return Some(format!(
                    "REMOVE_IF Contains({},{}) {}",
                    element, value, tag
                ));
            }
            if true_branch == "@keep()" && false_branch == "@keep()" {
                return Some(format!("KEEP {}", tag));
            }
        }
        "equals" => {
            let value = parts.get(2).unwrap_or(&"").trim_matches('"');
            if true_branch == "@quarantine()" {
                return Some(format!(
                    "REMOVE_IF Equals({},{}) {}",
                    element, value, tag
                ));
            }
        }
        "matches" => {
            let pattern = parts.get(2).unwrap_or(&"").trim_matches('"');
            if true_branch == "@quarantine()" {
                return Some(format!(
                    "REMOVE_IF Matches({},{}) {}",
                    element, pattern, tag
                ));
            }
        }
        "greaterthan" => {
            let value = parts.get(2).unwrap_or(&"");
            if true_branch == "@quarantine()" {
                return Some(format!(
                    "REMOVE_IF GreaterThan({},{}) {}",
                    element, value, tag
                ));
            }
        }
        _ => {}
    }

    Some(format!("# UNSUPPORTED @if: {}", action_text))
}

fn extract_function_args(text: &str) -> Option<String> {
    let start = text.find('(')?;
    let end = text.rfind(')')?;
    if start < end {
        Some(text[start + 1..end].to_string())
    } else {
        None
    }
}

fn extract_increment_param(text: &str) -> String {
    if let Some(args) = extract_function_args(text) {
        let parts: Vec<&str> = args.split(',').collect();
        if parts.len() >= 2 {
            let param_ref = parts[1].trim();
            if let Some(name) = param_ref.strip_prefix('@') {
                return name.to_string();
            }
        }
    }
    "DATEINC".to_string()
}

fn extract_param_ref(text: &str) -> Option<String> {
    let args = extract_function_args(text)?;
    let trimmed = args.trim();
    trimmed.strip_prefix('@').map(|s| s.to_string())
}

fn expand_inline_functions(text: &str) -> String {
    let mut s = text.trim().to_string();
    // Strip surrounding braces
    if s.starts_with('{') && s.ends_with('}') {
        s = s[1..s.len() - 1].to_string();
    }
    // Expand @param(@VAR) → var:VAR
    let param_re = Regex::new(r"@param\(@(\w+)\)").unwrap();
    s = param_re.replace_all(&s, "var:$1").to_string();
    // Expand @date() → func:date
    let date_re = Regex::new(r"@date\([^)]*\)").unwrap();
    s = date_re.replace_all(&s, "func:date").to_string();
    // Expand @time() → func:time
    let time_re = Regex::new(r"@time\([^)]*\)").unwrap();
    s = time_re.replace_all(&s, "func:time").to_string();
    s
}

// ---------------------------------------------------------------------------
// Pixel anonymizer script translation
// ---------------------------------------------------------------------------

fn translate_pixel_script(script_text: &str) -> Vec<String> {
    let mut lines = Vec::new();
    let mut blocks = parse_pixel_blocks(script_text);

    for (i, block) in blocks.drain(..).enumerate() {
        if i > 0 {
            lines.push(String::new());
        }
        lines.push(format!("LABEL {}", block.label));
        for (j, (op, predicate)) in block.conditions.iter().enumerate() {
            let prefix = if j == 0 {
                "  "
            } else if *op == "and" {
                "  + "
            } else {
                "  || "
            };
            lines.push(format!("{}{}", prefix, predicate));
        }
        for (x, y, w, h) in &block.coordinates {
            lines.push(format!("  ctpcoordinates {},{},{},{}", x, y, w, h));
        }
    }

    lines
}

struct PixelBlock {
    label: String,
    conditions: Vec<(String, String)>, // (operator, predicate)
    coordinates: Vec<(i32, i32, i32, i32)>,
}

fn parse_pixel_blocks(script: &str) -> Vec<PixelBlock> {
    let coord_re = Regex::new(r"\(\s*(-?\d+)\s*,\s*(-?\d+)\s*,\s*(-?\d+)\s*,\s*(-?\d+)\s*\)")
        .unwrap();
    let pred_re = Regex::new(
        "^(!?)(\\[[\\d\\w,]+\\]|\\w+)\\.(containsIgnoreCase|equals|equalsIgnoreCase|startsWith|startsWithIgnoreCase)\\(\"([^\"]*)\"\\)$",
    )
    .unwrap();

    let mut blocks = Vec::new();
    let lines: Vec<&str> = script.lines().collect();
    let mut i = 0;
    let mut title_lines: Vec<String> = Vec::new();

    while i < lines.len() {
        let trimmed = lines[i].trim();
        if trimmed.is_empty() {
            i += 1;
            continue;
        }

        if trimmed.contains('{') {
            // Gather condition text until closing brace
            let mut cond_text = String::new();
            let mut j = i;
            while j < lines.len() {
                let line = lines[j].trim();
                cond_text.push(' ');
                cond_text.push_str(line);
                if line.contains('}') {
                    j += 1;
                    break;
                }
                j += 1;
            }

            // Extract content between braces
            let brace_start = cond_text.find('{').unwrap_or(0);
            let brace_end = cond_text.rfind('}').unwrap_or(cond_text.len());
            let cond_inner = cond_text[brace_start + 1..brace_end].trim().to_string();

            // Gather coordinates
            let mut coordinates = Vec::new();
            while j < lines.len() {
                let line = lines[j].trim();
                if line.is_empty() {
                    j += 1;
                    continue;
                }
                if line.starts_with('(') {
                    for cap in coord_re.captures_iter(line) {
                        let x: i32 = cap[1].parse().unwrap_or(0);
                        let y: i32 = cap[2].parse().unwrap_or(0);
                        let w: i32 = cap[3].parse().unwrap_or(0);
                        let h: i32 = cap[4].parse().unwrap_or(0);
                        coordinates.push((x, y, w, h));
                    }
                    j += 1;
                } else {
                    break;
                }
            }

            // Parse conditions
            let conditions = parse_pixel_conditions(&cond_inner, &pred_re);

            let label = if !title_lines.is_empty() {
                title_lines.last().cloned().unwrap_or_default()
            } else {
                generate_label_from_conditions(&conditions)
            };

            blocks.push(PixelBlock {
                label,
                conditions,
                coordinates,
            });

            title_lines.clear();
            i = j;
        } else if trimmed.starts_with('(') {
            i += 1;
        } else {
            title_lines.push(trimmed.to_string());
            i += 1;
        }
    }

    blocks
}

fn parse_pixel_conditions(text: &str, pred_re: &Regex) -> Vec<(String, String)> {
    let parts = split_pixel_conditions(text);
    let mut result = Vec::new();
    for (i, (op, raw)) in parts.iter().enumerate() {
        let op_str = if i == 0 {
            "first"
        } else {
            op.as_str()
        };
        if let Some(predicate) = translate_pixel_predicate(raw.trim(), pred_re) {
            result.push((op_str.to_string(), predicate));
        }
    }
    result
}

fn split_pixel_conditions(text: &str) -> Vec<(String, String)> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut current_op = "first".to_string();

    for ch in text.chars() {
        match ch {
            '"' => {
                in_quotes = !in_quotes;
                current.push(ch);
            }
            '*' if !in_quotes => {
                let piece = current.trim().to_string();
                if !piece.is_empty() {
                    result.push((current_op.clone(), piece));
                }
                current_op = "and".to_string();
                current.clear();
            }
            '+' if !in_quotes => {
                let piece = current.trim().to_string();
                if !piece.is_empty() {
                    result.push((current_op.clone(), piece));
                }
                current_op = "or".to_string();
                current.clear();
            }
            _ => current.push(ch),
        }
    }
    let piece = current.trim().to_string();
    if !piece.is_empty() {
        result.push((current_op, piece));
    }
    result
}

fn translate_pixel_predicate(raw: &str, pred_re: &Regex) -> Option<String> {
    let caps = pred_re.captures(raw)?;
    let negated = &caps[1] == "!";
    let tag_raw = &caps[2];
    let method = &caps[3];
    let value = &caps[4];

    // Convert [GGGG,EEEE] to bare hex
    let tag = if tag_raw.starts_with('[') && tag_raw.ends_with(']') {
        tag_raw[1..tag_raw.len() - 1].replace(',', "")
    } else {
        tag_raw.to_string()
    };

    let kw = match method {
        "containsIgnoreCase" => {
            if negated {
                "notcontains"
            } else {
                "contains"
            }
        }
        "startsWith" | "startsWithIgnoreCase" => {
            if negated {
                "notstartswith"
            } else {
                "startswith"
            }
        }
        "equals" | "equalsIgnoreCase" => {
            if negated {
                "notequals"
            } else {
                "equals"
            }
        }
        _ => return None,
    };

    Some(format!("{} {} {}", kw, tag, value))
}

fn generate_label_from_conditions(conditions: &[(String, String)]) -> String {
    let mut fields = Vec::new();
    for (_, pred) in conditions.iter().take(3) {
        let parts: Vec<&str> = pred.splitn(3, ' ').collect();
        if parts.len() >= 2 {
            fields.push(parts[1].to_string());
        }
    }
    if fields.is_empty() {
        "Unknown".to_string()
    } else {
        fields.join(" ")
    }
}

// ---------------------------------------------------------------------------
// Filter script translation
// ---------------------------------------------------------------------------

fn translate_filter_script(filter_text: &str) -> Vec<String> {
    let filter_text = filter_text.trim();
    if filter_text.is_empty() || filter_text == "true." {
        return Vec::new();
    }

    let filter_pred_re = Regex::new(
        "(!?)(\\[[\\d\\w,]+\\]|\\w+)\\.(equals|equalsIgnoreCase|matches|contains|containsIgnoreCase|startsWith|startsWithIgnoreCase|endsWith|endsWithIgnoreCase|isLessThan|isGreaterThan)\\(\"([^\"]*)\"\\)",
    )
    .unwrap();

    let stripped = strip_outer_parens(filter_text);
    let or_groups = split_top_level(&stripped, "+");

    let mut lines = Vec::new();
    for (i, group) in or_groups.iter().enumerate() {
        let group = strip_outer_parens(group.trim());
        let conditions = extract_filter_conditions(&group, &filter_pred_re);
        if conditions.is_empty() {
            continue;
        }
        if !lines.is_empty() {
            lines.push(String::new());
        }
        lines.push(format!("LABEL filter_rule_{}", i));
        for (j, (op, predicate)) in conditions.iter().enumerate() {
            let prefix = if j == 0 {
                "  "
            } else if *op == "and" {
                "  + "
            } else {
                "  || "
            };
            lines.push(format!("{}{}", prefix, predicate));
        }
    }

    lines
}

fn strip_outer_parens(text: &str) -> String {
    let mut s = text.trim().to_string();
    while s.starts_with('(') && matching_paren(&s, 0) == Some(s.len() - 1) {
        s = s[1..s.len() - 1].trim().to_string();
    }
    s
}

fn matching_paren(text: &str, start: usize) -> Option<usize> {
    let mut depth = 0i32;
    let mut in_quotes = false;
    for (i, ch) in text[start..].chars().enumerate() {
        if ch == '"' {
            in_quotes = !in_quotes;
        } else if !in_quotes {
            if ch == '(' {
                depth += 1;
            } else if ch == ')' {
                depth -= 1;
                if depth == 0 {
                    return Some(start + i);
                }
            }
        }
    }
    None
}

fn split_top_level(text: &str, sep: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut depth = 0i32;
    let mut in_quotes = false;
    let sep_bytes = sep.as_bytes();
    let text_bytes = text.as_bytes();
    let mut i = 0;

    while i < text_bytes.len() {
        let ch = text_bytes[i];
        if ch == b'"' {
            in_quotes = !in_quotes;
            current.push(ch as char);
        } else if !in_quotes {
            if ch == b'(' {
                depth += 1;
                current.push(ch as char);
            } else if ch == b')' {
                depth -= 1;
                current.push(ch as char);
            } else if depth == 0 && text_bytes[i..].starts_with(sep_bytes) {
                parts.push(current.clone());
                current.clear();
                i += sep_bytes.len();
                continue;
            } else {
                current.push(ch as char);
            }
        } else {
            current.push(ch as char);
        }
        i += 1;
    }
    if !current.trim().is_empty() {
        parts.push(current);
    }
    parts
}

fn extract_filter_conditions(
    text: &str,
    pred_re: &Regex,
) -> Vec<(String, String)> {
    let and_parts = split_top_level(text, "*");
    let mut conditions = Vec::new();

    for (i, part) in and_parts.iter().enumerate() {
        let part = strip_outer_parens(part.trim());
        let or_subparts = split_top_level(&part, "+");

        if or_subparts.len() > 1 {
            for (j, subpart) in or_subparts.iter().enumerate() {
                let subpart = strip_outer_parens(subpart.trim());
                let preds = extract_predicates_from_atom(&subpart, pred_re);
                for (k, pred) in preds.iter().enumerate() {
                    let op = if i == 0 && j == 0 && k == 0 {
                        "first"
                    } else if j > 0 && k == 0 {
                        "or"
                    } else {
                        "and"
                    };
                    conditions.push((op.to_string(), pred.clone()));
                }
            }
        } else {
            let preds = extract_predicates_from_atom(&part, pred_re);
            for (k, pred) in preds.iter().enumerate() {
                let op = if i == 0 && k == 0 { "first" } else { "and" };
                conditions.push((op.to_string(), pred.clone()));
            }
        }
    }

    conditions
}

fn extract_predicates_from_atom(text: &str, pred_re: &Regex) -> Vec<String> {
    let mut results = Vec::new();
    for caps in pred_re.captures_iter(text) {
        let negated = &caps[1] == "!";
        let tag_raw = &caps[2];
        let method = &caps[3];
        let value = &caps[4];

        let tag = if tag_raw.starts_with('[') && tag_raw.ends_with(']') {
            tag_raw[1..tag_raw.len() - 1].replace(',', "")
        } else {
            tag_raw.to_string()
        };

        let pred = match method {
            "startsWith" | "startsWithIgnoreCase" => {
                let kw = if negated { "notstartswith" } else { "startswith" };
                format!("{} {} {}", kw, tag, value)
            }
            "contains" | "containsIgnoreCase" => {
                let kw = if negated { "notcontains" } else { "contains" };
                format!("{} {} {}", kw, tag, value)
            }
            "equals" | "equalsIgnoreCase" => {
                let kw = if negated { "notequals" } else { "equals" };
                format!("{} {} {}", kw, tag, value)
            }
            "endsWith" | "endsWithIgnoreCase" => {
                // Approximate with contains
                let kw = if negated { "notcontains" } else { "contains" };
                format!("{} {} {}", kw, tag, value)
            }
            "matches" => {
                let kw = if negated { "notcontains" } else { "contains" };
                format!("{} {} {}", kw, tag, value)
            }
            "isLessThan" | "isGreaterThan" => {
                // No direct equivalent — skip
                continue;
            }
            _ => continue,
        };
        results.push(pred);
    }
    results
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // --- XML parsing tests ---

    #[test]
    fn parse_parameters() {
        let xml = r#"<script>
            <p t="DATEINC">-3210</p>
            <p t="UIDROOT">1.2.840</p>
        </script>"#;
        let result = parse_anonymizer_xml(xml).unwrap();
        assert_eq!(result.params["DATEINC"], "-3210");
        assert_eq!(result.params["UIDROOT"], "1.2.840");
    }

    #[test]
    fn parse_disabled_elements_skipped() {
        let xml = r#"<script>
            <e en="T" t="00100010" n="PatientName">@keep()</e>
            <e en="F" t="00100020" n="PatientID">@remove()</e>
        </script>"#;
        let result = parse_anonymizer_xml(xml).unwrap();
        assert_eq!(result.elements.len(), 1);
        assert_eq!(result.elements[0].tag, "(0010,0010)");
    }

    #[test]
    fn parse_removal_rules() {
        let xml = r#"<script>
            <r en="T" t="privategroups">Remove private</r>
            <r en="F" t="unspecifiedelements">Remove unspecified</r>
            <r en="T" t="curves">Remove curves</r>
            <r en="T" t="overlays">Remove overlays</r>
        </script>"#;
        let result = parse_anonymizer_xml(xml).unwrap();
        assert!(result.remove_private_tags);
        assert!(!result.remove_unspecified_elements);
        // Curves and overlays become element removal actions
        assert!(result
            .elements
            .iter()
            .any(|e| e.tag == "(5000-501e,*)" && e.action == "@remove()"));
        assert!(result
            .elements
            .iter()
            .any(|e| e.tag == "(6000-601e,*)" && e.action == "@remove()"));
    }

    #[test]
    fn parse_keep_groups() {
        let xml = r#"<script>
            <k en="T" t="0018">Keep group 18</k>
            <k en="F" t="0028">Keep group 28</k>
            <k en="T" t="safeprivateelements">Keep safe privates</k>
        </script>"#;
        let result = parse_anonymizer_xml(xml).unwrap();
        assert_eq!(result.keep_groups.len(), 2);
        assert!(result.keep_groups.contains(&"0018".to_string()));
        assert!(result
            .keep_groups
            .contains(&"safeprivateelements".to_string()));
    }

    // --- Action translation tests ---

    #[test]
    fn translate_keep() {
        assert_eq!(
            translate_action("@keep()", "(0010,0010)"),
            Some("KEEP (0010,0010)".into())
        );
    }

    #[test]
    fn translate_remove() {
        assert_eq!(
            translate_action("@remove()", "(0010,0010)"),
            Some("REMOVE (0010,0010)".into())
        );
    }

    #[test]
    fn translate_empty() {
        assert_eq!(
            translate_action("@empty()", "(0010,0010)"),
            Some("BLANK (0010,0010)".into())
        );
    }

    #[test]
    fn translate_blank_action() {
        assert_eq!(
            translate_action("", "(0010,0010)"),
            Some("BLANK (0010,0010)".into())
        );
    }

    #[test]
    fn translate_require_bare() {
        assert_eq!(
            translate_action("@require()", "(0010,0010)"),
            Some("ADD (0010,0010)".into())
        );
    }

    #[test]
    fn translate_require_with_element() {
        assert_eq!(
            translate_action("@require(PatientID)", "(0010,0010)"),
            Some("ADD (0010,0010) func:contents(PatientID)".into())
        );
    }

    #[test]
    fn translate_require_with_default() {
        assert_eq!(
            translate_action(r#"@require(PatientID,"UNKNOWN")"#, "T"),
            Some("ADD T func:value(PatientID,UNKNOWN)".into())
        );
    }

    #[test]
    fn translate_hashuid() {
        assert_eq!(
            translate_action("@hashuid(@UIDROOT,this)", "(0020,000d)"),
            Some("REPLACE_ONLY (0020,000d) func:hashuid".into())
        );
    }

    #[test]
    fn translate_hash() {
        assert_eq!(
            translate_action("@hash(this,10)", "(0010,0020)"),
            Some("REPLACE_ONLY (0010,0020) func:hash".into())
        );
    }

    #[test]
    fn translate_incrementdate() {
        assert_eq!(
            translate_action("@incrementdate(this,@DATEINC)", "(0008,0020)"),
            Some("JITTER (0008,0020) var:DATEINC".into())
        );
    }

    #[test]
    fn translate_always_literal() {
        assert_eq!(
            translate_action("@always()YES", "(0012,0062)"),
            Some("REPLACE (0012,0062) YES".into())
        );
    }

    #[test]
    fn translate_always_function() {
        assert_eq!(
            translate_action("@always()@hash(this,10)", "(0010,0020)"),
            Some("REPLACE (0010,0020) func:hash".into())
        );
    }

    #[test]
    fn translate_always_integer() {
        assert_eq!(
            translate_action("@always()@integer(SeriesInstanceUID,seriesnum,5)", "T"),
            Some("REPLACE T func:integer".into())
        );
    }

    #[test]
    fn translate_param() {
        assert_eq!(
            translate_action("@param(@SITEID)", "(0012,0040)"),
            Some("REPLACE_ONLY (0012,0040) var:SITEID".into())
        );
    }

    #[test]
    fn translate_lookup_basic() {
        assert_eq!(
            translate_action("@lookup(this,ptid)", "(0010,0020)"),
            Some("REPLACE_ONLY (0010,0020) func:lookup".into())
        );
    }

    #[test]
    fn translate_lookup_with_action() {
        assert_eq!(
            translate_action("@lookup(this,ptid,keep)", "(0010,0020)"),
            Some("REPLACE_ONLY (0010,0020) func:lookup(keep)".into())
        );
    }

    #[test]
    fn translate_lookup_with_default() {
        assert_eq!(
            translate_action("@lookup(this,ptid,default,ANON)", "(0010,0020)"),
            Some("REPLACE_ONLY (0010,0020) func:lookup(default,ANON)".into())
        );
    }

    #[test]
    fn translate_append() {
        assert_eq!(
            translate_action(
                "@append(){CTP: @param(@PROFILENAME): @date():@time()}",
                "(0012,0063)"
            ),
            Some(
                "APPEND (0012,0063) CTP: var:PROFILENAME: func:date:func:time"
                    .into()
            )
        );
    }

    #[test]
    fn translate_process() {
        assert_eq!(
            translate_action("@process()", "(0008,1115)"),
            Some("PROCESS (0008,1115)".into())
        );
    }

    #[test]
    fn translate_date() {
        assert_eq!(
            translate_action("@date()", "(0008,0012)"),
            Some("REPLACE_ONLY (0008,0012) func:date".into())
        );
    }

    #[test]
    fn translate_time() {
        assert_eq!(
            translate_action("@time()", "(0008,0013)"),
            Some("REPLACE_ONLY (0008,0013) func:time".into())
        );
    }

    #[test]
    fn translate_integer() {
        assert_eq!(
            translate_action("@integer(SeriesInstanceUID,seriesnum,5)", "T"),
            Some("REPLACE T func:integer".into())
        );
    }

    #[test]
    fn translate_initials() {
        assert_eq!(
            translate_action("@initials(PatientName)", "T"),
            Some("REPLACE_ONLY T func:initials".into())
        );
    }

    #[test]
    fn translate_contents() {
        assert_eq!(
            translate_action("@contents(PatientID)", "T"),
            Some("REPLACE_ONLY T func:contents(PatientID)".into())
        );
    }

    #[test]
    fn translate_uppercase() {
        assert_eq!(
            translate_action("@uppercase(PatientName)", "T"),
            Some("REPLACE_ONLY T func:uppercase(PatientName)".into())
        );
    }

    #[test]
    fn translate_lowercase() {
        assert_eq!(
            translate_action("@lowercase(PatientName)", "T"),
            Some("REPLACE_ONLY T func:lowercase(PatientName)".into())
        );
    }

    #[test]
    fn translate_encrypt_skipped() {
        assert_eq!(translate_action(r#"@encrypt(this,"key")"#, "T"), None);
    }

    #[test]
    fn translate_bare_literal() {
        assert_eq!(
            translate_action("YES", "(0012,0062)"),
            Some("REPLACE_ONLY (0012,0062) YES".into())
        );
    }

    #[test]
    fn translate_code_sequence() {
        assert_eq!(
            translate_action("113100/113105/113107", "(0012,0064)"),
            Some("REPLACE_ONLY (0012,0064) 113100/113105/113107".into())
        );
    }

    #[test]
    fn translate_if_isblank_remove() {
        let result = translate_action(
            "@if(this,isblank){@remove()}{Removed by CTP}",
            "(0040,a075)",
        )
        .unwrap();
        assert!(result.contains("REMOVE_IF IsBlank(this)"));
        assert!(result.contains("REPLACE_ONLY_IF IsNotBlank(this)"));
    }

    #[test]
    fn translate_if_contains_quarantine() {
        let result = translate_action(
            r#"@if(ImageType,contains,"SCREEN SAVE"){@quarantine()}{@keep()}"#,
            "(0008,0008)",
        )
        .unwrap();
        assert!(result.contains("REMOVE_IF Contains(ImageType,SCREEN SAVE)"));
    }

    // --- Full translation test ---

    #[test]
    fn translate_full_script() {
        let xml = r#"<script>
            <p t="DATEINC">-100</p>
            <p t="UIDROOT">1.2.840</p>
            <e en="T" t="00080005" n="SpecificCharacterSet">@keep()</e>
            <e en="T" t="00080018" n="SOPInstanceUID">@hashuid(@UIDROOT,this)</e>
            <e en="T" t="00080020" n="StudyDate">@incrementdate(this,@DATEINC)</e>
            <e en="T" t="00100010" n="PatientName">@hash(this,10)</e>
            <e en="F" t="00100020" n="PatientID">@remove()</e>
            <r en="T" t="privategroups">Remove private</r>
            <r en="F" t="unspecifiedelements">Remove unspecified</r>
        </script>"#;

        let result = translate_ctp_scripts(Some(xml), None, None).unwrap();
        assert!(result.recipe_text.contains("FORMAT dicom"));
        assert!(result.recipe_text.contains("KEEP (0008,0005)"));
        assert!(result
            .recipe_text
            .contains("REPLACE_ONLY (0008,0018) func:hashuid"));
        assert!(result
            .recipe_text
            .contains("JITTER (0008,0020) var:DATEINC"));
        assert!(result
            .recipe_text
            .contains("REPLACE_ONLY (0010,0010) func:hash"));
        // Disabled element should not appear
        assert!(!result.recipe_text.contains("0010,0020"));
        assert_eq!(result.variables["DATEINC"], "-100");
        assert!(result.remove_private_tags);
        assert!(!result.remove_unspecified_elements);
    }

    // --- Filter translation test ---

    #[test]
    fn translate_simple_filter() {
        let filter = r#"![0008,0008].contains("SAVE") * ![0028,0301].contains("YES")"#;
        let lines = translate_filter_script(filter);
        assert!(!lines.is_empty());
        assert!(lines.iter().any(|l| l.contains("notcontains")));
    }

    // --- Pixel script translation test ---

    #[test]
    fn translate_pixel_block() {
        let script = r#"
GE CT
  { [0008,0060].containsIgnoreCase("CT") *
    [0008,0070].containsIgnoreCase("GE MEDICAL") }
  (0,0,512,110)
"#;
        let lines = translate_pixel_script(script);
        assert!(lines.iter().any(|l| l.starts_with("LABEL")));
        assert!(lines.iter().any(|l| l.contains("contains")));
        assert!(lines.iter().any(|l| l.contains("ctpcoordinates")));
    }
}
