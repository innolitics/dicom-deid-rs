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
    translate_ctp_scripts_with_blacklist(anonymizer_xml, pixel_script, filter_script, None)
}

/// Translate CTP scripts into a dicom-deid-rs recipe, with a separate blacklist filter.
///
/// The *blacklist_script* is emitted as a `%filter blacklist` section — files
/// matching it are rejected.  Combining a whitelist `filter_script` with a
/// blacklist gives CTP's "whitelist AND NOT blacklist" semantics without
/// trying to fold both into a single recipe expression.
pub fn translate_ctp_scripts_with_blacklist(
    anonymizer_xml: Option<&str>,
    pixel_script: Option<&str>,
    filter_script: Option<&str>,
    blacklist_script: Option<&str>,
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

    if let Some(blacklist_text) = blacklist_script {
        let blacklist_lines = translate_filter_script(blacklist_text);
        if !blacklist_lines.is_empty() {
            output.push_str("\n%filter blacklist\n\n");
            output.push_str(&blacklist_lines.join("\n"));
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
                            "unspecifiedelements" => remove_unspecified_elements = enabled == "T",
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
                return Err(DeidError::RecipeParse(format!("XML parse error: {}", e)));
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
        let parts: Vec<&str> = args
            .splitn(2, ',')
            .map(|s| s.trim().trim_matches('"'))
            .collect();
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
                return Some(format!("REMOVE_IF Contains({},{}) {}", element, value, tag));
            }
            if true_branch == "@keep()" && false_branch == "@keep()" {
                return Some(format!("KEEP {}", tag));
            }
        }
        "equals" => {
            let value = parts.get(2).unwrap_or(&"").trim_matches('"');
            if true_branch == "@quarantine()" {
                return Some(format!("REMOVE_IF Equals({},{}) {}", element, value, tag));
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
    let coord_re =
        Regex::new(r"\(\s*(-?\d+)\s*,\s*(-?\d+)\s*,\s*(-?\d+)\s*,\s*(-?\d+)\s*\)").unwrap();
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
        let op_str = if i == 0 { "first" } else { op.as_str() };
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
//
// The recipe format's per-LABEL evaluator is left-to-right with no operator
// precedence (`evaluate_conditions` in filter.rs).  CTP filters, by contrast,
// use proper boolean precedence with arbitrary nesting of `*` (AND), `+` (OR),
// and `!` (NOT).  To bridge them, the translator parses the CTP filter into
// an AST, normalizes to negation normal form (NOT pushed into predicates),
// converts to disjunctive normal form (OR of ANDs), then emits one LABEL per
// disjunct.  Within each LABEL all conditions are AND-joined (`+` prefix in
// recipe syntax), which the flat evaluator handles correctly.

/// Soft cap on DNF expansion to avoid pathological blowup.  The Stanford
/// filter normalizes to ~200 disjuncts; this gives plenty of headroom while
/// preventing a malformed filter from exhausting memory.
const MAX_DNF_DISJUNCTS: usize = 10_000;

#[derive(Debug, Clone, PartialEq, Eq)]
struct FilterPredicate {
    negated: bool,
    tag: String,
    method: String,
    value: String,
}

#[derive(Debug, Clone)]
enum FilterExpr {
    Pred(FilterPredicate),
    Not(Box<FilterExpr>),
    And(Vec<FilterExpr>),
    Or(Vec<FilterExpr>),
}

fn translate_filter_script(filter_text: &str) -> Vec<String> {
    let trimmed = filter_text.trim();
    if trimmed.is_empty() || trimmed == "true." {
        return Vec::new();
    }

    let expr = match parse_filter(trimmed) {
        Ok(expr) => expr,
        Err(e) => {
            eprintln!(
                "warning: failed to parse filter script ({}); falling back to empty filter",
                e
            );
            return Vec::new();
        }
    };

    let nnf = to_nnf(expr, false);
    let disjuncts = match to_dnf(nnf) {
        Ok(d) => d,
        Err(e) => {
            eprintln!(
                "warning: filter script DNF expansion exceeded cap ({}); falling back to empty filter",
                e
            );
            return Vec::new();
        }
    };

    let mut lines = Vec::new();
    let mut emitted = 0;
    for conjunct in disjuncts {
        // A conjunct with no recognized predicates means the original CTP
        // expression contained only unsupported predicate kinds (e.g.
        // `.exists()` or `.isBlank()`).  Skip rather than emit an empty
        // LABEL, which the recipe parser rejects.
        let preds: Vec<String> = conjunct.iter().filter_map(emit_predicate).collect();
        if preds.is_empty() {
            continue;
        }
        if !lines.is_empty() {
            lines.push(String::new());
        }
        lines.push(format!("LABEL filter_rule_{}", emitted));
        for (j, pred) in preds.iter().enumerate() {
            let prefix = if j == 0 { "  " } else { "  + " };
            lines.push(format!("{}{}", prefix, pred));
        }
        emitted += 1;
    }
    lines
}

fn emit_predicate(p: &FilterPredicate) -> Option<String> {
    let kw = match (p.method.as_str(), p.negated) {
        ("startsWith" | "startsWithIgnoreCase", false) => "startswith",
        ("startsWith" | "startsWithIgnoreCase", true) => "notstartswith",
        ("contains" | "containsIgnoreCase", false) => "contains",
        ("contains" | "containsIgnoreCase", true) => "notcontains",
        ("equals" | "equalsIgnoreCase", false) => "equals",
        ("equals" | "equalsIgnoreCase", true) => "notequals",
        // No native `endsWith` predicate in the recipe format; approximate
        // with `contains` (matches CTP semantics conservatively for the
        // device-vendor filters we care about).
        ("endsWith" | "endsWithIgnoreCase", false) => "contains",
        ("endsWith" | "endsWithIgnoreCase", true) => "notcontains",
        ("matches", false) => "contains",
        ("matches", true) => "notcontains",
        // No native numeric predicate — drop so the rest of the conjunct
        // still applies, matching the behavior of the previous translator.
        ("isLessThan" | "isGreaterThan", _) => return None,
        _ => return None,
    };
    Some(format!("{} {} {}", kw, p.tag, p.value))
}

// --- AST normalization ----------------------------------------------------

/// Push NOTs down to predicates (negation normal form) so the resulting tree
/// contains only `And`, `Or`, and `Pred` (with predicate-level negation).
fn to_nnf(expr: FilterExpr, negate: bool) -> FilterExpr {
    match expr {
        FilterExpr::Pred(p) => FilterExpr::Pred(FilterPredicate {
            negated: if negate { !p.negated } else { p.negated },
            ..p
        }),
        FilterExpr::Not(inner) => to_nnf(*inner, !negate),
        FilterExpr::And(items) => {
            let mapped: Vec<_> = items.into_iter().map(|e| to_nnf(e, negate)).collect();
            if negate {
                FilterExpr::Or(mapped)
            } else {
                FilterExpr::And(mapped)
            }
        }
        FilterExpr::Or(items) => {
            let mapped: Vec<_> = items.into_iter().map(|e| to_nnf(e, negate)).collect();
            if negate {
                FilterExpr::And(mapped)
            } else {
                FilterExpr::Or(mapped)
            }
        }
    }
}

/// Convert an NNF expression to disjunctive normal form: a vector of
/// conjunctions, each itself a vector of (possibly negated) predicates.
fn to_dnf(expr: FilterExpr) -> Result<Vec<Vec<FilterPredicate>>, String> {
    match expr {
        FilterExpr::Pred(p) => Ok(vec![vec![p]]),
        FilterExpr::Not(_) => {
            // Should not occur after to_nnf
            Err("internal: NOT survived NNF normalization".to_string())
        }
        FilterExpr::Or(items) => {
            let mut out = Vec::new();
            for item in items {
                let child = to_dnf(item)?;
                out.extend(child);
                if out.len() > MAX_DNF_DISJUNCTS {
                    return Err(format!("disjunct count exceeded {}", MAX_DNF_DISJUNCTS));
                }
            }
            Ok(out)
        }
        FilterExpr::And(items) => {
            // Cross product: AND(a, b) where a and b are DNFs becomes
            // OR over { x ++ y | x in a, y in b }.
            let mut acc: Vec<Vec<FilterPredicate>> = vec![Vec::new()];
            for child in items {
                let child_dnf = to_dnf(child)?;
                if child_dnf.is_empty() {
                    // Child reduces to FALSE → whole AND is FALSE → empty DNF.
                    return Ok(Vec::new());
                }
                if acc.len().saturating_mul(child_dnf.len()) > MAX_DNF_DISJUNCTS {
                    return Err(format!("disjunct count exceeded {}", MAX_DNF_DISJUNCTS));
                }
                let mut new_acc = Vec::with_capacity(acc.len() * child_dnf.len());
                for a in &acc {
                    for c in &child_dnf {
                        let mut combined = a.clone();
                        combined.extend(c.iter().cloned());
                        new_acc.push(combined);
                    }
                }
                acc = new_acc;
            }
            Ok(acc)
        }
    }
}

// --- Tokenizer + parser ---------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq)]
enum FilterToken {
    LParen,
    RParen,
    Star, // AND
    Plus, // OR
    Bang, // NOT
    Pred(FilterPredicate),
}

fn parse_filter(text: &str) -> Result<FilterExpr, String> {
    let tokens = tokenize_filter(text)?;
    if tokens.is_empty() {
        return Err("empty filter".to_string());
    }
    let mut pos = 0;
    let expr = parse_or(&tokens, &mut pos)?;
    if pos != tokens.len() {
        return Err(format!("unexpected trailing tokens after position {}", pos));
    }
    Ok(expr)
}

fn parse_or(tokens: &[FilterToken], pos: &mut usize) -> Result<FilterExpr, String> {
    let first = parse_and(tokens, pos)?;
    let mut items = vec![first];
    while matches!(tokens.get(*pos), Some(FilterToken::Plus)) {
        *pos += 1;
        items.push(parse_and(tokens, pos)?);
    }
    Ok(if items.len() == 1 {
        items.pop().unwrap()
    } else {
        FilterExpr::Or(items)
    })
}

fn parse_and(tokens: &[FilterToken], pos: &mut usize) -> Result<FilterExpr, String> {
    let first = parse_unary(tokens, pos)?;
    let mut items = vec![first];
    while matches!(tokens.get(*pos), Some(FilterToken::Star)) {
        *pos += 1;
        items.push(parse_unary(tokens, pos)?);
    }
    Ok(if items.len() == 1 {
        items.pop().unwrap()
    } else {
        FilterExpr::And(items)
    })
}

fn parse_unary(tokens: &[FilterToken], pos: &mut usize) -> Result<FilterExpr, String> {
    if matches!(tokens.get(*pos), Some(FilterToken::Bang)) {
        *pos += 1;
        let inner = parse_unary(tokens, pos)?;
        Ok(FilterExpr::Not(Box::new(inner)))
    } else {
        parse_atom(tokens, pos)
    }
}

fn parse_atom(tokens: &[FilterToken], pos: &mut usize) -> Result<FilterExpr, String> {
    match tokens.get(*pos) {
        Some(FilterToken::LParen) => {
            *pos += 1;
            let inner = parse_or(tokens, pos)?;
            match tokens.get(*pos) {
                Some(FilterToken::RParen) => {
                    *pos += 1;
                    Ok(inner)
                }
                other => Err(format!("expected ')' at token {}, got {:?}", pos, other)),
            }
        }
        Some(FilterToken::Pred(p)) => {
            *pos += 1;
            Ok(FilterExpr::Pred(p.clone()))
        }
        other => Err(format!(
            "expected predicate or '(' at token {}, got {:?}",
            pos, other
        )),
    }
}

fn tokenize_filter(text: &str) -> Result<Vec<FilterToken>, String> {
    // Predicate without leading `!` (NOT is its own token here).  Tag is a
    // `[group,element]` literal or a top-level keyword — sequence paths
    // (`SeqOfX::Field.method(...)`) are handled by the unknown-predicate
    // fallback below because the recipe format doesn't address nested
    // sequence items.
    let pred_re = Regex::new(
        r#"^(\[[\dA-Fa-f,]+\]|\w+)\.(equals|equalsIgnoreCase|matches|contains|containsIgnoreCase|startsWith|startsWithIgnoreCase|endsWith|endsWithIgnoreCase|isLessThan|isGreaterThan)\("([^"]*)"\)"#,
    )
    .map_err(|e| format!("predicate regex compile failed: {}", e))?;
    // Consume the full lexeme of any predicate-like atom that is NOT in the
    // supported set (e.g. `[0042,0011].exists()`) or that addresses a
    // sequence path (`SeqOfX::Field.equals(...)`), so the outer parser
    // doesn't choke on it.  The atom is dropped — yielding TRUE in the
    // surrounding conjunction, matching the previous translator's behavior
    // of silently skipping unknown predicates.
    let unknown_pred_re = Regex::new(r#"^(\[[\dA-Fa-f,]+\]|\w+(?:::\w+)*)\.\w+\([^)]*\)"#)
        .map_err(|e| format!("unknown-predicate regex compile failed: {}", e))?;

    let mut tokens = Vec::new();
    let bytes = text.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        let ch = bytes[i] as char;
        match ch {
            ' ' | '\t' | '\n' | '\r' => i += 1,
            '/' if i + 1 < bytes.len() && bytes[i + 1] as char == '/' => {
                // Line comment — skip to end of line.
                while i < bytes.len() && bytes[i] as char != '\n' {
                    i += 1;
                }
            }
            '/' if i + 1 < bytes.len() && bytes[i + 1] as char == '*' => {
                // Block comment — skip until `*/`.
                i += 2;
                while i + 1 < bytes.len()
                    && !(bytes[i] as char == '*' && bytes[i + 1] as char == '/')
                {
                    i += 1;
                }
                i = (i + 2).min(bytes.len());
            }
            '(' => {
                tokens.push(FilterToken::LParen);
                i += 1;
            }
            ')' => {
                tokens.push(FilterToken::RParen);
                i += 1;
            }
            '*' => {
                tokens.push(FilterToken::Star);
                i += 1;
            }
            '+' => {
                tokens.push(FilterToken::Plus);
                i += 1;
            }
            '!' => {
                tokens.push(FilterToken::Bang);
                i += 1;
            }
            _ => {
                let rest = &text[i..];
                if let Some(caps) = pred_re.captures(rest) {
                    let full_match = caps.get(0).unwrap().as_str();
                    let tag_raw = caps.get(1).unwrap().as_str();
                    let method = caps.get(2).unwrap().as_str().to_string();
                    let value = caps.get(3).unwrap().as_str().to_string();
                    let tag = if tag_raw.starts_with('[') && tag_raw.ends_with(']') {
                        tag_raw[1..tag_raw.len() - 1].replace(',', "")
                    } else {
                        tag_raw.to_string()
                    };
                    tokens.push(FilterToken::Pred(FilterPredicate {
                        negated: false,
                        tag,
                        method,
                        value,
                    }));
                    i += full_match.len();
                } else if let Some(caps) = unknown_pred_re.captures(rest) {
                    // Unrecognized predicate kind: synthesize a tautology so
                    // the surrounding boolean structure still parses.  We
                    // model "always TRUE" as `Or([])` would be FALSE — instead
                    // emit a pseudo-predicate that emit_predicate filters out,
                    // then collapse via FilterExpr::And([]) which DNF treats
                    // as TRUE.
                    let full_match = caps.get(0).unwrap().as_str();
                    tokens.push(FilterToken::Pred(FilterPredicate {
                        negated: false,
                        tag: String::new(),
                        method: "__unsupported__".to_string(),
                        value: String::new(),
                    }));
                    i += full_match.len();
                } else {
                    return Err(format!("unexpected character {:?} at position {}", ch, i));
                }
            }
        }
    }
    Ok(tokens)
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
        assert!(
            result
                .elements
                .iter()
                .any(|e| e.tag == "(5000-501e,*)" && e.action == "@remove()")
        );
        assert!(
            result
                .elements
                .iter()
                .any(|e| e.tag == "(6000-601e,*)" && e.action == "@remove()")
        );
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
        assert!(
            result
                .keep_groups
                .contains(&"safeprivateelements".to_string())
        );
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
            Some("APPEND (0012,0063) CTP: var:PROFILENAME: func:date:func:time".into())
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
        assert!(
            result
                .recipe_text
                .contains("REPLACE_ONLY (0008,0018) func:hashuid")
        );
        assert!(
            result
                .recipe_text
                .contains("JITTER (0008,0020) var:DATEINC")
        );
        assert!(
            result
                .recipe_text
                .contains("REPLACE_ONLY (0010,0010) func:hash")
        );
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

    /// Distribution of AND over OR: `A * (B + C)` must expand to TWO
    /// LABELs, `A AND B` and `A AND C`, so the flat evaluator sees a correct
    /// conjunction per disjunct.
    #[test]
    fn translate_filter_distributes_and_over_or() {
        let filter = r#"
            Manufacturer.containsIgnoreCase("GE")
            * (
                Modality.equals("CT")
                + Modality.equals("MR")
              )
        "#;
        let lines = translate_filter_script(filter);
        let labels: Vec<&String> = lines.iter().filter(|l| l.starts_with("LABEL")).collect();
        assert_eq!(labels.len(), 2, "expected two disjuncts after distribution");

        let joined = lines.join("\n");
        // First disjunct: Manufacturer GE AND Modality CT
        assert!(joined.contains("contains Manufacturer GE"));
        assert!(joined.contains("equals Modality CT"));
        // Second disjunct: Manufacturer GE AND Modality MR
        assert!(joined.contains("equals Modality MR"));
        // The Manufacturer predicate must be present in BOTH disjuncts, not
        // just OR'd at the top — split text around LABEL markers to verify.
        let sections: Vec<&str> = joined.split("LABEL filter_rule_").skip(1).collect();
        assert_eq!(sections.len(), 2);
        for sec in &sections {
            assert!(
                sec.contains("contains Manufacturer GE"),
                "Manufacturer condition must be repeated in each disjunct, got:\n{}",
                sec
            );
        }
    }

    /// Stanford-style nested OR inside AND: `A * B * C * (D + E)` must
    /// expand to two LABELs, each requiring A, B, C plus one alternative.
    #[test]
    fn translate_filter_preserves_nested_alternatives() {
        let filter = r#"
            Manufacturer.containsIgnoreCase("KONICA")
            * Modality.startsWithIgnoreCase("CR")
            * ManufacturerModelName.containsIgnoreCase("0402")
            * (
                Rows.equalsIgnoreCase("2446")
                + Rows.equalsIgnoreCase("2010")
              )
        "#;
        let lines = translate_filter_script(filter);
        let labels: Vec<&String> = lines.iter().filter(|l| l.starts_with("LABEL")).collect();
        assert_eq!(labels.len(), 2);

        let joined = lines.join("\n");
        // Each disjunct must contain all three prerequisite predicates.
        let sections: Vec<&str> = joined.split("LABEL filter_rule_").skip(1).collect();
        assert_eq!(sections.len(), 2);
        for sec in &sections {
            assert!(sec.contains("contains Manufacturer KONICA"));
            assert!(sec.contains("startswith Modality CR"));
            assert!(sec.contains("contains ManufacturerModelName 0402"));
        }
        // Disjunct 1 has Rows=2446, disjunct 2 has Rows=2010 — and neither
        // has both simultaneously (that was the old bug).
        assert!(sections[0].contains("equals Rows 2446"));
        assert!(!sections[0].contains("equals Rows 2010"));
        assert!(sections[1].contains("equals Rows 2010"));
        assert!(!sections[1].contains("equals Rows 2446"));
    }

    /// Top-level OR produces one LABEL per alternative.
    #[test]
    fn translate_filter_top_level_or() {
        let filter = r#"
            Manufacturer.containsIgnoreCase("GE")
            + Manufacturer.containsIgnoreCase("SIEMENS")
            + Manufacturer.containsIgnoreCase("PHILIPS")
        "#;
        let lines = translate_filter_script(filter);
        let labels: Vec<&String> = lines.iter().filter(|l| l.starts_with("LABEL")).collect();
        assert_eq!(labels.len(), 3);
    }

    /// NOT over a parenthesized expression is pushed down via De Morgan.
    #[test]
    fn translate_filter_negates_parenthesized_group() {
        let filter = r#"!(Modality.equals("CT") + Modality.equals("MR"))"#;
        let lines = translate_filter_script(filter);
        // !(CT OR MR) => notCT AND notMR => one LABEL with two negated preds.
        let labels: Vec<&String> = lines.iter().filter(|l| l.starts_with("LABEL")).collect();
        assert_eq!(labels.len(), 1);
        let joined = lines.join("\n");
        assert!(joined.contains("notequals Modality CT"));
        assert!(joined.contains("notequals Modality MR"));
    }

    /// Double-negation cancels out.
    #[test]
    fn translate_filter_double_negation() {
        let filter = r#"!(!Modality.equals("CT"))"#;
        let lines = translate_filter_script(filter);
        let joined = lines.join("\n");
        assert!(joined.contains("equals Modality CT"));
        assert!(!joined.contains("notequals"));
    }

    /// Comments in the CTP filter are skipped during tokenization.
    #[test]
    fn translate_filter_skips_comments() {
        let filter = r#"
            // leading comment
            Modality.equals("CT")  // trailing comment
            /* block comment with * and + inside */
            * Manufacturer.containsIgnoreCase("GE")
        "#;
        let lines = translate_filter_script(filter);
        let joined = lines.join("\n");
        assert!(joined.contains("equals Modality CT"));
        assert!(joined.contains("contains Manufacturer GE"));
    }

    /// Unsupported predicate kinds (e.g. `.exists()`) are silently dropped
    /// from the conjunction, matching the previous translator's behavior.
    #[test]
    fn translate_filter_drops_unsupported_predicates() {
        let filter = r#"Modality.equals("CT") * [0042,0011].exists()"#;
        let lines = translate_filter_script(filter);
        let joined = lines.join("\n");
        assert!(joined.contains("equals Modality CT"));
        // `.exists()` is not supported; we don't emit a predicate for it.
        assert!(!joined.contains("exists"));
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
