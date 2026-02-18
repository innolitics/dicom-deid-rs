use crate::error::DeidError;
use dicom_core::Tag;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
pub struct Recipe {
    pub format: String,
    pub header: Vec<HeaderAction>,
    pub filters: Vec<FilterSection>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FilterSection {
    pub filter_type: FilterType,
    pub labels: Vec<FilterLabel>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FilterType {
    Graylist,
    Blacklist,
}

#[derive(Debug, Clone, PartialEq)]
pub struct FilterLabel {
    pub name: String,
    pub conditions: Vec<Condition>,
    pub coordinates: Vec<CoordinateRegion>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Condition {
    pub operator: LogicalOp,
    pub predicate: Predicate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalOp {
    /// First condition in a group (no preceding operator).
    First,
    /// AND relationship with preceding condition (`+` prefix).
    And,
    /// OR relationship with preceding condition (`||` prefix).
    Or,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    Contains { field: String, value: String },
    NotContains { field: String, value: String },
    Equals { field: String, value: String },
    NotEquals { field: String, value: String },
    Missing { field: String },
    Empty { field: String },
    Present { field: String },
}

#[derive(Debug, Clone, PartialEq)]
pub struct CoordinateRegion {
    pub xmin: u32,
    pub ymin: u32,
    pub xmax: u32,
    pub ymax: u32,
    pub keep: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HeaderAction {
    pub action_type: ActionType,
    pub tag: TagSpecifier,
    pub value: Option<ActionValue>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActionType {
    Add,
    Replace,
    Remove,
    Blank,
    Keep,
    Jitter,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ActionValue {
    Literal(String),
    Variable(String),
    Function { name: String },
}

#[derive(Debug, Clone, PartialEq)]
pub enum TagSpecifier {
    Keyword(String),
    TagValue(Tag),
    Pattern(String),
    PrivateTag {
        group: u16,
        creator: String,
        element_offset: u8,
    },
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

const PREDICATE_KEYWORDS: &[&str] = &[
    "notcontains",
    "notequals",
    "contains",
    "equals",
    "missing",
    "empty",
    "present",
];

struct Parser {
    lines: Vec<String>,
    pos: usize,
}

impl Recipe {
    /// Parse a recipe from its textual representation.
    pub fn parse(input: &str) -> Result<Recipe, DeidError> {
        let mut parser = Parser::new(input);
        parser.run()
    }
}

impl Parser {
    fn new(input: &str) -> Self {
        let lines: Vec<String> = input
            .lines()
            .map(|line| strip_comment(line).trim().to_string())
            .collect();
        Parser { lines, pos: 0 }
    }

    fn current_line(&self) -> Option<&str> {
        self.lines.get(self.pos).map(|s| s.as_str())
    }

    fn advance(&mut self) {
        self.pos += 1;
    }

    fn skip_empty(&mut self) {
        while let Some(line) = self.current_line() {
            if line.is_empty() {
                self.advance();
            } else {
                break;
            }
        }
    }

    fn run(&mut self) -> Result<Recipe, DeidError> {
        let format = self.expect_format()?;
        let mut header = Vec::new();
        let mut filters = Vec::new();

        loop {
            self.skip_empty();
            let Some(line) = self.current_line() else {
                break;
            };

            if line == "%header" {
                self.advance();
                header.extend(self.parse_header()?);
            } else if let Some(rest) = line.strip_prefix("%filter ") {
                let filter_type = parse_filter_type(rest.trim())?;
                self.advance();
                let labels = self.parse_filter_section()?;
                filters.push(FilterSection {
                    filter_type,
                    labels,
                });
            } else {
                // Skip unrecognized lines (handles stray /* */ blocks, etc.)
                self.advance();
            }
        }

        Ok(Recipe {
            format,
            header,
            filters,
        })
    }

    fn expect_format(&mut self) -> Result<String, DeidError> {
        self.skip_empty();
        let Some(line) = self.current_line() else {
            return Err(DeidError::RecipeParse("expected FORMAT line".into()));
        };
        let Some(format) = line.strip_prefix("FORMAT ") else {
            return Err(DeidError::RecipeParse(format!(
                "expected FORMAT line, got: {}",
                line
            )));
        };
        let format = format.trim().to_string();
        if format != "dicom" {
            return Err(DeidError::UnsupportedFormat(format));
        }
        self.advance();
        Ok(format)
    }

    fn parse_header(&mut self) -> Result<Vec<HeaderAction>, DeidError> {
        let mut actions = Vec::new();
        loop {
            self.skip_empty();
            let Some(line) = self.current_line() else {
                break;
            };
            if line.starts_with('%') {
                break;
            }
            if let Some(action) = parse_header_action(line)? {
                actions.push(action);
            }
            self.advance();
        }
        Ok(actions)
    }

    fn parse_filter_section(&mut self) -> Result<Vec<FilterLabel>, DeidError> {
        let mut labels = Vec::new();
        loop {
            self.skip_empty();
            let Some(line) = self.current_line() else {
                break;
            };
            if line.starts_with('%') {
                break;
            }
            if let Some(name) = line.strip_prefix("LABEL ") {
                let name = name.trim().to_string();
                self.advance();
                let label = self.parse_filter_label(name)?;
                labels.push(label);
            } else {
                // Skip unrecognized lines
                self.advance();
            }
        }
        Ok(labels)
    }

    fn parse_filter_label(&mut self, name: String) -> Result<FilterLabel, DeidError> {
        let mut conditions = Vec::new();
        let mut coordinates = Vec::new();
        let mut is_first = true;

        loop {
            let Some(line) = self.current_line() else {
                break;
            };
            if line.is_empty() {
                self.advance();
                continue;
            }
            if line.starts_with('%') || line.starts_with("LABEL ") {
                break;
            }
            if is_coordinate_line(line) {
                coordinates.push(parse_coordinate_line(line)?);
            } else if is_condition_line(line) {
                let mut parsed = parse_condition_line(line, is_first)?;
                conditions.append(&mut parsed);
                is_first = false;
            }
            // else: silently skip unrecognized lines
            self.advance();
        }

        Ok(FilterLabel {
            name,
            conditions,
            coordinates,
        })
    }
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

fn strip_comment(line: &str) -> &str {
    line.split_once('#').map(|(l, _)| l).unwrap_or(line)
}

fn parse_filter_type(s: &str) -> Result<FilterType, DeidError> {
    match s {
        "graylist" => Ok(FilterType::Graylist),
        "blacklist" => Ok(FilterType::Blacklist),
        _ => Err(DeidError::RecipeParse(format!(
            "unknown filter type: {}",
            s
        ))),
    }
}

fn is_coordinate_line(line: &str) -> bool {
    line.starts_with("coordinates ")
        || line.starts_with("ctpcoordinates ")
        || line.starts_with("keepcoordinates ")
        || line.starts_with("ctpkeepcoordinates ")
}

fn parse_coordinate_line(line: &str) -> Result<CoordinateRegion, DeidError> {
    let (prefix, rest) = if let Some(rest) = line.strip_prefix("ctpkeepcoordinates ") {
        ("ctpkeep", rest)
    } else if let Some(rest) = line.strip_prefix("ctpcoordinates ") {
        ("ctp", rest)
    } else if let Some(rest) = line.strip_prefix("keepcoordinates ") {
        ("keep", rest)
    } else if let Some(rest) = line.strip_prefix("coordinates ") {
        ("plain", rest)
    } else {
        return Err(DeidError::RecipeParse(format!(
            "not a coordinate line: {}",
            line
        )));
    };

    let is_ctp = prefix.starts_with("ctp");
    let keep = prefix.contains("keep");

    let parts: Vec<&str> = rest.split(',').map(|s| s.trim()).collect();
    if parts.len() != 4 {
        return Err(DeidError::RecipeParse(format!(
            "expected 4 coordinate values, got {}: {}",
            parts.len(),
            line
        )));
    }

    let vals: Vec<u32> = parts
        .iter()
        .map(|s| {
            s.parse::<u32>()
                .map_err(|_| DeidError::RecipeParse(format!("invalid coordinate value: {}", s)))
        })
        .collect::<Result<_, _>>()?;

    let (xmin, ymin, v3, v4) = (vals[0], vals[1], vals[2], vals[3]);
    let (xmax, ymax) = if is_ctp {
        (xmin + v3, ymin + v4)
    } else {
        (v3, v4)
    };

    Ok(CoordinateRegion {
        xmin,
        ymin,
        xmax,
        ymax,
        keep,
    })
}

fn starts_with_predicate_keyword(s: &str) -> bool {
    PREDICATE_KEYWORDS.iter().any(|kw| {
        if let Some(rest) = s.strip_prefix(kw) {
            rest.is_empty() || rest.starts_with(' ')
        } else {
            false
        }
    })
}

fn is_condition_line(line: &str) -> bool {
    let trimmed = line
        .strip_prefix("||")
        .or_else(|| line.strip_prefix('+'))
        .unwrap_or(line)
        .trim_start();
    starts_with_predicate_keyword(trimmed)
}

fn parse_condition_line(line: &str, is_first: bool) -> Result<Vec<Condition>, DeidError> {
    let (first_op, content) = if let Some(rest) = line.strip_prefix("||") {
        (LogicalOp::Or, rest.trim_start())
    } else if let Some(rest) = line.strip_prefix('+') {
        (LogicalOp::And, rest.trim_start())
    } else {
        (
            if is_first {
                LogicalOp::First
            } else {
                LogicalOp::And
            },
            line,
        )
    };

    let parts = split_inline_operators(content);
    let mut conditions = Vec::new();
    for (i, (op, text)) in parts.iter().enumerate() {
        let op = if i == 0 { first_op } else { *op };
        let predicate = parse_predicate(text.trim())?;
        conditions.push(Condition {
            operator: op,
            predicate,
        });
    }
    Ok(conditions)
}

/// Split a condition string on inline `||` and `+` operators, but only when the
/// text following the operator starts with a valid predicate keyword. Otherwise
/// the `||`/`+` is treated as literal value content (e.g. `SOMATOM Definition AS+`).
fn split_inline_operators(content: &str) -> Vec<(LogicalOp, String)> {
    let mut parts: Vec<(LogicalOp, String)> = Vec::new();
    let mut current_start = 0;
    let mut search_from = 0;
    let mut pending_op = LogicalOp::First; // placeholder for first part
    let len = content.len();
    let bytes = content.as_bytes();

    while search_from < len {
        // Find next `||` or `+`, whichever comes first
        let mut found: Option<(usize, usize, LogicalOp)> = None;
        for i in search_from..len {
            if i + 1 < len && bytes[i] == b'|' && bytes[i + 1] == b'|' {
                found = Some((i, 2, LogicalOp::Or));
                break;
            }
            if bytes[i] == b'+' {
                found = Some((i, 1, LogicalOp::And));
                break;
            }
        }

        let Some((pos, op_len, op)) = found else {
            break;
        };

        let right = &content[pos + op_len..];
        let right_trimmed = right.trim_start();

        if starts_with_predicate_keyword(right_trimmed) {
            // Real operator: emit left part, update state
            let left = content[current_start..pos].to_string();
            parts.push((pending_op, left));
            pending_op = op;
            let trimmed_offset = right.len() - right_trimmed.len();
            current_start = pos + op_len + trimmed_offset;
            search_from = current_start;
        } else {
            // Not a real operator, keep scanning past it
            search_from = pos + op_len;
        }
    }

    // Remaining text is the last part
    let last = content[current_start..].to_string();
    parts.push((pending_op, last));

    parts
}

fn parse_predicate(text: &str) -> Result<Predicate, DeidError> {
    // Check longer keywords first to avoid prefix conflicts
    if let Some(rest) = text.strip_prefix("notcontains ") {
        let (field, value) = split_field_value(rest.trim())?;
        Ok(Predicate::NotContains { field, value })
    } else if let Some(rest) = text.strip_prefix("contains ") {
        let (field, value) = split_field_value(rest.trim())?;
        Ok(Predicate::Contains { field, value })
    } else if let Some(rest) = text.strip_prefix("notequals ") {
        let (field, value) = split_field_value(rest.trim())?;
        Ok(Predicate::NotEquals { field, value })
    } else if let Some(rest) = text.strip_prefix("equals ") {
        let (field, value) = split_field_value(rest.trim())?;
        Ok(Predicate::Equals { field, value })
    } else if let Some(rest) = text.strip_prefix("missing ") {
        Ok(Predicate::Missing {
            field: rest.trim().to_string(),
        })
    } else if let Some(rest) = text.strip_prefix("empty ") {
        Ok(Predicate::Empty {
            field: rest.trim().to_string(),
        })
    } else if let Some(rest) = text.strip_prefix("present ") {
        Ok(Predicate::Present {
            field: rest.trim().to_string(),
        })
    } else {
        Err(DeidError::RecipeParse(format!(
            "unknown predicate: {}",
            text
        )))
    }
}

fn split_field_value(text: &str) -> Result<(String, String), DeidError> {
    let (field, value) = text.split_once(' ').ok_or_else(|| {
        DeidError::RecipeParse(format!("expected field and value in predicate: {}", text))
    })?;
    Ok((field.to_string(), value.trim().to_string()))
}

fn parse_header_action(line: &str) -> Result<Option<HeaderAction>, DeidError> {
    let mut parts = line.splitn(3, ' ');
    let action_str = match parts.next() {
        Some(s) if !s.is_empty() => s,
        _ => return Ok(None),
    };

    let action_type = match action_str {
        "ADD" => ActionType::Add,
        "REPLACE" => ActionType::Replace,
        "REMOVE" => ActionType::Remove,
        "BLANK" => ActionType::Blank,
        "KEEP" => ActionType::Keep,
        "JITTER" => ActionType::Jitter,
        _ => return Ok(None), // Unrecognized line, silently skip
    };

    let tag_str = parts
        .next()
        .ok_or_else(|| DeidError::RecipeParse(format!("expected tag after {}", action_str)))?;

    let tag = parse_tag_specifier(tag_str)?;

    let value = match action_type {
        ActionType::Remove | ActionType::Blank | ActionType::Keep => None,
        _ => match parts.next() {
            Some(v) if !v.trim().is_empty() => Some(parse_action_value(v.trim())?),
            _ => None,
        },
    };

    Ok(Some(HeaderAction {
        action_type,
        tag,
        value,
    }))
}

fn parse_tag_specifier(s: &str) -> Result<TagSpecifier, DeidError> {
    if s.starts_with('(') && s.ends_with(')') {
        // (GGGG,EEEE) format
        let inner = &s[1..s.len() - 1];
        let (group_str, elem_str) = inner
            .split_once(',')
            .ok_or_else(|| DeidError::RecipeParse(format!("invalid tag format: {}", s)))?;
        let group = u16::from_str_radix(group_str.trim(), 16)
            .map_err(|_| DeidError::RecipeParse(format!("invalid tag group: {}", group_str)))?;
        let element = u16::from_str_radix(elem_str.trim(), 16)
            .map_err(|_| DeidError::RecipeParse(format!("invalid tag element: {}", elem_str)))?;
        Ok(TagSpecifier::TagValue(Tag(group, element)))
    } else if s.len() == 8 && s.chars().all(|c| c.is_ascii_hexdigit()) {
        let group = u16::from_str_radix(&s[0..4], 16)
            .map_err(|_| DeidError::RecipeParse(format!("invalid tag group: {}", &s[0..4])))?;
        let element = u16::from_str_radix(&s[4..8], 16)
            .map_err(|_| DeidError::RecipeParse(format!("invalid tag element: {}", &s[4..8])))?;
        Ok(TagSpecifier::TagValue(Tag(group, element)))
    } else if s.contains('"') {
        // PrivateTag: "GGGG","Creator","EE"
        let parts: Vec<&str> = s.split(',').collect();
        if parts.len() != 3 {
            return Err(DeidError::RecipeParse(format!(
                "invalid private tag: {}",
                s
            )));
        }
        let group_str = parts[0].trim().trim_matches('"');
        let creator = parts[1].trim().trim_matches('"').to_string();
        let offset_str = parts[2].trim().trim_matches('"');

        let group = u16::from_str_radix(group_str, 16).map_err(|_| {
            DeidError::RecipeParse(format!("invalid private tag group: {}", group_str))
        })?;
        let element_offset = u8::from_str_radix(offset_str, 16).map_err(|_| {
            DeidError::RecipeParse(format!("invalid private tag offset: {}", offset_str))
        })?;

        Ok(TagSpecifier::PrivateTag {
            group,
            creator,
            element_offset,
        })
    } else {
        Ok(TagSpecifier::Keyword(s.to_string()))
    }
}

fn parse_action_value(s: &str) -> Result<ActionValue, DeidError> {
    if let Some(var_name) = s.strip_prefix("var:") {
        Ok(ActionValue::Variable(var_name.to_string()))
    } else if let Some(rest) = s.strip_prefix("func:") {
        let name = rest.split_whitespace().next().unwrap_or(rest).to_string();
        Ok(ActionValue::Function { name })
    } else {
        Ok(ActionValue::Literal(s.to_string()))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // -- r-2-1 ---------------------------------------------------------------

    /// Requirement r-2-1
    #[test]
    fn r2_1_parse_complete_recipe() {
        let input = "\
FORMAT dicom

%filter graylist

LABEL Test Filter
contains Manufacturer GE
coordinates 0,0,512,100

%header

ADD PatientIdentityRemoved YES
REPLACE PatientID var:id
REMOVE OperatorsName YES
";
        let recipe = Recipe::parse(input).expect("should parse");
        assert_eq!(recipe.format, "dicom");
        assert_eq!(recipe.filters.len(), 1);
        assert_eq!(recipe.header.len(), 3);
    }

    // -- r-2-2 ---------------------------------------------------------------

    /// Requirement r-2-2
    #[test]
    fn r2_2_parse_format_declaration() {
        let input = "FORMAT dicom\n\n%header\n";
        let recipe = Recipe::parse(input).expect("should parse");
        assert_eq!(recipe.format, "dicom");
    }

    /// Requirement r-2-2
    #[test]
    fn r2_2_reject_unsupported_format() {
        let input = "FORMAT xml\n";
        let result = Recipe::parse(input);
        assert!(result.is_err(), "unsupported format should be rejected");
    }

    /// Requirement r-2-2
    #[test]
    fn r2_2_reject_missing_format() {
        let input = "%header\nADD PatientIdentityRemoved YES\n";
        let result = Recipe::parse(input);
        assert!(result.is_err(), "missing FORMAT line should be rejected");
    }

    // -- r-2-3 ---------------------------------------------------------------

    /// Requirement r-2-3
    #[test]
    fn r2_3_parse_header_section() {
        let input = "\
FORMAT dicom

%header

ADD PatientIdentityRemoved YES
";
        let recipe = Recipe::parse(input).expect("should parse");
        assert_eq!(recipe.header.len(), 1);
        assert_eq!(recipe.header[0].action_type, ActionType::Add);
    }

    /// Requirement r-2-3
    #[test]
    fn r2_3_parse_filter_graylist_section() {
        let input = "\
FORMAT dicom

%filter graylist

LABEL GE CT Scanner
contains Manufacturer GE
";
        let recipe = Recipe::parse(input).expect("should parse");
        assert_eq!(recipe.filters.len(), 1);
        assert_eq!(recipe.filters[0].filter_type, FilterType::Graylist);
    }

    /// Requirement r-2-3
    #[test]
    fn r2_3_parse_filter_blacklist_section() {
        let input = "\
FORMAT dicom

%filter blacklist

LABEL Unknown Modality
missing Modality
";
        let recipe = Recipe::parse(input).expect("should parse");
        assert_eq!(recipe.filters.len(), 1);
        assert_eq!(recipe.filters[0].filter_type, FilterType::Blacklist);
    }

    // -- r-2-4 ---------------------------------------------------------------

    /// Requirement r-2-4
    #[test]
    fn r2_4_full_line_comments_are_ignored() {
        let input = "\
FORMAT dicom
# This is a comment

%header

# Another comment
ADD PatientIdentityRemoved YES
";
        let recipe = Recipe::parse(input).expect("should parse");
        assert_eq!(recipe.header.len(), 1);
    }

    /// Requirement r-2-4
    #[test]
    fn r2_4_inline_comments_are_stripped() {
        let input = "\
FORMAT dicom

%filter graylist

LABEL Test Filter # this is a comment
contains Manufacturer GE # inline comment
";
        let recipe = Recipe::parse(input).expect("should parse");
        let label = &recipe.filters[0].labels[0];
        assert_eq!(label.name, "Test Filter");
        if let Predicate::Contains { field, value } = &label.conditions[0].predicate {
            assert_eq!(field, "Manufacturer");
            assert_eq!(value, "GE");
        } else {
            panic!("expected Contains predicate");
        }
    }

    // -- r-2-5 ---------------------------------------------------------------

    /// Requirement r-2-5
    #[test]
    fn r2_5_parse_label_directive() {
        let input = "\
FORMAT dicom

%filter graylist

LABEL GE LightSpeed CT Scanner
contains Manufacturer GE
";
        let recipe = Recipe::parse(input).expect("should parse");
        let label = &recipe.filters[0].labels[0];
        assert_eq!(label.name, "GE LightSpeed CT Scanner");
        assert_eq!(label.conditions.len(), 1);
    }

    /// Requirement r-2-5
    #[test]
    fn r2_5_parse_label_with_conditions_and_coordinates() {
        let input = "\
FORMAT dicom

%filter graylist

LABEL Complex Filter
contains Manufacturer GE
+ contains ManufacturerModelName LightSpeed
coordinates 0,0,512,100
";
        let recipe = Recipe::parse(input).expect("should parse");
        let label = &recipe.filters[0].labels[0];
        assert_eq!(label.name, "Complex Filter");
        assert_eq!(label.conditions.len(), 2);
        assert_eq!(label.coordinates.len(), 1);
    }

    /// Requirement r-2-5
    #[test]
    fn r2_5_multiple_labels_in_filter() {
        let input = "\
FORMAT dicom

%filter graylist

LABEL First Filter
contains Manufacturer GE

LABEL Second Filter
contains Manufacturer SIEMENS
";
        let recipe = Recipe::parse(input).expect("should parse");
        assert_eq!(recipe.filters[0].labels.len(), 2);
        assert_eq!(recipe.filters[0].labels[0].name, "First Filter");
        assert_eq!(recipe.filters[0].labels[1].name, "Second Filter");
    }

    // -- r-2-6 ---------------------------------------------------------------

    /// Requirement r-2-6-1
    #[test]
    fn r2_6_1_parse_contains_predicate() {
        let input = "\
FORMAT dicom

%filter graylist

LABEL Test
contains Manufacturer GE MEDICAL
";
        let recipe = Recipe::parse(input).expect("should parse");
        let cond = &recipe.filters[0].labels[0].conditions[0];
        assert_eq!(cond.operator, LogicalOp::First);
        assert_eq!(
            cond.predicate,
            Predicate::Contains {
                field: "Manufacturer".into(),
                value: "GE MEDICAL".into(),
            }
        );
    }

    /// Requirement r-2-6-2
    #[test]
    fn r2_6_2_parse_notcontains_predicate() {
        let input = "\
FORMAT dicom

%filter graylist

LABEL Test
notcontains Manufacturer SIEMENS
";
        let recipe = Recipe::parse(input).expect("should parse");
        let cond = &recipe.filters[0].labels[0].conditions[0];
        assert_eq!(
            cond.predicate,
            Predicate::NotContains {
                field: "Manufacturer".into(),
                value: "SIEMENS".into(),
            }
        );
    }

    /// Requirement r-2-6-3
    #[test]
    fn r2_6_3_parse_equals_predicate() {
        let input = "\
FORMAT dicom

%filter graylist

LABEL Test
equals Modality CT
";
        let recipe = Recipe::parse(input).expect("should parse");
        let cond = &recipe.filters[0].labels[0].conditions[0];
        assert_eq!(
            cond.predicate,
            Predicate::Equals {
                field: "Modality".into(),
                value: "CT".into(),
            }
        );
    }

    /// Requirement r-2-6-4
    #[test]
    fn r2_6_4_parse_notequals_predicate() {
        let input = "\
FORMAT dicom

%filter graylist

LABEL Test
notequals Modality MR
";
        let recipe = Recipe::parse(input).expect("should parse");
        let cond = &recipe.filters[0].labels[0].conditions[0];
        assert_eq!(
            cond.predicate,
            Predicate::NotEquals {
                field: "Modality".into(),
                value: "MR".into(),
            }
        );
    }

    /// Requirement r-2-6-5
    #[test]
    fn r2_6_5_parse_missing_predicate() {
        let input = "\
FORMAT dicom

%filter blacklist

LABEL Test
missing Manufacturer
";
        let recipe = Recipe::parse(input).expect("should parse");
        let cond = &recipe.filters[0].labels[0].conditions[0];
        assert_eq!(
            cond.predicate,
            Predicate::Missing {
                field: "Manufacturer".into(),
            }
        );
    }

    /// Requirement r-2-6-6
    #[test]
    fn r2_6_6_parse_empty_predicate() {
        let input = "\
FORMAT dicom

%filter blacklist

LABEL Test
empty Manufacturer
";
        let recipe = Recipe::parse(input).expect("should parse");
        let cond = &recipe.filters[0].labels[0].conditions[0];
        assert_eq!(
            cond.predicate,
            Predicate::Empty {
                field: "Manufacturer".into(),
            }
        );
    }

    /// Requirement r-2-6-7
    #[test]
    fn r2_6_7_parse_present_predicate() {
        let input = "\
FORMAT dicom

%filter graylist

LABEL Test
present BurnedInAnnotation
";
        let recipe = Recipe::parse(input).expect("should parse");
        let cond = &recipe.filters[0].labels[0].conditions[0];
        assert_eq!(
            cond.predicate,
            Predicate::Present {
                field: "BurnedInAnnotation".into(),
            }
        );
    }

    // -- r-2-7 ---------------------------------------------------------------

    /// Requirement r-2-7-1
    #[test]
    fn r2_7_1_parse_and_operator() {
        let input = "\
FORMAT dicom

%filter graylist

LABEL Test
contains Manufacturer GE
+ contains ManufacturerModelName LightSpeed
";
        let recipe = Recipe::parse(input).expect("should parse");
        let conditions = &recipe.filters[0].labels[0].conditions;
        assert_eq!(conditions.len(), 2);
        assert_eq!(conditions[0].operator, LogicalOp::First);
        assert_eq!(conditions[1].operator, LogicalOp::And);
    }

    /// Requirement r-2-7-2
    #[test]
    fn r2_7_2_parse_or_operator() {
        let input = "\
FORMAT dicom

%filter graylist

LABEL Test
contains Manufacturer GE
|| contains Manufacturer GEMS
";
        let recipe = Recipe::parse(input).expect("should parse");
        let conditions = &recipe.filters[0].labels[0].conditions;
        assert_eq!(conditions.len(), 2);
        assert_eq!(conditions[0].operator, LogicalOp::First);
        assert_eq!(conditions[1].operator, LogicalOp::Or);
    }

    /// Requirement r-2-7-3
    #[test]
    fn r2_7_3_parse_inline_operators() {
        let input = "\
FORMAT dicom

%filter blacklist

LABEL Test
missing Manufacturer || empty Manufacturer
";
        let recipe = Recipe::parse(input).expect("should parse");
        let conditions = &recipe.filters[0].labels[0].conditions;
        assert_eq!(conditions.len(), 2);
        assert_eq!(conditions[0].operator, LogicalOp::First);
        assert_eq!(
            conditions[0].predicate,
            Predicate::Missing {
                field: "Manufacturer".into()
            }
        );
        assert_eq!(conditions[1].operator, LogicalOp::Or);
        assert_eq!(
            conditions[1].predicate,
            Predicate::Empty {
                field: "Manufacturer".into()
            }
        );
    }

    /// Requirement r-2-7-4
    #[test]
    fn r2_7_4_parse_pipe_alternatives_in_value() {
        let input = "\
FORMAT dicom

%filter graylist

LABEL Test
contains ManufacturerModelName A400|A500|A600
";
        let recipe = Recipe::parse(input).expect("should parse");
        let cond = &recipe.filters[0].labels[0].conditions[0];
        // Pipe-separated values are stored as-is; treated as regex alternation at eval time.
        assert_eq!(
            cond.predicate,
            Predicate::Contains {
                field: "ManufacturerModelName".into(),
                value: "A400|A500|A600".into(),
            }
        );
    }

    // -- r-2-8 ---------------------------------------------------------------

    /// Requirement r-2-8-1
    #[test]
    fn r2_8_1_parse_coordinates() {
        let input = "\
FORMAT dicom

%filter graylist

LABEL Test
contains Manufacturer GE
coordinates 10,20,300,400
";
        let recipe = Recipe::parse(input).expect("should parse");
        let coords = &recipe.filters[0].labels[0].coordinates;
        assert_eq!(coords.len(), 1);
        assert_eq!(
            coords[0],
            CoordinateRegion {
                xmin: 10,
                ymin: 20,
                xmax: 300,
                ymax: 400,
                keep: false,
            }
        );
    }

    /// Requirement r-2-8-2
    #[test]
    fn r2_8_2_parse_ctpcoordinates_converted() {
        // CTP format: x, y, width, height -> xmin, ymin, xmin+width, ymin+height
        let input = "\
FORMAT dicom

%filter graylist

LABEL Test
contains Manufacturer GE
ctpcoordinates 10,20,100,50
";
        let recipe = Recipe::parse(input).expect("should parse");
        let coords = &recipe.filters[0].labels[0].coordinates;
        assert_eq!(coords.len(), 1);
        assert_eq!(
            coords[0],
            CoordinateRegion {
                xmin: 10,
                ymin: 20,
                xmax: 110, // 10 + 100
                ymax: 70,  // 20 + 50
                keep: false,
            }
        );
    }

    /// Requirement r-2-8-3
    #[test]
    fn r2_8_3_parse_keepcoordinates() {
        let input = "\
FORMAT dicom

%filter graylist

LABEL Test
contains Manufacturer GE
keepcoordinates 0,0,100,100
";
        let recipe = Recipe::parse(input).expect("should parse");
        let coords = &recipe.filters[0].labels[0].coordinates;
        assert_eq!(coords.len(), 1);
        assert!(coords[0].keep, "keepcoordinates should set keep=true");
    }

    /// Requirement r-2-8-3
    #[test]
    fn r2_8_3_parse_ctpkeepcoordinates() {
        let input = "\
FORMAT dicom

%filter graylist

LABEL Test
contains Manufacturer GE
ctpkeepcoordinates 10,20,100,50
";
        let recipe = Recipe::parse(input).expect("should parse");
        let coords = &recipe.filters[0].labels[0].coordinates;
        assert_eq!(coords.len(), 1);
        assert_eq!(
            coords[0],
            CoordinateRegion {
                xmin: 10,
                ymin: 20,
                xmax: 110,
                ymax: 70,
                keep: true,
            }
        );
    }

    /// Requirement r-2-8-4
    #[test]
    fn r2_8_4_multiple_coordinate_regions() {
        let input = "\
FORMAT dicom

%filter graylist

LABEL Test
contains Manufacturer GE
coordinates 0,0,512,100
coordinates 0,400,512,512
ctpcoordinates 100,100,50,50
";
        let recipe = Recipe::parse(input).expect("should parse");
        let coords = &recipe.filters[0].labels[0].coordinates;
        assert_eq!(coords.len(), 3);
    }

    // -- r-2-9 ---------------------------------------------------------------

    /// Requirement r-2-9-1
    #[test]
    fn r2_9_1_parse_literal_value() {
        let input = "\
FORMAT dicom

%header

ADD PatientIdentityRemoved YES
";
        let recipe = Recipe::parse(input).expect("should parse");
        assert_eq!(
            recipe.header[0].value,
            Some(ActionValue::Literal("YES".into()))
        );
    }

    /// Requirement r-2-9-2
    #[test]
    fn r2_9_2_parse_variable_reference() {
        let input = "\
FORMAT dicom

%header

REPLACE PatientID var:PATIENTID
";
        let recipe = Recipe::parse(input).expect("should parse");
        assert_eq!(
            recipe.header[0].value,
            Some(ActionValue::Variable("PATIENTID".into()))
        );
    }

    /// Requirement r-2-9-3
    #[test]
    fn r2_9_3_parse_function_reference() {
        let input = "\
FORMAT dicom

%header

REPLACE SOPInstanceUID func:hashuid
";
        let recipe = Recipe::parse(input).expect("should parse");
        assert_eq!(
            recipe.header[0].value,
            Some(ActionValue::Function {
                name: "hashuid".into()
            })
        );
    }

    // -- r-2-10 --------------------------------------------------------------

    /// Requirement r-2-10-1
    #[test]
    fn r2_10_1_graylist_filter_type() {
        let input = "\
FORMAT dicom

%filter graylist

LABEL Test
contains Manufacturer GE
coordinates 0,0,512,100
";
        let recipe = Recipe::parse(input).expect("should parse");
        assert_eq!(recipe.filters[0].filter_type, FilterType::Graylist);
        assert!(
            !recipe.filters[0].labels[0].coordinates.is_empty(),
            "graylist labels should support coordinate directives"
        );
    }

    /// Requirement r-2-10-2
    #[test]
    fn r2_10_2_blacklist_filter_type() {
        let input = "\
FORMAT dicom

%filter blacklist

LABEL Reject Unknown
missing Modality
";
        let recipe = Recipe::parse(input).expect("should parse");
        assert_eq!(recipe.filters[0].filter_type, FilterType::Blacklist);
    }
}
