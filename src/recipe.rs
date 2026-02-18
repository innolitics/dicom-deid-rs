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

impl Recipe {
    /// Parse a recipe from its textual representation.
    pub fn parse(input: &str) -> Result<Recipe, DeidError> {
        todo!()
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
