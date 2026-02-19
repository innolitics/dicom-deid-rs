use thiserror::Error;

#[derive(Debug, Error)]
pub enum DeidError {
    #[error("Recipe parse error: {0}")]
    RecipeParse(String),

    #[error("Unsupported format: {0}")]
    UnsupportedFormat(String),

    #[error("Tag resolution error: {0}")]
    TagResolution(String),

    #[error("Compressed pixel data cannot be masked without decompression: {0}")]
    CompressedPixelData(String),

    #[error("DICOM error: {0}")]
    Dicom(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Variable not found: {0}")]
    VariableNotFound(String),

    #[error("Function not found: {0}")]
    FunctionNotFound(String),
}
