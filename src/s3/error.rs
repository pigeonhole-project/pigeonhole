use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::body::Body;

#[derive(Debug)]
pub struct S3Error {
    status: StatusCode,
    code: &'static str,
    message: String,
    resource: String,
}

impl S3Error {
    pub fn no_such_bucket(bucket: &str) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "NoSuchBucket",
            message: "The specified bucket does not exist".into(),
            resource: format!("/{bucket}"),
        }
    }

    pub fn no_such_key(bucket: &str, key: &str) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "NoSuchKey",
            message: "The specified key does not exist".into(),
            resource: format!("/{bucket}/{key}"),
        }
    }

    pub fn bucket_not_empty(bucket: &str) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code: "BucketNotEmpty",
            message: "The bucket you tried to delete is not empty".into(),
            resource: format!("/{bucket}"),
        }
    }

    pub fn invalid_bucket_name(name: &str) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "InvalidBucketName",
            message: format!("The specified bucket is not valid: {name}"),
            resource: format!("/{name}"),
        }
    }

    pub fn invalid_argument(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "InvalidArgument",
            message: msg.into(),
            resource: "/".into(),
        }
    }

    pub fn method_not_allowed() -> Self {
        Self {
            status: StatusCode::METHOD_NOT_ALLOWED,
            code: "MethodNotAllowed",
            message: "The specified method is not allowed against this resource".into(),
            resource: "/".into(),
        }
    }

    pub fn access_denied() -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            code: "AccessDenied",
            message: "Access Denied".into(),
            resource: "/".into(),
        }
    }

    pub fn signature_mismatch() -> Self {
        Self {
            status: StatusCode::FORBIDDEN,
            code: "SignatureDoesNotMatch",
            message: "The request signature we calculated does not match the signature you provided".into(),
            resource: "/".into(),
        }
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "InternalError",
            message: msg.into(),
            resource: "/".into(),
        }
    }
}

impl From<anyhow::Error> for S3Error {
    fn from(e: anyhow::Error) -> Self {
        S3Error::internal(e.to_string())
    }
}

impl From<sqlx::Error> for S3Error {
    fn from(e: sqlx::Error) -> Self {
        S3Error::internal(e.to_string())
    }
}

impl IntoResponse for S3Error {
    fn into_response(self) -> Response {
        let body = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>{}</Code>
  <Message>{}</Message>
  <Resource>{}</Resource>
  <RequestId>tg3</RequestId>
</Error>"#,
            xml_escape(self.code),
            xml_escape(&self.message),
            xml_escape(&self.resource),
        );
        Response::builder()
            .status(self.status)
            .header(axum::http::header::CONTENT_TYPE, "application/xml")
            .body(Body::from(body))
            .unwrap()
    }
}

fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
