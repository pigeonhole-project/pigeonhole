//! The S3 calls of the UI's client panel, straight to the gateway's handlers
//! (no HTTP): what an S3 client would send.

use crate::Admin;
use anyhow::{Context, Result};
use bytes::{Bytes, BytesMut};
use http::{Extensions, HeaderMap, Method, Uri};
use s3s::dto::{CopyObjectInput, CreateBucketInput, DeleteObjectInput, GetObjectInput, ListObjectsV2Input};
use s3s::dto::{PutObjectInput, StreamingBlob};
use s3s::{S3Request, S3};

fn req<T>(input: T) -> S3Request<T> {
    S3Request {
        input,
        method: Method::GET,
        uri: Uri::from_static("/"),
        headers: HeaderMap::new(),
        extensions: Extensions::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

impl Admin {
    pub async fn create_bucket(&self, bucket: &str) -> Result<()> {
        self.s3.create_bucket(req(CreateBucketInput { bucket: bucket.into(), ..Default::default() })).await?;
        Ok(())
    }

    pub async fn put(&self, bucket: &str, key: &str, body: Bytes) -> Result<()> {
        let input = PutObjectInput {
            bucket: bucket.into(),
            key: key.into(),
            body: Some(StreamingBlob::from_bytes(body)),
            ..Default::default()
        };
        self.s3.put_object(req(input)).await?;
        Ok(())
    }

    /// The object, or the bytes `first..=last` of it.
    pub async fn get(&self, bucket: &str, key: &str, range: Option<(u64, u64)>) -> Result<Bytes> {
        let input = GetObjectInput {
            bucket: bucket.into(),
            key: key.into(),
            range: range.map(|(first, last)| s3s::dto::Range::Int { first, last: Some(last) }),
            ..Default::default()
        };
        let out = self.s3.get_object(req(input)).await?;
        let mut body = out.output.body.context("no body")?;
        let mut buf = BytesMut::new();
        while let Some(chunk) = futures_next(&mut body).await {
            buf.extend_from_slice(&chunk?);
        }
        Ok(buf.freeze())
    }

    pub async fn copy(&self, bucket: &str, from: &str, to: &str) -> Result<()> {
        let input = CopyObjectInput::builder()
            .bucket(bucket.into())
            .key(to.into())
            .copy_source(s3s::dto::CopySource::Bucket { bucket: bucket.into(), key: from.into(), version_id: None })
            .build()?;
        self.s3.copy_object(req(input)).await?;
        Ok(())
    }

    pub async fn delete(&self, bucket: &str, key: &str) -> Result<()> {
        let input = DeleteObjectInput { bucket: bucket.into(), key: key.into(), ..Default::default() };
        self.s3.delete_object(req(input)).await?;
        Ok(())
    }

    /// The buckets' names.
    pub async fn buckets(&self) -> Result<Vec<String>> {
        let out = self.s3.list_buckets(req(s3s::dto::ListBucketsInput::default())).await?;
        Ok(out.output.buckets.unwrap_or_default().into_iter().filter_map(|b| b.name).collect())
    }

    /// The keys in `bucket` with their sizes.
    pub async fn list(&self, bucket: &str) -> Result<Vec<(String, i64)>> {
        let input = ListObjectsV2Input { bucket: bucket.into(), ..Default::default() };
        let out = self.s3.list_objects_v2(req(input)).await?;
        Ok(out
            .output
            .contents
            .unwrap_or_default()
            .into_iter()
            .map(|o| (o.key.unwrap_or_default(), o.size.unwrap_or(0)))
            .collect())
    }
}

async fn futures_next(body: &mut StreamingBlob) -> Option<Result<Bytes>> {
    use futures::StreamExt;
    body.next().await.map(|r| r.map_err(|e| anyhow::anyhow!("{e}")))
}
