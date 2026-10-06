use crate::index::{Bucket, ObjectMeta};

pub fn list_all_my_buckets(buckets: &[Bucket]) -> String {
    let mut items = String::new();
    for b in buckets {
        items.push_str(&format!(
            "<Bucket><Name>{}</Name><CreationDate>{}</CreationDate></Bucket>",
            xml_escape(&b.name),
            xml_escape(&b.created_at),
        ));
    }
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Owner><ID>tg3</ID><DisplayName>tg3</DisplayName></Owner>
  <Buckets>{items}</Buckets>
</ListAllMyBucketsResult>"#
    )
}

#[allow(clippy::too_many_arguments)]
pub fn list_objects_v2(
    bucket: &str,
    prefix: &str,
    delimiter: Option<&str>,
    max_keys: i64,
    objects: &[ObjectMeta],
    common_prefixes: &[String],
    truncated: bool,
    next_token: Option<&str>,
) -> String {
    let mut contents = String::new();
    for o in objects {
        contents.push_str(&format!(
            "<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>&quot;{}&quot;</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
            xml_escape(&o.key),
            xml_escape(&o.mtime),
            xml_escape(&o.etag),
            o.size,
        ));
    }

    let mut cps = String::new();
    for p in common_prefixes {
        cps.push_str(&format!(
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            xml_escape(p)
        ));
    }

    let delim = delimiter
        .map(|d| format!("<Delimiter>{}</Delimiter>", xml_escape(d)))
        .unwrap_or_default();

    let next = next_token
        .map(|t| {
            format!(
                "<NextContinuationToken>{}</NextContinuationToken>",
                xml_escape(t)
            )
        })
        .unwrap_or_default();

    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>{}</Name>
  <Prefix>{}</Prefix>
  {delim}
  <MaxKeys>{max_keys}</MaxKeys>
  <IsTruncated>{}</IsTruncated>
  <KeyCount>{}</KeyCount>
  {next}
  {contents}
  {cps}
</ListBucketResult>"#,
        xml_escape(bucket),
        xml_escape(prefix),
        if truncated { "true" } else { "false" },
        objects.len() + common_prefixes.len(),
    )
}

pub fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
