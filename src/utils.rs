use anyhow::Result;
use base64::Engine;

// Shamelessly taken from clean_path crate https://docs.rs/clean-path/latest/clean_path/
pub fn clean_path(path: &str) -> String {
    match path {
        "" => return ".".to_string(),
        "." => return ".".to_string(),
        ".." => return "..".to_string(),
        "/" => return "/".to_string(),
        _ => {}
    }

    let mut out = vec![];
    let is_root = path.starts_with('/');

    let path = path.trim_end_matches('/');
    let num_segments = path.split('/').count();

    for segment in path.split('/') {
        match segment {
            "" => continue,
            "." => {
                if num_segments == 1 {
                    out.push(segment);
                };
                continue;
            }
            ".." => {
                let previous = out.pop();
                if previous.is_some() && !can_backtrack(previous.unwrap()) {
                    out.push(previous.unwrap());
                    out.push(segment);
                } else if previous.is_none() && !is_root {
                    out.push(segment);
                };
                continue;
            }
            _ => {
                out.push(segment);
            }
        };
    }

    let mut out_str = out.join("/");

    if is_root {
        out_str = format!("/{}", out_str);
    }

    if out_str.is_empty() {
        return ".".to_string();
    }

    out_str
}

fn can_backtrack(segment: &str) -> bool {
    match segment {
        "." => false,
        ".." => false,
        _ => true,
    }
}

#[derive(Debug)]
pub struct Query {
    pub key: String,
    pub val: String
}

pub fn get_queries(path: &str) -> Vec<Query> {
    let mut queries = Vec::new();
    if let Some(i) = path.find('?') {
        for query in path[i+1..].split('&') {
            if let Some((key, val)) = query.replace( '+', " " ).split_once('=') {
                let key = urlencoding::decode(key);
                let val = urlencoding::decode(val);
                if let Ok(key) = key {
                    if let Ok(val) = val {
                        queries.push(Query { key: key.to_string(), val: val.to_string() });
                    }
                }
            }
        }
    }

    queries
}

pub fn get_queries_val_for_keys<'a>(keys: &[&str], queries: &'a [Query]) -> Vec<Option<&'a str>> {
    let mut vals = vec![None; keys.len()];

    for i in 0..keys.len() {
        for query in queries {
            if query.key.eq(keys[i]) {
                vals[i] = Some(query.val.as_str());
                break;
            }
        }
    }

    vals
}

pub fn get_header< 'a >(key: &str, headers: &[httparse::Header< 'a >]) -> Option<&'a [ u8 ]> {
    let key = key.to_lowercase();
    for header in headers {
        if header.name.to_lowercase() == key {
            return Some(header.value)
        }
    }
    None
}

pub fn get_basic_auth( headers: &[httparse::Header] ) -> Result<Option<(String, String)>> {
    if let Some(auth) = get_header("Authorization", headers) {
        let basic_auth = std::str::from_utf8(auth)?.replace("Basic ", "");
        let bs64       = base64::engine::general_purpose::URL_SAFE;
        if let Some((name, pass)) = std::str::from_utf8(&bs64.decode(basic_auth)?)?.split_once(':') {
            return Ok(Some((String::from(name), String::from(pass))))
        }
    }
    Ok(None)
}
