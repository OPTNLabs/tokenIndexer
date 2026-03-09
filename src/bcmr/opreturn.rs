use url::Url;

#[derive(Debug, Clone)]
pub struct ParsedBcmrOpReturn {
    pub claimed_hash_hex: String,
    pub encoded_url_hex: String,
    pub decoded_url: String,
}

pub fn parse_bcmr_op_return(op_return_asm: &str) -> Option<ParsedBcmrOpReturn> {
    let tokens: Vec<&str> = op_return_asm.split_whitespace().collect();
    let magic_index = tokens.iter().position(|t| *t == "1380795202")?;
    if magic_index + 2 >= tokens.len() {
        return None;
    }

    let claimed_hash_hex = tokens[magic_index + 1].to_ascii_lowercase();
    let encoded_url_hex = tokens[magic_index + 2].to_ascii_lowercase();
    let decoded_url = normalize_bcmr_url(&encoded_url_hex)?;

    Some(ParsedBcmrOpReturn {
        claimed_hash_hex,
        encoded_url_hex,
        decoded_url,
    })
}

pub fn decode_hex_ascii(input: &str) -> Option<String> {
    let bytes = hex::decode(input).ok()?;
    String::from_utf8(bytes).ok()
}

fn normalize_bcmr_url(encoded_url_hex: &str) -> Option<String> {
    let mut decoded = decode_hex_ascii(encoded_url_hex)?;
    decoded = decoded.trim().to_string();
    if decoded.is_empty() {
        return None;
    }

    if decoded.starts_with("http://")
        || decoded.starts_with("https://")
        || decoded.starts_with("ipfs://")
    {
        // Keep explicit scheme as-is; worker applies transport security checks.
    } else if looks_like_ipfs_ref(&decoded) {
        decoded = format!("ipfs://{decoded}");
    } else {
        decoded = format!("https://{decoded}");
    }

    if let Ok(url) = Url::parse(&decoded) {
        if url.scheme() != "https" && url.scheme() != "http" && url.scheme() != "ipfs" {
            return None;
        }
        if (url.scheme() == "https" || url.scheme() == "http")
            && (url.path().is_empty() || url.path() == "/")
            && !decoded.contains("ipfs.nftstorage.link")
        {
            let with_well_known = decoded.trim_end_matches('/');
            decoded = format!("{with_well_known}/.well-known/bitcoin-cash-metadata-registry.json");
        }
    }

    Some(decoded)
}

fn looks_like_ipfs_ref(input: &str) -> bool {
    let lower = input.to_ascii_lowercase();
    lower.starts_with("bafy")
        || input.starts_with("Qm")
        || lower.starts_with("/ipfs/")
        || (!input.contains('.') && !input.contains('/'))
}
