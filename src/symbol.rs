pub fn normalize_for_pairing(symbol: &str, venue: &str) -> String {
    let symbol = symbol.trim();
    if symbol.is_empty() {
        return String::new();
    }
    let upper = symbol.to_uppercase();
    let mut cleaned = upper.replace(['-', '_'], "");
    if venue.starts_with("okex") && cleaned.ends_with("SWAP") {
        cleaned.truncate(cleaned.len().saturating_sub(4));
    }
    cleaned
}

pub fn normalize_for_whitelist(symbol: &str) -> String {
    let symbol = symbol.trim();
    if symbol.is_empty() {
        return String::new();
    }
    let mut cleaned = symbol.to_uppercase().replace(['-', '_'], "");
    if cleaned.ends_with("SWAP") {
        cleaned.truncate(cleaned.len().saturating_sub(4));
    }
    cleaned
}
