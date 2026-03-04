// Copyright (c) 2025 Chris Corbyn <chris@zizq.io>
// Licensed under the Business Source License 1.1. See LICENSE file for details.

//! License validation for paid feature gating.
//!
//! License keys are Ed25519-signed JWTs containing licensee info, tier, and
//! expiry. The public key is embedded at compile time via the
//! `ZIZQ_LICENSE_PUBLIC_KEY` environment variable. Dev builds without the
//! key cannot validate licenses (and that's fine).

use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::Deserialize;

/// Compile-time public key for license verification.
///
/// Set `ZIZQ_LICENSE_PUBLIC_KEY` to a PEM-encoded Ed25519 public key when
/// building release binaries. When unset, `from_token()` returns an error.
///
/// In development, this can be done by setting the env var in
/// .cargo/config.toml.
const LICENSE_PUBLIC_KEY: Option<&str> = option_env!("ZIZQ_LICENSE_PUBLIC_KEY");

/// Available license tiers, ordered by capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Tier {
    Pro,
    Enterprise,
}

impl std::fmt::Display for Tier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Tier::Pro => write!(f, "pro"),
            Tier::Enterprise => write!(f, "enterprise"),
        }
    }
}

/// Gated features that require a paid license.
///
/// Adding a paid feature: add a variant, set its minimum tier in
/// `min_tier()`, add a check in the relevant handler.
#[derive(Debug, Clone, Copy)]
pub enum Feature {
    /// Terminal UI dashboard.
    Tui,
    /// Example Pro feature for testing the gating machinery.
    ProExample,
    /// Example Enterprise feature for testing the gating machinery.
    EnterpriseExample,
}

impl Feature {
    /// Minimum tier required for this feature.
    pub fn min_tier(self) -> Tier {
        match self {
            Feature::Tui => Tier::Pro,
            Feature::ProExample => Tier::Pro,
            Feature::EnterpriseExample => Tier::Enterprise,
        }
    }
}

impl std::fmt::Display for Feature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Feature::Tui => write!(f, "terminal UI"),
            Feature::ProExample => write!(f, "pro example"),
            Feature::EnterpriseExample => write!(f, "enterprise example"),
        }
    }
}

/// JWT claims embedded in a license token.
#[derive(Deserialize)]
struct Claims {
    /// Licensee identifier (e.g. "lic_abc123").
    sub: String,
    /// Human-readable licensee/organisation name.
    name: String,
    /// Expiry timestamp (validated by the jsonwebtoken crate).
    exp: u64,
    /// Tier name: "pro", "enterprise".
    tier: String,
}

/// A validated license or the absence of one (free tier).
#[derive(Debug, Clone)]
pub enum License {
    /// No license — free tier only.
    Free,

    /// A verified license.
    Licensed {
        licensee_id: String,
        licensee_name: String,
        tier: Tier,
        expires_at: u64,
    },
}

impl License {
    /// Parse and validate a license JWT using the compiled-in public key.
    ///
    /// Returns an error if the public key was not compiled in, or if the
    /// token is invalid/expired.
    pub fn from_token(token: &str) -> Result<Self, String> {
        let pem = LICENSE_PUBLIC_KEY.ok_or("license validation not available in this build")?;
        from_token_with_key(token, pem)
    }

    /// Check whether this license permits a given feature.
    ///
    /// `now` is milliseconds since the Unix epoch.
    /// Checks both tier and expiry — a license that was valid at startup
    /// can still expire while the server is running.
    ///
    /// Returns `Ok(())` if allowed, or an error message suitable for
    /// returning to the client.
    pub fn require(&self, now: u64, feature: Feature) -> Result<(), String> {
        let min_tier = feature.min_tier();

        if let Some(tier) = self.tier() {
            if tier >= min_tier {
                if self.is_expired(now) {
                    return Err(format!(
                        "License expired. {feature} requires a {min_tier} license."
                    ));
                } else {
                    return Ok(());
                }
            }
        }

        Err(format!("{feature} requires a {min_tier} license"))
    }

    /// Returns true if the license has expired.
    ///
    /// `now` is milliseconds since the Unix epoch.
    /// `Free` is never considered expired (it was never valid).
    pub fn is_expired(&self, now: u64) -> bool {
        match self {
            License::Free => false,
            License::Licensed { expires_at, .. } => {
                let now_secs = now / 1000;
                now_secs >= *expires_at
            }
        }
    }

    /// Returns the tier if licensed, or `None` for free tier.
    pub fn tier(&self) -> Option<Tier> {
        match self {
            License::Free => None,
            License::Licensed { tier, .. } => Some(*tier),
        }
    }
}

/// Parse a tier string into a `Tier` enum value.
fn parse_tier(s: &str) -> Result<Tier, String> {
    match s {
        "pro" => Ok(Tier::Pro),
        "enterprise" => Ok(Tier::Enterprise),
        other => Err(format!("unknown license tier: {other}")),
    }
}

/// Validate a license JWT against an explicit PEM-encoded Ed25519 public key.
fn from_token_with_key(token: &str, pem: &str) -> Result<License, String> {
    let key = DecodingKey::from_ed_pem(pem.as_bytes())
        .map_err(|e| format!("invalid license public key: {e}"))?;

    let mut validation = Validation::new(Algorithm::EdDSA);
    validation.set_required_spec_claims(&["exp", "sub", "name"]);

    let data = jsonwebtoken::decode::<Claims>(token, &key, &validation)
        .map_err(|e| format!("license validation failed: {e}"))?;

    let tier = parse_tier(&data.claims.tier)?;

    Ok(License::Licensed {
        licensee_id: data.claims.sub,
        licensee_name: data.claims.name,
        tier,
        expires_at: data.claims.exp,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    use ed25519_dalek::SigningKey;
    use ed25519_dalek::pkcs8::spki::der::pem::LineEnding;
    use ed25519_dalek::pkcs8::{EncodePrivateKey, EncodePublicKey};
    use jsonwebtoken::{EncodingKey, Header};
    use rand::rngs::OsRng;
    use serde::Serialize;

    #[derive(Serialize)]
    struct TestClaims {
        sub: String,
        name: String,
        exp: u64,
        tier: String,
    }

    /// Generate an ephemeral Ed25519 keypair and return (private PEM, public PEM).
    fn gen_keypair() -> (String, String) {
        let signing_key = SigningKey::generate(&mut OsRng);
        let private_pem = signing_key
            .to_pkcs8_pem(LineEnding::LF)
            .expect("failed to encode private key");
        let public_pem = signing_key
            .verifying_key()
            .to_public_key_pem(LineEnding::LF)
            .expect("failed to encode public key");
        (private_pem.to_string(), public_pem)
    }

    /// Sign a JWT with the given private key PEM.
    fn sign_jwt(claims: &TestClaims, private_pem: &str) -> String {
        let key = EncodingKey::from_ed_pem(private_pem.as_bytes())
            .expect("failed to create encoding key");
        let header = Header::new(Algorithm::EdDSA);
        jsonwebtoken::encode(&header, claims, &key).expect("failed to sign JWT")
    }

    /// Convert seconds to milliseconds (matching the clock convention).
    fn secs_to_ms(secs: u64) -> u64 {
        secs * 1000
    }

    fn future_exp() -> u64 {
        // 1 year from now
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 365 * 24 * 3600
    }

    fn past_exp() -> u64 {
        // 1 hour ago
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - 3600
    }

    #[test]
    fn tier_ordering() {
        assert!(Tier::Pro < Tier::Enterprise);
    }

    #[test]
    fn valid_pro_license() {
        let (priv_pem, pub_pem) = gen_keypair();
        let claims = TestClaims {
            sub: "lic_123".into(),
            name: "Acme Corp".into(),
            exp: future_exp(),
            tier: "pro".into(),
        };
        let token = sign_jwt(&claims, &priv_pem);
        let lic = from_token_with_key(&token, &pub_pem).expect("should succeed");
        match lic {
            License::Licensed {
                licensee_id,
                licensee_name,
                tier,
                ..
            } => {
                assert_eq!(licensee_id, "lic_123");
                assert_eq!(licensee_name, "Acme Corp");
                assert_eq!(tier, Tier::Pro);
            }
            License::Free => panic!("expected Licensed"),
        }
    }

    #[test]
    fn valid_enterprise_license() {
        let (priv_pem, pub_pem) = gen_keypair();
        let claims = TestClaims {
            sub: "lic_456".into(),
            name: "Globex Inc".into(),
            exp: future_exp(),
            tier: "enterprise".into(),
        };
        let token = sign_jwt(&claims, &priv_pem);
        let lic = from_token_with_key(&token, &pub_pem).expect("should succeed");
        assert_eq!(lic.tier(), Some(Tier::Enterprise));
    }

    #[test]
    fn expired_license_rejected() {
        let (priv_pem, pub_pem) = gen_keypair();
        let claims = TestClaims {
            sub: "lic_789".into(),
            name: "Initech".into(),
            exp: past_exp(),
            tier: "pro".into(),
        };
        let token = sign_jwt(&claims, &priv_pem);
        let err = from_token_with_key(&token, &pub_pem).unwrap_err();
        assert!(
            err.contains("ExpiredSignature"),
            "expected expiry error, got: {err}"
        );
    }

    #[test]
    fn wrong_key_rejected() {
        let (priv_pem, _) = gen_keypair();
        let (_, other_pub_pem) = gen_keypair();
        let claims = TestClaims {
            sub: "lic_000".into(),
            name: "Test Org".into(),
            exp: future_exp(),
            tier: "pro".into(),
        };
        let token = sign_jwt(&claims, &priv_pem);
        let err = from_token_with_key(&token, &other_pub_pem).unwrap_err();
        assert!(
            err.contains("InvalidSignature"),
            "expected signature error, got: {err}"
        );
    }

    #[test]
    fn unknown_tier_rejected() {
        let (priv_pem, pub_pem) = gen_keypair();
        let claims = TestClaims {
            sub: "lic_000".into(),
            name: "Test Org".into(),
            exp: future_exp(),
            tier: "platinum".into(),
        };
        let token = sign_jwt(&claims, &priv_pem);
        let err = from_token_with_key(&token, &pub_pem).unwrap_err();
        assert!(
            err.contains("unknown license tier"),
            "expected tier error, got: {err}"
        );
    }

    #[test]
    fn garbage_token_rejected() {
        let (_, pub_pem) = gen_keypair();
        let err = from_token_with_key("not-a-jwt", &pub_pem).unwrap_err();
        assert!(
            err.contains("license validation failed"),
            "expected validation error, got: {err}"
        );
    }

    #[test]
    fn free_tier_accessors() {
        let lic = License::Free;
        assert_eq!(lic.tier(), None);
    }

    #[test]
    fn licensed_tier_accessor() {
        let lic = License::Licensed {
            licensee_id: "lic_test".into(),
            licensee_name: "Test Org".into(),
            tier: Tier::Pro,
            expires_at: 0,
        };
        assert_eq!(lic.tier(), Some(Tier::Pro));
    }

    #[test]
    fn is_expired_with_past_expiry() {
        let exp = 1_000_000;
        let lic = License::Licensed {
            licensee_id: "lic_test".into(),
            licensee_name: "Test Org".into(),
            tier: Tier::Pro,
            expires_at: exp,
        };
        assert!(lic.is_expired(secs_to_ms(exp + 1)));
    }

    #[test]
    fn is_expired_at_exact_boundary() {
        let exp = 1_000_000;
        let lic = License::Licensed {
            licensee_id: "lic_test".into(),
            licensee_name: "Test Org".into(),
            tier: Tier::Pro,
            expires_at: exp,
        };
        assert!(lic.is_expired(secs_to_ms(exp)));
    }

    #[test]
    fn is_not_expired_before_expiry() {
        let exp = 1_000_000;
        let lic = License::Licensed {
            licensee_id: "lic_test".into(),
            licensee_name: "Test Org".into(),
            tier: Tier::Pro,
            expires_at: exp,
        };
        assert!(!lic.is_expired(secs_to_ms(exp - 1)));
    }

    #[test]
    fn free_is_not_expired() {
        assert!(!License::Free.is_expired(u64::MAX));
    }

    fn licensed(tier: Tier, expires_at: u64) -> License {
        License::Licensed {
            licensee_id: "lic_test".into(),
            licensee_name: "Test Org".into(),
            tier,
            expires_at,
        }
    }

    #[test]
    fn require_free_rejects_pro_feature() {
        let err = License::Free.require(0, Feature::ProExample).unwrap_err();
        assert!(err.contains("pro"), "expected pro in error, got: {err}");
    }

    #[test]
    fn require_free_rejects_enterprise_feature() {
        let err = License::Free
            .require(0, Feature::EnterpriseExample)
            .unwrap_err();
        assert!(
            err.contains("enterprise"),
            "expected enterprise in error, got: {err}"
        );
    }

    #[test]
    fn require_pro_allows_pro_feature() {
        let lic = licensed(Tier::Pro, 2_000_000);
        lic.require(secs_to_ms(1_000_000), Feature::ProExample)
            .expect("pro license should allow pro feature");
    }

    #[test]
    fn require_pro_rejects_enterprise_feature() {
        let lic = licensed(Tier::Pro, 2_000_000);
        let err = lic
            .require(secs_to_ms(1_000_000), Feature::EnterpriseExample)
            .unwrap_err();
        assert!(
            err.contains("enterprise"),
            "expected enterprise in error, got: {err}"
        );
    }

    #[test]
    fn require_enterprise_allows_pro_feature() {
        let lic = licensed(Tier::Enterprise, 2_000_000);
        lic.require(secs_to_ms(1_000_000), Feature::ProExample)
            .expect("enterprise license should allow pro feature");
    }

    #[test]
    fn require_enterprise_allows_enterprise_feature() {
        let lic = licensed(Tier::Enterprise, 2_000_000);
        lic.require(secs_to_ms(1_000_000), Feature::EnterpriseExample)
            .expect("enterprise license should allow enterprise feature");
    }

    #[test]
    fn require_expired_license_rejects() {
        let lic = licensed(Tier::Pro, 1_000_000);
        let err = lic
            .require(secs_to_ms(1_000_001), Feature::ProExample)
            .unwrap_err();
        assert!(
            err.contains("expired"),
            "expected expired in error, got: {err}"
        );
    }
}
