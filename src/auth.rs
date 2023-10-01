use super::{Error, Result};
use rand;
use base64::{Engine as _, engine::general_purpose};
use rsa::{
    Pkcs1v15Encrypt, RsaPrivateKey,
    pkcs1v15::{SigningKey, VerifyingKey, Signature},
    signature::{Keypair, RandomizedSigner, SignatureEncoding, Verifier},
    sha2::{Digest, Sha256},
};

pub struct Authenticator {
    private_key: RsaPrivateKey,
    signing_key: SigningKey<Sha256>,
    verifying_key: VerifyingKey<Sha256>,
}

impl Authenticator {
    pub fn new(private_key: RsaPrivateKey) -> Self {
        let signing_key = SigningKey::<Sha256>::new(private_key.clone());
        let verifying_key = signing_key.verifying_key();
        Authenticator {
            private_key, signing_key, verifying_key
        }
    }

    pub fn random(bits: usize) -> Result<Self> {
        let mut rng = rand::thread_rng();
        Ok(Authenticator::new(RsaPrivateKey::new(&mut rng, bits)?))
    }

    pub fn random_2048() -> Result<Self> {
        Authenticator::random(2048)
    }

    // Accessors
    pub fn private_key(&self) -> &RsaPrivateKey {
        &self.private_key
    }

    pub fn signing_key(&self) -> &SigningKey<Sha256> {
        &self.signing_key
    }

    pub fn verifying_key(&self) -> &VerifyingKey<Sha256> {
        &self.verifying_key
    }

    pub fn sign(&self, text: &str) -> String {
        let mut rng = rand::thread_rng();
        general_purpose::STANDARD_NO_PAD.encode(self.signing_key.sign_with_rng(&mut rng, text.as_bytes()).to_bytes())
    }

    pub fn verify(&self, text: &str, signature: &str) -> Result<()> {
        Ok(self.verifying_key.verify(text.as_bytes(), &Signature::try_from(general_purpose::STANDARD_NO_PAD.decode(signature)?.as_slice())?)?)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    #[test]
    fn test_signature() {
        let auth = Authenticator::random_2048().unwrap();
        let signature = auth.sign("Hello Sarus !");
        println!("{signature}");
        auth.verify("Hello Sarus !", &signature).expect("OK");
    }
}