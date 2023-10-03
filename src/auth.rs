use super::{Error, Result};
use rand;
use base64::{Engine as _, engine::general_purpose};
use rsa::{
    RsaPrivateKey,
    pkcs1v15::{SigningKey, VerifyingKey, Signature},
    signature::{Keypair, RandomizedSigner, SignatureEncoding, Verifier},
    sha2::Sha256,
    pkcs8::{EncodePrivateKey, DecodePrivateKey, spki::der::pem::LineEnding},

};

const SIZE: usize = 2048;

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

    pub fn get(path: &str) -> Result<Self> {
        Authenticator::try_load(path).or_else(|_| {
            let auth = Authenticator::random(SIZE)?;
            auth.save(path)?;
            Ok(auth)
        })
    }

    pub fn try_load(path: &str) -> Result<Self> {
        let private_key = DecodePrivateKey::read_pkcs8_pem_file(path)?;
        Ok(Authenticator::new(private_key))
    }

    pub fn save(&self, path: &str) -> Result<()> {
        Ok(self.private_key.write_pkcs8_pem_file(path, LineEnding::CRLF)?)
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
        let auth = Authenticator::get("secret_key.pem").unwrap();
        let signature = auth.sign("Hello Sarus !");
        println!("{signature}");
        auth.verify("Hello Sarus !", &signature).expect("OK");
    }
}