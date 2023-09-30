use super::{Error, Result};
use rand;
use rsa::{
    Pkcs1v15Encrypt, RsaPrivateKey, RsaPublicKey,
    pkcs1v15::{SigningKey, VerifyingKey},
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
}
