use super::{Error, Result};
use rsa::{
    Pkcs1v15Encrypt, RsaPrivateKey, RsaPublicKey,
    pkcs1v15::{SigningKey, VerifyingKey},
    signature::{Keypair, RandomizedSigner, SignatureEncoding, Verifier},
    sha2::{Digest, Sha256},
};

pub struct Authenticator {
    private_key: RsaPrivateKey,
    public_key: RsaPublicKey,
    signing_key: SigningKey<Sha256>,
    verifying_key: VerifyingKey<Sha256>,
}

impl Authenticator {
    pub fn new(private_key: RsaPrivateKey) -> Self {
        let public_key = RsaPublicKey::from(&private_key);
        let signing_key = SigningKey::<Sha256>::new(&private_key);
        let verifying_key = signing_key.verifying_key();
        Authenticator {
            private_key, public_key, signing_key, verifying_key
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

    pub fn public_key(&self) -> &RsaPublicKey {
        &self.public_key
    }

    pub fn public_key(&self) -> &RsaPublicKey {
        &self.public_key
    }
}
