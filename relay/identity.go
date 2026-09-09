package relay

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
)

// nonceSize — длина случайной строки, которую клиент подписывает.
//
// 32 байта берутся не из криптографических соображений (хватило бы и 16), а
// чтобы совпасть с размером выхода SHA-256 и не заводить второй размер.
const nonceSize = 32

// NewNonce — случайная строка на одно подключение.
//
// Она нужна, чтобы подпись нельзя было переиспользовать. Без неё клиент
// подписывал бы что-то постоянное, и подслушавший одну подпись входил бы
// под чужим именем сколько угодно раз.
func NewNonce() ([]byte, error) {
	nonce := make([]byte, nonceSize)
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	return nonce, nil
}

// VerifySignature проверяет, что подпись под nonce сделана этим ключом.
func VerifySignature(publicKey, nonce, signature []byte) bool {
	if len(publicKey) != ed25519.PublicKeySize {
		return false
	}
	return ed25519.Verify(ed25519.PublicKey(publicKey), nonce, signature)
}

// Fingerprint — короткое читаемое представление ключа.
//
// Сам ключ в логах неудобен, а отличать «тот же Bob» от «другой Bob с тем же
// именем» нужно глазами. Шестнадцати шестнадцатеричных знаков для этого
// достаточно: подбирать коллизию к чужому отпечатку никому не выгодно, тут
// решает не отпечаток, а подпись.
func Fingerprint(publicKey []byte) string {
	sum := sha256.Sum256(publicKey)
	return hex.EncodeToString(sum[:8])
}
