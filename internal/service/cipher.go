package service

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
)

// KeySize - размер ключа AES-256 в байтах (256 бит).
const KeySize = 32

// ErrShortCiphertext возвращается, когда на расшифровку пришло меньше данных, чем занимает тег аутентификации GCM.
var ErrShortCiphertext = errors.New("ciphertext is too short")

// GenerateKey генерирует случайный AES-256 ключ
func GenerateKey() ([]byte, error) {
	key := make([]byte, KeySize) // 32 байта, потому что AES-256 требует именно 32 байта
	if _, err := rand.Read(key); err != nil {
		return nil, err
	}
	return key, nil
}

// Encrypt шифрует plaintext ключом в режиме AES-256-GCM и возвращает ciphertext
// вместе со сгенерированным nonce. Nonce не секретен и хранится рядом с ciphertext
// (в нашем случае - отдельной колонкой в БД2), но обязан быть уникальным для каждого
// (ключ, plaintext), поэтому генерируется заново на каждый вызов.
func Encrypt(key, plaintext []byte) (ciphertext, nonce []byte, err error) {
	gcm, err := newGCM(key)
	if err != nil {
		return nil, nil, err
	}

	nonce = make([]byte, gcm.NonceSize()) // обычно 12 байт
	if _, err := rand.Read(nonce); err != nil {
		return nil, nil, err
	}

	// Seal(nil, ...) => возвращает только ciphertext (с тегом аутентификации внутри),
	// nonce отдаём отдельно, чтобы явно положить его в свою колонку.
	return gcm.Seal(nil, nonce, plaintext, nil), nonce, nil
}

// Decrypt расшифровывает ciphertext ключом и nonce, которыми он был зашифрован.
func Decrypt(key, ciphertext, nonce []byte) ([]byte, error) {
	gcm, err := newGCM(key)
	if err != nil {
		return nil, err
	}
	if len(ciphertext) < gcm.Overhead() {
		return nil, ErrShortCiphertext
	}
	return gcm.Open(nil, nonce, ciphertext, nil)
}

// хелпер для GCM, в который вынесен дублирующийся код
func newGCM(key []byte) (cipher.AEAD, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}
