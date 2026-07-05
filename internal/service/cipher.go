package service

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
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

	// 2. Вызывается обёртка GCM с гарантиями
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	//                                 1,2,3,4,5,6,7,8,9,10,11,12 (12 байт)
	// 3. выделяется память под nonce [0,0,0,0,0,0,0,0,0,0,0,0]
	nonce := make([]byte, gcm.NonceSize())

	// 4. nonce заполняется рандомными байтами
	if _, err = rand.Read(nonce); err != nil {
		return nil, err
	}

	// 5. теперь у нас есть ключ, nonce, а сверху мы получили данные, соответственно нужно
	// зашифровать данные и получить результат, который мы возвращаем
	return gcm.Seal(nil, nonce, data, nil), nil
	// вроде как в аргумент dst можно было передать nonce и не создавать отдельное поле в базе, но насколько я понял
	// такой подход тяжело читать и дебажить
}

// Decrypt будет расшифровывать слайс данных по ключу
// логика выполнения практически такая же, как у Encrypt
func Decrypt(key []byte, data []byte) ([]byte, error) {
	// 1. Создаём блок для AES шифра
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	// 2. Указываем метод работы с блоком
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	// 3. Вместо Seal(шифрования) возвращаем Open(расшифровку)
	return gcm.Open(nil, data, nil, nil)
}
