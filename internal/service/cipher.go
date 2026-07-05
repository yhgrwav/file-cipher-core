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
	key := make([]byte, 32)  // 32 потому что AES-256 требует именно 32 байта
	_, err := rand.Read(key) // заполняем байты рандомными значениями
	if err != nil {
		return []byte{}, err
	}
	return key, nil // и возвращаем ключ
}

// Encrypt будет шифровать какой-то кусочек (в случае ТЗ - 50 кб) данных
func Encrypt(key []byte, data []byte) ([]byte, error) {
	// Существует несколько сущностей, с которыми нужно будет работать:
	// 1. Cipher - шифровщик
	// 2. Block - это сущность, которая резервирует, а затем и заполняет
	// 3. GCM - обёртка, умеющая работать с нашим шифровщиком(AES-256), гарантируя целостность данных
	// 4. Nonce - уникальный идентификатор, часть шифра данных в блоке
	//
	// Логика должна быть такая:
	// Придумал nonce -> nonce + "*[]byte*" + key -> cipher -> hj3905j3h3 + tag = блок(чанк) ~50 байт

	// 1. Создаётся блок (чанк)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
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
