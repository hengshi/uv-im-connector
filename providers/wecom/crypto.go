package wecom

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

func DecryptAttachment(encrypted []byte, aesKey string) ([]byte, error) {
	if len(encrypted) == 0 {
		return nil, errors.New("encrypted attachment is empty")
	}
	if len(encrypted)%aes.BlockSize != 0 {
		return nil, fmt.Errorf("encrypted attachment length %d is not a multiple of AES block size", len(encrypted))
	}
	key, err := DecodeAESKey(aesKey)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	plain := make([]byte, len(encrypted))
	cipher.NewCBCDecrypter(block, key[:aes.BlockSize]).CryptBlocks(plain, encrypted)
	return unpadPKCS7(plain)
}

func DecodeAESKey(value string) ([]byte, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil, errors.New("missing aeskey")
	}
	padded := value
	if remainder := len(padded) % 4; remainder != 0 {
		padded += strings.Repeat("=", 4-remainder)
	}
	candidates := []struct {
		value    string
		encoding *base64.Encoding
	}{
		{value: value, encoding: base64.RawStdEncoding},
		{value: padded, encoding: base64.StdEncoding},
		{value: value, encoding: base64.RawURLEncoding},
		{value: padded, encoding: base64.URLEncoding},
	}
	decodedLen := 0
	for _, candidate := range candidates {
		key, err := candidate.encoding.DecodeString(candidate.value)
		if err != nil {
			continue
		}
		if len(key) == 32 {
			return key, nil
		}
		if decodedLen == 0 {
			decodedLen = len(key)
		}
	}
	if decodedLen != 0 {
		return nil, fmt.Errorf("aeskey decoded to %d bytes, want 32", decodedLen)
	}
	return nil, errors.New("invalid aeskey")
}

func unpadPKCS7(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, errors.New("decrypted attachment is empty")
	}
	padding := int(data[len(data)-1])
	if padding < 1 || padding > 32 || padding > len(data) {
		return nil, fmt.Errorf("invalid PKCS#7 padding length %d", padding)
	}
	for _, b := range data[len(data)-padding:] {
		if int(b) != padding {
			return nil, errors.New("invalid PKCS#7 padding bytes")
		}
	}
	return data[:len(data)-padding], nil
}
