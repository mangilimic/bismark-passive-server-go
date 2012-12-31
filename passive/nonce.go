package passive

import (
	"log"
)

type Nonce int64

func (nonce *Nonce) Next() []byte {
	encodedNonce, err := encodeLexicographicInt64(int64(*nonce))
	if err != nil {
		log.Fatalf("Error encoding nonce: %v", err)
	}
	(*nonce)++
	if *nonce < 0 {
		log.Fatalf("Ran out of nonces.")
	}
	return encodedNonce
}
