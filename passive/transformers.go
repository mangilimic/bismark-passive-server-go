package passive

import (
	"github.com/sburnett/transformer/key"
)

type TraceKey struct {
	NodeId               []byte
	AnonymizationContext []byte
	SessionId            int64
	SequenceNumber       int32
}

func DecodeTraceKey(encodedKey []byte) *TraceKey {
	decodedKey := new(TraceKey)
	key.DecodeOrDie(
		encodedKey,
		&decodedKey.NodeId,
		&decodedKey.AnonymizationContext,
		&decodedKey.SessionId,
		&decodedKey.SequenceNumber)
	return decodedKey
}

func EncodeTraceKey(decodedKey *TraceKey) []byte {
	return key.EncodeOrDie(
		decodedKey.NodeId,
		decodedKey.AnonymizationContext,
		decodedKey.SessionId,
		decodedKey.SequenceNumber)
}

type SessionKey struct {
	NodeId               []byte
	AnonymizationContext []byte
	SessionId            int64
}

func DecodeSessionKey(encodedKey []byte) *SessionKey {
	decodedKey := new(SessionKey)
	key.DecodeOrDie(
		encodedKey,
		&decodedKey.NodeId,
		&decodedKey.AnonymizationContext,
		&decodedKey.SessionId)
	return decodedKey
}

func EncodeSessionKey(decodedKey *SessionKey) []byte {
	return key.EncodeOrDie(
		decodedKey.NodeId,
		decodedKey.AnonymizationContext,
		decodedKey.SessionId)
}
