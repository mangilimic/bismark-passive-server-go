package passive

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
)

func encodeInt64(num int64) []byte {
	encodedNum := make([]byte, binary.Size(num))
	bytesForNum := binary.PutVarint(encodedNum, num)
	return encodedNum[:bytesForNum]
}

func decodeInt64(encoded []byte) (int64, error) {
	value, bytesRead := binary.Varint(encoded)
	if value == 0 && bytesRead <= 0 {
		return 0, fmt.Errorf("Error decoding VarInt")
	}
	return value, nil
}

func encodeLexicographicInt64(num int64) ([]byte, error) {
	if num < 0 {
		return nil, fmt.Errorf("Lexicographic encoding only works for non-negative numbers. Cannot encode %v.", num)
	}
	return []byte(fmt.Sprintf("%.20d", num)), nil
}

func encodeLexicographicInt32(num int32) ([]byte, error) {
	if num < 0 {
		return nil, fmt.Errorf("Lexicographic encoding only works for non-negative numbers. Cannot encode %v.", num)
	}
	return []byte(fmt.Sprintf("%.10d", num)), nil
}

func decodeLexicographicInt64(encoded []byte) (int64, error) {
	return strconv.ParseInt(string(encoded), 10, 64)
}

func decodeLexicographicInt32(encoded []byte) (int32, error) {
	num, err := strconv.ParseInt(string(encoded), 10, 32)
	return int32(num), err
}

func encodeLexicographicReverseInt64(num int64) ([]byte, error) {
	return encodeLexicographicInt64(math.MaxInt64 - num)
}

func encodeLexicographicReverseInt32(num int32) ([]byte, error) {
	return encodeLexicographicInt32(math.MaxInt32 - num)
}

func encodeReverseInt64(num int64) []byte {
	if num < 0 {
		panic("Reverse coding for int64 only works for non-negative numbers")
	}
	return []byte(fmt.Sprintf("%.20d", math.MaxInt64 - num))
}

func encodeReverseInt32(num int32) []byte {
	if num < 0 {
		panic("Reverse coding for int32 only works for non-negative numbers")
	}
	return []byte(fmt.Sprintf("%.10d", math.MaxInt32 - num))
}

func parseKey(key []byte) [][]byte {
	return bytes.Split(key, []byte{':'})
}

func makeKey(table string, pieces ...[]byte) []byte {
	fullPieces := make([][]byte, len(pieces) + 1)
	fullPieces[0] = []byte(table)
	validateTableName(fullPieces[0])
	for i := 0; i < len(pieces); i += 1 {
		fullPieces[i + 1] = pieces[i]
	}
	return bytes.Join(fullPieces, []byte(":"))
}
