package passive

import (
	"time"
)

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a < b {
		return b
	}
	return a
}

func truncateTimestampToHour(timestampMicroseconds int64) int64 {
	timestamp := time.Unix(convertMicrosecondsToSeconds(timestampMicroseconds), 0)
	return time.Date(timestamp.Year(), timestamp.Month(), timestamp.Day(), timestamp.Hour(), 0, 0, 0, time.UTC).Unix()
}

func convertMicrosecondsToSeconds(timestamp int64) int64 {
	return timestamp / int64(1000000)
}

func convertSecondsToMicroseconds(timestamp int64) int64 {
	return timestamp * int64(1000000)
}
