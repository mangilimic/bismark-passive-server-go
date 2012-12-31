package passive_test

import (
	. "bismark/passive"
	"fmt"
)

func ExampleNonceNext() {
	var nonce Nonce
	fmt.Printf("%s\n", nonce.Next())
	fmt.Printf("%s\n", nonce.Next())

	// Output:
	// 00000000000000000000
	// 00000000000000000001
}
