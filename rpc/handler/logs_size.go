package handler

import (
	"math/bits"
	"unicode/utf8"

	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/ethereum/go-ethereum/common/hexutil"
	web3types "github.com/openweb3/web3go/types"
)

// Empty string placeholders account for JSON syntax. Add fixed-width address,
// hashes and five 0x prefixes (data plus four quantities) without encoding them.
const ethLogBaseBytes = len(`{"address":"","blockHash":"","blockNumber":"",`+
	`"blockTimestamp":"","data":"","logIndex":"","removed":false,`+
	`"topics":null,"transactionHash":"","transactionIndex":""}`) + 42 + 66 + 66 + 10
const cfxLogBaseBytes = len(`{"address":"","topics":null,"data":""}`) + 2

// Sizes describe the log-array payload, excluding the JSON-RPC envelope.
func ethLogJSONSize(log *web3types.Log) uint64 {
	size := uint64(ethLogBaseBytes + 2*len(log.Data) + hexDigits(log.BlockNumber) + hexDigits(log.BlockTimestamp) + hexDigits(uint64(log.Index)) + hexDigits(uint64(log.TxIndex)))
	if log.Topics != nil {
		size -= 4 // replace null with an array
		size += 2
		if len(log.Topics) > 0 {
			size += uint64(69*len(log.Topics) - 1)
		}
	}
	if log.Removed {
		size-- // true is one byte shorter than false
	}
	if log.LogType != nil {
		size += uint64(len(`,"logType":`) + jsonStringSize(*log.LogType))
	}
	if log.TransactionLogIndex != nil {
		size += uint64(len(`,"transactionLogIndex":`) + 4 + hexDigits(uint64(*log.TransactionLogIndex)))
	}
	return size
}

func cfxLogJSONSize(log *cfxtypes.Log) uint64 {
	// CIP-37 has a variable network prefix, 42 body/checksum characters,
	// and optionally a type prefix. Avoid constructing the address string.
	addressBytes := len(log.Address.GetNetworkType()) + 1 + 42
	if cfxaddress.GetConfig().AddressStringVerbose {
		addressBytes += len("type.") + len(log.Address.GetAddressType()) + 1
	}
	size := uint64(cfxLogBaseBytes + addressBytes + 2*len(log.Data))
	if log.Topics != nil {
		size -= 4
		size += 2
		for i, topic := range log.Topics {
			if i > 0 {
				size++
			}
			size += uint64(len(topic) + 2)
		}
	}
	if log.BlockHash != nil {
		size += uint64(len(`,"blockHash":`) + len(*log.BlockHash) + 2)
	}
	if log.TransactionHash != nil {
		size += uint64(len(`,"transactionHash":`) + len(*log.TransactionHash) + 2)
	}
	for _, field := range []struct {
		key   string
		value *hexutil.Big
	}{
		{`epochNumber`, log.EpochNumber},
		{`transactionIndex`, log.TransactionIndex},
		{`logIndex`, log.LogIndex},
		{`transactionLogIndex`, log.TransactionLogIndex},
		{`blockTimestamp`, log.BlockTimestamp},
	} {
		if field.value == nil {
			continue
		}
		digits := max(1, (field.value.ToInt().BitLen()+3)/4)
		size += uint64(len(field.key) + 4 + 4 + digits) // comma/key/colon, quotes/0x and hex digits
		if field.value.ToInt().Sign() < 0 {
			size++
		}
	}
	if log.Space != nil {
		size += uint64(len(`,"space":`) + jsonStringSize(string(*log.Space)))
	}
	return size
}

func hexDigits(value uint64) int { return max(1, (bits.Len64(value)+3)/4) }

// Match encoding/json escaping for the few variable text fields. Binary data
// and typed hex/base32 fields need only length arithmetic, regardless of size.
func jsonStringSize(value string) int {
	size := 2
	for i := 0; i < len(value); {
		c := value[i]
		if c < utf8.RuneSelf {
			switch c {
			case '\\', '"', '\b', '\f', '\n', '\r', '\t':
				size += 2
			case '<', '>', '&':
				size += 6
			default:
				if c < 0x20 {
					size += 6
				} else {
					size++
				}
			}
			i++
			continue
		}
		r, width := utf8.DecodeRuneInString(value[i:])
		if r == utf8.RuneError && width == 1 || r == '\u2028' || r == '\u2029' {
			size += 6
		} else {
			size += width
		}
		i += width
	}
	return size
}
