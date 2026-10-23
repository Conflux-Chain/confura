package handler

import (
	"math/bits"
	"unicode/utf8"

	cfxtypes "github.com/Conflux-Chain/go-conflux-sdk/types"
	"github.com/Conflux-Chain/go-conflux-sdk/types/cfxaddress"
	"github.com/ethereum/go-ethereum/common/hexutil"
	web3types "github.com/openweb3/web3go/types"
)

// These sizes track the current SDK JSON encoders, not Go struct memory. When
// SDK fields, tags or MarshalJSON change, update the layout and the Marshal
// comparison/fuzz tests together. The RPC envelope is intentionally excluded.
// Valid RPC hashes and addresses contain no characters requiring JSON escaping.
const (
	jsonHashBytes = 66 // 0x plus 64 hex digits, excluding the surrounding quotes
	// Empty strings include their quotes. Add one address (42), two hashes,
	// and five 0x prefixes (data plus four required numeric quantities).
	ethLogBaseBytes = len(`{"address":"","blockHash":"","blockNumber":"",`+
		`"blockTimestamp":"","data":"","logIndex":"","removed":false,`+
		`"topics":null,"transactionHash":"","transactionIndex":""}`) + 42 + 2*jsonHashBytes + 5*len("0x")
	cfxLogBaseBytes = len(`{"address":"","topics":null,"data":""}`) + len("0x")
)

// logArraySize accounts for the brackets and commas shared by all log sources.
// Its zero value describes [], so empty batches need no special handling.
type logArraySize struct {
	contentBytes uint64
	count        uint64
}

func (s *logArraySize) add(logBytes uint64) {
	if s.count > 0 {
		s.contentBytes++ // JSON comma between adjacent log objects.
	}
	s.contentBytes += logBytes
	s.count++
}

func (s logArraySize) size() uint64 { return uint64(len("[]")) + s.contentBytes }

// ethLogJSONSize sizes one object, without array delimiters or separators.
func ethLogJSONSize(log *web3types.Log) uint64 {
	// Required fields: fixed JSON layout plus variable hex digits. Each data
	// byte becomes two hex digits; the 0x prefix is already in the base size.
	size := uint64(ethLogBaseBytes + 2*len(log.Data))
	size += uint64(hexDigits(log.BlockNumber) + hexDigits(log.BlockTimestamp) + hexDigits(uint64(log.Index)) + hexDigits(uint64(log.TxIndex)))
	if log.Topics != nil {
		size = size - uint64(len("null")) + uint64(len("[]")) // nil is null; nonnil is an array.
		if len(log.Topics) > 0 {
			// Each quoted hash uses 68 bytes, plus one comma except at the start.
			size += uint64((jsonHashBytes+2+1)*len(log.Topics) - 1)
		}
	}
	if log.Removed {
		size-- // true is one byte shorter than false
	}
	// Pointer fields use omitempty: only nonnil values add the key and value.
	if log.LogType != nil {
		size += uint64(len(`,"logType":`) + jsonStringSize(*log.LogType))
	}
	if log.TransactionLogIndex != nil {
		size += uint64(len(`,"transactionLogIndex":`) + len(`"0x"`) + hexDigits(uint64(*log.TransactionLogIndex)))
	}
	return size
}

// cfxLogJSONSize follows the SDK's required fields and optional pointer fields.
func cfxLogJSONSize(log *cfxtypes.Log) uint64 {
	// CIP-37 has a variable network prefix, 42 body/checksum characters,
	// and optionally a type prefix. Avoid constructing the address string.
	addressBytes := len(log.Address.GetNetworkType()) + 1 + 42
	if cfxaddress.GetConfig().AddressStringVerbose {
		addressBytes += len("type.") + len(log.Address.GetAddressType()) + 1
	}
	size := uint64(cfxLogBaseBytes + addressBytes + 2*len(log.Data))
	if log.Topics != nil {
		size = size - uint64(len("null")) + uint64(len("[]"))
		for i, topic := range log.Topics {
			if i > 0 {
				size++
			}
			size += uint64(len(topic) + 2)
		}
	}
	// Optional hashes are strings; the JSON punctuation includes comma and key.
	if log.BlockHash != nil {
		size += uint64(len(`,"blockHash":`) + len(*log.BlockHash) + 2)
	}
	if log.TransactionHash != nil {
		size += uint64(len(`,"transactionHash":`) + len(*log.TransactionHash) + 2)
	}
	// Optional quantities may exceed uint64; BitLen gives their hex digit count.
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
		size += uint64(len(field.key) + len(`,"":`) + len(`"0x"`) + digits)
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
