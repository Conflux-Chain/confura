package acl

const (
	// pre-defined default allowlist name
	DefaultAllowList = "default"
)

// AllowList access control rule list.
type AllowList struct {
	ID   uint32 // Allowlist ID
	Name string // Allowlist name

	// Any requests which query addresses that are not in the allowlist are rejected.
	ContractAddresses []string

	// The allowed methods list. If the list is empty, all methods will be accpeted.
	AllowMethods []string

	// The disallowed methods list. If the list is empty, no methods will be rejected.
	DisallowMethods []string

	// Restricted `User-Agent` request headers
	UserAgents []string

	// Restricted `Origin` request headers
	Origins []string
}

func NewAllowList(id uint32, name string) *AllowList {
	return &AllowList{
		ID:   id,
		Name: name,
	}
}
