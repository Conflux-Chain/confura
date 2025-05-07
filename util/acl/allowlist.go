package acl

const (
	// Pre-defined default allowlist name
	DefaultAllowList = "default"
)

// AllowList access control rule list.
type AllowList struct {
	ID   uint32 // Allowlist ID
	Name string // Allowlist name

	// Any requests which query addresses that are not in the allowlist are rejected.
	ContractAddresses []string

	// The allowed methods list. If the list is empty, all methods will be accepted.
	AllowMethods []string

	// The disallowed methods list. If the list is empty, no methods will be rejected.
	DisallowMethods []string

	// Restricted `User-Agent` request headers
	UserAgents []string

	// Restricted `Origin` request headers
	Origins []string
}

// NewAllowList creates a new AllowList instance with the given ID and name.
func NewAllowList(id uint32, name string) *AllowList {
	return &AllowList{
		ID:   id,
		Name: name,
	}
}

// IsMethodAllowed checks if a given method is allowed according to the allowlist rules.
// Priority:
// 1. If the method is listed in DisallowMethods — it is rejected.
// 2. If AllowMethods is not empty — only listed methods are accepted.
// 3. If AllowMethods is empty — all methods are accepted (unless disallowed above).
func (al *AllowList) IsMethodAllowed(method string) bool {
	if contains(al.DisallowMethods, method) {
		return false
	}

	if len(al.AllowMethods) > 0 {
		return contains(al.AllowMethods, method)
	}

	return true
}

// contains checks if a slice contains a specific string.
func contains(list []string, item string) bool {
	for _, v := range list {
		if v == item {
			return true
		}
	}
	return false
}
