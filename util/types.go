package util

import "reflect"

// Helper function to check if interface value is nil, since "i == nil" checks nil interface case only.
// Refer to https://mangatmodi.medium.com/go-check-nil-interface-the-right-way-d142776edef1 for more details.
func IsInterfaceValNil(i interface{}) bool {
	if i == nil {
		return true
	}

	if reflect.ValueOf(i).Kind() == reflect.Ptr && reflect.ValueOf(i).IsNil() {
		return true
	}

	return false
}
