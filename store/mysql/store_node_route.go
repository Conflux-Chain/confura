package mysql

import (
	"time"

	"gorm.io/gorm"
)

// NodeRoute node route configurations
type NodeRoute struct {
	// route key such as access token
	Key string `gorm:"unique;size:128;not null"`
	// desingated route group such as `cfxvip`
	Group string `gorm:"size:64;not null"`

	CreatedAt time.Time
	UpdatedAt time.Time
}

func (NodeRoute) TableName() string {
	return "node_routes"
}

type NodeRouteStore struct {
	*baseStore
}

func NewNodeRouteStore(db *gorm.DB) *NodeRouteStore {
	return &NodeRouteStore{baseStore: newBaseStore(db)}
}

func (nrs *NodeRouteStore) GetRouteGroup(routeKey string) (string, error) {
	var res NodeRoute

	exists, err := nrs.exists(&res, "key = ? AND", routeKey)
	if err != nil || !exists {
		return "", err
	}

	return res.Group, nil
}
