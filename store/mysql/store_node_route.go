package mysql

import (
	"time"

	"gorm.io/gorm"
)

// NodeRoute node route configurations
type NodeRoute struct {
	ID uint32
	// route key such as access token
	RouteKey string `gorm:"unique;size:128;not null"`
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

func (nrs *NodeRouteStore) AddNodeRoute(routeKey, routeGroup string) error {
	return nrs.db.Create(&NodeRoute{
		RouteKey: routeKey,
		Group:    routeGroup,
	}).Error
}

func (nrs *NodeRouteStore) DeleteNodeRoute(routeKey string) (bool, error) {
	res := nrs.db.Delete(&NodeRoute{}, "route_key = ?", routeKey)
	return res.RowsAffected > 0, res.Error
}

func (nrs *NodeRouteStore) FindNodeRoute(routeKey string) (*NodeRoute, error) {
	var res NodeRoute

	exists, err := nrs.exists(&res, "route_key = ?", routeKey)
	if err != nil || !exists {
		return nil, err
	}

	return &res, nil
}

type NodeRouteFilter struct {
	KeySet []string // route key set
	Limit  int      // result limit size (<= 0 means none)
}

func (nrs *NodeRouteStore) LoadNodeRoutes(filter NodeRouteFilter) (res []*NodeRoute, err error) {
	db := nrs.db

	if len(filter.KeySet) > 0 {
		db = db.Where("route_key IN (?)", filter.KeySet)
	}

	if filter.Limit > 0 {
		db = db.Limit(filter.Limit)
	}

	var nodeRoutes []*NodeRoute

	err = db.FindInBatches(&nodeRoutes, 200, func(tx *gorm.DB, batch int) error {
		res = append(res, nodeRoutes...)
		return nil
	}).Error

	return res, err
}
