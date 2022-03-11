package mysql

import "gorm.io/gorm"

// User represents a VIP user that provide specific archive node to query historical event logs.
type User struct {
	ID          uint32
	Name        string `gorm:"size:256;not null;unique"`
	Description string `gorm:"size:1024"`
	ApiKey      string `gorm:"size:256;not null;unique"`
	NodeUrl     string `gorm:"size:256;not null"`
}

func (User) TableName() string {
	return "users"
}

type UserStore struct {
	*baseStore
}

func newUserStore(db *gorm.DB) *UserStore {
	return &UserStore{
		baseStore: newBaseStore(db),
	}
}

func (us *UserStore) GetUserByKey(key string) (*User, bool, error) {
	var user User
	exists, err := us.exists(&user, "api_key = ?", key)
	return &user, exists, err
}
