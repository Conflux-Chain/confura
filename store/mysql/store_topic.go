package mysql

import (
	lru "github.com/hashicorp/golang-lru"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

const defaultTopicCacheSize = 4096

// Topic represents the topic0 signature entry of an event log and its associated metadata.
type Topic struct {
	// id and hash are immutable once created.
	ID   uint64 `gorm:"primaryKey;autoIncrement"`
	Hash string `gorm:"size:66;not null;unique"`

	// num of persisted event logs
	LogCount int `gorm:"not null;default:0"`
	// latest updated epoch height
	LatestUpdatedEpoch uint64 `gorm:"index:index_lue;not null;default:0"`
}

func (Topic) TableName() string {
	return "topics"
}

type TopicStore struct {
	*baseStore

	idCache   *lru.Cache // uint64 -> *Topic
	hashCache *lru.Cache // string -> *Topic
}

func NewTopicStore(db *gorm.DB, cacheSize ...int) *TopicStore {
	size := defaultTopicCacheSize
	if len(cacheSize) > 0 && cacheSize[0] > 0 {
		size = cacheSize[0]
	}

	idCache, _ := lru.New(size)
	hashCache, _ := lru.New(size)

	return &TopicStore{
		baseStore: newBaseStore(db),
		idCache:   idCache,
		hashCache: hashCache,
	}
}

func (s *TopicStore) cache(t *Topic) {
	s.idCache.Add(t.ID, t)
	s.hashCache.Add(t.Hash, t)
}

// GetTopicById retrieves topic by id from db and also loads the cache.
func (s *TopicStore) GetTopicById(id uint64) (*Topic, bool, error) {
	return s.load("id = ?", id)
}

// GetTopicByHash retrieves topic by hash from db and also loads the cache.
func (s *TopicStore) GetTopicByHash(hash string) (*Topic, bool, error) {
	return s.load("hash = ?", hash)
}

// GetHash returns the hash for a given id from cache or db if not found.
func (s *TopicStore) GetHash(id uint64) (string, bool, error) {
	if v, ok := s.idCache.Get(id); ok {
		return v.(*Topic).Hash, true, nil
	}

	topic, ok, err := s.load("id = ?", id)
	if !ok || err != nil {
		return "", false, err
	}
	return topic.Hash, true, nil
}

// GetID returns the id for a given hash from cache or db if not found.
func (s *TopicStore) GetID(hash string) (uint64, bool, error) {
	if v, ok := s.hashCache.Get(hash); ok {
		return v.(*Topic).ID, true, nil
	}

	topic, ok, err := s.load("hash = ?", hash)
	if !ok || err != nil {
		return 0, false, err
	}
	return topic.ID, true, nil
}

// GetOrCreate returns existing topic or creates new one.
// Returns (id, isNew, error). Not thread-safe.
func (s *TopicStore) GetOrCreate(hash string) (uint64, bool, error) {
	if id, ok, err := s.GetID(hash); ok || err != nil {
		return id, false, errors.WithMessagef(err, "failed to get topic id for hash %s", hash)
	}

	t := &Topic{Hash: hash}
	if err := s.db.Create(t).Error; err != nil {
		return 0, false, errors.WithMessagef(err, "failed to create topic for hash %s", hash)
	}

	s.cache(t)
	return t.ID, true, nil
}

// BatchAdd adds absent topics in batch and returns the number of new added.
func (s *TopicStore) BatchAdd(hashes map[string]bool) (int, error) {
	var newTopics []Topic

	// Skip topics that already exists
	for h := range hashes {
		_, ok, err := s.GetID(h)
		if err != nil {
			return 0, errors.WithMessage(err, "failed to get topic id by hash")
		}

		if !ok {
			newTopics = append(newTopics, Topic{Hash: h})
		}
	}

	if len(newTopics) == 0 {
		return 0, nil
	}

	// create in batch
	if err := s.db.Create(&newTopics).Error; err != nil {
		return 0, errors.WithMessage(err, "failed to batch create topics")
	}

	logrus.WithField("newTopics", newTopics).Debug("Succeeded to add new topics into db")

	// update cache
	for i := range newTopics {
		s.cache(&newTopics[i])
	}

	return len(newTopics), nil
}

// GetUpdatedSince returns all topics updated since the given epoch.
func (s *TopicStore) GetUpdatedSince(epoch uint64) ([]*Topic, error) {
	var topics []*Topic
	err := s.db.Where("latest_updated_epoch >= ?", epoch).Find(&topics).Error
	return topics, err
}

// UpdateStats updates log count and last epoch for the specified topic.
func (s *TopicStore) UpdateStats(tx *gorm.DB, id uint64, delta int, epoch uint64) error {
	updates := map[string]any{"latest_updated_epoch": epoch}

	if delta > 0 {
		updates["log_count"] = gorm.Expr("log_count + ?", delta)
	} else if delta < 0 {
		updates["log_count"] = gorm.Expr("GREATEST(0, CAST(log_count AS SIGNED) - ?)", -delta)
	}

	return tx.Model(&Topic{}).Where("id = ?", id).Updates(updates).Error
}

// load fetches topic from db and updates cache.
func (s *TopicStore) load(query string, args ...interface{}) (*Topic, bool, error) {
	// Could improve when QPS is very high:
	// 1. Use mutex lock to allow only 1 thread to read from db.
	// 2. Cache non-existent address.
	var t Topic
	exists, err := s.exists(&t, query, args...)
	if err != nil || !exists {
		return nil, false, err
	}

	s.cache(&t)
	return &t, true, nil
}
