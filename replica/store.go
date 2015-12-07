package replica

import (
	"sync"
)

type KeyValue interface {
	Key() string
	Value() []byte
}

func NewTestStore(size int) Store {
	store := new(store)
	store.id = 0
	store.store = make(map[string]KeyValue, size)
	return store
}

func NewKey(key string) KeyValue {
	return &keyValue{key: key}
}

func NewKeyValue(key string, value []byte) KeyValue {
	return &keyValue{key: key, value: value}
}

type Store interface {
	Get(string) KeyValue
	Put(KeyValue) error
	GetMulti([]string) []KeyValue
	PutMulti([]KeyValue) error
	Id() int
}

type keyValue struct {
	key   string
	value []byte
}

func (kv *keyValue) Value() []byte {
	return kv.value
}

func (kv *keyValue) Key() string {
	return kv.key
}

type store struct {
	id    int
	store map[string]KeyValue
	sync.RWMutex
	db        string
	connected bool
}

func (s *store) Id() int {
	return s.id
}

func (s *store) Get(k string) KeyValue {
	s.RLock()
	defer s.RUnlock()
	if value, ok := s.store[k]; ok {
		return value
	}
	return &keyValue{k, []byte("")}
}

func (s *store) GetMulti(keys []string) []KeyValue {
	s.RLock()
	defer s.RUnlock()
	ret := make([]KeyValue, len(keys))
	for i, key := range keys {
		if value, present := s.store[key]; present {
			ret[i] = value
		}
	}
	return ret
}

func (s *store) Put(kv KeyValue) error {
	s.Lock()
	defer s.Unlock()
	s.store[kv.Key()] = kv
	return nil
}

func (s *store) PutMulti(kvs []KeyValue) error {
	s.Lock()
	defer s.Unlock()
	for _, kv := range kvs {
		s.store[kv.Key()] = kv
	}
	return nil
}
