package redis

import (
	"context"
	"encoding/json"

	"causal-consistency-shim/internal/shim"

	"github.com/redis/go-redis/v9"
)

// RedisCache implements the Cache interface using Redis
type RedisCache struct {
	client *redis.Client
}

// NewRedisCache creates a new Redis cache
func NewRedisCache(addr, password string) *RedisCache {
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       0,
	})
	return &RedisCache{client: client}
}

// Get retrieves a write from Redis
func (c *RedisCache) Get(ctx context.Context, key string) (*shim.Write, error) {
	val, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, shim.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	var write shim.Write
	if err := json.Unmarshal(val, &write); err != nil {
		return nil, err
	}
	return &write, nil
}

// Put stores a write in Redis
func (c *RedisCache) Put(ctx context.Context, write shim.Write) error {
	data, err := json.Marshal(write)
	if err != nil {
		return err
	}
	return c.client.Set(ctx, write.Key, data, 0).Err()
}

// Delete removes a write from Redis
func (c *RedisCache) Delete(ctx context.Context, key string) error {
	return c.client.Del(ctx, key).Err()
}
