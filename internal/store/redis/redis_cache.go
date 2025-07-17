package redis

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
)

// RedisCache implements the Cache interface using Redis
// RedisCache implements a Redis-based cache for the shim
type RedisCache struct {
	client *redis.Client
}

// NewRedisCache creates a new Redis cache instance
func NewRedisCache(addr string, db int) *RedisCache {
	return &RedisCache{
		client: redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: "", // no password set
			DB:       db, // use default DB
		}),
	}
}

// Get retrieves a value from the cache
func (r *RedisCache) Get(ctx context.Context, key string) (string, error) {
	val, err := r.client.Get(ctx, key).Result()
	if err == redis.Nil {
		return "", nil
	}
	return val, err
}

// Put stores a value in the cache
func (r *RedisCache) Put(ctx context.Context, key, value string, ttl time.Duration) error {
	if ttl <= 0 {
		ttl = time.Hour // Default TTL if not specified
	}
	return r.client.Set(ctx, key, value, ttl).Err()
}

// Delete removes a value from the cache
func (r *RedisCache) Delete(ctx context.Context, key string) error {
	return r.client.Del(ctx, key).Err()
}

// Close closes the Redis client connection
func (r *RedisCache) Close() error {
	return r.client.Close()
}
