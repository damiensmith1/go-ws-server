package redisx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/damiensmith1/go-ws-server/internal/protocol"
)

// SchedulerSetKey is the Redis sorted-set holding pending jobs. Score = the
// next execution time as Unix milliseconds.
const SchedulerSetKey = "jobScheduler"

// claimKeyPrefix + jobId is a SETNX claim used to ensure that, when many
// instances run the scheduler, exactly one of them executes a given job.
const claimKeyPrefix = "job:claim:"

// StoredJob is the JSON shape stored in the scheduler zset, matching the
// TS layer ({jobId, jobData}).
type StoredJob struct {
	JobID   string            `json:"jobId"`
	JobData *protocol.JobData `json:"jobData"`
}

// AddJob inserts a job into the scheduler. Returns an error if a job with
// the same ID already exists in the set.
func AddJob(ctx context.Context, c redis.UniversalClient, j *protocol.JobData) error {
	if j == nil || j.JobID == "" {
		return errors.New("job must have a jobId")
	}
	executeAt, err := protocol.ParseExecuteAt(j.ExecuteAt)
	if err != nil {
		return fmt.Errorf("executeAt: %w", err)
	}

	exists, err := jobExists(ctx, c, j.JobID)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("Job with ID %s already exists in the scheduler.", j.JobID)
	}

	stored := StoredJob{JobID: j.JobID, JobData: j}
	payload, err := json.Marshal(stored)
	if err != nil {
		return fmt.Errorf("marshal stored job: %w", err)
	}

	score := float64(executeAt.UnixMilli())
	return c.ZAdd(ctx, SchedulerSetKey, redis.Z{Score: score, Member: payload}).Err()
}

func jobExists(ctx context.Context, c redis.UniversalClient, jobID string) (bool, error) {
	members, err := c.ZRange(ctx, SchedulerSetKey, 0, -1).Result()
	if err != nil {
		return false, fmt.Errorf("zrange scheduler: %w", err)
	}
	for _, m := range members {
		var s StoredJob
		if json.Unmarshal([]byte(m), &s) != nil {
			continue
		}
		if s.JobID == jobID {
			return true, nil
		}
	}
	return false, nil
}

// RemoveJob deletes the job with the given ID from the scheduler. Errors
// if the job is not present.
func RemoveJob(ctx context.Context, c redis.UniversalClient, jobID string) error {
	members, err := c.ZRange(ctx, SchedulerSetKey, 0, -1).Result()
	if err != nil {
		return fmt.Errorf("zrange scheduler: %w", err)
	}
	for _, m := range members {
		var s StoredJob
		if json.Unmarshal([]byte(m), &s) != nil {
			continue
		}
		if s.JobID == jobID {
			if _, err := c.ZRem(ctx, SchedulerSetKey, m).Result(); err != nil {
				return fmt.Errorf("zrem job: %w", err)
			}
			return nil
		}
	}
	return fmt.Errorf("Job with ID %s does not exist in the scheduler.", jobID)
}

// DueJobsResult is what ListDueJobs returns: each due member with its raw
// payload (so the caller can ZREM by exact-match member) and parsed view.
type DueJobsResult struct {
	RawMember string
	Job       StoredJob
}

// ListDueJobs returns every scheduler member with score ≤ nowMs (as Unix
// milliseconds), parsed.
func ListDueJobs(ctx context.Context, c redis.UniversalClient, nowMs int64) ([]DueJobsResult, error) {
	raws, err := c.ZRangeByScore(ctx, SchedulerSetKey, &redis.ZRangeBy{
		Min: "0",
		Max: fmt.Sprintf("%d", nowMs),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("zrangebyscore scheduler: %w", err)
	}
	out := make([]DueJobsResult, 0, len(raws))
	for _, raw := range raws {
		var s StoredJob
		if err := json.Unmarshal([]byte(raw), &s); err != nil {
			continue
		}
		out = append(out, DueJobsResult{RawMember: raw, Job: s})
	}
	return out, nil
}

// RemoveJobMember atomically deletes a specific zset member. Returns true
// when the member was present and removed.
func RemoveJobMember(ctx context.Context, c redis.UniversalClient, member string) (bool, error) {
	n, err := c.ZRem(ctx, SchedulerSetKey, member).Result()
	if err != nil {
		return false, fmt.Errorf("zrem job member: %w", err)
	}
	return n > 0, nil
}

// AddRescheduledJob inserts a job at a new execution time. The caller is
// expected to update stored.JobData.ExecuteAt before calling.
func AddRescheduledJob(ctx context.Context, c redis.UniversalClient, stored StoredJob, atMs int64) error {
	payload, err := json.Marshal(stored)
	if err != nil {
		return fmt.Errorf("marshal stored job: %w", err)
	}
	return c.ZAdd(ctx, SchedulerSetKey, redis.Z{Score: float64(atMs), Member: payload}).Err()
}

// NextDueAt returns the score (Unix ms) of the soonest pending job, or
// (-1, false, nil) if the scheduler is empty.
func NextDueAt(ctx context.Context, c redis.UniversalClient) (int64, bool, error) {
	zs, err := c.ZRangeWithScores(ctx, SchedulerSetKey, 0, 0).Result()
	if err != nil {
		return -1, false, fmt.Errorf("zrange scheduler: %w", err)
	}
	if len(zs) == 0 {
		return -1, false, nil
	}
	return int64(zs[0].Score), true, nil
}

// ClaimJob atomically reserves the right to execute a given job for ttl.
// Returns true if this caller won the claim; false if another instance
// claimed it first.
func ClaimJob(ctx context.Context, c redis.UniversalClient, jobID, instanceID string, ttl time.Duration) (bool, error) {
	if ttl <= 0 {
		ttl = 30 * time.Second
	}
	ok, err := c.SetNX(ctx, claimKeyPrefix+jobID, instanceID, ttl).Result()
	if err != nil {
		return false, fmt.Errorf("setnx job claim: %w", err)
	}
	return ok, nil
}

// JobCount returns the number of pending jobs, used at startup to decide
// whether to start the scheduler tick loop.
func JobCount(ctx context.Context, c redis.UniversalClient) (int64, error) {
	return c.ZCard(ctx, SchedulerSetKey).Result()
}
