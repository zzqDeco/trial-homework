package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func main() {
	brokers := flag.String("brokers", "localhost:9092", "comma-separated Kafka brokers")
	minBids := flag.Int64("min-bids", 10001, "minimum required messages in bid-requests")
	minImpressions := flag.Int64("min-impressions", 10001, "minimum required messages in impressions")
	timeoutSec := flag.Int("timeout-seconds", 20, "request timeout seconds")
	flag.Parse()

	brokerList := strings.Split(*brokers, ",")
	client, err := kgo.NewClient(kgo.SeedBrokers(brokerList...))
	if err != nil {
		fmt.Printf("failed to init kafka client: %v\n", err)
		os.Exit(1)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeoutSec)*time.Second)
	defer cancel()

	bidCount, err := topicMessageCount(ctx, client, "bid-requests")
	if err != nil {
		fmt.Printf("failed counting bid-requests: %v\n", err)
		os.Exit(1)
	}

	impCount, err := topicMessageCount(ctx, client, "impressions")
	if err != nil {
		fmt.Printf("failed counting impressions: %v\n", err)
		os.Exit(1)
	}

	bidPass := bidCount >= *minBids
	impPass := impCount >= *minImpressions

	fmt.Printf("bid-requests: %d (%s, threshold=%d)\n", bidCount, passFail(bidPass), *minBids)
	fmt.Printf("impressions: %d (%s, threshold=%d)\n", impCount, passFail(impPass), *minImpressions)

	if !(bidPass && impPass) {
		os.Exit(1)
	}
}

func topicMessageCount(ctx context.Context, client *kgo.Client, topic string) (int64, error) {
	partitions, err := topicPartitions(ctx, client, topic)
	if err != nil {
		return 0, err
	}
	if len(partitions) == 0 {
		return 0, fmt.Errorf("topic %s has no partitions", topic)
	}

	earliest, err := listOffsets(ctx, client, topic, partitions, -2)
	if err != nil {
		return 0, fmt.Errorf("earliest offsets: %w", err)
	}
	latest, err := listOffsets(ctx, client, topic, partitions, -1)
	if err != nil {
		return 0, fmt.Errorf("latest offsets: %w", err)
	}

	var total int64
	for _, p := range partitions {
		e, eok := earliest[p]
		l, lok := latest[p]
		if !eok || !lok {
			return 0, fmt.Errorf("missing offsets for partition %d", p)
		}
		if l < e {
			continue
		}
		total += (l - e)
	}

	return total, nil
}

func topicPartitions(ctx context.Context, client *kgo.Client, topic string) ([]int32, error) {
	req := kmsg.NewPtrMetadataRequest()
	topicReq := kmsg.NewMetadataRequestTopic()
	topicReq.Topic = &topic
	req.Topics = append(req.Topics, topicReq)

	resp, err := req.RequestWith(ctx, client)
	if err != nil {
		return nil, err
	}

	for _, t := range resp.Topics {
		if t.Topic == nil || *t.Topic != topic {
			continue
		}
		if t.ErrorCode != 0 {
			return nil, fmt.Errorf("metadata error code=%d", t.ErrorCode)
		}
		parts := make([]int32, 0, len(t.Partitions))
		for _, p := range t.Partitions {
			if p.ErrorCode != 0 {
				continue
			}
			parts = append(parts, p.Partition)
		}
		sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })
		return parts, nil
	}

	return nil, fmt.Errorf("topic not found: %s", topic)
}

func listOffsets(ctx context.Context, client *kgo.Client, topic string, partitions []int32, timestamp int64) (map[int32]int64, error) {
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1

	topicReq := kmsg.NewListOffsetsRequestTopic()
	topicReq.Topic = topic
	for _, p := range partitions {
		partReq := kmsg.NewListOffsetsRequestTopicPartition()
		partReq.Partition = p
		partReq.Timestamp = timestamp
		topicReq.Partitions = append(topicReq.Partitions, partReq)
	}
	req.Topics = append(req.Topics, topicReq)

	offsets := make(map[int32]int64, len(partitions))
	shards := client.RequestSharded(ctx, req)
	for _, shard := range shards {
		if shard.Err != nil {
			return nil, shard.Err
		}
		resp, ok := shard.Resp.(*kmsg.ListOffsetsResponse)
		if !ok {
			return nil, fmt.Errorf("unexpected response type %T", shard.Resp)
		}
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				if p.ErrorCode != 0 {
					return nil, fmt.Errorf("list offsets error topic=%s partition=%d code=%d", topic, p.Partition, p.ErrorCode)
				}
				offsets[p.Partition] = p.Offset
			}
		}
	}

	return offsets, nil
}

func passFail(ok bool) string {
	if ok {
		return "PASS"
	}
	return "FAIL"
}
