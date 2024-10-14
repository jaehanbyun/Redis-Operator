package utils

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-logr/logr"
	redisv1beta1 "github.com/jaehanbyun/redis-operator/api/v1beta1"
	"github.com/redis/go-redis/v9"
	"k8s.io/client-go/kubernetes"
)

func getRedisClusterSlots(ctx context.Context, redisClient *redis.Client, logger logr.Logger, nodeID string) (int, error) {
	totalSlots := 0

	redisSlots, err := redisClient.ClusterSlots(ctx).Result()
	if err != nil {
		logger.Error(err, "Failed to get cluster slots")
		return 0, err
	}

	for _, slot := range redisSlots {
		for _, node := range slot.Nodes {
			if node.ID == nodeID {
				totalSlots += int(slot.End - slot.Start + 1)
				break
			}
		}
	}

	logger.V(1).Info("Total cluster slots to be transferred from node", "NodeID", nodeID, "TotalSlots", totalSlots)
	return totalSlots, nil
}

func ReShardRedisCluster(ctx context.Context, k8scl kubernetes.Interface, redisCluster *redisv1beta1.RedisCluster, logger logr.Logger, master redisv1beta1.RedisNodeStatus) error {
	masterPodName := master.PodName
	redisClient := ConfigureRedisClient(k8scl, redisCluster, logger, masterPodName)
	defer redisClient.Close()

	totalSlots, err := getRedisClusterSlots(ctx, redisClient, logger, master.NodeID)
	if err != nil {
		logger.Error(err, "Failed to get slots of master node", "MasterNodeID", master.NodeID)
		return err
	}

	if totalSlots == 0 {
		logger.Info("No slots to migrate from master", "MasterNodeID", master.NodeID)
		return nil
	}

	var targetMaster redisv1beta1.RedisNodeStatus
	for _, m := range redisCluster.Status.MasterMap {
		if m.NodeID != master.NodeID {
			targetMaster = m
			break
		}
	}

	if targetMaster.NodeID == "" {
		return fmt.Errorf("no target master available for slot migration")
	}

	masterAddress := GetRedisServerAddress(k8scl, logger, redisCluster.Namespace, master.PodName)
	reshardCmd := []string{
		"redis-cli", "--cluster", "reshard", masterAddress,
		"--cluster-from", master.NodeID,
		"--cluster-to", targetMaster.NodeID,
		"--cluster-slots", strconv.Itoa(totalSlots),
		"--cluster-yes",
	}

	logger.Info("Resharding slots from master", "Command", reshardCmd)
	output, err := RunRedisCLI(k8scl, redisCluster.Namespace, master.PodName, reshardCmd)
	if err != nil {
		logger.Error(err, "Error resharding slots", "Output", output)
		return err
	}

	err = WaitForSlotMigration(ctx, redisClient, logger, master.NodeID, 5*time.Minute)
	if err != nil {
		logger.Error(err, "Slot migration did not complete", "MasterNodeID", master.NodeID)
		return err
	}

	logger.Info("Slot migration completed", "MasterNodeID", master.NodeID)
	return nil
}

func WaitForSlotMigration(ctx context.Context, redisClient *redis.Client, logger logr.Logger, nodeID string, timeout time.Duration) error {
	startTime := time.Now()
	for {
		elapsed := time.Since(startTime)
		if elapsed > timeout {
			return fmt.Errorf("slot migration did not complete within the timeout period")
		}

		totalSlots, err := getRedisClusterSlots(ctx, redisClient, logger, nodeID)
		if err != nil {
			logger.Error(err, "Failed to get slots of node", "NodeID", nodeID)
			return err
		}

		if totalSlots == 0 {
			logger.Info("Slot migration completed", "NodeID", nodeID)
			return nil
		}

		logger.Info("Waiting for slot migration to complete...", "NodeID", nodeID, "RemainingSlots", totalSlots)
		time.Sleep(2 * time.Second)
	}
}

func AddMasterToCluster(k8scl kubernetes.Interface, redisCluster *redisv1beta1.RedisCluster, logger logr.Logger, newMaster redisv1beta1.RedisNodeStatus) error {
	var existingMaster redisv1beta1.RedisNodeStatus
	for _, master := range redisCluster.Status.MasterMap {
		existingMaster = master
		break
	}

	existingMasterAddress := GetRedisServerAddress(k8scl, logger, redisCluster.Namespace, existingMaster.PodName)
	newMasterAddress := GetRedisServerAddress(k8scl, logger, redisCluster.Namespace, newMaster.PodName)

	addNodeCmd := []string{"redis-cli", "--cluster", "add-node", newMasterAddress, existingMasterAddress}
	output, err := RunRedisCLI(k8scl, redisCluster.Namespace, existingMaster.PodName, addNodeCmd)
	if err != nil {
		logger.Error(err, "Error adding new master node to cluster", "Output", output)
		return err
	}
	logger.Info("Successfully added new master node to cluster", "Node", newMaster.PodName)

	if err := WaitForClusterStabilization(k8scl, redisCluster, logger); err != nil {
		logger.Error(err, "Error during cluster stabilization")
		return err
	}

	rebalanceCmd := []string{"redis-cli", "--cluster", "rebalance", existingMasterAddress, "--cluster-use-empty-masters"}
	logger.Info("Rebalance command", "Command", rebalanceCmd)
	output, err = RunRedisCLI(k8scl, redisCluster.Namespace, existingMaster.PodName, rebalanceCmd)
	if err != nil {
		logger.Error(err, "Error during cluster rebalancing", "Output", output)
		return err
	}
	logger.Info("Cluster rebalancing completed successfully")

	return nil
}
