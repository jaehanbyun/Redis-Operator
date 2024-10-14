package utils

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	redisv1beta1 "github.com/jaehanbyun/redis-operator/api/v1beta1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type ClusterNodeInfo struct {
	NodeID       string
	PodName      string
	Flags        string
	MasterNodeID string
}

func GetClusterNodesInfo(k8scl kubernetes.Interface, redisCluster *redisv1beta1.RedisCluster, logger logr.Logger) ([]ClusterNodeInfo, error) {
	var firstMasterPodName string
	for _, master := range redisCluster.Status.MasterMap {
		firstMasterPodName = master.PodName
		break
	}

	if firstMasterPodName == "" {
		logger.Info("No master nodes in the cluster")
		return []ClusterNodeInfo{}, nil
	}

	port := ExtractPortFromPodName(firstMasterPodName)
	cmd := []string{"redis-cli", "-h", "localhost", "-p", fmt.Sprintf("%d", port), "cluster", "nodes"}
	output, err := RunRedisCLI(k8scl, redisCluster.Namespace, firstMasterPodName, cmd)
	if err != nil {
		logger.Error(err, "Error executing Redis CLI command", "Command", strings.Join(cmd, " "))
		return nil, err
	}

	logger.Info("Output of redis-cli cluster nodes command", "Output", output)

	var nodesInfo []ClusterNodeInfo
	lines := strings.Split(output, "\n")
	for _, line := range lines {
		if line == "" {
			continue
		}
		parts := strings.Split(line, " ")
		nodeID := parts[0]
		flags := parts[2]
		masterNodeID := ""
		if len(parts) > 3 && parts[3] != "-" {
			masterNodeID = parts[3]
		}

		podName, err := GetPodNameByNodeID(k8scl, redisCluster.Namespace, nodeID, logger)
		if err != nil {
			logger.Error(err, "Failed to find Pod by NodeID", "NodeID", nodeID)
		}

		nodesInfo = append(nodesInfo, ClusterNodeInfo{
			NodeID:       nodeID,
			PodName:      podName,
			Flags:        flags,
			MasterNodeID: masterNodeID,
		})
	}

	return nodesInfo, nil
}

func SetupRedisCluster(ctx context.Context, cl client.Client, k8scl kubernetes.Interface, redisCluster *redisv1beta1.RedisCluster, logger logr.Logger) error {
	desiredMastersCount := redisCluster.Spec.Masters
	currentMasterCount := int32(len(redisCluster.Status.MasterMap))

	// Initialize MasterMap if nil
	if redisCluster.Status.MasterMap == nil {
		redisCluster.Status.MasterMap = make(map[string]redisv1beta1.RedisNodeStatus)
	}

	for i := int32(0); i < desiredMastersCount-currentMasterCount; i++ {
		port := redisCluster.Spec.BasePort + currentMasterCount + i
		err := CreateMasterPod(ctx, cl, k8scl, redisCluster, logger, port)
		if err != nil {
			return err
		}

		podName := fmt.Sprintf("rediscluster-%s-%d", redisCluster.Name, port)
		if err := WaitForPodReady(ctx, cl, redisCluster, logger, podName); err != nil {
			logger.Error(err, "Error waiting for master Pod to be ready", "Pod", podName)
			return err
		}

		redisNodeID, err := GetRedisNodeID(ctx, k8scl, logger, redisCluster.Namespace, podName)
		if err != nil {
			logger.Error(err, "Failed to extract Redis node ID", "Pod", podName)
			return err
		}

		if err := UpdatePodLabelWithRedisID(ctx, cl, redisCluster, logger, podName, redisNodeID); err != nil {
			logger.Error(err, "Failed to update Pod label", "Pod", podName)
			return err
		}

		redisCluster.Status.MasterMap[redisNodeID] = redisv1beta1.RedisNodeStatus{
			PodName: podName,
			NodeID:  redisNodeID,
		}
		redisCluster.Status.ReadyMasters = int32(len(redisCluster.Status.MasterMap))

		if err := cl.Status().Update(ctx, redisCluster); err != nil {
			logger.Error(err, "Error updating RedisCluster status")
			return err
		}
	}

	return nil
}

func UpdateClusterStatus(ctx context.Context, cl client.Client, k8scl kubernetes.Interface, redisCluster *redisv1beta1.RedisCluster, logger logr.Logger) error {
	nodesInfo, err := GetClusterNodesInfo(k8scl, redisCluster, logger)
	if err != nil {
		logger.Error(err, "Failed to get cluster node information")
		return err
	}

	redisCluster.Status.MasterMap = make(map[string]redisv1beta1.RedisNodeStatus)
	redisCluster.Status.ReplicaMap = make(map[string]redisv1beta1.RedisNodeStatus)

	if len(nodesInfo) == 0 {
		logger.Info("No cluster node information found. Assuming initial state")
	} else {
		for _, node := range nodesInfo {
			flagsList := strings.Split(node.Flags, ",")
			if containsFlag(flagsList, "master") {
				redisCluster.Status.MasterMap[node.NodeID] = redisv1beta1.RedisNodeStatus{
					PodName: node.PodName,
					NodeID:  node.NodeID,
				}
			} else if containsFlag(flagsList, "slave") {
				redisCluster.Status.ReplicaMap[node.NodeID] = redisv1beta1.RedisNodeStatus{
					PodName:      node.PodName,
					NodeID:       node.NodeID,
					MasterNodeID: node.MasterNodeID,
				}
			}
		}
	}

	redisCluster.Status.ReadyMasters = int32(len(redisCluster.Status.MasterMap))
	redisCluster.Status.ReadyReplicas = int32(len(redisCluster.Status.ReplicaMap))

	if err := cl.Status().Update(ctx, redisCluster); err != nil {
		logger.Error(err, "Error updating RedisCluster status")
		return err
	}

	return nil
}
