package k8sutils

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	redisv1beta1 "github.com/jaehanbyun/redis-operator/api/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// GenerateRedisProbe returns Liveness and Readiness probes
func GenerateRedisProbe(port int32) *corev1.Probe {
	return &corev1.Probe{
		ProbeHandler: corev1.ProbeHandler{
			Exec: &corev1.ExecAction{
				Command: []string{
					"redis-cli",
					"-p", fmt.Sprintf("%d", port),
					"ping",
				},
			},
		},
		InitialDelaySeconds: 15,
		PeriodSeconds:       10,
		FailureThreshold:    3,
	}
}

// GenerateAnnotations returns a list of annotations for prometheus metrics
func GenerateAnnotations(port int32) map[string]string {
	return map[string]string{
		"prometheus.io/port":   fmt.Sprintf("%d", port+5000),
		"prometheus.io/scrape": "true",
	}
}

// GenerateLabels returns a set of labels for the given info
func GenerateLabels(clusterName string, port int32) map[string]string {
	return map[string]string{
		"clusterName": clusterName,
		"port":        fmt.Sprintf("%d", port),
	}
}

// GenerateRedisClusterAsOwner returns a list of OwnerReferences for the given RedisCluster resource
func GenerateRedisClusterAsOwner(redisCluster *redisv1beta1.RedisCluster, crName string) []metav1.OwnerReference {
	return []metav1.OwnerReference{
		*metav1.NewControllerRef(redisCluster, redisv1beta1.GroupVersion.WithKind("RedisCluster")),
	}
}

// ExtractPortFromPodName extracts the port number from a Pod's name
func ExtractPortFromPodName(podName string) int32 {
	parts := strings.Split(podName, "-")
	portStr := parts[len(parts)-1]
	port, _ := strconv.Atoi(portStr)

	return int32(port)
}

// CreateMasterPod creates a Redis master Pod with the given port
func CreateMasterPod(ctx context.Context, cl client.Client, k8scl kubernetes.Interface, redisCluster *redisv1beta1.RedisCluster, logger logr.Logger, port int32) error {
	podName := fmt.Sprintf("rediscluster-%s-%d", redisCluster.Name, port)
	masterPod := GenerateRedisPodDef(redisCluster, port, "")
	if err := cl.Create(ctx, masterPod); err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

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

	return nil
}

// CreateReplicaPod creates a Redis replica Pod attached to the specified master node
func CreateReplicaPod(ctx context.Context, cl client.Client, k8scl kubernetes.Interface, redisCluster *redisv1beta1.RedisCluster, logger logr.Logger, port int32, masterNodeID string) error {
	podName := fmt.Sprintf("rediscluster-%s-%d", redisCluster.Name, port)
	replicaPod := GenerateRedisPodDef(redisCluster, port, masterNodeID)
	err := cl.Create(ctx, replicaPod)
	if err != nil && !errors.IsAlreadyExists(err) {
		return err
	}

	if err := WaitForPodReady(ctx, cl, redisCluster, logger, podName); err != nil {
		logger.Error(err, "Error waiting for replica Pod to be ready", "Pod", podName)
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

	return nil
}

// WaitForPodReady waits until the specified Pod is in the Running state and its containers are ready
func WaitForPodReady(ctx context.Context, cl client.Client, redisCluster *redisv1beta1.RedisCluster, logger logr.Logger, podName string) error {
	pod := &corev1.Pod{}
	for {
		err := cl.Get(ctx, client.ObjectKey{Namespace: redisCluster.Namespace, Name: podName}, pod)
		if err != nil {
			if errors.IsNotFound(err) {
				logger.Info("Pod not yet created, retrying...", "Pod", podName)
				time.Sleep(2 * time.Second)
				continue
			}
			logger.Error(err, "Failed to get Pod", "Pod", podName)
			return err
		}

		if pod.Status.Phase == corev1.PodRunning && pod.Status.ContainerStatuses[0].Ready {
			logger.Info("Pod is ready", "Pod", podName)
			break
		}

		logger.Info("Pod not ready yet, retrying...", "Pod", podName)
		time.Sleep(5 * time.Second)
	}
	return nil
}

// FindAvailablePort finds an available port for a new Redis Pod within a specified range
func FindAvailablePort(cl client.Client, redisCluster *redisv1beta1.RedisCluster, logger logr.Logger) (int32, error) {
	usedPorts := make(map[int32]bool)
	existingPods := &corev1.PodList{}
	opts := []client.ListOption{
		client.InNamespace(redisCluster.Namespace),
		client.MatchingLabels{"clusterName": redisCluster.Name},
	}

	err := cl.List(context.TODO(), existingPods, opts...)
	if err != nil {
		return 0, err
	}

	for _, pod := range existingPods.Items {
		podIP := ExtractPortFromPodName(pod.Name)
		logger.Info("Pod info", "PodName", pod.Name, "PodIP", pod.Status.PodIP, "Phase", pod.Status.Phase)
		usedPorts[podIP] = true
	}

	basePort := redisCluster.Spec.BasePort
	maxPort := basePort + redisCluster.Spec.Masters + redisCluster.Spec.Masters*redisCluster.Spec.Replicas + 100
	for i := int32(0); i < maxPort; i++ {
		port := basePort + i
		if !usedPorts[port] {
			return port, nil
		}
	}

	return 0, fmt.Errorf("no available ports found in the range")
}

// UpdatePodLabelWithRedisID updates the Pod's labels to include the Redis node ID
func UpdatePodLabelWithRedisID(ctx context.Context, cl client.Client, redisCluster *redisv1beta1.RedisCluster, logger logr.Logger, podName string, redisNodeID string) error {
	pod := &corev1.Pod{}
	err := cl.Get(ctx, client.ObjectKey{Namespace: redisCluster.Namespace, Name: podName}, pod)
	if err != nil {
		logger.Error(err, "Failed to get Pod", "Pod", podName)
		return err
	}

	pod.Labels["redisNodeID"] = redisNodeID

	if err := cl.Update(ctx, pod); err != nil {
		logger.Error(err, "Error updating Pod label", "Pod", podName)
		return err
	}

	logger.Info("Pod label updated successfully", "Pod", podName, "redisNodeID", redisNodeID)
	return nil
}

// GenerateRedisPodDef generates a Pod definition for a Redis instance
func GenerateRedisPodDef(redisCluster *redisv1beta1.RedisCluster, port int32, matchMasterNodeID string) *corev1.Pod {
	podName := fmt.Sprintf("rediscluster-%s-%d", redisCluster.Name, port)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        podName,
			Namespace:   redisCluster.Namespace,
			Labels:      GenerateLabels(redisCluster.Name, port),
			Annotations: GenerateAnnotations(port),
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(redisCluster, redisv1beta1.GroupVersion.WithKind("RedisCluster")),
			},
		},
		Spec: corev1.PodSpec{
			HostNetwork: true, // Enable HostNetwork
			Containers: []corev1.Container{
				{
					Name:  "redis",
					Image: redisCluster.Spec.Image,
					Ports: []corev1.ContainerPort{
						{
							ContainerPort: port,
							Name:          "redis",
						},
						{
							ContainerPort: port + 10000,
							Name:          "redis-bus",
						},
					},
					Command: []string{
						"redis-server",
						"--port", fmt.Sprintf("%d", port),
						"--cluster-enabled", "yes",
						"--cluster-port", fmt.Sprintf("%d", port+10000),
						"--cluster-node-timeout", "5000",
						"--maxmemory", redisCluster.Spec.Maxmemory,
					},
					ReadinessProbe: GenerateRedisProbe(port),
					LivenessProbe:  GenerateRedisProbe(port),
				},
				{
					Name:  "redis-exporter",
					Image: "oliver006/redis_exporter:latest",
					Ports: []corev1.ContainerPort{
						{
							ContainerPort: port + 5000,
							Name:          "redis-exporter",
						},
					},
					Args: []string{
						"--web.listen-address", fmt.Sprintf(":%d", port+5000),
						"--redis.addr", fmt.Sprintf("localhost:%d", port),
					},
				},
			},
		},
	}

	// Apply Redis resource settings if provided
	if redisCluster.Spec.Resources != nil {
		pod.Spec.Containers[0].Resources = *redisCluster.Spec.Resources
	}

	// Apply RedisExporter resource settings if provided
	if redisCluster.Spec.ExporterResources != nil {
		pod.Spec.Containers[1].Resources = *redisCluster.Spec.ExporterResources
	}

	// If matchMasterNodeID is provided, set PodAntiAffinity
	if matchMasterNodeID != "" {
		pod.Spec.Affinity = &corev1.Affinity{
			PodAntiAffinity: &corev1.PodAntiAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{
					{
						LabelSelector: &metav1.LabelSelector{
							MatchExpressions: []metav1.LabelSelectorRequirement{
								{
									Key:      "redisNodeID",
									Operator: metav1.LabelSelectorOpIn,
									Values:   []string{matchMasterNodeID},
								},
							},
						},
						TopologyKey: "kubernetes.io/hostname",
					},
				},
			},
		}
	}

	return pod
}

// GetPodNameByNodeID searches the Pod name associated with a given Redis node ID
func GetPodNameByNodeID(k8scl kubernetes.Interface, namespace string, nodeID string, logger logr.Logger) (string, error) {
	podList, err := k8scl.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("redisNodeID=%s", nodeID),
	})
	if err != nil {
		logger.Error(err, "Failed to search Pod using", "NodeID", nodeID)
		return "", err
	}
	if len(podList.Items) == 0 {
		return "", fmt.Errorf("cannot find Pod with NodeID %s in %s namespace", nodeID, namespace)
	}
	return podList.Items[0].Name, nil
}

// containsFlag checks if a target flag exists within a list of flags
func containsFlag(flags []string, target string) bool {
	for _, flag := range flags {
		if flag == target {
			return true
		}
	}
	return false
}

// DeleteRedisPod deletes the specified Redis Pod from the cluster
func DeleteRedisPod(ctx context.Context, cl client.Client, k8scl kubernetes.Interface, redisCluster *redisv1beta1.RedisCluster, logger logr.Logger, podName string) error {
	pod := &corev1.Pod{}
	err := cl.Get(ctx, client.ObjectKey{Namespace: redisCluster.Namespace, Name: podName}, pod)
	if err != nil {
		if errors.IsNotFound(err) {
			logger.Info("Replica Pod to delete does not exist", "Pod", podName)
			return nil
		}
		logger.Error(err, "Failed to get Pod", "Pod", podName)
		return err
	}

	err = cl.Delete(ctx, pod)
	if err != nil {
		logger.Error(err, "Error deleting Pod", "Pod", podName)
		return err
	}

	logger.Info("Pod deletion requested", "Pod", podName)
	return nil
}

// GetMastersToRemove selects a list of master node IDs to be removed from the cluster
func GetMastersToRemove(redisCluster *redisv1beta1.RedisCluster, mastersToRemove int32, logger logr.Logger) []string {
	mastersToRemoveList := make([]string, 0, mastersToRemove)
	count := int32(0)

	for nodeID := range redisCluster.Status.MasterMap {
		mastersToRemoveList = append(mastersToRemoveList, nodeID)
		count++
		if count >= mastersToRemove {
			break
		}
	}

	logger.Info("Masters selected for removal", "Masters", mastersToRemoveList)
	return mastersToRemoveList
}

// GetReplicasToRemoveFromMaster selects replica node IDs attached to a specific master node for removal from the cluster
func GetReplicasToRemoveFromMaster(redisCluster *redisv1beta1.RedisCluster, masterNodeID string, replicasToRemove int32, logger logr.Logger) []string {
	var replicasToRemoveList []string
	count := int32(0)

	for nodeID, replica := range redisCluster.Status.ReplicaMap {
		if replica.MasterNodeID == masterNodeID {
			replicasToRemoveList = append(replicasToRemoveList, nodeID)
			count++
			if count >= replicasToRemove {
				break
			}
		}
	}

	logger.Info("Replicas selected for removal from master", "MasterNodeID", masterNodeID, "Replicas", replicasToRemoveList)
	return replicasToRemoveList
}

// WaitForNodeRole waits until the specified node transitions to the expected role (e.g., "slave" or "master")
func WaitForNodeRole(k8scl kubernetes.Interface, redisCluster *redisv1beta1.RedisCluster, logger logr.Logger, nodeID string, expectedRole string, timeout time.Duration) error {
	startTime := time.Now()
	for {
		elapsed := time.Since(startTime)
		if elapsed > timeout {
			return fmt.Errorf("node %s did not transition to role %s", nodeID, expectedRole)
		}

		nodesInfo, err := GetClusterNodesInfo(k8scl, redisCluster, logger)
		if err != nil {
			logger.Error(err, "Failed to get cluster node information")
			return err
		}

		for _, node := range nodesInfo {
			if node.NodeID == nodeID {
				flagsList := strings.Split(node.Flags, ",")
				if containsFlag(flagsList, expectedRole) {
					return nil
				}
				break
			}
		}
		time.Sleep(2 * time.Second)
	}
}
