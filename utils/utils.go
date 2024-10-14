package utils

import (
	"bytes"
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
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/remotecommand"
	ctrl "sigs.k8s.io/controller-runtime"
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
	}
}

func GenerateAnnotations(port int32) map[string]string {
	return map[string]string{
		"prometheus.io/port":   fmt.Sprintf("%d", port+5000),
		"prometheus.io/scrape": "true",
	}
}

func GenerateLabels(clusterName string, port int32) map[string]string {
	return map[string]string{
		"clusterName": clusterName,
		"port":        fmt.Sprintf("%d", port),
	}
}

func GenerateRedisClusterAsOwner(redisCluster *redisv1beta1.RedisCluster, crName string) []metav1.OwnerReference {
	return []metav1.OwnerReference{
		*metav1.NewControllerRef(redisCluster, redisv1beta1.GroupVersion.WithKind("RedisCluster")),
	}
}

func ExtractPortFromPodName(podName string) int32 {
	parts := strings.Split(podName, "-")
	portStr := parts[len(parts)-1]
	port, _ := strconv.Atoi(portStr)

	return int32(port)
}

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

func GetRedisServerIP(k8scl kubernetes.Interface, logger logr.Logger, namespace string, podName string) string {
	pod, err := k8scl.CoreV1().Pods(namespace).Get(context.TODO(), podName, metav1.GetOptions{})
	if err != nil {
		logger.Error(err, "Failed to get Redis Server IP", "Pod", podName)
		return ""
	}
	return pod.Status.PodIP
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

func GetPodNameByNodeID(k8scl kubernetes.Interface, namespace string, nodeID string, logger logr.Logger) (string, error) {
	podList, err := k8scl.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: fmt.Sprintf("redisNodeID=%s", nodeID),
	})
	if err != nil {
		logger.Error(err, "NodeID로 Pod를 조회하는 데 실패했습니다.", "NodeID", nodeID)
		return "", err
	}
	if len(podList.Items) == 0 {
		logger.Info("해당 NodeID를 가진 Pod를 찾을 수 없습니다.", "NodeID", nodeID)
		return "", fmt.Errorf("NodeID %s를 가진 Pod를 찾을 수 없습니다", nodeID)
	}
	return podList.Items[0].Name, nil
}

func containsFlag(flags []string, target string) bool {
	for _, flag := range flags {
		if flag == target {
			return true
		}
	}
	return false
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

func GetRedisServerAddress(k8scl kubernetes.Interface, logger logr.Logger, namespace, podName string) string {
	ip := GetRedisServerIP(k8scl, logger, namespace, podName)
	port := ExtractPortFromPodName(podName)
	logger.Info(fmt.Sprintf("RedisServerAddress of %s: %s", podName, fmt.Sprintf("%s:%d", ip, port)))
	return fmt.Sprintf("%s:%d", ip, port)
}

func CreateClusterCommand(k8scl kubernetes.Interface, redisCluster *redisv1beta1.RedisCluster, logger logr.Logger) []string {
	cmd := []string{"redis-cli", "--cluster", "create"}

	for _, master := range redisCluster.Status.MasterMap {
		addr := GetRedisServerAddress(k8scl, logger, redisCluster.Namespace, master.PodName)
		cmd = append(cmd, addr)
	}

	cmd = append(cmd, "--cluster-replicas", "0", "--cluster-yes")

	logger.V(1).Info("Redis cluster creation command generated", "Command", cmd)
	return cmd
}

type ClusterNodeInfo struct {
	NodeID       string
	PodName      string
	Flags        string
	MasterNodeID string
}

func GetRedisNodeID(ctx context.Context, k8scl kubernetes.Interface, logger logr.Logger, namespace string, podName string) (string, error) {
	port := ExtractPortFromPodName(podName)

	cmd := []string{"redis-cli", "-h", "localhost", "-p", fmt.Sprintf("%d", port), "cluster", "myid"}

	output, err := RunRedisCLI(k8scl, namespace, podName, cmd)
	if err != nil {
		logger.Error(err, "Error getting Redis node ID", "Command", strings.Join(cmd, " "))
		return "", fmt.Errorf("failed to get Redis node ID: %w", err)
	}

	nodeID := strings.TrimSpace(output)
	if nodeID == "" {
		return "", fmt.Errorf("redis node ID not found")
	}

	return nodeID, nil
}

func RunRedisCLI(k8scl kubernetes.Interface, namespace string, podName string, cmd []string) (string, error) {
	config, err := ctrl.GetConfig()
	if err != nil {
		return "", fmt.Errorf("failed to get Kubernetes client config: %w", err)
	}

	req := k8scl.CoreV1().RESTClient().
		Post().
		Resource("pods").
		Name(podName).
		Namespace(namespace).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Command:   cmd,
			Container: "redis",
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	exec, err := remotecommand.NewSPDYExecutor(config, "POST", req.URL())
	if err != nil {
		return "", fmt.Errorf("failed to create SPDY executor: %w", err)
	}

	var exeOut, exeErr bytes.Buffer
	err = exec.StreamWithContext(context.Background(), remotecommand.StreamOptions{
		Stdout: &exeOut,
		Stderr: &exeErr,
		Tty:    false,
	})
	if err != nil {
		return "", fmt.Errorf("failed to execute command: %w, stdout: %s, stderr: %s", err, exeOut.String(), exeErr.String())
	}

	return exeOut.String(), nil
}
