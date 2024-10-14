/*
Copyright 2024 jaehanbyun.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/go-logr/logr"
	redisv1beta1 "github.com/jaehanbyun/redis-operator/api/v1beta1"
	"github.com/jaehanbyun/redis-operator/utils"
)

// RedisClusterReconciler reconciles a RedisCluster object
type RedisClusterReconciler struct {
	client.Client
	K8sClient kubernetes.Interface
	Log       logr.Logger
	Scheme    *runtime.Scheme
}

//+kubebuilder:rbac:groups=redis.redis,resources=redisclusters,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=redis.redis,resources=redisclusters/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=redis.redis,resources=redisclusters/finalizers,verbs=update

//+kubebuilder:rbac:groups="",resources=pods,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups="",resources=pods/exec,verbs=create

const RedisClusterFinalizer string = "redisClusterFinalizer"

func (r *RedisClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	clusterLogger := r.Log.WithValues("redisCluster", req.NamespacedName)
	clusterLogger.Info("Reconciling RedisCluster")

	redisCluster := &redisv1beta1.RedisCluster{}
	if err := r.Get(ctx, req.NamespacedName, redisCluster); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		clusterLogger.Error(err, "Failed to get RedisCluster")
		return ctrl.Result{}, err
	}

	// Handle deletion
	if !redisCluster.DeletionTimestamp.IsZero() {
		clusterLogger.Info("Deleting RedisCluster")
		if controllerutil.ContainsFinalizer(redisCluster, RedisClusterFinalizer) {
			return r.reconcileDelete(ctx, clusterLogger, redisCluster)
		}
		return ctrl.Result{}, nil
	}

	// Add finalizer
	if !controllerutil.ContainsFinalizer(redisCluster, RedisClusterFinalizer) {
		clusterLogger.Info("Adding finalizer")
		controllerutil.AddFinalizer(redisCluster, RedisClusterFinalizer)
		err := r.Update(ctx, redisCluster)
		if err != nil {
			clusterLogger.Info("Error adding finalizer")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	desiredMasterCount := redisCluster.Spec.Masters
	desiredReplicasPerMaster := redisCluster.Spec.Replicas

	currentMasterCount := int32(len(redisCluster.Status.MasterMap))
	currentReplicaCount := int32(len(redisCluster.Status.ReplicaMap))

	// Initialize cluster
	if currentMasterCount == 0 && desiredMasterCount > 0 {
		clusterLogger.Info("Initializing cluster")
		if err := utils.SetupRedisCluster(ctx, r.Client, r.K8sClient, redisCluster, clusterLogger); err != nil {
			clusterLogger.Error(err, "Error setting up Redis cluster")
			return ctrl.Result{}, err
		}

		cmd := utils.CreateClusterCommand(r.K8sClient, redisCluster, clusterLogger)
		var firstMasterPodName string
		for _, master := range redisCluster.Status.MasterMap {
			firstMasterPodName = master.PodName
			break
		}

		_, err := utils.RunRedisCLI(r.K8sClient, redisCluster.Namespace, firstMasterPodName, cmd)
		if err != nil {
			clusterLogger.Error(err, "Error running cluster creation command")
			return ctrl.Result{}, err
		}
		clusterLogger.Info("Redis cluster created successfully")

		if err := utils.UpdateClusterStatus(ctx, r.Client, r.K8sClient, redisCluster, clusterLogger); err != nil {
			clusterLogger.Error(err, "Error updating cluster status")
			return ctrl.Result{}, err
		}

		currentMasterCount = int32(len(redisCluster.Status.MasterMap))
	}

	// Handle master scaling
	// if the current number of master nodes is less than the desired count
	if currentMasterCount < desiredMasterCount {
		mastersToAdd := desiredMasterCount - currentMasterCount
		clusterLogger.Info("Scaling up masters", "Masters to add", mastersToAdd)
		for i := int32(0); i < mastersToAdd; i++ {
			port, err := utils.FindAvailablePort(r.Client, redisCluster, clusterLogger)
			if err != nil {
				clusterLogger.Error(err, "Failed to find available port")
				return ctrl.Result{}, err
			}

			if err := utils.CreateMasterPod(ctx, r.Client, r.K8sClient, redisCluster, clusterLogger, port); err != nil {
				clusterLogger.Error(err, "Error creating master Pod")
				return ctrl.Result{}, err
			}

			newMasterPodName := fmt.Sprintf("rediscluster-%s-%d", redisCluster.Name, port)
			if err := utils.WaitForPodReady(ctx, r.Client, redisCluster, clusterLogger, newMasterPodName); err != nil {
				clusterLogger.Error(err, "Error waiting for new master Pod to be ready", "Pod", newMasterPodName)
				return ctrl.Result{}, err
			}

			newMasterNodeID, err := utils.GetRedisNodeID(ctx, r.K8sClient, clusterLogger, redisCluster.Namespace, newMasterPodName)
			if err != nil {
				clusterLogger.Error(err, "Error getting NodeID of new master", "Pod", newMasterPodName)
				return ctrl.Result{}, err
			}

			newMaster := redisv1beta1.RedisNodeStatus{
				PodName: newMasterPodName,
				NodeID:  newMasterNodeID,
			}

			if err := utils.AddMasterToCluster(r.K8sClient, redisCluster, clusterLogger, newMaster); err != nil {
				clusterLogger.Error(err, "Error adding new master to cluster", "NodeID", newMasterNodeID)
				return ctrl.Result{}, err
			}

			if err := utils.UpdateClusterStatus(ctx, r.Client, r.K8sClient, redisCluster, clusterLogger); err != nil {
				clusterLogger.Error(err, "Error updating cluster status")
				return ctrl.Result{}, err
			}

			currentMasterCount = int32(len(redisCluster.Status.MasterMap))
		}
	} else if currentMasterCount > desiredMasterCount {
		mastersToRemove := currentMasterCount - desiredMasterCount
		clusterLogger.Info("Scaling down masters", "Masters to remove", mastersToRemove)

		mastersToRemoveList := utils.GetMastersToRemove(redisCluster, mastersToRemove, clusterLogger)

		for _, masterNodeID := range mastersToRemoveList {
			master := redisCluster.Status.MasterMap[masterNodeID]

			err := utils.ReShardRedisCluster(ctx, r.K8sClient, redisCluster, clusterLogger, master)
			if err != nil {
				clusterLogger.Error(err, "Error migrating slots from master", "MasterNodeID", masterNodeID)
				return ctrl.Result{}, err
			}

			err = utils.RemoveReplicasOfMaster(ctx, r.Client, r.K8sClient, redisCluster, clusterLogger, masterNodeID)
			if err != nil {
				clusterLogger.Error(err, "Error removing replicas of master", "MasterNodeID", masterNodeID)
				return ctrl.Result{}, err
			}

			err = utils.DeleteRedisPod(ctx, r.Client, r.K8sClient, redisCluster, clusterLogger, master.PodName)
			if err != nil {
				clusterLogger.Error(err, "Error deleting master Pod", "PodName", master.PodName)
				return ctrl.Result{}, err
			}

			delete(redisCluster.Status.MasterMap, masterNodeID)
			redisCluster.Status.ReadyMasters = int32(len(redisCluster.Status.MasterMap))

			if err := r.Status().Update(ctx, redisCluster); err != nil {
				clusterLogger.Error(err, "Error updating RedisCluster status")
				return ctrl.Result{}, err
			}
		}
		if err := utils.UpdateClusterStatus(ctx, r.Client, r.K8sClient, redisCluster, clusterLogger); err != nil {
			clusterLogger.Error(err, "Error updating cluster status")
			return ctrl.Result{}, err
		}

		currentMasterCount = int32(len(redisCluster.Status.MasterMap))
	}

	masterToReplicas := make(map[string]int32)
	for masterID := range redisCluster.Status.MasterMap {
		masterToReplicas[masterID] = 0
	}
	for _, replica := range redisCluster.Status.ReplicaMap {
		masterToReplicas[replica.MasterNodeID]++
	}

	desiredTotalReplicas := desiredReplicasPerMaster * currentMasterCount
	if desiredTotalReplicas > currentReplicaCount {
		replicasToAdd := desiredTotalReplicas - currentReplicaCount
		clusterLogger.Info("Scaling up replicas", "Replicas to add", replicasToAdd)

		for i := int32(0); i < replicasToAdd; i++ {
			var assignedMasterID string
			for masterID, count := range masterToReplicas {
				if count < desiredReplicasPerMaster {
					assignedMasterID = masterID
					break
				}
			}

			port, err := utils.FindAvailablePort(r.Client, redisCluster, clusterLogger)
			if err != nil {
				clusterLogger.Error(err, "Failed to find replica port")
				return ctrl.Result{}, err
			}

			if err := utils.CreateReplicaPod(ctx, r.Client, r.K8sClient, redisCluster, clusterLogger, port, assignedMasterID); err != nil {
				clusterLogger.Error(err, "Error creating replica Pod")
				return ctrl.Result{}, err
			}

			newReplicaPodName := fmt.Sprintf("rediscluster-%s-%d", redisCluster.Name, port)
			if err := utils.WaitForPodReady(ctx, r.Client, redisCluster, clusterLogger, newReplicaPodName); err != nil {
				clusterLogger.Error(err, "Error waiting for new replica Pod to be ready", "Pod", newReplicaPodName)
				return ctrl.Result{}, err
			}

			newReplicaNodeID, err := utils.GetRedisNodeID(ctx, r.K8sClient, clusterLogger, redisCluster.Namespace, newReplicaPodName)
			if err != nil {
				clusterLogger.Error(err, "Error getting NodeID of replica", "Pod", newReplicaPodName)
				return ctrl.Result{}, err
			}

			if redisCluster.Status.ReplicaMap == nil {
				redisCluster.Status.ReplicaMap = make(map[string]redisv1beta1.RedisNodeStatus)
			}
			redisCluster.Status.ReplicaMap[newReplicaNodeID] = redisv1beta1.RedisNodeStatus{
				PodName:      newReplicaPodName,
				NodeID:       newReplicaNodeID,
				MasterNodeID: assignedMasterID,
			}

			newReplica := redisCluster.Status.ReplicaMap[newReplicaNodeID]

			if err := utils.AddReplicaToMaster(r.K8sClient, redisCluster, clusterLogger, newReplica); err != nil {
				clusterLogger.Error(err, "Error adding replica to master", "ReplicaNodeID", newReplicaNodeID)
				return ctrl.Result{}, err
			}

			if err := utils.UpdateClusterStatus(ctx, r.Client, r.K8sClient, redisCluster, clusterLogger); err != nil {
				clusterLogger.Error(err, "Error updating cluster status")
				return ctrl.Result{}, err
			}

			masterToReplicas[assignedMasterID]++
		}
	} else if desiredTotalReplicas < currentReplicaCount {
		replicasToRemove := currentReplicaCount - desiredTotalReplicas
		clusterLogger.Info("Scaling down replicas", "Replicas to remove", replicasToRemove)

		for masterID, replicas := range masterToReplicas {
			excessReplicas := replicas - desiredReplicasPerMaster
			if excessReplicas > 0 {
				replicasList := utils.GetReplicasToRemoveFromMaster(redisCluster, masterID, excessReplicas, clusterLogger)

				for _, replicaNodeID := range replicasList {
					replica := redisCluster.Status.ReplicaMap[replicaNodeID]

					err := utils.RemoveNodeFromCluster(r.K8sClient, redisCluster, clusterLogger, replica)
					if err != nil {
						clusterLogger.Error(err, "Error removing replica from cluster", "ReplicaNodeID", replicaNodeID)
						return ctrl.Result{}, err
					}

					err = utils.DeleteRedisPod(ctx, r.Client, r.K8sClient, redisCluster, clusterLogger, replica.PodName)
					if err != nil {
						clusterLogger.Error(err, "Error deleting replica Pod", "PodName", replica.PodName)
						return ctrl.Result{}, err
					}

					delete(redisCluster.Status.ReplicaMap, replicaNodeID)
					redisCluster.Status.ReadyReplicas = int32(len(redisCluster.Status.ReplicaMap))

					masterToReplicas[masterID]--

					if err := r.Status().Update(ctx, redisCluster); err != nil {
						clusterLogger.Error(err, "Error updating RedisCluster status")
						return ctrl.Result{}, err
					}

					replicasToRemove--
					if replicasToRemove == 0 {
						break
					}
				}
			}

			if replicasToRemove == 0 {
				break
			}
		}
		if err := utils.UpdateClusterStatus(ctx, r.Client, r.K8sClient, redisCluster, clusterLogger); err != nil {
			clusterLogger.Error(err, "Error updating cluster status")
			return ctrl.Result{}, err
		}
	}

	if err := r.Status().Update(ctx, redisCluster); err != nil {
		clusterLogger.Error(err, "Error updating RedisCluster status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{
		Requeue:      true,
		RequeueAfter: time.Second * 10,
	}, nil
}

func (r *RedisClusterReconciler) reconcileDelete(ctx context.Context, logger logr.Logger, redisCluster *redisv1beta1.RedisCluster) (ctrl.Result, error) {
	logger.Info("Removing finalizer")

	controllerutil.RemoveFinalizer(redisCluster, RedisClusterFinalizer)
	err := r.Update(ctx, redisCluster)
	if err != nil {
		logger.Error(err, "Error removing finalizer")
		return ctrl.Result{}, err
	}
	logger.Info("Finalizer removed")
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *RedisClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&redisv1beta1.RedisCluster{}).
		Complete(r)
}
