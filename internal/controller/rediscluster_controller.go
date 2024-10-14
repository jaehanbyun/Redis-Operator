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
	// desiredReplicasPerMaster := redisCluster.Spec.Replicas

	currentMasterCount := int32(len(redisCluster.Status.MasterMap))
	// currentReplicaCount := int32(len(redisCluster.Status.ReplicaMap))

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
