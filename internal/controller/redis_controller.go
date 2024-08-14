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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"github.com/go-logr/logr"
	redisv1beta1 "github.com/jaehanbyun/redis-operator/api/v1beta1"
)

const RedisFinalizer string = "redisFinalizer"

// RedisReconciler reconciles a Redis object
type RedisReconciler struct {
	client.Client
	Log    logr.Logger
	Scheme *runtime.Scheme
}

//+kubebuilder:rbac:groups=redis.redis,resources=redis,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=redis.redis,resources=redis/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=redis.redis,resources=redis/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the Hello object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.8.3/pkg/reconcile
func (r *RedisReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	redisLogger := r.Log.WithValues("redis", req.NamespacedName)

	var redis redisv1beta1.Redis
	if err := r.Get(ctx, req.NamespacedName, &redis); err != nil {
		if errors.IsNotFound(err) {
			return ctrl.Result{}, err
		}
		redisLogger.Error(err, "Redis를 가져오는 데 실패했습니다!")
		return ctrl.Result{}, err
	}

	// Finalinzer 추가
	if !controllerutil.ContainsFinalizer(&redis, RedisFinalizer) {
		redisLogger.Info("Finalizer 추가")
		controllerutil.AddFinalizer(&redis, RedisFinalizer)
		return ctrl.Result{}, r.Update(ctx, &redis)
	}

	// 삭제
	if !redis.DeletionTimestamp.IsZero() {
		redisLogger.Info("Redis 삭제")
		return r.reconcileDelete(ctx, redisLogger, &redis)
	}

	redisLogger.Info("StatefulSet 생성")

	err := r.createOrUpdateStatefulSet(ctx, &redis)
	if err != nil {
		redisLogger.Error(err, "StatefulSet을 생성 또는 업데이트하는 데 실패했습니다!")
		return ctrl.Result{}, err
	}

	redisLogger.Info("Service 생성")
	err = r.createOrUpdateService(ctx, &redis)
	if err != nil {
		redisLogger.Error(err, "Service를 생성 또는 업데이트하는 데 실패했습니다!")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *RedisReconciler) createOrUpdateStatefulSet(ctx context.Context, redis *redisv1beta1.Redis) error {
	statefulSet := appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:      redis.Name,
			Namespace: redis.Namespace,
			Labels:    map[string]string{"label": redis.ObjectMeta.Name, "app": redis.ObjectMeta.Name},
		},
		Spec: appsv1.StatefulSetSpec{
			ServiceName: redis.Name,
			Replicas:    int32Ptr(1),
			Selector: &metav1.LabelSelector{
				MatchLabels: map[string]string{"label": redis.ObjectMeta.Name},
			},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{
					Labels: map[string]string{"app": redis.Name},
				},
				Spec: corev1.PodSpec{
					Containers: []corev1.Container{
						{
							Name:  "redis",
							Image: redis.Spec.Image,
							Ports: []corev1.ContainerPort{
								{
									ContainerPort: redis.Spec.Port,
								},
							},
							Resources: redis.Spec.Resources,
							Command: []string{
								"redis-server",
								"--port", fmt.Sprintf("%d", redis.Spec.Port),
							},
							ReadinessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{
										Command: []string{
											"redis-cli",
											"-p", fmt.Sprintf("%d", redis.Spec.Port+5000),
											"ping",
										},
									},
								},
								InitialDelaySeconds: 10,
								PeriodSeconds:       5,
							},
							LivenessProbe: &corev1.Probe{
								ProbeHandler: corev1.ProbeHandler{
									Exec: &corev1.ExecAction{
										Command: []string{
											"redis-cli",
											"-p", fmt.Sprintf("%d", redis.Spec.Port+5000),
											"ping",
										},
									},
								},
								InitialDelaySeconds: 10,
								PeriodSeconds:       5,
							},
						},
						{
							Name:  "redis-exporter",
							Image: redis.Spec.ExporterImage,
							Ports: []corev1.ContainerPort{
								{
									ContainerPort: redis.Spec.ExporterPort,
								},
							},
							Resources: redis.Spec.ExporterResources,
						},
					},
				},
			},
			VolumeClaimTemplates: []corev1.PersistentVolumeClaim{
				{
					ObjectMeta: metav1.ObjectMeta{
						Name: "redis-data",
					},
					Spec: corev1.PersistentVolumeClaimSpec{
						AccessModes: []corev1.PersistentVolumeAccessMode{
							corev1.ReadWriteOnce,
						},
						Resources: corev1.ResourceRequirements{
							Requests: corev1.ResourceList{
								corev1.ResourceStorage: resource.MustParse(redis.Spec.VolumeSize),
							},
						},
					},
				},
			},
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, &statefulSet, func() error {
		statefulSet.Spec.Template.Spec.Containers[0].Image = redis.Spec.Image
		statefulSet.Spec.Template.Spec.Containers[0].Resources = redis.Spec.Resources
		statefulSet.Spec.Template.Spec.Containers[0].ReadinessProbe = redis.Spec.ReadinessProbe
		statefulSet.Spec.Template.Spec.Containers[0].LivenessProbe = redis.Spec.LivenessProbe
		statefulSet.Spec.Template.Spec.Containers[1].Image = redis.Spec.ExporterImage
		statefulSet.Spec.Template.Spec.Containers[1].Resources = redis.Spec.ExporterResources
		return nil
	})
	if err != nil {
		return fmt.Errorf("StatefulSet 생성 또는 업데이트 중 오류 발생: %v", err)
	}

	return nil
}

func int32Ptr(i int32) *int32 {
	return &i
}

func (r *RedisReconciler) createOrUpdateService(ctx context.Context, redis *redisv1beta1.Redis) error {
	service := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      redis.Name,
			Namespace: redis.Namespace,
			Labels:    map[string]string{"app": redis.Name},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{
				"app": redis.Name,
			},
			Ports: []corev1.ServicePort{
				{
					Name:       "redis",
					Protocol:   corev1.ProtocolTCP,
					Port:       redis.Spec.Port,
					TargetPort: intstr.FromInt(int(redis.Spec.Port)),
				},
				{
					Name:       "redis-exporter",
					Protocol:   corev1.ProtocolTCP,
					Port:       redis.Spec.Port + 5000,
					TargetPort: intstr.FromInt(int(redis.Spec.Port + 5000)),
				},
			},
			ClusterIP: "None",
			Type:      corev1.ServiceTypeNodePort,
		},
	}

	_, err := controllerutil.CreateOrUpdate(ctx, r.Client, service, func() error {
		service.Spec.Ports[0].Port = 10000
		service.Spec.Ports[0].TargetPort = intstr.FromInt(10000)
		service.Spec.Ports[1].Port = 15000
		service.Spec.Ports[1].TargetPort = intstr.FromInt(15000)
		service.Spec.Type = corev1.ServiceTypeNodePort
		service.Spec.Selector = map[string]string{
			"app": redis.Name,
		}
		service.Spec.ClusterIP = "None"
		return nil
	})
	if err != nil {
		return fmt.Errorf("service 생성 또는 업데이트 중 오류 발생: %v", err)
	}

	return nil
}

func (r *RedisReconciler) reconcileDelete(ctx context.Context, logger logr.Logger, redis *redisv1beta1.Redis) (ctrl.Result, error) {
	logger.Info("Finalizer를 제거하는 중입니다.")

	controllerutil.RemoveFinalizer(redis, RedisFinalizer)
	err := r.Update(ctx, redis)
	if err != nil {
		logger.Error(err, "Finalizer 제거 중 오류 발생")
		return ctrl.Result{}, fmt.Errorf("error removing finalizer %s", err)
	}
	logger.Info("Finalizer 제거 완료")
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *RedisReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&redisv1beta1.Redis{}).
		Complete(r)
}
