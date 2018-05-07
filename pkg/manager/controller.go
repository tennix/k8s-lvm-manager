package manager

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/tennix/k8s-lvm-manager/pkg/util"
	"k8s.io/api/core/v1"
	apierr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

type NodePatch struct {
	Op    string `json:"op"`
	Path  string `json:"path"`
	Value string `json:"value,omitempty"`
}

type Controller struct {
	lvm             LVManager
	domainName      string
	nodeName        string
	provisionerName string
	kubeCli         kubernetes.Interface

	controller cache.Controller
	store      cache.Store
	queue      *workqueue.Type
}

func NewController(cli kubernetes.Interface, lvm LVManager, domainName, nodeName, provisionerName string) *Controller {
	ctrl := &Controller{
		kubeCli:         cli,
		nodeName:        nodeName,
		provisionerName: provisionerName,
		domainName:      domainName,
		lvm:             lvm,
		queue:           workqueue.New(),
	}
	ctrl.store, ctrl.controller = cache.NewInformer(
		&cache.ListWatch{
			ListFunc: cache.ListFunc(func(opts metav1.ListOptions) (runtime.Object, error) {
				return ctrl.kubeCli.CoreV1().PersistentVolumeClaims(metav1.NamespaceAll).List(opts)
			}),
			WatchFunc: cache.WatchFunc(func(opts metav1.ListOptions) (watch.Interface, error) {
				return ctrl.kubeCli.CoreV1().PersistentVolumeClaims(metav1.NamespaceAll).Watch(opts)
			}),
		},
		&v1.PersistentVolumeClaim{},
		30*time.Second,
		cache.ResourceEventHandlerFuncs{
			AddFunc: ctrl.enqueuePVC,
			UpdateFunc: func(old, cur interface{}) {
				ctrl.enqueuePVC(cur)
			},
			DeleteFunc: ctrl.enqueuePVC,
		},
	)
	return ctrl
}

func (c *Controller) Run(workers int, stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()
	defer c.queue.ShutDown()
	glog.Infof("Starting LVM controller")
	go c.controller.Run(stopCh)
	for i := 0; i < workers; i++ {
		go wait.Until(c.worker, time.Second, stopCh)
	}
	<-stopCh
	glog.Infof("Shutting down LVM controller")
}

func (c *Controller) worker() {
	for {
		func() {
			key, quit := c.queue.Get()
			if quit {
				return
			}
			defer c.queue.Done(key)
			if err := c.syncPVC(key.(string)); err != nil {
				glog.Error(err)
			}
			if err := c.lvm.SyncLVMStatus(); err != nil {
				glog.Error(err)
			}
		}()
	}
}

func (c *Controller) enqueuePVC(obj interface{}) {
	key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
	if err != nil {
		glog.Errorf("cant' get key for obj: %v, err: %v", obj, err)
	}
	c.queue.Add(key)
}

func (c *Controller) syncPVC(key string) error {
	startTime := time.Now()
	defer func() {
		glog.Infof("Finished syncing Pod[%s] (%v)", key, time.Now().Sub(startTime))
	}()

	obj, exists, err := c.store.GetByKey(key)
	if err != nil {
		return err
	}
	ns, pvcName, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	if !exists {
		return c.ReleaseLV(ns, pvcName)
	}
	pvc, ok := obj.(*v1.PersistentVolumeClaim)
	if !ok {
		return fmt.Errorf("object %v is not a PersistentVolumeClaim", obj)
	}
	ann := pvc.GetAnnotations()
	nodeName, ok := ann[util.AnnProvisionerNode]
	if !ok || nodeName != c.nodeName {
		glog.Infof("PVC %s/%s not scheduled or not managed by me", ns, pvcName)
		return nil
	}
	hostPath, ok := ann[util.AnnProvisionerHostPath]
	if !ok || hostPath != "" {
		glog.Infof("PVC %s/%s doesn't contain hostPath annotation or already provisioned", ns, pvcName)
		return nil
	}

	vgName := ann[util.AnnProvisionerVGName]
	lvName := ann[util.AnnProvisionerLVName]
	size := ann[util.AnnProvisionerLVSize]
	// fsType := ann[util.AnnProvisionerLVFsType]
	fsType := "ext4"
	if err := c.lvm.AllocateLV(lvName, vgName, size); err != nil {
		glog.Errorf("failed to allocate LV")
		return err
	}
	if err := c.lvm.FormatLV(lvName, vgName, fsType); err != nil {
		return err
	}
	hostPath, err = c.lvm.MountLV(lvName, vgName)
	if err != nil {
		return err
	}
	ann[util.AnnProvisionerHostPath] = hostPath
	_, err = c.kubeCli.CoreV1().PersistentVolumeClaims(ns).Update(pvc)
	if err != nil {
		glog.Errorf("failed to update PVC %s/%s: %v", ns, pvcName, err)
		return err
	}

	return nil
}

func (c *Controller) UpdateNodeStatus(vgs map[string]VolumeGroup) error {
	if len(vgs) == 0 {
		return nil
	}
	vg, found := vgs["loopback-disk"]
	if !found {
		return nil
	}
	patches := []NodePatch{
		{
			Op:    "add",
			Path:  fmt.Sprintf("/status/capacity/%s~1%s", c.domainName, vg.Name),
			Value: strings.ToUpper(vg.Size),
		},
	}
	data, err := json.Marshal(patches)
	if err != nil {
		glog.Errorf("failed to marshal patches %v: %v", patches, err)
		return err
	}
	glog.Infof("patch: %s", data)
	_, err = c.kubeCli.CoreV1().Nodes().Patch(c.nodeName, types.JSONPatchType, data, "status")
	if err != nil {
		glog.Errorf("failed to patch status for node %s: %v", c.nodeName, err)
		return err
	}
	return nil
}

func (c *Controller) ReleaseLV(pvcNamespace, pvcName string) error {
	opts := metav1.ListOptions{}
	pvList, err := c.kubeCli.CoreV1().PersistentVolumes().List(opts)
	if err != nil {
		glog.Errorf("failed to list pv")
		return err
	}
	var pv *v1.PersistentVolume
	for _, item := range pvList.Items {
		ref := item.Spec.ClaimRef
		if ref != nil && ref.Namespace == pvcNamespace && ref.Name == pvcName {
			pv = &item
			break
		}
	}
	if pv == nil {
		glog.Infof("no pv found for pvc %s/%s", pvcNamespace, pvcName)
		return nil
	}
	pvName := pv.GetName()
	ann := pv.GetAnnotations()
	if node := ann[util.AnnProvisionerNode]; node != c.nodeName {
		glog.Infof("pv %s not managed by me", pvName)
		return nil
	}
	lvName := ann[util.AnnProvisionerLVName]
	vgName := ann[util.AnnProvisionerVGName]
	if err := c.lvm.UnmountLV(lvName); err != nil {
		return err
	}
	if err := c.lvm.RemoveLV(lvName, vgName); err != nil {
		return err
	}
	return wait.Poll(3*time.Second, 30*time.Second, func() (bool, error) {
		pv.Annotations[util.AnnProvisionerLVDeleted] = "true"
		_, err = c.kubeCli.CoreV1().PersistentVolumes().Update(pv)
		if err == nil {
			return true, nil
		}
		if apierr.IsConflict(err) {
			pv, err = c.kubeCli.CoreV1().PersistentVolumes().Get(pvName, metav1.GetOptions{})
			if err != nil {
				glog.Errorf("failed to get PV %s: %v", pvName, err)
				return false, nil
			}
			return false, nil
		}
		glog.Errorf("failed to update PV %s annotations: %v", pvName, err)
		return false, nil
	})
}
