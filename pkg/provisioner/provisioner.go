package provisioner

import (
	"errors"

	"github.com/golang/glog"
	"github.com/kubernetes-incubator/external-storage/lib/controller"
	"github.com/tennix/k8s-lvm-manager/pkg/util"
	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

type Controller struct {
	kubeCli kubernetes.Interface
}

var _ controller.Provisioner = &Controller{}

func New(kubeCli kubernetes.Interface) controller.Provisioner {
	return &Controller{
		kubeCli: kubeCli,
	}
}

func (c *Controller) Provision(opts controller.VolumeOptions) (*v1.PersistentVolume, error) {
	pvc := opts.PVC
	ns := pvc.GetNamespace()
	name := pvc.GetName()
	ann := pvc.GetAnnotations()

	nodeName := ann[util.AnnProvisionerNode]
	if nodeName == "" {
		glog.Infof("pvc %s/%s doesn't contain nodeName annotation", ns, name)
		return nil, errors.New("pvc doesn't contain nodeName annotation")
	}
	podName := ann[util.AnnProvisionerPodName]
	if podName == "" {
		glog.Infof("pvc %s/%s doesn't contain podName annotation", ns, name)
		return nil, errors.New("pvc doesn't contain podName annotation")
	}
	// pod, err := c.kubeCli.CoreV1().Pods(ns).Get(podName, metav1.GetOptions{})
	// if err != nil {
	// 	glog.Errorf("failed to get pod %s/%s: %v", ns, podName, err)
	// 	return nil, err
	// }

	hostPath, ok := ann[util.AnnProvisionerHostPath]
	if ok && hostPath != "" {
		return &v1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				Name: opts.PVName,
				Annotations: map[string]string{
					util.AnnProvisionerNode:     nodeName,
					util.AnnProvisionerHostPath: hostPath,
					util.AnnProvisionerPodName:  podName,
				},
			},
			Spec: v1.PersistentVolumeSpec{
				PersistentVolumeReclaimPolicy: opts.PersistentVolumeReclaimPolicy,
				AccessModes:                   opts.PVC.Spec.AccessModes,
				Capacity: v1.ResourceList{
					v1.ResourceName(v1.ResourceStorage): opts.PVC.Spec.Resources.Requests[v1.ResourceStorage],
				},
				PersistentVolumeSource: v1.PersistentVolumeSource{
					HostPath: &v1.HostPathVolumeSource{
						Path: hostPath,
					},
				},
			},
		}, nil
	}
	return nil, errors.New("waiting for lvm volume manager creating LV")
}

func (c *Controller) Delete(pv *v1.PersistentVolume) error {
	// pvName := pv.GetName()
	// ann := pv.GetAnnotations()
	// node := ann[util.AnnProvisionerNode]
	// if node != c.nodeName {
	// 	return &controller.IgnoredError{fmt.Sprintf("PV[%s] is not managed by this provisioner, managed by: %s, skipping", pvName, node)}
	// }

	// lvName := ann[AnnProvisionerLVName]

	return nil
}
