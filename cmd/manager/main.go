package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/golang/glog"
	"github.com/tennix/k8s-lvm-manager/pkg/manager"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type fsRow struct {
	FS      string
	Dir     string
	Type    string
	Options string
	Dump    uint
	Pass    uint
}

type fsTab struct {
	path    string
	modTime time.Time
	fsTable []fsRow
}

const (
	fstabFile              = "/etc/fstab"
	AnnProvisionerNode     = "volume-provisioner.pingcap.com/node"
	AnnProvisionerHostPath = "volume-provisioner.pingcap.com/hostpath"
	AnnProvisionerLVName   = "volume-provisioner.pingcap.com/lvName"
	AnnProvisionerLVFsType = "volume-provisioner.pingcap.com/fsType"
)

var (
	kubeconfig string
	baseDir    string
	workers    int
	fstab      fsTab
	domainName string
	fsType     string
	duration   = 5 * time.Second
)

func init() {
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig file, omit this if run in cluster")
	flag.StringVar(&domainName, "domain-name", "pingcap.com", "domain name of extended resource")
	flag.StringVar(&baseDir, "base-dir", "/data", "base directory for mount point")
	flag.IntVar(&workers, "workers", 5, "count of workers for controller")
	flag.StringVar(&fsType, "fs-type", "ext4", "LV fs type")
	flag.Parse()

}

func main() {
	nodeName := os.Getenv("MY_NODE_NAME")
	if nodeName == "" {
		glog.Fatalf("MY_NODE_NAME environment variable not set")
	}

	mgr := manager.LVManager{BaseDir: baseDir}

	provisionerName := fmt.Sprintf("%s/lvm-volume-provisioner", domainName)

	if err := mgr.SyncLVMStatus(); err != nil {
		glog.Fatalf("failed to sync lvm status: %v", err)
	}
	var err error
	var cfg *rest.Config
	if kubeconfig == "" {
		cfg, err = rest.InClusterConfig()
	} else {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	if err != nil {
		glog.Fatalf("failed to get kube config: %v", err)
	}

	glog.Infof("LVM: %+v", mgr.LVM)

	cli, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		glog.Fatalf("failed to get kubernetes clientset: %v", err)
	}

	controller := manager.NewController(cli, mgr, domainName, nodeName, provisionerName)

	if err := controller.UpdateNodeStatus(mgr.LVM.VG); err != nil {
		glog.Fatalf("failed to update node status: %v", err)
	}
	wait.Forever(func() {
		controller.Run(workers, wait.NeverStop)
	}, duration)
}
