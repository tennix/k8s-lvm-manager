package main

import (
	"flag"
	"time"

	"github.com/golang/glog"
	"github.com/tennix/k8s-lvm-manager/pkg/scheduler"
	"github.com/tennix/k8s-lvm-manager/pkg/util"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

var (
	duration     = 5 * time.Second
	kubeconfig   string
	port         int
	storageClass string
	domainName   string
)

func init() {
	flag.StringVar(&kubeconfig, "kubeconfig", "", "Path to kubeconfig file, omit this if run in cluster")
	flag.StringVar(&storageClass, "storage-class", "lvm-volume-provisioner", "storage class for volume provisioner")
	flag.StringVar(&domainName, "domain-name", "pingcap.com", "domain name of extended resource")
	flag.IntVar(&port, "port", 10262, "The port that the tidb scheduler's http service runs on (default 10262)")
	flag.Parse()
}

func main() {
	var cfg *rest.Config
	var err error
	if kubeconfig == "" {
		cfg, err = rest.InClusterConfig()
	} else {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	}
	if err != nil {
		glog.Fatalf("failed to build rest.Config: %v", err)
	}
	cfg.QPS = util.ClientCfgQPS
	cfg.Burst = util.ClientCfgBurst

	kubeCli, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		glog.Fatalf("failed to get kubernetes Clientset: %v", err)
	}

	glog.Infof("start listening on :%d", port)
	wait.Forever(func() {
		scheduler.StartServer(kubeCli, port, domainName, storageClass)
	}, duration)
}
