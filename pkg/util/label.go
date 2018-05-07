package util

const (
	AnnProvisionerPodName   = "volume-provisioner.pingcap.com/podName"
	AnnProvisionerHostPath  = "volume-provisioner.pingcap.com/hostPath"
	AnnProvisionerNode      = "volume-provisioner.pingcap.com/node"
	AnnProvisionerVGName    = "volume-provisioner.pingcap.com/vgName"
	AnnProvisionerLVName    = "volume-provisioner.pingcap.com/lvName"
	AnnProvisionerLVSize    = "volume-provisioner.pingcap.com/lvSize"
	AnnProvisionerLVFsType  = "volume-provisioner.pingcap.com/fsType"
	AnnProvisionerLVDeleted = "volume-provisioner.pingcap.com/lvDeleted"
	ClientCfgQPS            = 10
	ClientCfgBurst          = 10
)
