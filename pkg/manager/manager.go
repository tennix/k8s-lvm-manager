package manager

import (
	"encoding/json"
	"os/exec"
	"path"

	"github.com/golang/glog"
)

type LVManager struct {
	BaseDir string
	LVM     LVM
}

type LVMReport struct {
	Report []Report `json:"report"`
}

type Report struct {
	PV []PV `json:"pv"`
	VG []VG `json:"vg"`
	LV []LV `json:"lv"`
}

type LV struct {
	LVUUID string `json:"lv_uuid"`
	LVName string `json:"lv_name"`
	LVSize string `json:"lv_size"`
	LVPath string `json:"lv_path"`
	VGName string `json:"vg_name"`
}

type PV struct {
	PVUUID string `json:"pv_uuid"`
	PVName string `json:"pv_name"`
	VGName string `json:"vg_name"`
	PVSize string `json:"pv_size"`
	PVFree string `json:"pv_free"`
}

type VG struct {
	VGUUID  string `json:"vg_uuid"`
	VGName  string `json:"vg_name"`
	VGSize  string `json:"vg_size"`
	VGFree  string `json:"vg_free"`
	LVCount string `json:"lv_count"`
	PVCount string `json:"pv_count"`
	VGTags  string `json:"vg_tags"`
}

type LVM struct {
	PV map[string]PV
	VG map[string]VG
	LV map[string]LV
}

func scanLVM() (LVMReport, error) {
	var report LVMReport
	vg_cols := "vg_uuid,vg_name,vg_size,vg_free,lv_count,pv_count,vg_tags"
	vgs, err := exec.Command("vgs", "loopback-disk", "-o", vg_cols, "--reportformat", "json").Output()
	if err != nil {
		glog.Errorf("failed to list vg: %v", err)
		return report, err
	}
	glog.Infof("vgs: %s", vgs)
	if err := json.Unmarshal(vgs, &report); err != nil {
		glog.Errorf("failed to unmarshal %s: %v", vgs, err)
		return report, err
	}
	glog.Infof("lvm: %+v", report)

	pv_cols := "pv_uuid,pv_name,vg_name,pv_size,pv_free"
	pvs, err := exec.Command("pvs", "-o", pv_cols, "--reportformat", "json").Output()
	if err != nil {
		glog.Errorf("failed to list pv: %v", err)
		return report, err
	}
	glog.Infof("pvs: %s", pvs)
	if err := json.Unmarshal(pvs, &report); err != nil {
		glog.Errorf("failed to unmarshal %s: %v", pvs, err)
		return report, err
	}
	glog.Infof("lvm: %+v", report)

	lv_cols := "lv_uuid,lv_name,lv_size,lv_path,vg_name"
	lvs, err := exec.Command("lvs", "-o", lv_cols, "--reportformat", "json").Output()
	if err != nil {
		glog.Errorf("failed to list lv: %v", err)
		return report, err
	}
	glog.Infof("lvs: %s", lvs)
	if err := json.Unmarshal(lvs, &report); err != nil {
		glog.Errorf("failed to unmarshal %s: %v", lvs, err)
		return report, err
	}
	glog.Infof("lvm: %+v", report)
	return report, nil
}

func (m *LVManager) SyncLVMStatus() error {
	pvs := map[string]PV{}
	vgs := map[string]VG{}
	lvs := map[string]LV{}
	report, err := scanLVM()
	if err != nil {
		return err
	}
	for _, lvm := range report.Report {
		for _, vg := range lvm.VG {
			vgs[vg.VGName] = vg
		}
		for _, pv := range lvm.PV {
			pvs[pv.PVName] = pv
		}
		for _, lv := range lvm.LV {
			lvs[lv.LVName] = lv
		}
	}
	m.LVM = LVM{
		PV: pvs,
		VG: vgs,
		LV: lvs,
	}
	return nil
}

func (m *LVManager) AllocateLV(lvName, vgName string, size string) error {
	output, err := exec.Command("lvcreate", "--name", lvName, "--size", size, vgName).Output()
	if err != nil {
		glog.Errorf("failed to create LV %s with size %s: %v", lvName, size, err)
		return err
	}
	glog.Infof("lvcreate output: %s\n", output)
	return nil
}

func (m *LVManager) FormatLV(name string, fsType string) error {
	output, err := exec.Command("mkfs", "--type", fsType, name).Output()
	if err != nil {
		glog.Errorf("failed to format LV %s to %s: %v", name, fsType, err)
		return err
	}
	glog.Infof("mkfs output: %s\n", output)
	return nil
}

func (m *LVManager) MountLV(name string) error {
	dir := path.Join(m.BaseDir, name)
	output, err := exec.Command("mount", name, dir).Output()
	if err != nil {
		glog.Infof("failed to mount LV %s to %s: %v", name, dir, err)
		return err
	}
	glog.Infof("mount output: %s\n", output)
	return nil
}

func (m *LVManager) UnmountLV(name string) error {
	dir := path.Join(m.BaseDir, name)
	output, err := exec.Command("umount", dir).Output()
	if err != nil {
		glog.Errorf("failed to umount LV %s: %v", name, err)
		return err
	}
	glog.Infof("umount output: %s", output)
	return nil
}

func (m *LVManager) RemoveLV(name string) error {
	output, err := exec.Command("lvremove", name, "--yes").Output()
	if err != nil {
		glog.Errorf("failed to remove LV %s: %v", name, err)
		return err
	}
	glog.Infof("lvremove output: %s", output)
	return nil
}

// TODO: persistent LV in fstab
// func (t *fsTab) load() error {
// 	f, err := os.Open(fstabFile)
// 	if err != nil {
// 		return err
// 	}
// 	defer f.Close()

// 	scanner := bufio.NewScanner(f)
// 	for scanner.Scan() {
// 		text := scanner.Text()
// 		if strings.HasPrefix(text, "#") {
// 			continue
// 		}
// 		tab := strings.Fields(text)
// 		if len(tab) != 6 {
// 			glog.Printf("invalid fstab format: %s\n", tab)
// 			continue
// 		}
// 		row := fsRow{
// 			FS:      tab[0],
// 			Dir:     tab[1],
// 			Type:    tab[2],
// 			Options: tab[3],
// 			Dump:    tab[4],
// 			Pass:    tab[5],
// 		}
// 		t.fsTable = append(t.fsTable, row)
// 	}
// }
