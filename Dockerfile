FROM centos:7

RUN yum install -yy lvm2 e2fsprogs

ADD bin/lvm-volume-manager /usr/local/bin/lvm-volume-manager
ADD bin/lvm-volume-provisioner /usr/local/bin/lvm-volume-provisioner
ADD bin/lvm-scheduler /usr/local/bin/lvm-scheduler

ENTRYPOINT ["lvm-volume-manager"]
