FROM alpine:3.5

ADD repositories /etc/apk/repositories

RUN apk update && apk add lvm2

ADD bin/lvm-volume-manager /usr/local/bin/lvm-volume-manager
ADD bin/lvm-scheduler /usr/local/bin/lvm-scheduler

ENTRYPOINT ["lvm-volume-manager"]
