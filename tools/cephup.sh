docker run -d --net ceph --ip 172.25.0.5 \
-v /srv/ceph/etc/ceph:/etc/ceph \
-v /srv/ceph/var/lib/ceph/:/var/lib/ceph/ \
-e MON_IP=172.25.0.5 \
-e CEPH_PUBLIC_NETWORK=172.25.0.0/16 \
ceph/daemon mon


docker run -d --net ceph --ip 172.25.0.6 \
-v /srv/ceph/etc/ceph:/etc/ceph \
-v /srv/ceph/var/lib/ceph/:/var/lib/ceph/ \
-v /srv/ceph/osd0:/var/lib/ceph/osd \
-e OSD_TYPE=directory \
ceph/daemon osd

docker run -d --net ceph --ip 172.25.0.7 \
-v /srv/ceph/etc/ceph:/etc/ceph \
-v /srv/ceph/var/lib/ceph/:/var/lib/ceph/ \
-v /srv/ceph/osd1:/var/lib/ceph/osd \
-e OSD_TYPE=directory \
ceph/daemon osd

docker run -d --net ceph --ip 172.25.0.8 \
-v /srv/ceph/etc/ceph:/etc/ceph \
-v /srv/ceph/var/lib/ceph/:/var/lib/ceph/ \
-v /srv/ceph/osd2:/var/lib/ceph/osd \
-e OSD_TYPE=directory \
ceph/daemon osd

docker run -d --net ceph --ip 172.25.0.9 \
-v /srv/ceph/etc/ceph:/etc/ceph \
-v /srv/ceph/var/lib/ceph/:/var/lib/ceph/ \
-v /srv/ceph/osd3:/var/lib/ceph/osd \
-e OSD_TYPE=directory \
ceph/daemon osd
