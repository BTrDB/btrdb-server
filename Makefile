
qserver:
	go build cal-sdb.org/quasar/qserver

qtool:
	go build cal-sdb.org/quasar/qtool

cleanbins:
	rm -f qserver qtool

bins: cleanbins qserver qtool
	
cleandb:
	rm -f /srv/quasar/*.db
	rm -f /srv/quasartestdb/*
	mongo quasar --eval 'db.superblocks.remove({})'

newdbs: cleandb bins
	./qserver -makedb 1 -dbpath /srv/quasartestdb/
	./qserver -makedb 1 -dbpath /srv/quasar/
