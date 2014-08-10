
qserver:
	go build cal-sdb.org/quasar/qserver

qtool:
	go build cal-sdb.org/quasar/qtool

cleanbins:
	rm qserver qtool

bins: cleanbins qserver qtool
	
cleandb:
	rm /srv/quasar/*.db
	mongo quasar --eval 'db.superblocks.remove({})'

