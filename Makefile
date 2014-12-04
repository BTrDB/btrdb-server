
bqserver:
	go build -o bin/qserver github.com/SoftwareDefinedBuildings/quasar/qserver

cleanbins:
	rm -f bin/qserver bin/qtool

bins: cleanbins bqserver 

cleandb:
	rm -f /srv/quasar/*.db
	rm -f /srv/quasartestdb/*
	mongo quasar2 --eval 'db.superblocks.remove({})'

newdbs: cleandb bins
	./bin/qserver -makedb
