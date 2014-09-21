
bqserver:
	go build -o bin/qserver github.com/SoftwareDefinedBuildings/quasar/qserver

bqtool:
	go build -o bin/qtool github.com/SoftwareDefinedBuildings/quasar/qtool

cleanbins:
	rm -f bin/qserver bin/qtool

bins: cleanbins bqserver bqtool

serve: bins
	bin/qserver -cache 16 -cpnp ":4410" -http ":9000"
	
cleandb:
	rm -f /srv/quasar/*.db
	rm -f /srv/quasartestdb/*
	mongo quasar --eval 'db.superblocks.remove({})'

newdbs: cleandb bins
	./bin/qserver -makedb 1 -dbpath /srv/quasartestdb/
	./bin/qserver -makedb 1 -dbpath /srv/quasar/
