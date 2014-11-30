package bprovider

import (
	"github.com/SoftwareDefinedBuildings/quasar/internal/fileprovider"
	"testing"
)

func makeFileProvider() *fileprovider.FileStorageProvider {
	params := map[string]string {
		"dbpath":"/srv/quasartestdb",
	}
	fp := new(fileprovider.FileStorageProvider)
	fp.Initialize(params)
	return fp
}

func x_RW1(t *testing.T, sp StorageProvider) {
	
}

var _ Segment = new(fileprovider.FileProviderSegment)
var _ StorageProvider = new(fileprovider.FileStorageProvider)
func Test_FP_RW1(t *testing.T){
	/*fp := makeFileProvider()
	x_RW1(t, fp)*/
}

