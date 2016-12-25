package version

import "fmt"

const Major = 4
const Minor = 0

//Will be set at build time to Major.Minor.Build
var VersionString string
var BuildDate string

func FullVersion() string {
	if VersionString == "" {
		return fmt.Sprintf("%d.%d.x (skunkworks build)", Major, Minor)
	}
	return VersionString
}
